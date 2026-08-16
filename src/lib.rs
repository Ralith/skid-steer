mod context;
mod type_id_map;

pub use context::Context;

use std::{
    any::{Any, TypeId},
    collections::hash_map,
    fmt,
    future::Future,
    hash::Hash,
    mem,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock, Weak,
    },
};

use tokio::sync::SetOnce;

use type_id_map::TypeIdMap;

/// Dispatches work to asynchronously populate [Asset] handles with data
///
/// A [Loader] constructs handles that refer to data while it's [load](Self::load)ed in the
/// background. For any work to happen, [next_task] must be polled regularly on a clone of the
/// [Loader], and the [Task]s it yields must be [ran](Task::run). For best performance, call
/// [next_task] in a loop, spawning off the [Task]s into a multithreaded executor without waiting
/// for their completion.
///
/// Arbitrary references can be passed to [`Source::load`] through a [`Context`]. The context is
/// also available when an asset is [`free`](Source::free)d. This enables advanced use cases like:
///
/// - Reading assets directly into GPU memory
/// - Loading dependency assets, by passing in a reflexive `&Loader`
/// - Caching intermediate results
/// - Sharing resources between [`Source::load`] implementations without contention or globals
///
/// [next_task]: Self::next_task
#[derive(Clone, Default)]
pub struct Loader(Arc<LoaderShared>);

impl Loader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin loading `source`, immediately returning the [Asset] that will contain it
    pub fn load<S: Source>(&self, source: S) -> Asset<S::Output> {
        let asset = self.0.create_asset::<S>();
        {
            let mut asset = CancelGuard(Some(asset.clone()));
            let loader_shared = Arc::downgrade(&self.0);
            self.0.increment_active_task_count();
            // The channel is unbounded, so it can never become full. If the channel is closed, we
            // silently drop the task.
            _ = self.0.work_send.try_send(Task {
                work: Box::new(move |context| {
                    Box::pin(async move {
                        let Some(data) = source.load(context).await else {
                            return;
                        };
                        let asset = asset.0.take().unwrap();
                        // Guaranteed to succeed because there are no other callers of `set`
                        asset
                            .0
                            .data
                            .set(Some(data))
                            .unwrap_or_else(|_| unreachable!());

                        if let Some(loader_shared) = loader_shared.upgrade() {
                            // Incrementing the `live_asset_count` generally does not have any special synchronization requirements.
                            loader_shared
                                .live_asset_count
                                .fetch_add(1, Ordering::Relaxed);

                            // As we want to ensure that `active_task_count` never reaches 0 unless tasks are fully drained,
                            // one situation we need to be careful about is when the user starts loading the asset but drops
                            // all handles before the asset is fully loaded and is ready to drain the `Loader`.
                            // In such a situation, we cannot allow `active_task_count` to reach 0 until after that asset has been
                            // freed. To accomplish that, we `drop(asset)` before `decrement_active_task_count` so that if that
                            // was the last asset handle, the destructor of `AssetShared` would be called, resulting in a call
                            // to `increment_active_task_count`. This ensures that anyone observing `active_task_count` will see
                            // it increase before it decreases again, ensuring that it never reaches 0 until after the asset has been
                            // freed later on.
                            drop(asset);
                            loader_shared.decrement_active_task_count();
                        }
                    })
                }),
            });
        }
        asset
    }

    pub fn load_cached<T: Source + Hash + Clone + Sync + Eq>(&self, key: T) -> Asset<T::Output> {
        if let Some(x) = self.0.cache.read().unwrap().get(&TypeId::of::<T>()) {
            return self.load_cached_inner(key, &**x);
        }
        match self.0.cache.write().unwrap().entry(TypeId::of::<T>()) {
            hash_map::Entry::Occupied(e) => self.load_cached_inner(key, &**e.get()),
            hash_map::Entry::Vacant(e) => {
                self.load_cached_inner(key, &**e.insert(Box::new(Cache::<T>::default())))
            }
        }
    }

    fn load_cached_inner<T: Source + Hash + Clone + Sync + Eq>(
        &self,
        key: T,
        table: &(dyn Any + Send + Sync),
    ) -> Asset<T::Output> {
        let inner = table.downcast_ref::<Cache<T>>().unwrap();
        if let Some(asset) = inner.read().unwrap().get(&key) {
            return asset.clone();
        }
        match inner.write().unwrap().entry(key.clone()) {
            hash_map::Entry::Occupied(e) => e.get().clone(),
            hash_map::Entry::Vacant(e) => e.insert(self.load(key)).clone(),
        }
    }

    /// Removes all assets from the cache. This may create new work that needs to be run
    /// with [Self::next_task] if it results in assets needing to be freed.
    pub fn clear_cache(&self) {
        self.0.cache.write().unwrap().clear();
    }

    /// Yields a work item that should be ran on a background thread pool to make progress
    ///
    /// This future is cancel-safe, but see also [Task::run]. Yields `None` iff [close](Self::close)
    /// has been called.
    pub async fn next_task(&self) -> Option<Task> {
        self.0.work_recv.recv().await.ok()
    }

    /// Like [next_task](Self::next_task), except returning `None` immediately if no tasks are
    /// currently queued
    pub fn try_next_task(&self) -> Option<Task> {
        self.0.work_recv.try_recv().ok()
    }

    /// Wait until there are currently no active tasks. Note that there may still be some assets
    /// that need to be freed before it is safe to close the `Loader`.
    pub async fn drain(&self) {
        let notified = self.0.drained.notified();

        // We need to use `Acquire` ordering here for consistency with `is_drained`. While calling
        // `drained.notified()` might already meet the synchronization requirements, the code is easier
        // to reason about if we do not rely on that.
        if self.0.active_task_count.load(Ordering::Acquire) == 0 {
            return;
        }

        // Given the synchronization requirements of `drained`, we do not need to check `active_task_count`
        // again, so a loop is not required here.
        notified.await
    }

    /// Returns whether there are currently no active tasks running
    pub fn is_drained(&self) -> bool {
        // We use `Acquire` ordering here to benefit from the synchronization requirements of `active_task_count`
        self.0.active_task_count.load(Ordering::Acquire) == 0
    }

    /// Whether all assets loaded by this Loader have been freed. Note that this method should be called after
    /// the `Loader` is drained, as otherwise, new assets could be created that also need freeing after this method
    /// returns `true`.
    pub fn all_assets_freed(&self) -> bool {
        // We use `Acquire` ordering here to benefit from the synchronization requirements of `live_asset_count`.
        // This makes this method suitable to use with an assertion. The guarantees we get if we observe
        // a value of 0 ensure that we can avoid undefined behavior if freeing an asset is a soundness requirement.
        // The fact that we *will* observe a value of 0 under certain conditions ensures that the user will
        // not get spurious assertion failures when using this method for that purpose.
        self.0.live_asset_count.load(Ordering::Acquire) == 0
    }

    /// Disable submission of new work, signaling callers of [next_task](Self::next_task) to shut
    /// down
    pub fn close(&self) {
        self.0.work_send.close();
    }

    /// Whether [close](Self::close) has been called
    pub fn is_closed(&self) -> bool {
        self.0.work_recv.is_closed()
    }
}

struct CancelGuard<T: Send + 'static>(Option<Asset<T>>);

impl<T: Send + 'static> Drop for CancelGuard<T> {
    fn drop(&mut self) {
        let Some(asset) = self.0.take() else {
            return;
        };
        // Notify consumers that this asset will never be loaded
        _ = asset.0.data.set(None);
        // Decrement active task count, since the asset will never be loaded
        if let Some(loader) = asset.0.loader.upgrade() {
            loader.decrement_active_task_count()
        }
    }
}

struct LoaderShared {
    work_send: async_channel::Sender<Task>,
    work_recv: async_channel::Receiver<Task>,
    cache: RwLock<TypeIdMap<Box<dyn Any + Send + Sync>>>,
    /// Synchronization requirements: If an `Acquire` operation sees this reach 0, and no new `load` or
    /// asset drop is running in parallel, the user can guarantee that there are no in-progress tasks
    /// or tasks in the queue, and all side-effects from the tasks that had been started are observable.
    active_task_count: AtomicUsize,
    /// A notification will only be triggered for this field if we have the same guarantees as when
    /// `active_task_count` reaches 0.
    drained: tokio::sync::Notify,
    /// Synchronization requirements:
    ///
    /// If an `Acquire` operation sees this reach 0 while no asset loading is running in parallel
    /// (such as after the `Loader` is drained), then we can guarantee that the thread running the
    /// `Acquire` operation will be able to rely on the completion of all assets' [`Source::free`]
    /// functions and observe the side effects of these operations.
    ///
    /// In addition, if all assets have been dropped, and there are no in-progress tasks or tasks in the
    /// queue, then we can guarantee that loading this field will always yield a value of 0.
    live_asset_count: AtomicUsize,
}

impl LoaderShared {
    fn create_asset<S: Source>(self: &Arc<Self>) -> Asset<S::Output> {
        let loader = Arc::downgrade(self);
        Asset(Arc::new(AssetShared {
            data: SetOnce::default(),
            loader,
            free_fn: S::free,
        }))
    }

    fn free<T: Send + 'static>(self: Arc<Self>, x: T, f: fn(T, &Context)) {
        let loader_shared = Arc::downgrade(&self);
        self.increment_active_task_count();
        _ = self.work_send.try_send(Task {
            work: Box::new(move |ctx| {
                f(x, ctx);
                if let Some(loader_shared) = loader_shared.upgrade() {
                    // We need to use `Release` ordering here to guarantee that any other thread that observes
                    // `live_asset_count` becoming 0 also observes any side effects of the completion of `f(x, ctx)`.
                    loader_shared
                        .live_asset_count
                        .fetch_sub(1, Ordering::Release);
                    // We make sure to decrement the `active_task_count` *after* decrementing the `live_asset_count` so that
                    // we can guarantee that `live_asset_count` returns 0 if it is checked after `Loader::drain` is completed.
                    loader_shared.decrement_active_task_count();
                }
                Box::pin(async {})
            }),
        });
    }

    fn increment_active_task_count(&self) {
        // Incrementing the `active_task_count` generally does not have any special synchronization requirements.
        self.active_task_count.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_active_task_count(&self) {
        // We need to use `Release` ordering here to guarantee that a thread loading this value with
        // `Acquire` can observe any side effects of all code running before this method was called, such as asset
        // loading or freeing. Since we notify `self.drained` based on `old_count`, we need `Acquire` ordering here, too.
        let old_count = self.active_task_count.fetch_sub(1, Ordering::AcqRel);
        if old_count == 1 {
            self.drained.notify_waiters();
        }
    }
}

impl Default for LoaderShared {
    fn default() -> Self {
        let (work_send, work_recv) = async_channel::unbounded();
        Self {
            work_send,
            work_recv,
            cache: RwLock::default(),
            active_task_count: AtomicUsize::new(0),
            drained: tokio::sync::Notify::new(),
            live_asset_count: AtomicUsize::new(0),
        }
    }
}

impl Drop for LoaderShared {
    fn drop(&mut self) {
        // Drain the channel so any in-flight Assets get gracefully abandoned
        self.work_recv.close();
        while let Ok(_) = self.work_recv.try_recv() {}
    }
}

type Cache<T> = RwLock<foldhash::HashMap<T, Asset<<T as Source>::Output>>>;

type LoadFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

/// A work item from [Loader::next_task]
pub struct Task {
    work: Box<dyn for<'a> FnOnce(&'a Context) -> LoadFuture<'a> + Send + 'static>,
}

impl Task {
    /// Execute the work item
    ///
    /// This future is cancel-safe if and only if every [Source::load] future used with the
    /// associated [Loader] is cancel-safe.
    ///
    /// This future is `!Send`, and hence must ran on a thread-local executor such as tokio's
    /// `LocalRuntime`.
    pub async fn run(self, context: &Context<'_>) {
        (self.work)(context).await;
    }
}

/// Description of an asset from which it can be loaded
///
/// # Example
/// ```
/// # use std::{fs, path::PathBuf};
/// # use skid_steer::*;
/// # struct Image;
/// # fn decode_png(x: Vec<u8>) -> Option<Image> { None }
/// struct Sprite(PathBuf);
/// impl Source for Sprite {
///     type Output = Image;
///     async fn load(self, _: &Context<'_>) -> Option<Image> {
///         let data = fs::read(&self.0).ok()?;
///         Some(decode_png(data)?)
///     }
/// }
/// ```
pub trait Source: Send + 'static {
    /// Type of data available after the asset has been loaded
    type Output: Send + Sync + 'static;

    /// Load the asset described by `self`, returning [None] on failure
    ///
    /// Reasonable implementations might:
    /// - Read data from disk
    /// - Fetch data from a remote server
    /// - Decode or transform data for more efficient access
    /// - Procedurally generate data
    /// - Upload data to a GPU
    ///
    /// To facilitate use of thread-local state, implementations may produce `!Send` futures.
    fn load<'a>(self, context: &'a Context<'a>) -> impl Future<Output = Option<Self::Output>> + 'a;

    /// Dispose of the output after all [Asset] references have been dropped
    ///
    /// Most implementations won't need to implement this. Useful if [Self::Output] refers to
    /// resources stored elsewhere without RAII, e.g. in unsafely managed GPU memory.
    fn free(_output: Self::Output, _context: &Context) {}
}

/// Handle to data that might not be available yet
pub struct Asset<T: Send + 'static>(Arc<AssetShared<T>>);

impl<T: 'static + Send> Asset<T> {
    /// Get the current value, if it's loaded
    pub fn try_get(&self) -> Option<&T> {
        self.0.data.get().and_then(|x| x.as_ref())
    }

    /// Get the current value once it's loaded, or `None` if it's [abandoned](Self::is_abandoned)
    pub async fn get(&self) -> Option<&T> {
        self.0.data.wait().await.as_ref()
    }

    /// Whether `try_get` is guaranteed to return `None` forever
    ///
    /// Indicates that either the [Source::load] operation returned [None], or the [Loader] was
    /// dropped before the [Source::load] operation ran.
    #[inline]
    pub fn is_abandoned(&self) -> bool {
        self.0.data.get().is_some_and(Option::is_none)
    }
}

impl<T: fmt::Debug + Send + 'static> fmt::Debug for Asset<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.data.fmt(f)
    }
}

impl<T: Send + 'static> Clone for Asset<T> {
    fn clone(&self) -> Self {
        Asset(self.0.clone())
    }
}

struct AssetShared<T: Send + 'static> {
    data: SetOnce<Option<T>>,
    loader: Weak<LoaderShared>,
    free_fn: fn(T, &Context),
}

impl<T: Send + 'static> Drop for AssetShared<T> {
    fn drop(&mut self) {
        // Send the underlying `T` back to the loader to be freed w/ the proper context.
        let Some(Some(data)) = mem::take(&mut self.data).into_inner() else {
            // This asset was never loaded
            return;
        };
        if let Some(loader) = self.loader.upgrade() {
            loader.free(data, self.free_fn);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use pollster::block_on;

    #[derive(Hash, Eq, PartialEq, Copy, Clone)]
    struct Trivial;

    impl Source for Trivial {
        type Output = ();

        async fn load<'a>(self, _: &'a Context<'_>) -> Option<()> {
            Some(())
        }
    }

    struct Failed;

    impl Source for Failed {
        type Output = ();

        async fn load<'a>(self, _: &'a Context<'_>) -> Option<()> {
            None
        }
    }

    #[test]
    fn smoke() {
        let loader = Loader::new();
        let asset = loader.load(Trivial);
        assert!(asset.try_get().is_none());

        let load_task = loader.try_next_task().unwrap();
        assert!(loader.try_next_task().is_none());
        block_on(load_task.run(&Context::new()));
        assert!(asset.try_get().is_some());
        block_on(asset.get());
        drop(asset);

        let free_task = loader.try_next_task().unwrap();
        assert!(loader.try_next_task().is_none());
        block_on(free_task.run(&Context::new()));

        loader.close();
        assert!(loader.is_closed());
        assert!(block_on(loader.next_task()).is_none());
    }

    #[test]
    fn abandoned_by_dropped_loader() {
        let loader = Loader::new();
        let asset = loader.load(Trivial);
        assert!(!asset.is_abandoned());
        drop(loader);
        assert!(asset.is_abandoned());
    }

    #[test]
    fn abandoned_by_failed_load() {
        let loader = Loader::new();
        let asset = loader.load(Failed);
        assert!(!asset.is_abandoned());
        let load_task = loader.try_next_task().unwrap();
        block_on(load_task.run(&Context::new()));
        assert!(asset.is_abandoned());
    }

    #[test]
    fn cache() {
        let loader = Loader::new();
        let asset = loader.load_cached(Trivial);
        let asset2 = loader.load_cached(Trivial);
        assert!(Arc::ptr_eq(&asset.0, &asset2.0))
    }

    #[test]
    fn must_free() {
        struct MustFree;

        struct NumAliveCounter(Mutex<u32>);

        impl Source for MustFree {
            type Output = ();

            async fn load(self, context: &Context<'_>) -> Option<()> {
                let num_alive = context.get::<NumAliveCounter>().unwrap();
                *num_alive.0.lock().unwrap() += 1;
                Some(())
            }

            fn free(_output: Self::Output, context: &Context) {
                let num_alive = context.get::<NumAliveCounter>().unwrap();
                *num_alive.0.lock().unwrap() -= 1;
            }
        }

        let loader = Loader::new();
        let mut context = Context::new();
        let num_alive_counter = NumAliveCounter(Mutex::new(0));
        context.insert(&num_alive_counter);
        let asset = loader.load(MustFree);

        assert!(!loader.is_drained());
        assert!(loader.all_assets_freed());

        let load_task = loader.try_next_task().unwrap();
        assert!(loader.try_next_task().is_none());
        block_on(load_task.run(&context));
        assert!(asset.try_get().is_some());
        assert_eq!(*num_alive_counter.0.lock().unwrap(), 1);

        block_on(loader.drain());
        assert!(loader.is_drained());
        assert!(!loader.all_assets_freed());

        drop(asset);
        assert!(!loader.is_drained());
        assert!(!loader.all_assets_freed());

        let free_task = loader.try_next_task().unwrap();
        assert!(loader.try_next_task().is_none());
        block_on(free_task.run(&context));
        assert!(loader.is_drained());
        assert!(loader.all_assets_freed());
        assert_eq!(*num_alive_counter.0.lock().unwrap(), 0);
    }
}
