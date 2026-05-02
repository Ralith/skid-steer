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
    sync::{Arc, RwLock, Weak},
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
    }
}

struct LoaderShared {
    work_send: async_channel::Sender<Task>,
    work_recv: async_channel::Receiver<Task>,
    cache: RwLock<TypeIdMap<Box<dyn Any + Send + Sync>>>,
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

    fn free<T: Send + 'static>(&self, x: T, f: fn(T, &Context)) {
        _ = self.work_send.try_send(Task {
            work: Box::new(move |ctx| {
                f(x, ctx);
                Box::pin(async {})
            }),
        });
    }
}

impl Default for LoaderShared {
    fn default() -> Self {
        let (work_send, work_recv) = async_channel::unbounded();
        Self {
            work_send,
            work_recv,
            cache: RwLock::default(),
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

type LoadFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// A work item from [Loader::next_task]
pub struct Task {
    work: Box<dyn for<'a> FnOnce(&'a Context) -> LoadFuture<'a> + Send + 'static>,
}

impl Task {
    /// Execute the work item
    ///
    /// This future is cancel-safe if and only if every [Source::load] future used with the
    /// associated [Loader] is cancel-safe.
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
    fn load<'a>(
        self,
        context: &'a Context<'a>,
    ) -> impl Future<Output = Option<Self::Output>> + Send + 'a;

    /// Dispose of the output after all [Asset] references have been dropped
    ///
    /// Most implementations won't need to implement this. Useful if [Self::Output] refers to
    /// resources stored elsewhere without RAII, e.g. in unsafely managed GPU memory.
    fn free(_output: Self::Output, _context: &Context) {}
}

/// Handle to data that might not be available yet
pub struct Asset<T: Send + 'static>(Arc<AssetShared<T>>);

impl<T: 'static + Send + Sync> Asset<T> {
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
}
