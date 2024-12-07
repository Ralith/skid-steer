use std::{
    fmt, future, mem,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::Poll,
};

use atomic_waker::AtomicWaker;

/// Dispatches work to asynchronously populate [Asset] handles with data
///
/// A [Loader] constructs handles that refer to data while it's [load](Self::load) in the
/// background. For any work to happen, [next_task] must be polled regularly on a clone of the
/// [Loader], and the [Task]s it yields must be [ran](Task::run). For best performance, call
/// [next_task] in a loop, spawning off the [Task]s into a multithreaded executor without waiting
/// for their completion.
///
/// While a [Source] is [load](Source::load)ing, it may access the [Loader] and a user-controlled
/// context. The context is also available when an asset is [free](Source::free)d. This enables
/// advanced use cases like:
///
/// - Reading assets directly into GPU memory
/// - Loading other assets
/// - Caching intermediate results
///
/// [next_task]: Self::next_task
pub struct Loader<C = ()>(Arc<LoaderShared<C>>);

impl<C: Sync + 'static> Loader<C> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin loading `source`, immediately returning the [Asset] that will contain it
    pub fn load<S: Source<C>>(&self, source: S) -> Asset<S::Output> {
        let asset = self.0.create_asset::<S>();
        {
            let asset = asset.clone();
            // The channel is unbounded, so it can never become full. If the channel is closed, we
            // silently drop the task.
            _ = self.0.work_send.try_send(Box::new(move |loader, context| {
                Box::pin(async move {
                    let Some(data) = source.load(&loader, context).await else {
                        return;
                    };
                    // Guaranteed to succeed because there are no other callers of `set`
                    asset.0.data.set(data).unwrap_or_else(|_| unreachable!());
                    asset.0.waker.wake();
                })
            }));
        }
        asset
    }

    /// Yields a work item that should be ran on a background thread pool to make progress
    ///
    /// This future is cancel-safe, but see also [Task::run]. Yields `None` iff [close](Self::close)
    /// has been called.
    pub async fn next_task(&self) -> Option<Task<C>> {
        let work = self.0.work_recv.recv().await.ok()?;
        let loader = self.clone();
        Some(Task { loader, work })
    }

    /// Like [next_task](Self::next_task), except returning `None` immediately if no tasks are
    /// currently queued
    pub fn try_next_task(&self) -> Option<Task<C>> {
        let work = self.0.work_recv.try_recv().ok()?;
        let loader = self.clone();
        Some(Task { loader, work })
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

impl<C> Clone for Loader<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<C> Default for Loader<C> {
    fn default() -> Self {
        Self(Default::default())
    }
}

struct LoaderShared<C> {
    work_send: async_channel::Sender<WorkMsg<C>>,
    work_recv: async_channel::Receiver<WorkMsg<C>>,
}

impl<C: 'static> LoaderShared<C> {
    fn create_asset<S: Source<C>>(&self) -> Asset<S::Output> {
        let work_send = self.work_send.clone();
        Asset(Arc::new(AssetShared {
            data: OnceLock::default(),
            free_send: Box::new(move |x| {
                _ = work_send.try_send(Box::new(|_, ctx| {
                    S::free(x, ctx);
                    Box::pin(async {})
                }));
            }),
            waker: AtomicWaker::new(),
        }))
    }
}

impl<C> Default for LoaderShared<C> {
    fn default() -> Self {
        let (work_send, work_recv) = async_channel::unbounded();
        Self {
            work_send,
            work_recv,
        }
    }
}

type LoadFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
type WorkMsg<C> = Box<dyn for<'a> FnOnce(&'a Loader<C>, &'a C) -> LoadFuture<'a> + Send + 'static>;

/// A work item from [Loader::next_task]
pub struct Task<C> {
    loader: Loader<C>,
    work: WorkMsg<C>,
}

impl<C> Task<C> {
    /// Execute the work item
    ///
    /// This future is cancel-safe if and only if every [Source::load] future used with the
    /// associated [Loader] is cancel-safe.
    pub async fn run(self, context: &C) {
        (self.work)(&self.loader, context).await;
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
/// impl<C: Sync> Source<C> for Sprite {
///     type Output = Image;
///     async fn load<'a>(self, _: &'a Loader<C>, _: &'a C) -> Option<Image> {
///         let data = fs::read(&self.0).ok()?;
///         Some(decode_png(data)?)
///     }
/// }
/// ```
pub trait Source<C>: Send + 'static {
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
        loader: &'a Loader<C>,
        context: &'a C,
    ) -> impl Future<Output = Option<Self::Output>> + Send + 'a;

    /// Dispose of the output after all [Asset] references have been dropped
    ///
    /// Most implementations won't need to implement this. Useful if [Self::Output] refers to
    /// resources stored elsewhere without RAII, e.g. in unsafely managed GPU memory.
    fn free(_output: Self::Output, _context: &C) {}
}

/// Handle to data that might not be available yet
#[derive(Debug)]
pub struct Asset<T: 'static>(Arc<AssetShared<T>>);

impl<T: 'static + Send + Sync> Asset<T> {
    /// Get the current value, if it's loaded
    pub fn try_get(&self) -> Option<&T> {
        self.0.data.get()
    }

    /// Get the current value once it's loaded
    pub async fn get(&self) -> &T {
        // Fast path
        if let Some(x) = self.0.data.get() {
            return x;
        }
        future::poll_fn(|cx| {
            self.0.waker.register(cx.waker());
            match self.0.data.get() {
                None => return Poll::Pending,
                Some(x) => return Poll::Ready(x),
            }
        })
        .await
    }
}

impl<T: 'static> Clone for Asset<T> {
    fn clone(&self) -> Self {
        Asset(self.0.clone())
    }
}

impl<T: 'static> Drop for Asset<T> {
    fn drop(&mut self) {
        // If this is the last reference to the asset, send the underlying `Arc` back to the loader
        // to be freed w/ the proper context.
        let Some(shared) = Arc::get_mut(&mut self.0) else {
            // This isn't the last reference
            return;
        };
        let Some(data) = mem::take(&mut shared.data).into_inner() else {
            // This asset was never loaded
            return;
        };
        let send = mem::replace(&mut shared.free_send, Box::new(|_| {}));
        send(data);
    }
}

struct AssetShared<T: 'static> {
    data: OnceLock<T>,
    free_send: Box<dyn FnOnce(T) + Send + Sync>,
    waker: AtomicWaker,
}

impl<T: fmt::Debug + 'static> fmt::Debug for AssetShared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.data.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pollster::block_on;

    struct Trivial;

    impl<C: Sync> Source<C> for Trivial {
        type Output = ();

        async fn load<'a>(self, _: &'a Loader<C>, _: &'a C) -> Option<()> {
            Some(())
        }
    }

    #[test]
    fn smoke() {
        let loader = Loader::<()>::new();
        let asset = loader.load(Trivial);
        assert!(asset.try_get().is_none());

        let load_task = loader.try_next_task().unwrap();
        assert!(loader.try_next_task().is_none());
        block_on(load_task.run(&()));
        assert!(asset.try_get().is_some());
        block_on(asset.get());
        drop(asset);

        let free_task = loader.try_next_task().unwrap();
        assert!(loader.try_next_task().is_none());
        block_on(free_task.run(&()));

        loader.close();
        assert!(loader.is_closed());
        assert!(block_on(loader.next_task()).is_none());
    }
}
