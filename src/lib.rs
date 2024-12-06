use std::{
    any::Any,
    fmt, mem,
    pin::Pin,
    sync::{Arc, OnceLock},
};

pub struct Loader<C = ()>(Arc<LoaderShared<C>>);

impl<C: Sync + 'static> Loader<C> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin loading `source`, immediately returning the [Asset] that will contain it
    pub fn spawn_load<S: Source<C>>(&self, source: S) -> Asset<S::Output> {
        let asset = self.0.create_asset::<S>();
        {
            let asset = asset.clone();
            self.0
                .work_send
                .try_send(Box::new(move |loader, context| {
                    Box::pin(async move {
                        let Some(data) = source.load(&loader, context).await else {
                            return;
                        };
                        _ = asset.0.as_ref().unwrap().data.set(data);
                    })
                }))
                .unwrap();
        }
        asset
    }

    /// Load `source`, waiting until it's loaded before yielding the [Asset] that contains it
    pub async fn load<S: Source<C>>(&self, source: S) -> Asset<S::Output> {
        let asset = self.0.create_asset::<S>();
        // TODO
        asset
    }

    /// Yields a work item that should be ran on a background thread pool to make progress
    pub async fn next_task<'a>(&self) -> Option<Task<C>> {
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
    free_send: async_channel::Sender<FreeMsg>,
    free_recv: async_channel::Receiver<FreeMsg>,
}

impl<C: 'static> LoaderShared<C> {
    fn create_asset<S: Source<C>>(&self) -> Asset<S::Output> {
        Asset(Some(Arc::new(AssetShared {
            data: OnceLock::default(),
            free_fn: |x, ctx| {
                let Ok(x) = Arc::downcast::<AssetShared<S::Output>>(x) else {
                    unreachable!()
                };
                // Unwrap guaranteed to succeed here because `Asset::drop` checks for successful
                // `Arc::get_mut` before dispatching a free message
                let x = Arc::into_inner(x).unwrap().data;
                let Some(data) = x.into_inner() else {
                    // Asset was never loaded
                    return;
                };
                S::free(data, ctx.downcast_ref::<C>().unwrap())
            },
            free_send: Some(self.free_send.clone()),
        })))
    }

    fn free(&self, context: &C) {
        while let Ok(msg) = self.free_recv.try_recv() {
            (msg.f)(msg.asset, context);
        }
    }
}

impl<C> Default for LoaderShared<C> {
    fn default() -> Self {
        let (free_send, free_recv) = async_channel::unbounded();
        let (work_send, work_recv) = async_channel::unbounded();
        Self {
            work_send,
            work_recv,
            free_send,
            free_recv,
        }
    }
}

struct FreeMsg {
    asset: Arc<dyn Any + Send + Sync>,
    f: FreeFn,
}
/// A type-erased [Source::free] implementation
type FreeFn = fn(Arc<dyn Any + Send + Sync>, &dyn Any);

type LoadFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
type WorkMsg<C> = Box<dyn for<'a> FnOnce(&'a Loader<C>, &'a C) -> LoadFuture<'a> + Send + 'static>;

pub struct Task<C> {
    loader: Loader<C>,
    work: WorkMsg<C>,
}

impl<C> Task<C> {
    /// Execute the work item
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
/// # fn decode_png(x: Vec<u8>) -> Option<Vec<u8>> { None }
/// struct Sprite(PathBuf);
/// impl<C> Source<C> for Sprite {
///     type Output = Vec<u8>;
///     fn load<'a>(self, loader: &'a Loader<C>, _: &'a C) -> impl Future<Output = Option<Self::Output>> + Send + 'a {
///         async move {
///             let data = fs::read(&self.0).ok()?;
///             Some(decode_png(data)?)
///         }
///     }
/// }
///
/// ```
pub trait Source<C>: Send + 'static {
    /// Type of data available after the asset has been loaded
    type Output: Send + Sync + 'static;

    /// Load the asset described by `self`, returning [None] on failure
    fn load<'a>(
        self,
        loader: &'a Loader<C>,
        context: &'a C,
    ) -> impl Future<Output = Option<Self::Output>> + Send + 'a;

    /// Dispose of this asset's output when it's no longer needed
    ///
    /// Useful if [Self::Output] refers to resources stored elsewhere, e.g. in GPU memory
    fn free(output: Self::Output, context: &C) {}
}

#[derive(Debug)]
pub struct Asset<T: 'static + Send + Sync>(Option<Arc<AssetShared<T>>>);

impl<T: 'static + Send + Sync> Asset<T> {
    /// Get the current value, if it's loaded
    pub fn get(&self) -> Option<&T> {
        self.0.as_ref().unwrap().data.get()
    }
}

impl<T: 'static + Send + Sync> Clone for Asset<T> {
    fn clone(&self) -> Self {
        Asset(self.0.clone())
    }
}

impl<T: 'static + Send + Sync> Drop for Asset<T> {
    fn drop(&mut self) {
        // If this is the last reference to the asset, send the underlying `Arc` back to the loader
        // to be freed w/ the proper context.
        let mut this = self.0.take().unwrap();
        let Some(this_mut) = Arc::get_mut(&mut this) else {
            // This isn't the last reference
            return;
        };
        let sender = mem::take(&mut this_mut.free_send).unwrap();
        _ = sender.try_send(FreeMsg {
            f: this.free_fn,
            asset: this,
        });
    }
}

struct AssetShared<T: 'static> {
    data: OnceLock<T>,
    free_fn: FreeFn,
    free_send: Option<async_channel::Sender<FreeMsg>>,
}

impl<T: fmt::Debug + 'static> fmt::Debug for AssetShared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.data.fmt(f)
    }
}
