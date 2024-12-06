use std::{
    any::Any,
    fmt,
    marker::PhantomData,
    mem,
    sync::{mpsc, Arc, OnceLock},
};

#[derive(Clone)]
pub struct Loader<C = ()>(Arc<LoaderShared>, PhantomData<fn(&C)>);

impl<C: 'static> Loader<C> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin loading `source`, immediately returning the [Asset] that will contain it
    pub fn spawn_load<S: Source<C>>(&self, source: S) -> Asset<S::Output> {
        let asset = self.0.create_asset::<C, S>();
        // TODO
        asset
    }

    /// Load `source`, waiting until it's loaded before yielding the [Asset] that contains it
    pub async fn load<S: Source<C>>(&self, source: S) -> Asset<S::Output> {
        let asset = self.0.create_asset::<C, S>();
        // TODO
        asset
    }

    /// Handle `load` operations called by other threads, until all other `Loader`s are dropped
    ///
    /// May be called from multiple threads to enable parallelism
    pub async fn drive(self, context: &C) {
        todo!()
    }
}

impl<C> Default for Loader<C> {
    fn default() -> Self {
        Self(Default::default(), PhantomData)
    }
}

struct LoaderShared {
    free_send: mpsc::Sender<FreeMsg>,
    free_recv: mpsc::Receiver<FreeMsg>,
}

impl LoaderShared {
    fn create_asset<C: 'static, S: Source<C>>(&self) -> Asset<S::Output> {
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

    fn free<C: 'static>(&self, context: &C) {
        for msg in self.free_recv.try_iter() {
            (msg.f)(msg.asset, context);
        }
    }
}

impl Default for LoaderShared {
    fn default() -> Self {
        let (free_send, free_recv) = mpsc::channel();
        Self {
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
        _ = sender.send(FreeMsg {
            f: this.free_fn,
            asset: this,
        });
    }
}

struct AssetShared<T: 'static> {
    data: OnceLock<T>,
    free_fn: FreeFn,
    free_send: Option<mpsc::Sender<FreeMsg>>,
}

impl<T: fmt::Debug + 'static> fmt::Debug for AssetShared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.data.fmt(f)
    }
}
