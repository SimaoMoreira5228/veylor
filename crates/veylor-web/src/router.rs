use std::path::PathBuf;

use crate::handler::{AsyncHandler, Handler, RouteHandler, erase, erase_async};

pub struct MethodRouter<S: 'static> {
    pub(crate) entries: Vec<MethodEntry<S>>,
}

pub(crate) struct MethodEntry<S: 'static> {
    pub(crate) method: &'static [u8],
    pub(crate) handler: RouteHandler<S>,
    pub(crate) managed: bool,
}

impl<S: 'static> Default for MethodRouter<S> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<S: 'static> MethodRouter<S> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on<T, H>(mut self, method: &'static [u8], handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.entries.push(MethodEntry {
            method,
            handler: erase(handler),
            managed: false,
        });
        self
    }

    pub fn get<T, H>(self, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.on(b"GET", handler)
    }

    pub fn post<T, H>(self, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.on(b"POST", handler)
    }

    pub fn put<T, H>(self, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.on(b"PUT", handler)
    }

    pub fn patch<T, H>(self, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.on(b"PATCH", handler)
    }

    pub fn delete<T, H>(self, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.on(b"DELETE", handler)
    }

    pub fn head<T, H>(self, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.on(b"HEAD", handler)
    }

    pub fn get_async<T, H>(self, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.on_async(b"GET", handler)
    }

    pub fn post_async<T, H>(self, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.on_async(b"POST", handler)
    }

    pub fn put_async<T, H>(self, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.on_async(b"PUT", handler)
    }

    pub fn patch_async<T, H>(self, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.on_async(b"PATCH", handler)
    }

    pub fn delete_async<T, H>(self, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.on_async(b"DELETE", handler)
    }

    pub fn on_async<T, H>(mut self, method: &'static [u8], handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.entries.push(MethodEntry {
            method,
            handler: erase_async(handler),
            managed: true,
        });
        self
    }

    pub fn managed(mut self) -> Self {
        for entry in &mut self.entries {
            entry.managed = true;
        }
        self
    }
}

pub struct Router<S: 'static = ()> {
    pub(crate) state: S,
    pub(crate) routes: Vec<RouteSpec<S>>,
    pub(crate) mounts: Vec<(Vec<u8>, PathBuf)>,
    pub(crate) fallback: Option<RouteHandler<S>>,
    pub(crate) body_cap: usize,
}

pub(crate) struct RouteSpec<S: 'static> {
    pub(crate) path: Vec<u8>,
    pub(crate) methods: MethodRouter<S>,
}

const DEFAULT_BODY_CAP: usize = 64 * 1024;

impl Router<()> {
    pub fn new() -> Self {
        Self::with_state(())
    }
}

impl Default for Router<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: 'static> Router<S> {
    pub fn with_state(state: S) -> Self {
        Self {
            state,
            routes: Vec::new(),
            mounts: Vec::new(),
            fallback: None,
            body_cap: DEFAULT_BODY_CAP,
        }
    }

    pub fn route(mut self, path: &str, methods: MethodRouter<S>) -> Self {
        self.routes.push(RouteSpec {
            path: path.as_bytes().to_vec(),
            methods,
        });
        self
    }

    pub fn get<T, H>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.route(path, MethodRouter::new().get(handler))
    }

    pub fn post<T, H>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.route(path, MethodRouter::new().post(handler))
    }

    pub fn put<T, H>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.route(path, MethodRouter::new().put(handler))
    }

    pub fn patch<T, H>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.route(path, MethodRouter::new().patch(handler))
    }

    pub fn delete<T, H>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.route(path, MethodRouter::new().delete(handler))
    }

    pub fn get_async<T, H>(self, path: &str, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.route(path, MethodRouter::new().get_async(handler))
    }

    pub fn post_async<T, H>(self, path: &str, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.route(path, MethodRouter::new().post_async(handler))
    }

    pub fn put_async<T, H>(self, path: &str, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.route(path, MethodRouter::new().put_async(handler))
    }

    pub fn patch_async<T, H>(self, path: &str, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.route(path, MethodRouter::new().patch_async(handler))
    }

    pub fn delete_async<T, H>(self, path: &str, handler: H) -> Self
    where
        H: AsyncHandler<T, S>,
    {
        self.route(path, MethodRouter::new().delete_async(handler))
    }

    pub fn mount(mut self, prefix: &str, dir: impl Into<PathBuf>) -> Self {
        self.mounts.push((prefix.as_bytes().to_vec(), dir.into()));
        self
    }

    pub fn fallback<T, H>(mut self, handler: H) -> Self
    where
        H: Handler<T, S>,
    {
        self.fallback = Some(erase(handler));
        self
    }

    pub fn body_limit(mut self, bytes: usize) -> Self {
        self.body_cap = bytes;
        self
    }
}
