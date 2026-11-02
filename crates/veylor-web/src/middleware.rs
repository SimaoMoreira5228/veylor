use std::sync::Arc;

use crate::handler::RouteHandler;
use crate::request::Request;
use crate::response::Response;

pub struct Ctx<'a, S: 'static> {
    request: Request<'a, S>,
}

impl<'a, S: 'static> Ctx<'a, S> {
    pub fn request(&self) -> &Request<'a, S> {
        &self.request
    }
}

pub struct Next<'a, S: 'static> {
    layers: &'a [Arc<dyn Middleware<S>>],
    index: usize,
    handler: &'a RouteHandler<S>,
}

impl<S: 'static> Next<'_, S> {
    pub fn run(self, ctx: &mut Ctx<'_, S>) -> Response {
        match self.layers.get(self.index) {
            Some(layer) => {
                let next = Next {
                    layers: self.layers,
                    index: self.index + 1,
                    handler: self.handler,
                };
                layer.call(ctx, next)
            }
            None => (self.handler)(ctx.request(), ctx.request().state()),
        }
    }
}

pub trait Middleware<S: 'static>: Send + Sync + 'static {
    fn call<'a, 'b>(&self, ctx: &'a mut Ctx<'b, S>, next: Next<'a, S>) -> Response;
}

impl<S: 'static, F> Middleware<S> for F
where
    F: for<'a, 'b> Fn(&'a mut Ctx<'b, S>, Next<'a, S>) -> Response + Send + Sync + 'static,
{
    fn call<'a, 'b>(&self, ctx: &'a mut Ctx<'b, S>, next: Next<'a, S>) -> Response {
        (self)(ctx, next)
    }
}

pub(crate) fn run_chain<S: 'static>(
    layers: &[Arc<dyn Middleware<S>>],
    handler: &RouteHandler<S>,
    request: Request<'_, S>,
) -> Response {
    let mut ctx = Ctx { request };
    Next {
        layers,
        index: 0,
        handler,
    }
    .run(&mut ctx)
}
