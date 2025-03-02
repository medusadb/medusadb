use std::task::Poll;

use futures::{AsyncRead, Future, future::BoxFuture};
use pin_project::pin_project;

/// A structure that prepends a mandatory future to an `AsyncRead` before it can get polled.
///
/// This is typically used to wraps `AsyncRead` with semaphore, or make sure the actual underlying
/// streams are opened right before being actually polled to avoid expirations (for instance with
/// AWS S3 signed requests which expire).
#[pin_project(project = AsyncReadFnProj)]
pub enum AsyncReadInit<'f, R> {
    /// The instance has never been polled.
    Unpolled(#[pin] BoxFuture<'f, std::io::Result<R>>),
    /// The instance has already been polled at least once.
    Polled(#[pin] R),
}

impl<'f, R> AsyncReadInit<'f, R> {
    /// Instantiate a new `AsyncReadFn`.
    pub fn new(f: impl Future<Output = std::io::Result<R>> + Send + 'f) -> Self {
        Self::Unpolled(Box::pin(f))
    }
}

impl<R> AsyncRead for AsyncReadInit<'_, R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            let r = match self.as_mut().project() {
                AsyncReadFnProj::Unpolled(f) => match f.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(r)) => r,
                },
                AsyncReadFnProj::Polled(r) => return r.poll_read(cx, buf),
            };

            *self.as_mut() = AsyncReadInit::Polled(r);
        }
    }
}
