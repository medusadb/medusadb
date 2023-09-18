use std::task::Poll;

use futures::AsyncRead;
use pin_project::pin_project;
use tokio::sync::OwnedSemaphorePermit;

/// An `AsyncRead` that holds a generic semaphore permit until read completely.
#[pin_project(project = AsyncPermitReadImpl)]
pub enum AsyncPermitRead<Inner> {
    /// The file is being read.
    Reading {
        /// The name of the reader, for debugging purposes.
        name: String,

        /// The semaphore permit.
        permit: OwnedSemaphorePermit,

        /// The inner file handle.
        #[pin]
        inner: Inner,
    },
    /// The file has been read.
    Done,
}

impl<Inner> AsyncPermitRead<Inner> {
    /// Instantiate a new ``AsyncPermitRead``.
    pub fn new(name: impl Into<String>, permit: OwnedSemaphorePermit, inner: Inner) -> Self {
        let name = name.into();

        tracing::debug!("Acquired permit for async reader `{name}`.");

        Self::Reading {
            name,
            permit,
            inner,
        }
    }
}

impl<Inner: AsyncRead + Unpin> AsyncRead for AsyncPermitRead<Inner> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            match self.as_mut().project() {
                AsyncPermitReadImpl::Reading { name, inner, .. } => {
                    match inner.poll_read(cx, buf) {
                        Poll::Ready(Ok(size)) if size == 0 => {
                            tracing::debug!(
                                "Async reader `{name}` is now done reading: returning permit."
                            );
                        }
                        Poll::Ready(Ok(cnt)) => {
                            tracing::trace!(
                                "Read polling on async reader `{name}` yielded {cnt} bytes"
                            );

                            return Poll::Ready(Ok(cnt));
                        }
                        Poll::Ready(Err(err)) => {
                            tracing::trace!(
                                "Read polling on async reader `{name}` yielded an error: {err}"
                            );

                            return Poll::Ready(Err(err));
                        }
                        Poll::Pending => {
                            tracing::trace!("Read polling on async reader `{name}` is pending...");

                            return Poll::Pending;
                        }
                    }
                }
                AsyncPermitReadImpl::Done => return Poll::Ready(Ok(0)),
            };

            // We are done reading: release the inner stream and the permit right away in case
            // the instance is kept around.
            *self.as_mut() = Self::Done;
        }
    }
}
