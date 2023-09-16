use std::task::Poll;

use futures::AsyncRead;
use pin_project::pin_project;
use tokio::sync::OwnedSemaphorePermit;

/// An `AsyncRead` that holds a generic semaphore permit until read completely.
#[pin_project(project = AsyncPermitReadImpl)]
pub enum AsyncPermitRead<Inner> {
    /// The file is being read.
    Reading {
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
    pub fn new(permit: OwnedSemaphorePermit, inner: Inner) -> Self {
        Self::Reading { permit, inner }
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
                AsyncPermitReadImpl::Reading { inner, .. } => match inner.poll_read(cx, buf) {
                    Poll::Ready(Ok(size)) if size == 0 => {}
                    res => return res,
                },
                AsyncPermitReadImpl::Done => return Poll::Ready(Ok(0)),
            };

            // We are done reading: release the inner stream and the permit right away in case
            // the instance is kept around.
            *self.as_mut() = Self::Done;
        }
    }
}
