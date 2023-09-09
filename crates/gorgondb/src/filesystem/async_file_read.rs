use std::{path::Path, task::Poll};

use async_compat::{Compat, CompatExt};
use fs4::tokio::AsyncFileExt;
use futures::AsyncRead;
use pin_project::pin_project;
use tokio::sync::OwnedSemaphorePermit;

/// An `AsyncRead` that hold a semaphore permit to a file.
#[pin_project(project = FilesystemAsyncReadImpl)]
pub enum AsyncFileRead {
    /// The file is being read.
    Reading {
        /// The semaphore permit.
        permit: OwnedSemaphorePermit,

        /// The inner file handle.
        #[pin]
        inner: Compat<tokio::fs::File>,
    },
    /// The file has been read.
    Done,
}

impl AsyncFileRead {
    /// Open a file from its path and borrowing the specified permit.
    pub async fn open(
        permit: OwnedSemaphorePermit,
        path: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        let path = path.as_ref();

        let file = tokio::fs::File::options()
            .read(true)
            .open(&path)
            .await
            .map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!("failed to open `{}`: {err}", path.display()),
                )
            })?;

        // Ensure that no other process is or will be writing to the file as long as we have it.
        file.try_lock_shared().map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!("failed to lock `{}`: {err}", path.display()),
            )
        })?;

        let inner = file.compat();

        Ok(Self::Reading { permit, inner })
    }
}

impl AsyncRead for AsyncFileRead {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            match self.as_mut().project() {
                FilesystemAsyncReadImpl::Reading { inner, .. } => match inner.poll_read(cx, buf) {
                    Poll::Ready(Ok(size)) if size == 0 => {}
                    res => return res,
                },
                FilesystemAsyncReadImpl::Done => return Poll::Ready(Ok(0)),
            };

            // We are done reading: release the inner stream and the permit right away in case
            // the instance is kept around.
            *self.as_mut() = Self::Done;
        }
    }
}
