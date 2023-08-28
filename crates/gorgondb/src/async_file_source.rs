use std::path::{Path, PathBuf};

use async_compat::{Compat, CompatExt};
use bytes::Bytes;
use fs4::tokio::AsyncFileExt;
use futures::{future::BoxFuture, AsyncRead, AsyncReadExt, AsyncSeek};
use pin_project::pin_project;
use reflink::reflink;
use tokio::io::AsyncWriteExt;

use crate::AsyncSource;

/// A source file on disk, opened in read-only.
#[pin_project]
pub struct AsyncFileSource {
    #[pin]
    file: Compat<tokio::fs::File>,

    path: PathBuf,
    size: u64,
}

impl AsyncFileSource {
    /// Open a file on disk.
    pub async fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let file = tokio::fs::File::options().read(true).open(&path).await?;

        // Ensure that no other process is or will be writing to the file as long as we have it.
        file.try_lock_shared()?;

        let size = file.metadata().await?.len();

        Ok(Self {
            file: file.compat(),
            path,
            size,
        })
    }

    /// Get the inner `tokio::fs::File`.
    pub fn into_file(self) -> tokio::fs::File {
        self.file.into_inner()
    }

    /// Load the source file into the memory.
    pub async fn into_cursor(mut self) -> std::io::Result<std::io::Cursor<Bytes>> {
        let mut buf = Vec::with_capacity(
            self.size
                .try_into()
                .expect("failed to convert u64 to usize"),
        );
        self.file.read_to_end(&mut buf).await?;

        Ok(std::io::Cursor::new(buf.into()))
    }
}

impl From<AsyncFileSource> for tokio::fs::File {
    fn from(value: AsyncFileSource) -> Self {
        value.into_file()
    }
}

impl AsyncRead for AsyncFileSource {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.project().file.poll_read(cx, buf)
    }
}

impl AsyncSeek for AsyncFileSource {
    fn poll_seek(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        pos: std::io::SeekFrom,
    ) -> std::task::Poll<std::io::Result<u64>> {
        self.project().file.poll_seek(cx, pos)
    }
}

impl AsyncSource for AsyncFileSource {
    fn size(&self) -> u64 {
        self.size
    }

    fn path(&self) -> Option<&Path> {
        Some(&self.path)
    }

    fn write_to_file<'s>(&'s mut self, path: &'s Path) -> BoxFuture<'s, std::io::Result<()>> {
        Box::pin(async move {
            tracing::debug!(
                "Source has a path on the local filesystem (`{}`): will attempt a copy using `reflink`.",
                self.path.display(),
            );

            match reflink(&self.path, path) {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to copy file using `reflink` ({err}): will fallback to in-memory copy."
                    );
                }
            }

            let mut target = tokio::fs::File::options()
                .create_new(true)
                .open(path)
                .await?;

            target.try_lock_exclusive()?;
            futures::io::copy(self, &mut target.compat_mut()).await?;
            target.shutdown().await?;

            Ok(())
        })
    }
}
