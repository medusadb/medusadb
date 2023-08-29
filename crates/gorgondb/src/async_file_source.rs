use std::path::{Path, PathBuf};

use async_compat::{Compat, CompatExt};
use bytes::Bytes;
use fs4::tokio::AsyncFileExt;
use futures::{AsyncRead, AsyncReadExt, AsyncSeek};
use pin_project::pin_project;

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

        let size = file
            .metadata()
            .await
            .map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!("failed to read metadata for `{}`: {err}", path.display()),
                )
            })?
            .len();

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
}
