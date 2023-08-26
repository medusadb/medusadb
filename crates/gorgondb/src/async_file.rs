use std::path::Path;

use async_compat::{Compat, CompatExt};
use bytes::Bytes;
use futures::{AsyncRead, AsyncReadExt, AsyncSeek};
use pin_project::pin_project;

/// A source file on disk, opened in read-only.
#[pin_project]
pub struct AsyncFileSource {
    #[pin]
    file: Compat<tokio::fs::File>,

    pub(crate) size: u64,
}

impl AsyncFileSource {
    /// Open a file on disk.
    pub async fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = tokio::fs::File::options().read(true).open(path).await?;
        let size = file.metadata().await?.len();

        Ok(Self {
            file: file.compat(),
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
