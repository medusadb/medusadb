//! A ``Gorgon`` implements methods to read and write blobs of data.

use std::{cmp::min, task::Poll};

use futures::{
    io::{copy, BufReader},
    AsyncBufRead, AsyncRead,
};
use pin_project::pin_project;

use crate::{Cairn, RemoteRef};

/// An error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[pin_project]
struct AsyncReadHash<R> {
    hasher: blake3::Hasher,
    total_size: u64,
    #[pin]
    inner: BufReader<R>,
}

impl<R: AsyncRead> AsyncReadHash<R> {
    fn new(inner: R) -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            total_size: 0,
            // Using a 16K buffer capacity allows SIMD optimizations to kick-in when computing
            // Blake3 hashes.
            inner: BufReader::with_capacity(16 * 1024, inner),
        }
    }

    fn into_cairn(self) -> Cairn {
        let hash = self.hasher.finalize().as_bytes().to_vec().into();

        RemoteRef {
            ref_size: self.total_size,
            hash_algorithm: crate::HashAlgorithm::Blake3,
            hash,
        }
        .into()
    }
}

impl<R: AsyncRead> AsyncRead for AsyncReadHash<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut this = self.project();

        match this.inner.as_mut().poll_fill_buf(cx) {
            Poll::Ready(Ok(data)) => {
                let cnt = min(data.len(), buf.len());
                let data = &data[..cnt];
                *this.total_size += cnt as u64;

                this.hasher.update(data);

                buf.copy_from_slice(data);
                this.inner.consume(cnt);

                Poll::Ready(Ok(cnt))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct Gorgon {}

impl Gorgon {
    /// Write a value and persist it.
    pub async fn write(r: impl AsyncRead + Unpin) -> Result<Cairn> {
        let mut r = AsyncReadHash::new(r);

        let mut w = futures::io::sink();
        copy(&mut r, &mut w).await?;

        Ok(r.into_cairn())
    }
}
