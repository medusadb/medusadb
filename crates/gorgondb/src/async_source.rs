use std::{borrow::Cow, path::Path};

use bytes::Bytes;
use futures::{future::BoxFuture, AsyncRead};

use crate::{AsyncFileSource, AsyncSourceChain};

/// A convenience type for boxed `AsyncRead` that can be unpinned.
pub type BoxAsyncRead<'s> = Box<dyn AsyncRead + Send + Unpin + 's>;

/// A source of data that can be read asynchronously.
#[derive(Debug)]
pub enum AsyncSource<'d> {
    /// The source is a buffer in memory, either owned or borrowed.
    Memory(Cow<'d, [u8]>),

    /// The source is a chain of `AsyncSource`.
    Chain(AsyncSourceChain<'d>),

    /// The source is a file on disk.
    File(AsyncFileSource),

    /// The source is AWS.
    #[cfg(feature = "aws")]
    Aws(crate::storage::AwsAsyncSource<'d>),
}

impl<'d> AsyncSource<'d> {
    /// Get the size of the data.
    pub fn size(&self) -> u64 {
        match self {
            Self::Memory(buf) => buf.len().try_into().expect("buffer size should fit a u64"),
            Self::Chain(chain) => chain.size(),
            Self::File(file) => file.size(),
            Self::Aws(source) => source.size(),
        }
    }

    /// Get the path, on disk, pointing to the referenced data, if there is one.
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Memory(_) | Self::Chain(_) => None,
            Self::File(file) => Some(file.path()),
            #[cfg(feature = "aws")]
            Self::Aws(_) => None,
        }
    }

    /// Get a reference to the data, if it lives in memory.
    pub fn data(&self) -> Option<&[u8]> {
        match self {
            Self::Memory(buf) => Some(buf),
            Self::Chain(_) | Self::File(_) => None,
            #[cfg(feature = "aws")]
            Self::Aws(source) => source.data(),
        }
    }

    /// Get the data, if it lives in memory.
    pub fn into_data(self) -> Result<Vec<u8>, Self> {
        match self {
            Self::Memory(buf) => Ok(buf.to_vec()),
            Self::Chain(_) | Self::File(_) => Err(self),
            #[cfg(feature = "aws")]
            Self::Aws(source) => source.into_data().map_err(Self::Aws),
        }
    }

    /// Transform the source in a buffer in memory, reading the entirety of the data first if
    /// necessary.
    pub async fn read_all_into_vec(self) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Memory(buf) => Ok(buf.to_vec()),
            Self::Chain(chain) => chain.read_all_into_vec().await,
            Self::File(file) => file.read_all_into_vec().await,
            #[cfg(feature = "aws")]
            Self::Aws(source) => source
                .read_all_into_vec()
                .await
                .map_err(|err| err.into_io_error()),
        }
    }

    /// Get an asynchronous reader from this `AsyncSource`.
    pub fn get_async_read(&'d self) -> BoxFuture<std::io::Result<BoxAsyncRead<'d>>> {
        match self {
            Self::Memory(buf) => {
                Box::pin(async move { Ok(Box::new(futures::io::Cursor::new(buf)) as BoxAsyncRead) })
            }
            Self::Chain(chain) => Box::pin(async move { chain.get_async_read().await }),
            Self::File(file) => Box::pin(async move { file.get_async_read().await }),
            #[cfg(feature = "aws")]
            Self::Aws(source) => Box::pin(async move {
                source
                    .get_async_read()
                    .await
                    .map_err(|err| err.into_io_error())
            }),
        }
    }
}

impl<'d> From<Cow<'d, [u8]>> for AsyncSource<'d> {
    fn from(value: Cow<'d, [u8]>) -> Self {
        Self::Memory(value)
    }
}

impl<'d> From<&'d [u8]> for AsyncSource<'d> {
    fn from(value: &'d [u8]) -> Self {
        Cow::Borrowed(value).into()
    }
}

impl From<Vec<u8>> for AsyncSource<'_> {
    fn from(value: Vec<u8>) -> Self {
        Cow::<[u8]>::Owned(value).into()
    }
}

impl From<Bytes> for AsyncSource<'_> {
    fn from(value: Bytes) -> Self {
        value.to_vec().into()
    }
}

impl<'d> From<AsyncSourceChain<'d>> for AsyncSource<'d> {
    fn from(value: AsyncSourceChain<'d>) -> Self {
        Self::Chain(value)
    }
}

impl From<AsyncFileSource> for AsyncSource<'_> {
    fn from(value: AsyncFileSource) -> Self {
        Self::File(value)
    }
}

impl<'d> From<crate::storage::AwsAsyncSource<'d>> for AsyncSource<'d> {
    fn from(value: crate::storage::AwsAsyncSource<'d>) -> Self {
        Self::Aws(value)
    }
}
