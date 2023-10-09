use std::{borrow::Cow, path::Path};

use futures::{future::BoxFuture, AsyncRead};

use crate::{AsyncFileSource, AsyncSourceChain};

/// A convenience type for boxed `AsyncRead` that can be unpinned.
pub type BoxAsyncRead<'s> = Box<dyn AsyncRead + Send + Unpin + 's>;

/// A source of data that can be read asynchronously.
#[derive(Debug)]
pub enum AsyncSource<'d> {
    /// The source is a static buffer in memory.
    Static(&'static [u8]),

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
    /// Get a static async source.
    pub const fn from_static(buf: &'static [u8]) -> Self {
        Self::Static(buf)
    }

    /// Get the size of the data.
    pub fn size(&self) -> u64 {
        match self {
            Self::Static(buf) => buf.len().try_into().expect("buffer size should fit a u64"),
            Self::Memory(buf) => buf.len().try_into().expect("buffer size should fit a u64"),
            Self::Chain(chain) => chain.size(),
            Self::File(file) => file.size(),
            Self::Aws(source) => source.size(),
        }
    }

    /// Get the path, on disk, pointing to the referenced data, if there is one.
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Static(_) | Self::Memory(_) | Self::Chain(_) => None,
            Self::File(file) => Some(file.path()),
            #[cfg(feature = "aws")]
            Self::Aws(_) => None,
        }
    }

    /// Get a reference to the static data, if the source supports it.
    pub fn static_data(&self) -> Option<&'static [u8]> {
        match self {
            Self::Static(buf) => Some(buf),
            _ => None,
        }
    }

    /// Get the static data, if source supports it.
    pub fn into_static_data(self) -> Result<&'static [u8], Self> {
        match self {
            Self::Static(buf) => Ok(buf),
            _ => Err(self),
        }
    }

    /// Get a reference to the data, if it lives in memory.
    pub fn data(&self) -> Option<&[u8]> {
        match self {
            Self::Static(buf) => Some(buf),
            Self::Memory(buf) => Some(buf),
            Self::Chain(_) | Self::File(_) => None,
            #[cfg(feature = "aws")]
            Self::Aws(source) => source.data(),
        }
    }

    /// Get the data, if it lives in memory.
    pub fn into_data(self) -> Result<Vec<u8>, Self> {
        match self {
            Self::Static(buf) => Ok(buf.to_owned()),
            Self::Memory(buf) => Ok(buf.to_vec()),
            Self::Chain(_) | Self::File(_) => Err(self),
            #[cfg(feature = "aws")]
            Self::Aws(source) => source.into_data().map_err(Self::Aws),
        }
    }

    /// Transform the source in a buffer in memory, reading the entirety of the data first if
    /// necessary.
    pub async fn read_all_into_memory(self) -> std::io::Result<Cow<'d, [u8]>> {
        match self {
            Self::Static(buf) => Ok(buf.into()),
            Self::Memory(buf) => Ok(buf),
            Self::Chain(chain) => chain.read_all_into_memory().await.map(Into::into),
            Self::File(file) => file.read_all_into_memory().await.map(Into::into),
            #[cfg(feature = "aws")]
            Self::Aws(source) => source
                .read_all_into_memory()
                .await
                .map_err(|err| err.into_io_error()),
        }
    }

    /// Transform the source into a buffer in memory, eventually copying it to guarantee an owned
    /// or static lifetime buffer.
    pub async fn read_all_into_owned_memory(self) -> std::io::Result<Cow<'static, [u8]>> {
        match self {
            Self::Static(buf) => Ok(buf.into()),
            Self::Memory(Cow::Owned(buf)) => Ok(buf.into()),
            Self::Memory(Cow::Borrowed(buf)) => Ok(buf.to_owned().into()),
            Self::Chain(chain) => chain.read_all_into_memory().await.map(Into::into),
            Self::File(file) => file.read_all_into_memory().await.map(Into::into),
            #[cfg(feature = "aws")]
            Self::Aws(source) => match source
                .read_all_into_memory()
                .await
                .map_err(|err| err.into_io_error())?
            {
                Cow::Owned(buf) => Ok(buf.into()),
                Cow::Borrowed(buf) => Ok(buf.to_owned().into()),
            },
        }
    }

    /// Get an asynchronous reader from this `AsyncSource`.
    pub fn get_async_read(&'d self) -> BoxFuture<std::io::Result<BoxAsyncRead<'d>>> {
        match self {
            Self::Static(buf) => {
                Box::pin(async move { Ok(Box::new(futures::io::Cursor::new(buf)) as BoxAsyncRead) })
            }
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
