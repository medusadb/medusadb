//! Low-level remote reference types and implementations.

#[cfg(feature = "aws")]
mod aws;
mod cache;
mod filesystem;
mod memory;

#[cfg(feature = "aws")]
pub use aws::{
    AsyncSource as AwsAsyncSource, AwsStorage, Error as AwsError, Result as AwsResult,
    S3AsyncSource as AwsS3AsyncSource,
};
pub use cache::Cache;
pub use filesystem::FilesystemStorage;
pub use memory::MemoryStorage;

use async_trait::async_trait;
use tracing::debug;

use crate::{
    RemoteRef,
    gorgon::{Retrieve, Store},
};

/// An error type for storage implementations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occured.
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    /// Data corruption was detected.
    #[error("data corruption: {0}")]
    DataCorruption(String),

    /// An AWS error.
    #[cfg(feature = "aws")]
    #[error("AWS operation failed")]
    Aws(#[from] AwsError),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A struct that can persist and recover values remotely.
#[derive(Debug)]
pub struct Storage {
    cache: Cache,
    impl_: StorageImpl,
}

impl From<FilesystemStorage> for Storage {
    fn from(value: FilesystemStorage) -> Self {
        Self {
            cache: Cache::default(),
            impl_: StorageImpl::Filesystem(value),
        }
    }
}

#[cfg(feature = "aws")]
impl From<aws::AwsStorage> for Storage {
    fn from(value: aws::AwsStorage) -> Self {
        Self {
            cache: Cache::default(),
            impl_: StorageImpl::Aws(value),
        }
    }
}

impl Storage {
    /// Instantiate a new storage suitable for testing.
    ///
    /// The returned store will keep everything in memory only.
    pub fn new_for_tests() -> Self {
        Self {
            cache: Cache::default(),
            impl_: StorageImpl::Memory(MemoryStorage::default()),
        }
    }

    /// Get the cache associated to this storage.
    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Get the cache associated to this storage.
    pub fn cache_mut(&mut self) -> &mut Cache {
        &mut self.cache
    }
}

#[async_trait]
impl Retrieve for Storage {
    /// Retrieve a value
    async fn retrieve<'s>(
        &'s self,
        remote_ref: &RemoteRef,
    ) -> Result<Option<crate::AsyncSource<'s>>> {
        if let Some(r) = self.cache.retrieve(remote_ref).await {
            debug!("Found cache entry for `{remote_ref}`: reading from cache.");

            return Ok(Some(r));
        }

        debug!("No cache entry was found for `{remote_ref}`: fetching the value upstream.");

        match self.impl_.retrieve(remote_ref).await? {
            Some(source) => self.cache.store(remote_ref, source).await.map(Some),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl Store for Storage {
    /// Store a value and ensures it has the proper remote ref.
    async fn store(
        &self,
        remote_ref: &RemoteRef,
        mut source: crate::AsyncSource<'_>,
    ) -> Result<()> {
        source = self.cache.store(remote_ref, source).await?;

        self.impl_.store(remote_ref, source).await
    }
}

/// `Storage` uses static dispatch to call concrete implementations.
#[derive(Debug)]
enum StorageImpl {
    /// Store everything in memory, for testing purposes only.
    Memory(MemoryStorage),

    /// Store files on the file-system.
    Filesystem(FilesystemStorage),

    /// Store blobs in AWS S3 and DynamoDB.
    #[cfg(feature = "aws")]
    Aws(aws::AwsStorage),
}

#[async_trait]
impl Retrieve for StorageImpl {
    /// Retrieve a value
    async fn retrieve<'s>(
        &'s self,
        remote_ref: &RemoteRef,
    ) -> Result<Option<crate::AsyncSource<'s>>> {
        match self {
            Self::Memory(storage) => Ok(storage.retrieve(remote_ref)),
            Self::Filesystem(storage) => Ok(storage.retrieve(remote_ref).await?.map(Into::into)),
            #[cfg(feature = "aws")]
            Self::Aws(storage) => Ok(storage.retrieve(remote_ref).await?.map(Into::into)),
        }
    }
}

#[async_trait]
impl Store for StorageImpl {
    /// Store a value and ensures it has the proper remote ref.
    async fn store(&self, remote_ref: &RemoteRef, source: crate::AsyncSource<'_>) -> Result<()> {
        match self {
            Self::Memory(storage) => {
                storage.store(remote_ref, source).await?;
            }
            Self::Filesystem(storage) => {
                storage.store(remote_ref, source).await?;
            }
            #[cfg(feature = "aws")]
            Self::Aws(storage) => storage.store(remote_ref, source).await?,
        }

        Ok(())
    }
}
