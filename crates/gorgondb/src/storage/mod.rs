//! Low-level remote reference types and implementations.

#[cfg(feature = "aws")]
mod aws;
mod filesystem;

#[cfg(feature = "aws")]
pub use aws::{
    AsyncSource as AwsAsyncSource, AwsStorage, Error as AwsError, Result as AwsResult,
    S3AsyncSource as AwsS3AsyncSource,
};
pub use filesystem::FilesystemStorage;

use async_trait::async_trait;

use crate::{
    gorgon::{Retrieve, Store},
    RemoteRef,
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
///
/// `Storage` uses static dispatch to call concrete implementations. They are designed to be cheap
/// to clone.
#[derive(Debug, Clone)]
pub enum Storage {
    /// Store files on the file-system.
    Filesystem(FilesystemStorage),

    /// Store blobs in AWS S3 and DynamoDB.
    #[cfg(feature = "aws")]
    Aws(aws::AwsStorage),
}

#[async_trait]
impl Retrieve for Storage {
    /// Retrieve a value
    async fn retrieve<'s>(
        &'s self,
        remote_ref: &RemoteRef,
    ) -> Result<Option<crate::AsyncSource<'s>>> {
        match self {
            Self::Filesystem(storage) => Ok(storage.retrieve(remote_ref).await?.map(Into::into)),
            #[cfg(feature = "aws")]
            Self::Aws(storage) => Ok(storage.retrieve(remote_ref).await?.map(Into::into)),
        }
    }
}

#[async_trait]
impl Store for Storage {
    /// Store a value and ensures it has the proper remote ref.
    async fn store(&self, remote_ref: &RemoteRef, source: crate::AsyncSource<'_>) -> Result<()> {
        match self {
            Self::Filesystem(storage) => storage.store(remote_ref, source).await?,
            #[cfg(feature = "aws")]
            Self::Aws(storage) => storage.store(remote_ref, source).await?,
        }

        Ok(())
    }
}
