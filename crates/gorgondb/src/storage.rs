//! Low-level remote reference types and implementations.

#[cfg(feature = "aws")]
mod aws;
mod filesystem;

#[cfg(feature = "aws")]
pub use aws::{
    AsyncAwsS3Source, AsyncAwsSource, AwsStorage, Error as AwsError, Result as AwsResult,
};
pub use filesystem::FilesystemStorage;

use crate::{AsyncSource, HashAlgorithm, RemoteRef};

/// An error type for storage implementations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occured.
    #[error("I/O: {0}")]
    IO(#[from] std::io::Error),

    /// Data corruption was detected.
    #[error("data corruption: {0}")]
    DataCorruption(String),

    /// An AWS error.
    #[cfg(feature = "aws")]
    #[error("aws: {0}")]
    Aws(#[from] AwsError),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A struct that can persist and recover values remotely.
///
/// `Storage` uses static dispatch to call concrete implementations.
#[derive(Debug, Clone)]
pub enum Storage {
    /// Store files on the file-system.
    Filesystem(FilesystemStorage),

    /// Store blobs in AWS S3 and DynamoDB.
    #[cfg(feature = "aws")]
    Aws(aws::AwsStorage),
}

impl Storage {
    /// Retrieve a value
    pub(crate) async fn retrieve(&self, remote_ref: &RemoteRef) -> Result<AsyncSource> {
        match self {
            Self::Filesystem(storage) => Ok(storage.retrieve(remote_ref).await?.into()),
            #[cfg(feature = "aws")]
            Self::Aws(storage) => Ok(storage.retrieve(remote_ref).await?.into()),
        }
    }

    /// Store a value and ensures it has the proper remote ref.
    pub(crate) async fn store(
        &self,
        hash_algorithm: HashAlgorithm,
        source: impl Into<AsyncSource<'_>>,
    ) -> Result<RemoteRef> {
        let source = source.into();

        let hash = hash_algorithm
            .async_hash_to_vec(source.get_async_read().await?)
            .await?
            .into();

        let remote_ref = RemoteRef {
            ref_size: source.size(),
            hash_algorithm,
            hash,
        };

        self.store_unchecked(&remote_ref, source).await?;

        Ok(remote_ref)
    }

    /// Store a value.
    ///
    /// Regardless of the actual storage logic, callers should not assume that any kind of check will
    /// be made on the actual content of the stored values to ensure their hash match their `RemoteRef`
    /// hash.
    ///
    /// Attempting to store a value with a non-matching hash will cause silent data corruption. Be
    /// careful and do NOT do it.
    async fn store_unchecked(&self, remote_ref: &RemoteRef, source: AsyncSource<'_>) -> Result<()> {
        match self {
            Self::Filesystem(storage) => storage.store(remote_ref, source).await?,
            #[cfg(feature = "aws")]
            Self::Aws(storage) => storage.store(remote_ref, source).await?,
        }

        Ok(())
    }
}
