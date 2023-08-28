mod filesystem;

pub use filesystem::FilesystemStorage;

use crate::{AsyncSource, HashAlgorithm, RemoteRef};

/// An error type for storage implementations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O: {0}")]
    IO(#[from] std::io::Error),

    #[error("data corruption: {0}")]
    DataCorruption(String),
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
}

impl Storage {
    /// Store a value and ensures it has
    pub(crate) async fn store(
        &self,
        ref_size: u64,
        hash_algorithm: HashAlgorithm,
        mut source: impl AsyncSource,
    ) -> Result<RemoteRef> {
        let hash = hash_algorithm.async_hash_to_vec(&mut source).await?.into();

        let remote_ref = RemoteRef {
            ref_size,
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
    async fn store_unchecked(
        &self,
        remote_ref: &RemoteRef,
        source: impl AsyncSource,
    ) -> Result<()> {
        match self {
            Self::Filesystem(storage) => storage.store(remote_ref, source).await,
        }
    }
}
