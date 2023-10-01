//! A ``Gorgon`` implements methods to read and write blobs of data.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::gorgon::{Result, StoreOptions};
use crate::{AsyncSource, BlobId, Gorgon, Storage, Transaction};

/// A `Client` provides method to store and retrieve values.
#[derive(Debug)]
pub struct Client {
    pub(crate) gorgon: Gorgon,
    pub(crate) storage: Storage,
}

impl Client {
    /// Instantiate a new `Gorgon` using the specified storage.
    pub fn new(storage: Storage) -> Self {
        let gorgon = Gorgon::default();
        Self { gorgon, storage }
    }

    /// Retrieve a value from a file on disk.
    pub async fn retrieve_to_file(&self, blob_id: BlobId, path: impl AsRef<Path>) -> Result<()> {
        self.gorgon
            .retrieve_to_file_from(&self.storage, blob_id, path)
            .await
    }

    /// Retrieve a value.
    pub async fn retrieve(&self, blob_id: BlobId) -> Result<AsyncSource<'_>> {
        self.gorgon.retrieve_from(&self.storage, blob_id).await
    }

    /// Store a file from the disk.
    ///
    /// This is a convenience method.
    pub async fn store_from_file(
        &self,
        path: impl Into<PathBuf>,
        options: &StoreOptions,
    ) -> Result<BlobId> {
        self.gorgon
            .store_from_file_in(&self.storage, path, options)
            .await
    }

    /// Store and persist a value.
    ///
    /// Upon success, a `BlobId` describing the value is returned. Losing the resulting `BlobId`
    /// equates to losing the value. It is the caller's responsibility to store [`BlobIds`](`BlobId`)
    /// appropriately.
    pub async fn store(
        &self,
        source: impl Into<AsyncSource<'_>>,
        options: &StoreOptions,
    ) -> Result<BlobId> {
        let source = source.into();

        self.gorgon.store_in(&self.storage, source, options).await
    }

    /// Start a new transaction on the client.
    pub fn start_transaction<'d>(self: &Arc<Self>) -> std::io::Result<Transaction<'d>> {
        Transaction::new(Arc::clone(self))
    }
}
