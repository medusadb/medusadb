//! A ``Gorgon`` implements methods to read and write blobs of data.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::gorgon::{Result, StoreOptions};
use crate::{AsyncFileSource, AsyncSource, BlobId, Gorgon, Storage, Transaction};

/// A `Client` provides method to store and retrieve values.
#[derive(Debug)]
pub struct Client {
    pub(crate) gorgon: Gorgon,
    pub(crate) storage: Arc<Storage>,
}

impl Client {
    /// Instantiate a new `Client` using the specified storage.
    pub fn new(storage: impl Into<Storage>) -> Self {
        let gorgon = Gorgon::default();
        let storage = Arc::new(storage.into());

        Self { gorgon, storage }
    }

    /// Instantiate a new `Client` for tests.
    pub fn new_for_tests() -> Self {
        Self::new(Storage::new_for_tests())
    }

    /// Retrieve a value from a file on disk.
    pub async fn retrieve_to_file(
        &self,
        blob_id: &BlobId,
        path: impl AsRef<Path>,
    ) -> Result<AsyncFileSource> {
        self.gorgon
            .retrieve_to_file_from(self.storage.as_ref(), blob_id, path)
            .await
    }

    /// Retrieve a value and read it in memory.
    ///
    /// If the value doesn't exist in the transaction itself, it will be looked-for in the base
    /// storage instead.
    pub async fn retrieve_to_memory<'s>(
        &'s self,
        blob_id: &'s BlobId,
    ) -> Result<Cow<'static, [u8]>> {
        self.gorgon
            .retrieve_to_memory_from(self.storage.as_ref(), blob_id)
            .await
    }

    /// Retrieve a value.
    pub async fn retrieve<'s>(&'s self, blob_id: &'s BlobId) -> Result<AsyncSource<'s>> {
        self.gorgon
            .retrieve_from(self.storage.as_ref(), blob_id)
            .await
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
            .store_from_file_in(self.storage.as_ref(), path, options)
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

        self.gorgon
            .store_in(self.storage.as_ref(), source, options)
            .await
    }

    /// Start a new transaction on the client.
    pub fn start_transaction(&self) -> std::io::Result<Transaction> {
        Transaction::new(self.gorgon.clone(), self.storage.clone())
    }
}
