//! Transaction support.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    gorgon::{Result, StoreOptions},
    AsyncSource, BlobId, Client,
};

use self::storage::Storage;

mod storage;

/// A transaction represents a local staging space for blobs than can either be committed or
/// discarded. It ref-counts all store operations and support un-storing a blob which allows
/// efficient incremental changes to a blobs database.
#[derive(Debug)]
pub struct Transaction {
    client: Arc<Client>,
    storage: storage::Storage,
}

impl Transaction {
    const DISK_THRESHOLD: u64 = 1024 * 1024;

    pub(crate) fn new(client: Arc<Client>) -> std::io::Result<Self> {
        let storage = Storage::new(Self::DISK_THRESHOLD, client.gorgon.filesystem().clone())?;

        // TODO: Storage should hold a reference to it's sub-storage to support partially known
        // ledgers!

        Ok(Self { client, storage })
    }

    /// Retrieve a value from a file on disk.
    pub async fn retrieve_to_file(&self, blob_id: BlobId, path: impl AsRef<Path>) -> Result<()> {
        self.client
            .gorgon
            .retrieve_to_file_from(&self.storage, blob_id, path)
            .await
    }

    /// Retrieve a value.
    pub async fn retrieve(&self, blob_id: BlobId) -> Result<AsyncSource<'_>> {
        self.client
            .gorgon
            .retrieve_from(&self.storage, blob_id)
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
        self.client
            .gorgon
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

        self.client
            .gorgon
            .store_in(&self.storage, source, options)
            .await
    }
}
