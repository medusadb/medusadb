//! Transaction support.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    gorgon::{Result, StoreOptions},
    AsyncSource, BlobId, Gorgon,
};

use self::storage::Storage;

mod storage;

/// A transaction represents a local staging space for blobs than can either be committed or
/// discarded. It ref-counts all store operations and support un-storing a blob which allows
/// efficient incremental changes to a blobs database.
#[derive(Debug)]
pub struct Transaction {
    gorgon: Gorgon,
    storage: Storage,
}

impl Transaction {
    const DISK_THRESHOLD: u64 = 1024 * 1024;

    pub(crate) fn new(
        gorgon: Gorgon,
        base_storage: Arc<crate::storage::Storage>,
    ) -> std::io::Result<Self> {
        let storage = Storage::new(
            Self::DISK_THRESHOLD,
            gorgon.filesystem().clone(),
            base_storage,
        )?;

        Ok(Self { gorgon, storage })
    }

    /// Retrieve a value to a file on disk.
    ///
    /// If the value doesn't exist in the transaction itself, it will be looked-for in the base
    /// storage instead.
    pub async fn retrieve_to_file(&self, blob_id: BlobId, path: impl AsRef<Path>) -> Result<()> {
        self.gorgon
            .retrieve_to_file_from(&self.storage, blob_id, path)
            .await
    }

    /// Retrieve a value.
    ///
    /// If the value doesn't exist in the transaction itself, it will be looked-for in the base
    /// storage instead.
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

    /// Unstore a value.
    ///
    /// A value that was unstored exactly as many times as it was stored in a `Transaction` will
    /// not actually be stored anywhere.
    ///
    /// Unstoring a value that wasn't stored is a no-op.
    pub async fn unstore(&self, blob_id: BlobId) -> Result<()> {
        self.gorgon.unstore_from(&self.storage, blob_id).await
    }

    /// Commit the transaction, ensuring all its referenced blobs are persisted to its associated
    /// base storage.
    ///
    /// If the commit fails, it is possible some values will have been copied while other may have
    /// not.
    pub async fn commit(self) -> Result<()> {
        self.storage.commit().await.map_err(Into::into)
    }
}
