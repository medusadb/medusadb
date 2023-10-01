//! Transaction support.

use std::sync::Arc;

use crate::{gorgon::StoreOptions, AsyncSource, BlobId, Client};

use self::storage::Storage;

mod storage;

/// A transaction represents a local staging space for blobs than can either be committed or
/// discarded. It ref-counts all store operations and support un-storing a blob which allows
/// efficient incremental changes to a blobs database.
#[derive(Debug)]
pub struct Transaction<'d> {
    client: Arc<Client>,
    storage: storage::Storage<'d>,
}

impl<'d> Transaction<'d> {
    pub(crate) fn new(client: Arc<Client>) -> std::io::Result<Self> {
        let storage = Storage::new(client.gorgon.filesystem().clone())?;

        Ok(Self { client, storage })
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
    ) -> crate::gorgon::Result<BlobId> {
        let source = source.into();

        self.client
            .gorgon
            .store_in(&self.storage, source, options)
            .await
    }
}
