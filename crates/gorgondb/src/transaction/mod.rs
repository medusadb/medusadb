//! Transaction support.

use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    sync::Arc,
};

use tracing::trace;

use crate::{
    AsyncFileSource, AsyncSource, BlobId, Gorgon,
    gorgon::{Error, Result, StoreOptions},
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
        name: impl Into<Cow<'static, str>>,
        gorgon: Gorgon,
        base_storage: Arc<crate::storage::Storage>,
    ) -> std::io::Result<Self> {
        let storage = Storage::new(
            name,
            Self::DISK_THRESHOLD,
            gorgon.filesystem().clone(),
            base_storage,
        )?;

        Ok(Self { gorgon, storage })
    }

    /// Fork the transaction, duplicating its uncommited content in memory.
    ///
    /// The resulting transaction is exactly identical to the initial one, but the two are
    /// independant after the fork.
    pub async fn fork(&self, name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            gorgon: self.gorgon.clone(),
            storage: self.storage.fork(name).await,
        }
    }

    /// Get the name of the transaction.
    pub fn name(&self) -> &str {
        self.storage.name()
    }

    /// Get the blobs referenced by this transaction.
    pub async fn get_blobs(&self) -> impl Iterator<Item = BlobId> + use<> {
        self.storage
            .get_remote_refs()
            .await
            .into_iter()
            .map(BlobId::RemoteRef)
    }

    /// Retrieve a value to a file on disk.
    ///
    /// If the value doesn't exist in the transaction itself, it will be looked-for in the base
    /// storage instead.
    pub async fn retrieve_to_file(
        &self,
        blob_id: &BlobId,
        path: impl AsRef<Path>,
    ) -> Result<AsyncFileSource> {
        self.gorgon
            .retrieve_to_file_from(&self.storage, blob_id, path)
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
            .retrieve_to_memory_from(&self.storage, blob_id)
            .await
    }

    /// Retrieve a value.
    ///
    /// If the value doesn't exist in the transaction itself, it will be looked-for in the base
    /// storage instead.
    pub async fn retrieve<'s>(&'s self, blob_id: &'s BlobId) -> Result<AsyncSource<'s>> {
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

        let blob_id = self.gorgon.store_in(&self.storage, source, options).await?;

        trace!(
            "Stored blob `{blob_id}` to transaction `{}`.",
            self.storage.name()
        );

        Ok(blob_id)
    }

    /// Unstore a value.
    ///
    /// A value that was unstored exactly as many times as it was stored in a `Transaction` will
    /// not actually be stored anywhere.
    ///
    /// Unstoring a value that wasn't stored is a no-op.
    pub async fn unstore(&self, blob_id: &BlobId) -> Result<()> {
        trace!(
            "Unstored blob `{blob_id}` from transaction `{}`.",
            self.storage.name()
        );

        self.gorgon.unstore_from(&self.storage, blob_id).await
    }

    /// Commit the transaction, ensuring all its referenced blobs are persisted to its associated
    /// base storage.
    ///
    /// If the commit fails, it is possible some blobs will have been persisted while other may
    /// have not. In this case, the transaction is guaranteed to only contain the blobs that could
    /// not be persisted and it is safe to retry and commit the transaction.
    pub async fn commit(self) -> Result<(), (Self, Error)> {
        self.storage.commit().await.map_err(|(storage, err)| {
            (
                Self {
                    storage,
                    gorgon: self.gorgon,
                },
                err.into(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;

    use crate::{Client, Filesystem};

    use super::*;

    const TEST_BUFFER_A: &[u8] = &[0x0A; 1024];
    const TEST_BUFFER_B: &[u8] = &[0x0B; 1024];
    const TEST_BUFFER_C: &[u8] = &[0x0C; 1024];
    const TEST_BUFFER_D: &[u8] = &[0x0D; 1024];

    #[traced_test]
    #[tokio::test]
    async fn test_transaction() {
        let filesystem = Filesystem::default();
        let tempdir = tempfile::tempdir().unwrap();
        let storage = filesystem.new_storage(tempdir.path()).unwrap();
        let client = Client::new(storage);

        let id_a = client
            .store(
                AsyncSource::from_static(TEST_BUFFER_A),
                &StoreOptions::default(),
            )
            .await
            .unwrap();

        // Sanity-check: the buffer A can be read in the client.
        let buf = client.retrieve_to_memory(&id_a).await.unwrap();
        assert_eq!(buf, TEST_BUFFER_A);

        // Start two different transactions.
        let transaction_one = client.start_transaction("tx1").unwrap();
        let transaction_two = client.start_transaction("tx2").unwrap();

        // We write buffer B to the transaction one, twice.
        //
        // The second write should be a no-op.
        let id_b = transaction_one
            .store(
                AsyncSource::from_static(TEST_BUFFER_B),
                &StoreOptions::default(),
            )
            .await
            .unwrap();
        let id_b_2 = transaction_one
            .store(
                AsyncSource::from_static(TEST_BUFFER_B),
                &StoreOptions::default(),
            )
            .await
            .unwrap();

        // We un-write the buffer B once, leaving still one reference to it.
        assert_eq!(id_b, id_b_2);
        transaction_one.unstore(&id_b_2).await.unwrap();

        // We write buffers C and D to the transaction two.
        let id_c = transaction_two
            .store(
                AsyncSource::from_static(TEST_BUFFER_C),
                &StoreOptions::default(),
            )
            .await
            .unwrap();
        let id_d = transaction_two
            .store(
                AsyncSource::from_static(TEST_BUFFER_D),
                &StoreOptions::default(),
            )
            .await
            .unwrap();

        // We un-write buffer D, leaving a zero reference count, effectively deleting the value
        // from the transaction.
        transaction_two.unstore(&id_d).await.unwrap();

        // Try to read buffer A through transaction one: this should succeed, as the transaction
        // uses the client storage as a fallback storage.
        let buf = transaction_one.retrieve_to_memory(&id_a).await.unwrap();
        assert_eq!(buf, TEST_BUFFER_A);

        // Reading buffer B from transaction one should work, as the entry still has a positive
        // reference count in that transaction.
        let buf = transaction_one.retrieve_to_memory(&id_b).await.unwrap();
        assert_eq!(buf, TEST_BUFFER_B);

        // Reading buffer C from transaction one should fail, as the entry never existed there.
        assert!(matches!(
            transaction_one
                .retrieve_to_memory(&id_c)
                .await
                .unwrap_err(),
            Error::RemoteRefNotFound(remote_ref) if id_c == BlobId::RemoteRef(remote_ref.clone())
        ));

        // Reading buffer C from transaction two should work, as the entry still has a positive
        // reference count in that transaction.
        let buf = transaction_two.retrieve_to_memory(&id_c).await.unwrap();
        assert_eq!(buf, TEST_BUFFER_C);

        // Reading buffer D from transaction two should fail, as the entry no longer has
        // references.
        assert!(matches!(
            transaction_one
                .retrieve_to_memory(&id_d)
                .await
                .unwrap_err(),
            Error::RemoteRefNotFound(remote_ref) if id_d == BlobId::RemoteRef(remote_ref.clone())
        ));

        // Reading buffer B from client should fail, as the entry was never commited there.
        assert!(matches!(
            client
                .retrieve_to_memory(&id_b)
                .await
                .unwrap_err(),
            Error::RemoteRefNotFound(remote_ref) if id_b == BlobId::RemoteRef(remote_ref.clone())
        ));

        transaction_one
            .commit()
            .await
            .expect("failed to commit transaction one");

        // The client can now read buffer B as the transaction one was committed.
        let buf = client.retrieve_to_memory(&id_b).await.unwrap();
        assert_eq!(buf, TEST_BUFFER_B);

        // The transaction two can also read buffer B, as the transaction one was committed and it
        // uses the client storage as a fallback storage.
        let buf = transaction_two.retrieve_to_memory(&id_b).await.unwrap();
        assert_eq!(buf, TEST_BUFFER_B);

        transaction_two
            .commit()
            .await
            .expect("failed to commit transaction two");

        // The client can now read buffer B as the transaction one was committed.
        let buf = client.retrieve_to_memory(&id_c).await.unwrap();
        assert_eq!(buf, TEST_BUFFER_C);

        // Reading buffer D from client should fail, as the entry had a zero reference count before
        // commit, and was hence never committed.
        assert!(matches!(
            client
                .retrieve_to_memory(&id_d)
                .await
                .unwrap_err(),
            Error::RemoteRefNotFound(remote_ref) if id_d == BlobId::RemoteRef(remote_ref.clone())
        ));
    }
}
