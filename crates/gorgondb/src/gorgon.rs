//! A ``Gorgon`` implements methods to read and write blobs of data.

use std::path::{Path, PathBuf};

use futures::{future::BoxFuture, TryStreamExt};
use humansize::{FormatSize, BINARY};
use tracing::{instrument, Level};

use crate::{
    ledger::Ledger, AsyncSource, AsyncSourceChain, BlobId, Filesystem, FragmentationMethod,
    HashAlgorithm, Storage,
};

/// An error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occured.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    /// A `BlobId` error occured.
    #[error("blob id error: {0}")]
    BlobId(#[from] crate::blob_id::Error),

    /// A storage error occured.
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::Error),

    /// A fragmentation error occured.
    #[error("fragmentation error: {0}")]
    Fragmentation(#[from] crate::fragmentation::Error),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The options for the store operation.
#[derive(Debug, Clone, Default)]
pub struct StoreOptions {
    /// If set to true, the `Gorgon` will not attempt to fragment bigger values.
    ///
    /// Using this option might produce a less deduplicated database. Use with caution.
    disable_fragmentation: bool,
}

/// A `Gorgon` implements high-level primitives to store and retrieve data blobs.
#[derive(Debug)]
pub struct Gorgon {
    hash_algorithm: HashAlgorithm,
    fragmentation_method: FragmentationMethod,
    filesystem: Filesystem,
    storage: Storage,
}

impl Gorgon {
    /// Instantiate a new `Gorgon` using the specified storage.
    pub fn new(storage: Storage) -> Self {
        let filesystem = Filesystem::default();

        Self {
            hash_algorithm: HashAlgorithm::Blake3,
            fragmentation_method: FragmentationMethod::Fastcdc(Default::default()),
            filesystem,
            storage,
        }
    }

    /// Retrieve a value from a file on disk.
    pub async fn retrieve_to_file(&self, blob_id: BlobId, path: impl AsRef<Path>) -> Result<()> {
        let source = self.retrieve(blob_id).await?;

        self.filesystem
            .save_source(path, source)
            .await
            .map_err(Into::into)
    }

    /// Retrieve a value.
    #[instrument(level=Level::INFO, skip(self, blob_id), fields(blob_id=blob_id.to_string()))]
    pub fn retrieve(&self, blob_id: BlobId) -> BoxFuture<Result<AsyncSource<'_>>> {
        Box::pin(async move {
            Ok(match blob_id {
                BlobId::SelfContained(buf) => {
                    tracing::debug!("Blob is self-contained: retrieving from memory.");

                    buf.into()
                }
                BlobId::Ledger { blob_id, .. } => {
                    tracing::debug!("Blob is a ledger with id `{blob_id}`.");

                    let ledger_source = self.retrieve(*blob_id).await?;
                    let ledger =
                        Ledger::async_read_from(ledger_source.get_async_read().await?).await?;

                    match ledger {
                        Ledger::LinearAggregate { blob_ids } => {
                            tracing::debug!(
                                "Ledger is a linear aggregate of {} blob ids.",
                                blob_ids.len()
                            );

                            let sources = futures::future::join_all(
                                blob_ids.into_iter().map(|blob_id| self.retrieve(blob_id)),
                            )
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>>>()?;

                            tracing::debug!("Got all sources from ledger.");

                            AsyncSourceChain::new(sources).into()
                        }
                    }
                }
                BlobId::RemoteRef(remote_ref) => {
                    tracing::debug!(
                        "Blob is stored remotely in ref `{remote_ref}`: fetching from storage..."
                    );

                    self.storage.retrieve(&remote_ref).await?
                }
            })
        })
    }

    /// Store a file from the disk.
    ///
    /// This is a convenience method.
    pub async fn store_from_file(
        &self,
        path: impl Into<PathBuf>,
        options: &StoreOptions,
    ) -> Result<BlobId> {
        let source = self.filesystem.load_source(path).await?;

        self.store(source, options).await
    }

    /// Store and persist a value.
    ///
    /// Upon success, a `BlobId` describing the value is returned. Losing the resulting `BlobId`
    /// equates to losing the value. It is the caller's responsibility to store [`BlobIds`](`BlobId`)
    /// appropriately.
    pub fn store<'s>(
        &'s self,
        source: impl Into<AsyncSource<'s>>,
        options: &'s StoreOptions,
    ) -> BoxFuture<'s, Result<BlobId>> {
        let source = source.into();

        Box::pin(async move {
            let ref_size = source.size();

            if ref_size < BlobId::MAX_SELF_CONTAINED_SIZE {
                let data = source.read_all_into_vec().await?;

                BlobId::self_contained(data).map_err(Into::into)
            } else if !options.disable_fragmentation
                && ref_size > self.fragmentation_method.min_size()
            {
                tracing::debug!(
                    "Source is bigger than the fragmentation threshold ({} > {}): splitting in chunks...",
                    ref_size.format_size(BINARY),
                    self.fragmentation_method.min_size().format_size(BINARY),
                );

                let mut blob_ids =
                    Vec::with_capacity(self.fragmentation_method.fragments_count_hint(ref_size));
                let r = source.get_async_read().await?;
                let stream = self.fragmentation_method.fragment(r);

                tokio::pin!(stream);

                {
                    // Make sure fragmentation is disabled for storing each of the fragments, to
                    // avoid infinite recursion.
                    let mut options = options.clone();
                    options.disable_fragmentation = true;

                    while let Some(fragment) = stream.try_next().await? {
                        let blob_id = self.store(fragment, &options).await?;
                        blob_ids.push(blob_id);
                    }
                }

                tracing::debug!(
                    "Fragmentation yielded {} chunks: creating linear aggregate ledger...",
                    blob_ids.len()
                );

                let ledger = Ledger::LinearAggregate { blob_ids };

                let blob_id = Box::new(self.store(ledger.to_vec(), options).await?);

                Ok(BlobId::Ledger { ref_size, blob_id })
            } else {
                self.storage
                    .store(self.hash_algorithm, source)
                    .await
                    .map(Into::into)
                    .map_err(Into::into)
            }
        })
    }
}
