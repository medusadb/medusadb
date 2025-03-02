//! A ``Gorgon`` implements methods to read and write blobs of data.

use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use futures::{TryStreamExt, future::BoxFuture};
use humansize::{BINARY, FormatSize};
use tracing::{Level, instrument};

use crate::{
    AsyncFileSource, AsyncSource, AsyncSourceChain, BlobId, Filesystem, FragmentationMethod,
    HashAlgorithm, RemoteRef, ledger::Ledger,
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

    /// A remote reference does not exist.
    #[error("a remote-reference was not found: {0}")]
    RemoteRefNotFound(RemoteRef),
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
#[derive(Debug, Clone)]
pub struct Gorgon {
    hash_algorithm: HashAlgorithm,
    fragmentation_method: FragmentationMethod,
    filesystem: Filesystem,
}

impl Default for Gorgon {
    fn default() -> Self {
        Self {
            hash_algorithm: HashAlgorithm::Blake3,
            fragmentation_method: FragmentationMethod::Fastcdc(Default::default()),
            filesystem: Filesystem::default(),
        }
    }
}

impl Gorgon {
    /// Get the associated filesystem instance.
    pub fn filesystem(&self) -> &Filesystem {
        &self.filesystem
    }

    /// Retrieve a value from the specified storage and persist it to the specified path on the
    /// local disk.
    pub async fn retrieve_to_file_from<'s>(
        &'s self,
        storage: &'s (impl Retrieve + Sync),
        blob_id: &'s BlobId,
        path: impl AsRef<Path>,
    ) -> Result<AsyncFileSource> {
        let source = self.retrieve_from(storage, blob_id).await?;

        self.filesystem
            .save_source(path, source)
            .await
            .map_err(Into::into)
    }

    /// Retrieve a value from the specified storage and read it in memory.
    ///
    /// This is a convenience method that will use the most efficient method to read the value.
    pub async fn retrieve_to_memory_from<'s>(
        &'s self,
        storage: &'s (impl Retrieve + Sync),
        blob_id: &'s BlobId,
    ) -> Result<Cow<'static, [u8]>> {
        let source = self.retrieve_from(storage, blob_id).await?;

        let source_size: usize = source.size().try_into().unwrap();

        let data = source.read_all_into_owned_memory().await?;

        debug_assert_eq!(
            data.len(),
            source_size,
            "data does not have the expected size"
        );

        Ok(data)
    }

    /// Retrieve a value from the specified storage.
    #[instrument(level=Level::INFO, skip(self, storage, blob_id), fields(blob_id=blob_id.to_string()))]
    fn retrieve_from_owned<'s>(
        &'s self,
        storage: &'s (impl Retrieve + Sync),
        blob_id: BlobId,
    ) -> BoxFuture<'s, Result<AsyncSource<'s>>> {
        Box::pin(async move {
            Ok(match blob_id {
                BlobId::SelfContained(buf) => {
                    tracing::debug!("Blob is self-contained: retrieving from memory.");

                    buf.into()
                }
                BlobId::Ledger { blob_id, .. } => {
                    tracing::debug!("Blob is a ledger with id `{blob_id}`.");

                    let ledger_source = self.retrieve_from_owned(storage, *blob_id).await?;
                    let ledger =
                        Ledger::async_read_from(ledger_source.get_async_read().await?).await?;

                    match ledger {
                        Ledger::LinearAggregate { blob_ids } => {
                            tracing::debug!(
                                "Ledger is a linear aggregate of {} blob ids.",
                                blob_ids.len()
                            );

                            let sources = futures::future::join_all(
                                blob_ids
                                    .into_iter()
                                    .map(|blob_id| self.retrieve_from_owned(storage, blob_id)),
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
                        "Blob points to {} bytes stored remotely in ref `{remote_ref}`: fetching from storage...",
                        remote_ref.ref_size()
                    );

                    match storage.retrieve(&remote_ref).await? {
                        Some(source) => {
                            debug_assert_eq!(
                                source.size(),
                                remote_ref.ref_size(),
                                "source has an unexpected size"
                            );

                            source
                        }
                        None => return Err(Error::RemoteRefNotFound(remote_ref)),
                    }
                }
            })
        })
    }

    /// Retrieve a value from the specified storage.
    #[instrument(level=Level::INFO, skip(self, storage, blob_id), fields(blob_id=blob_id.to_string()))]
    pub fn retrieve_from<'s>(
        &'s self,
        storage: &'s (impl Retrieve + Sync),
        blob_id: &'s BlobId,
    ) -> BoxFuture<'s, Result<AsyncSource<'s>>> {
        Box::pin(async move {
            Ok(match blob_id {
                BlobId::SelfContained(buf) => {
                    tracing::debug!("Blob is self-contained: retrieving from memory.");

                    buf.clone().into()
                }
                BlobId::Ledger { blob_id, .. } => {
                    tracing::debug!("Blob is a ledger with id `{blob_id}`.");

                    let ledger_source = self.retrieve_from(storage, blob_id).await?;
                    let ledger =
                        Ledger::async_read_from(ledger_source.get_async_read().await?).await?;

                    match ledger {
                        Ledger::LinearAggregate { blob_ids } => {
                            tracing::debug!(
                                "Ledger is a linear aggregate of {} blob ids.",
                                blob_ids.len()
                            );

                            let sources = futures::future::join_all(
                                blob_ids
                                    .into_iter()
                                    .map(|blob_id| self.retrieve_from_owned(storage, blob_id)),
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
                        "Blob points to {} bytes stored remotely in ref `{remote_ref}`: fetching from storage...",
                        remote_ref.ref_size()
                    );

                    match storage.retrieve(remote_ref).await? {
                        Some(source) => {
                            debug_assert_eq!(
                                source.size(),
                                remote_ref.ref_size(),
                                "source has an unexpected size"
                            );

                            source
                        }
                        None => return Err(Error::RemoteRefNotFound(remote_ref.clone())),
                    }
                }
            })
        })
    }

    /// Store a file from the disk in the specified storage.
    ///
    /// This is a convenience method.
    pub async fn store_from_file_in(
        &self,
        storage: &(impl Store + Sync),
        path: impl Into<PathBuf>,
        options: &StoreOptions,
    ) -> Result<BlobId> {
        match self.filesystem.load_source(path).await? {
            Some(source) => self.store_in(storage, source.into(), options).await,
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "fail to read source file",
            )
            .into()),
        }
    }

    /// Store and persist a value in the specified storage.
    ///
    /// Upon success, a `BlobId` describing the value is returned. Losing the resulting `BlobId`
    /// equates to losing the value. It is the caller's responsibility to store [`BlobIds`](`BlobId`)
    /// appropriately.
    #[instrument(level=Level::INFO, skip(self, storage, source))]
    pub(crate) fn store_in<'s>(
        &'s self,
        storage: &'s (impl Store + Sync),
        source: AsyncSource<'s>,
        options: &'s StoreOptions,
    ) -> BoxFuture<'s, Result<BlobId>> {
        Box::pin(async move {
            let ref_size = source.size();

            if ref_size < BlobId::MAX_SELF_CONTAINED_SIZE {
                let data = match source.static_data() {
                    Some(data) => Cow::Borrowed(data),
                    None => match source.read_all_into_memory().await? {
                        Cow::Owned(b) => Cow::Owned(b),
                        Cow::Borrowed(s) => Cow::Owned(s.to_owned()),
                    },
                };

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
                        let blob_id = self.store_in(storage, fragment.into(), &options).await?;
                        blob_ids.push(blob_id);
                    }
                }

                tracing::debug!(
                    "Fragmentation yielded {} chunks: creating linear aggregate ledger...",
                    blob_ids.len()
                );

                let ledger = Ledger::LinearAggregate { blob_ids };

                let blob_id = Box::new(
                    self.store_in(storage, ledger.to_vec().into(), options)
                        .await?,
                );

                Ok(BlobId::Ledger { ref_size, blob_id })
            } else {
                let hash = self
                    .hash_algorithm
                    .async_hash_to_vec(source.get_async_read().await?)
                    .await?
                    .into();

                let remote_ref = RemoteRef {
                    ref_size: source.size(),
                    hash_algorithm: self.hash_algorithm,
                    hash,
                };

                storage.store(&remote_ref, source).await?;

                Ok(remote_ref.into())
            }
        })
    }

    pub(crate) fn unstore_from<'s>(
        &'s self,
        storage: &'s (impl Retrieve + Unstore + Sync),
        blob_id: &'s BlobId,
    ) -> BoxFuture<'s, Result<()>> {
        Box::pin(async move {
            match blob_id {
                BlobId::SelfContained(_) => {}
                BlobId::Ledger { blob_id, .. } => {
                    tracing::debug!("Blob is a ledger with id `{blob_id}`.");

                    let ledger_source = self.retrieve_from(storage, blob_id).await?;
                    let ledger =
                        Ledger::async_read_from(ledger_source.get_async_read().await?).await?;

                    match ledger {
                        Ledger::LinearAggregate { blob_ids } => {
                            tracing::debug!(
                                "Ledger is a linear aggregate of {} blob ids.",
                                blob_ids.len()
                            );

                            futures::future::join_all(
                                blob_ids
                                    .iter()
                                    .map(|blob_id| self.unstore_from(storage, blob_id)),
                            )
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>>>()?;
                        }
                    }
                }
                BlobId::RemoteRef(remote_ref) => {
                    storage.unstore(remote_ref).await;
                }
            };

            Ok(())
        })
    }
}

/// A trait for types that can retrieve remote blobs.
#[async_trait]
pub trait Retrieve {
    /// Retrieve a remote reference.
    ///
    /// If it doesn't exist, `Ok(None)` is returned.
    async fn retrieve<'s>(
        &'s self,
        remote_ref: &RemoteRef,
    ) -> crate::storage::Result<Option<crate::AsyncSource<'s>>>;
}

/// A trait for types that can store remote blobs.
#[async_trait]
pub trait Store {
    async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: crate::AsyncSource<'_>,
    ) -> crate::storage::Result<()>;
}

/// A trait for types that can unstore remote blobs.
#[async_trait]
pub trait Unstore {
    /// Unstore a value.
    ///
    /// If `unstore` is called as many times as `store` was called for a value, the `store` is
    /// effectively cancelled, exactly as if it did not happen.
    async fn unstore(&self, remote_ref: &RemoteRef);
}
