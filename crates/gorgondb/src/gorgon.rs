//! A ``Gorgon`` implements methods to read and write blobs of data.

use std::path::{Path, PathBuf};

use futures::{future::BoxFuture, TryStreamExt};

use crate::{
    ledger::Ledger, storage::FilesystemStorage, AsyncSource, AsyncSourceChain, Cairn, Filesystem,
    FragmentationMethod, HashAlgorithm, Storage,
};

/// An error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occured.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    /// A `Cairn` error occured.
    #[error("cairn error: {0}")]
    Cairn(#[from] crate::cairn::Error),

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

impl Default for Gorgon {
    fn default() -> Self {
        let filesystem = Filesystem::default();
        let storage =
            Storage::Filesystem(FilesystemStorage::new(filesystem.clone(), "test").unwrap());

        Self {
            hash_algorithm: HashAlgorithm::Blake3,
            fragmentation_method: FragmentationMethod::Fastcdc(Default::default()),
            filesystem,
            storage,
        }
    }
}

impl Gorgon {
    /// Retrieve a value from a file on disk.
    pub async fn retrieve_to_file(&self, cairn: Cairn, path: impl AsRef<Path>) -> Result<()> {
        let source = self.retrieve(cairn).await?;

        self.filesystem
            .save_source(path, source)
            .await
            .map_err(Into::into)
    }

    /// Retrieve a value.
    pub fn retrieve(&self, cairn: Cairn) -> BoxFuture<Result<AsyncSource<'_>>> {
        Box::pin(async move {
            Ok(match cairn {
                Cairn::SelfContained(buf) => buf.into(),
                Cairn::Ledger { cairn, .. } => {
                    let ledger_source = self.retrieve(*cairn).await?;
                    let ledger =
                        Ledger::async_read_from(ledger_source.get_async_read().await?).await?;

                    match ledger {
                        Ledger::LinearAggregate { cairns } => {
                            let sources = futures::future::join_all(
                                cairns.into_iter().map(|cairn| self.retrieve(cairn)),
                            )
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>>>()?;

                            AsyncSourceChain::new(sources).into()
                        }
                    }
                }
                Cairn::RemoteRef(remote_ref) => self.storage.retrieve(&remote_ref).await?,
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
    ) -> Result<Cairn> {
        let source = self.filesystem.load_source(path).await?;

        self.store(source, options).await
    }

    /// Store and persist a value.
    ///
    /// Upon success, a `Cairn` describing the value is returned. Losing the resulting `Cairn`
    /// equates to losing the value. It is the caller's responsibility to store [`Cairns`](`Cairn`)
    /// appropriately.
    pub fn store<'s>(
        &'s self,
        source: impl Into<AsyncSource<'s>>,
        options: &'s StoreOptions,
    ) -> BoxFuture<'s, Result<Cairn>> {
        let source = source.into();

        Box::pin(async move {
            let ref_size = source.size();

            if ref_size < Cairn::MAX_SELF_CONTAINED_SIZE {
                let data = source.read_all_into_vec().await?;

                Cairn::self_contained(data).map_err(Into::into)
            } else if !options.disable_fragmentation
                && ref_size > self.fragmentation_method.min_size()
            {
                let mut cairns =
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
                        let cairn = self.store(fragment, &options).await?;
                        cairns.push(cairn);
                    }
                }

                let ledger = Ledger::LinearAggregate { cairns };

                let cairn = Box::new(self.store(ledger.to_vec(), options).await?);

                Ok(Cairn::Ledger { ref_size, cairn })
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
