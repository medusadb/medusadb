//! A ``Gorgon`` implements methods to read and write blobs of data.

use std::path::{Path, PathBuf};

use async_compat::CompatExt;
use futures::{future::BoxFuture, AsyncRead, AsyncReadExt, TryStreamExt};

use crate::{
    ledger::Ledger, storage::FilesystemStorage, AsyncFileSource, AsyncSource, Cairn,
    FragmentationMethod, HashAlgorithm, Storage,
};

/// An error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("cairn error: {0}")]
    Cairn(#[from] crate::cairn::Error),

    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::Error),

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

#[derive(Debug)]
pub struct Gorgon {
    hash_algorithm: HashAlgorithm,
    fragmentation_method: FragmentationMethod,
    storage: Storage,
}

impl Default for Gorgon {
    fn default() -> Self {
        Self {
            hash_algorithm: HashAlgorithm::Blake3,
            fragmentation_method: FragmentationMethod::Fastcdc(Default::default()),
            storage: Storage::Filesystem(FilesystemStorage::new("test").unwrap()),
        }
    }
}

impl Gorgon {
    /// Retrieve a value from a file on disk.
    pub async fn retrieve_to_file(&self, cairn: Cairn, path: impl AsRef<Path>) -> Result<()> {
        let mut source = self.retrieve(cairn).await?;

        // TODO: Reuse AsyncSource here.
        let mut target = tokio::fs::File::create(path).await?;
        tokio::io::copy(&mut source.compat_mut(), &mut target).await?;

        Ok(())
    }

    /// Retrieve a value.
    pub fn retrieve(&self, cairn: Cairn) -> BoxFuture<Result<Box<dyn AsyncRead + Unpin + Send>>> {
        Box::pin(async move {
            Ok(match cairn {
                Cairn::SelfContained(buf) => Box::new(futures::io::Cursor::new(buf)),
                Cairn::Ledger { cairn, .. } => {
                    let ledger_source = self.retrieve(*cairn).await?;
                    let ledger = Ledger::async_read_from(ledger_source).await?;

                    match ledger {
                        Ledger::LinearAggregate { cairns } => {
                            let streams = futures::future::join_all(
                                cairns.into_iter().map(|cairn| self.retrieve(cairn)),
                            )
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>>>()?;

                            streams
                                .into_iter()
                                .reduce(|res, stream| Box::new(res.chain(stream)))
                                .unwrap_or_else(|| Box::new(futures::io::empty()))
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
        let source = AsyncFileSource::open(path).await?;

        self.store(source, options).await
    }

    /// Store and persist a value.
    ///
    /// Upon success, a `Cairn` describing the value is returned. Losing the resulting `Cairn`
    /// equates to losing the value. It is the caller's responsibility to store [`Cairns`](`Cairn`)
    /// appropriately.
    pub fn store<'s>(
        &'s self,
        mut source: impl AsyncSource + Send + 's,
        options: &'s StoreOptions,
    ) -> BoxFuture<'s, Result<Cairn>> {
        Box::pin(async move {
            let ref_size = source.size();

            if ref_size < Cairn::MAX_SELF_CONTAINED_SIZE {
                let mut data =
                    vec![0; ref_size.try_into().expect("failed to convert u64 to usize")];
                source.read_to_end(&mut data).await?;

                Cairn::self_contained(data).map_err(Into::into)
            } else if !options.disable_fragmentation
                && ref_size > self.fragmentation_method.min_size()
            {
                let mut cairns =
                    Vec::with_capacity(self.fragmentation_method.fragments_count_hint(ref_size));
                let stream = self.fragmentation_method.fragment(source);

                tokio::pin!(stream);

                {
                    // Make sure fragmentation is disabled for storing each of the fragments, to
                    // avoid infinite recursion.
                    let mut options = options.clone();
                    options.disable_fragmentation = true;

                    while let Some(fragment) = stream.try_next().await? {
                        let cairn = self
                            .store(futures::io::Cursor::new(fragment), &options)
                            .await?;
                        cairns.push(cairn);
                    }
                }

                let ledger = Ledger::LinearAggregate { cairns };

                let cairn = Box::new(
                    self.store(futures::io::Cursor::new(ledger.to_vec()), options)
                        .await?,
                );

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
