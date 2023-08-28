//! A ``Gorgon`` implements methods to read and write blobs of data.

use futures::{future::BoxFuture, AsyncReadExt, TryStreamExt};

use crate::{
    ledger::Ledger, storage::FilesystemStorage, AsyncSource, Cairn, FragmentationMethod,
    HashAlgorithm, Storage,
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
            storage: Storage::Filesystem(FilesystemStorage::new("test")),
        }
    }
}

impl Gorgon {
    /// Store and persist a value.
    ///
    /// Upon success, a `Cairn` describing the value is returned. Losing the resulting `Cairn`
    /// equates to losing the value. It is the caller's responsibility to store [`Cairns`](`Cairn`)
    /// appropriately.
    pub fn store<'s>(
        &'s self,
        mut source: impl AsyncSource + Send + 's,
    ) -> BoxFuture<'s, Result<Cairn>> {
        Box::pin(async move {
            let ref_size = source.size();

            if ref_size < Cairn::MAX_SELF_CONTAINED_SIZE {
                let mut data =
                    vec![0; ref_size.try_into().expect("failed to convert u64 to usize")];
                source.read_to_end(&mut data).await?;

                Cairn::self_contained(data).map_err(Into::into)
            } else if ref_size > self.fragmentation_method.min_size() {
                let mut cairns =
                    Vec::with_capacity(self.fragmentation_method.fragments_count_hint(ref_size));
                let stream = self.fragmentation_method.fragment(source);

                tokio::pin!(stream);

                while let Some(fragment) = stream.try_next().await? {
                    let cairn = self.store(futures::io::Cursor::new(fragment)).await?;
                    cairns.push(cairn);
                }

                let ledger = Ledger::LinearAggregate { cairns };

                let cairn = Box::new(
                    self.store(futures::io::Cursor::new(ledger.to_vec()))
                        .await?,
                );

                Ok(Cairn::Ledger { ref_size, cairn })
            } else {
                self.storage
                    .store(ref_size, self.hash_algorithm, source)
                    .await
                    .map(Into::into)
                    .map_err(Into::into)
            }
        })
    }
}
