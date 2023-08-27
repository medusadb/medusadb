//! A ``Gorgon`` implements methods to read and write blobs of data.

use futures::{future::BoxFuture, io::copy, AsyncReadExt, TryStreamExt};

use crate::{ledger::Ledger, AsyncSource, Cairn, FragmentationMethod, HashAlgorithm, RemoteRef};

/// An error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("cairn error: {0}")]
    Cairn(#[from] crate::cairn::Error),

    #[error("fragmentation error: {0}")]
    Fragmentation(#[from] crate::fragmentation::Error),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Gorgon {
    hash_algorithm: HashAlgorithm,
    fragmentation_method: FragmentationMethod,
}

impl Default for Gorgon {
    fn default() -> Self {
        Self {
            hash_algorithm: HashAlgorithm::Blake3,
            fragmentation_method: FragmentationMethod::Fastcdc(Default::default()),
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
        mut r: impl AsyncSource + Send + 's,
    ) -> BoxFuture<'s, Result<Cairn>> {
        Box::pin(async move {
            let ref_size = r.size();

            if ref_size < Cairn::MAX_SELF_CONTAINED_SIZE {
                let mut data =
                    vec![0; ref_size.try_into().expect("failed to convert u64 to usize")];
                r.read_to_end(&mut data).await?;

                Cairn::self_contained(data).map_err(Into::into)
            } else if ref_size > self.fragmentation_method.min_size() {
                let mut cairns =
                    Vec::with_capacity(self.fragmentation_method.fragments_count_hint(ref_size));
                let stream = self.fragmentation_method.fragment(r);

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
                let hash = self.hash_algorithm.async_hash_to_vec(&mut r).await?.into();

                // TODO: do the actual storage...
                let mut w = futures::io::sink();
                copy(&mut r, &mut w).await?;

                let remote_ref = RemoteRef {
                    ref_size,
                    hash_algorithm: self.hash_algorithm,
                    hash,
                };

                Ok(remote_ref.into())
            }
        })
    }
}
