use std::path::Path;

use async_compat::{Compat, CompatExt};
use fs4::tokio::AsyncFileExt;
use tokio::sync::OwnedSemaphorePermit;

use crate::AsyncPermitRead;

/// An `AsyncRead` that holds a semaphore permit to a file.
pub type AsyncFileRead = AsyncPermitRead<Compat<tokio::fs::File>>;

impl AsyncFileRead {
    /// Open a file from its path and borrowing the specified permit.
    pub async fn open(
        permit: OwnedSemaphorePermit,
        path: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        let path = path.as_ref();
        let name = format!("file://{}", path.display());

        let file = tokio::fs::File::options()
            .read(true)
            .open(&path)
            .await
            .map_err(|err| {
                std::io::Error::new(err.kind(), format!("failed to open `{name}`: {err}"))
            })?;

        // Ensure that no other process is or will be writing to the file as long as we have it.
        file.try_lock_shared().map_err(|err| {
            std::io::Error::new(err.kind(), format!("failed to lock `{name}`: {err}"))
        })?;

        let inner = file.compat();

        Ok(Self::new(name, permit, inner))
    }
}
