use std::path::PathBuf;

use async_compat::CompatExt;
use hex::ToHex;
use reflink::reflink;
use tokio::io::AsyncWriteExt;

use crate::{AsyncSource, RemoteRef};

use super::Result;

/// A storage that stores value on disk.
#[derive(Debug, Clone)]
pub struct FilesystemStorage {
    root: PathBuf,
}

impl FilesystemStorage {
    /// Instantiate a new filesystem storage storing its file at the specified location.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();

        Self { root }
    }

    /// Store a value on disk.
    ///
    /// If the file already exists, it is assumed to exist and contain the expected value. As such,
    /// the function will return immediately, in success.
    pub async fn store(&self, remote_ref: &RemoteRef, source: impl AsyncSource) -> Result<()> {
        let target = self.get_path(remote_ref);

        if let Some(source_path) = source.source_path() {
            tracing::debug!(
                "Source has a path on the local filesystem (`{}`): will attempt a copy using `reflink`.",
                source_path.display(),
            );

            match reflink(source_path, &target) {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to store file using `reflink` ({err}): will fallback to in-memory copy"
                    );
                }
            }
        }

        match tokio::fs::File::options()
            .create_new(true)
            .open(target)
            .await
        {
            Ok(mut target) => {
                futures::io::copy(source, &mut target.compat_mut()).await?;
                target.shutdown().await?;

                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn get_path(&self, remote_ref: &RemoteRef) -> PathBuf {
        self.root.join(remote_ref.to_vec().encode_hex::<String>())
    }
}
