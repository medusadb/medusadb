use std::path::PathBuf;

use async_compat::CompatExt;
use fs4::tokio::AsyncFileExt;
use hex::ToHex;
use reflink::reflink;
use tokio::io::AsyncWriteExt;

use crate::{AsyncFileSource, AsyncSource, RemoteRef};

use super::Result;

/// A storage that stores value on disk.
#[derive(Debug, Clone)]
pub struct FilesystemStorage {
    root: PathBuf,
}

impl FilesystemStorage {
    /// Instantiate a new filesystem storage storing its file at the specified location.
    pub fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;

        Ok(Self { root })
    }

    /// Retrieve a value on disk.
    pub async fn retrieve(&self, remote_ref: &RemoteRef) -> Result<AsyncFileSource> {
        let source = self.get_path(remote_ref);

        AsyncFileSource::open(source).await.map_err(Into::into)
    }

    /// Store a value on disk.
    ///
    /// If the file already exists, it is assumed to exist and contain the expected value. As such,
    /// the function will return immediately, in success.
    pub async fn store(&self, remote_ref: &RemoteRef, source: impl AsyncSource) -> Result<()> {
        let target = self.get_path(remote_ref);

        if tokio::fs::try_exists(&target).await.map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!("failed to check if file `{}` exists", target.display()),
            )
        })? {
            return Ok(());
        }

        if let Some(path) = source.path() {
            tracing::debug!(
                "Source has a path on the local filesystem (`{}`): will attempt a copy using `reflink`.",
                path.display(),
            );

            match reflink(path, &target) {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => {
                    tracing::warn!(
                    "Failed to copy file using `reflink` ({err}): will fallback to in-memory copy."
                );
                }
            }
        }

        match tokio::fs::File::options()
            .create_new(true)
            .open(&target)
            .await
        {
            Ok(mut f) => {
                f.try_lock_exclusive().map_err(|err| {
                    std::io::Error::new(
                        err.kind(),
                        format!("failed to lock file `{}` exclusively", target.display()),
                    )
                })?;
                futures::io::copy(source, &mut f.compat_mut())
                    .await
                    .map_err(|err| {
                        std::io::Error::new(
                            err.kind(),
                            format!("failed to copy file `{}`", target.display()),
                        )
                    })?;
                f.shutdown().await.map_err(|err| {
                    std::io::Error::new(
                        err.kind(),
                        format!("failed to close file `{}`", target.display()),
                    )
                })?;

                Ok(())
            }
            Err(err) => Err(std::io::Error::new(
                err.kind(),
                format!("failed to create file `{}`", target.display()),
            )
            .into()),
        }
    }

    fn get_path(&self, remote_ref: &RemoteRef) -> PathBuf {
        self.root.join(remote_ref.to_vec().encode_hex::<String>())
    }
}
