//! An interface for the filesystem.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_compat::CompatExt;
use fs4::tokio::AsyncFileExt;
use reflink::reflink;
use tokio::{io::AsyncWriteExt, sync::Semaphore};

mod async_file_read;
mod async_file_source;
pub use async_file_read::AsyncFileRead;
pub use async_file_source::AsyncFileSource;

use crate::{AsyncSource, storage::FilesystemStorage};

/// An interface to interact with the filesystem.
///
/// It's main responsibility is ensuring that the system never opens too many file descriptors at
/// once, which is likely to happen when using big ledgers blobs.
#[derive(Debug, Clone)]
pub struct Filesystem {
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl Default for Filesystem {
    fn default() -> Self {
        let semaphore = Arc::new(Semaphore::new(20));

        Self { semaphore }
    }
}

impl Filesystem {
    /// Instantiate a new filesystem storage at the specified location.
    pub fn new_storage(&self, root: impl Into<PathBuf>) -> std::io::Result<FilesystemStorage> {
        FilesystemStorage::new(self.clone(), root.into())
    }

    /// Instantiate a new filesystem storage suitable for caching.
    ///
    /// This storage will use an appropriate, OS-dependant location to store its values.
    pub fn new_caching_storage(&self, suffix: &str) -> std::io::Result<FilesystemStorage> {
        let root = dirs::cache_dir()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "could not determine OS cache directory".to_owned(),
                )
            })?
            .join(suffix);

        self.new_storage(root)
    }

    /// Load a file source from the disk.
    pub async fn load_source(
        &self,
        path: impl Into<PathBuf>,
    ) -> std::io::Result<Option<AsyncFileSource>> {
        let semaphore = self.semaphore.clone();

        AsyncFileSource::open(semaphore, path.into()).await
    }

    /// Store the content of an `AsyncSource` on disk.
    ///
    /// Returns an AsyncFileSource pointing to it.
    pub async fn save_source(
        &self,
        path: impl AsRef<Path>,
        source: impl Into<AsyncSource<'_>>,
    ) -> std::io::Result<AsyncFileSource> {
        let path = path.as_ref();
        let source = source.into();

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!("failed to create parent directory for `{}`", path.display()),
                )
            })?;
        }

        if let Some(source_path) = source.path() {
            tracing::debug!(
                "Source has a path on the local filesystem (`{}`): will attempt a copy using `reflink`.",
                source_path.display(),
            );

            match reflink(source_path, path) {
                Ok(()) => {
                    return Ok(AsyncFileSource::new(
                        self.semaphore.clone(),
                        path.into(),
                        source.size(),
                    ));
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to copy file using `reflink` ({err}): will fallback to in-memory copy."
                    );
                }
            }
        }

        match tokio::fs::File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .await
        {
            Ok(mut f) => {
                f.try_lock_exclusive().map_err(|err| {
                    std::io::Error::new(
                        err.kind(),
                        format!("failed to lock file `{}` exclusively", path.display()),
                    )
                })?;

                match source.data() {
                    Some(data) => {
                        // If we have the data ready in memory, write it to disk directly.
                        f.write_all(data).await.map_err(|err| {
                            std::io::Error::new(
                                err.kind(),
                                format!("faild to write file `{}`", path.display()),
                            )
                        })?;
                    }
                    _ => {
                        let r = source.get_async_read().await.map_err(|err| {
                            std::io::Error::new(
                                err.kind(),
                                format!(
                                    "failed to get source reader when writing `{}`",
                                    path.display()
                                ),
                            )
                        })?;
                        futures::io::copy(r, &mut f.compat_mut())
                            .await
                            .map_err(|err| {
                                std::io::Error::new(
                                    err.kind(),
                                    format!("failed to copy file `{}`", path.display()),
                                )
                            })?;
                    }
                }

                f.shutdown().await.map_err(|err| {
                    std::io::Error::new(
                        err.kind(),
                        format!("failed to close file `{}`", path.display()),
                    )
                })?;

                Ok(AsyncFileSource::new(
                    self.semaphore.clone(),
                    path.into(),
                    source.size(),
                ))
            }
            Err(err) => Err(std::io::Error::new(
                err.kind(),
                format!("failed to create file `{}`: {err}", path.display()),
            )),
        }
    }
}
