use std::{
    path::{Path, PathBuf},
    sync::Arc,
    task::Poll,
};

use async_compat::{Compat, CompatExt};
use fs4::tokio::AsyncFileExt;
use futures::AsyncRead;
use hex::ToHex;
use pin_project::pin_project;
use reflink::reflink;
use tokio::{
    io::AsyncWriteExt,
    sync::{OwnedSemaphorePermit, Semaphore},
};

use crate::{AsyncReadInit, AsyncSource, RemoteRef};

use super::Result;

/// A storage that stores value on disk.
#[derive(Debug, Clone)]
pub struct FilesystemStorage {
    root: PathBuf,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl FilesystemStorage {
    /// Instantiate a new filesystem storage storing its file at the specified location.
    pub fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;

        let semaphore = Arc::new(Semaphore::new(20));

        Ok(Self { root, semaphore })
    }

    /// Retrieve a value on disk.
    pub async fn retrieve(&self, remote_ref: &RemoteRef) -> AsyncReadInit<'static, impl AsyncRead> {
        let source = self.get_path(remote_ref);
        let semaphore = self.semaphore.clone();

        AsyncReadInit::new(async move {
            let permit = semaphore.acquire_owned().await.map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "failed to acquire semaphore for reading `{}`: {err}",
                        source.display()
                    ),
                )
            })?;

            FilesystemAsyncRead::load(permit, source).await
        })
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
            .create(true)
            .write(true)
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
                format!("failed to create file `{}`: {err}", target.display()),
            )
            .into()),
        }
    }

    fn get_path(&self, remote_ref: &RemoteRef) -> PathBuf {
        self.root.join(remote_ref.to_vec().encode_hex::<String>())
    }
}

#[pin_project(project = FilesystemAsyncReadImpl)]
enum FilesystemAsyncRead<Inner> {
    Reading {
        permit: OwnedSemaphorePermit,

        #[pin]
        inner: Inner,
    },
    Done,
}

impl FilesystemAsyncRead<Compat<tokio::fs::File>> {
    async fn load(permit: OwnedSemaphorePermit, path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();

        let file = tokio::fs::File::options()
            .read(true)
            .open(&path)
            .await
            .map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!("failed to open `{}`: {err}", path.display()),
                )
            })?;

        let inner = file.compat();

        Ok(Self::Reading { permit, inner })
    }
}

impl<Inner: AsyncRead + Unpin> AsyncRead for FilesystemAsyncRead<Inner> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            match self.as_mut().project() {
                FilesystemAsyncReadImpl::Reading { inner, .. } => match inner.poll_read(cx, buf) {
                    Poll::Ready(Ok(size)) if size == 0 => {}
                    res => return res,
                },
                FilesystemAsyncReadImpl::Done => return Poll::Ready(Ok(0)),
            };

            // We are done reading: release the inner stream and the permit right away in case
            // the instance is kept around.
            *self.as_mut() = Self::Done;
        }
    }
}
