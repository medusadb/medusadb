use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::AsyncReadExt;
use tokio::sync::Semaphore;

use crate::{AsyncReadInit, BoxAsyncRead};

use super::AsyncFileRead;

/// A source file on disk.
pub struct AsyncFileSource {
    path: PathBuf,
    size: u64,
    semaphore: Arc<Semaphore>,
}

impl AsyncFileSource {
    /// Create an async file source for the file whose path is specified.
    ///
    /// Creating an `AsyncRead` from this source will require that the specified semaphore still
    /// has some permits to lend.
    pub(crate) async fn open(semaphore: Arc<Semaphore>, path: PathBuf) -> std::io::Result<Self> {
        let size = tokio::fs::metadata(&path)
            .await
            .map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!("failed to read metadata for `{}`: {err}", path.display()),
                )
            })?
            .len();

        Ok(Self {
            path,
            size,
            semaphore,
        })
    }

    async fn get_async_file_read(
        semaphore: Arc<Semaphore>,
        path: &Path,
    ) -> std::io::Result<AsyncFileRead> {
        let permit = semaphore.acquire_owned().await.map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "failed to acquire semaphore for reading `{}`: {err}",
                    path.display()
                ),
            )
        })?;

        AsyncFileRead::open(permit, path).await
    }

    /// Get the size of the referenced file.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the path of the referenced file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read all the content of the file into memory.
    pub async fn read_all_into_vec(self) -> std::io::Result<Vec<u8>> {
        let mut data = vec![
            0;
            self.size
                .try_into()
                .expect("failed to convert u64 to usize")
        ];

        let mut r = Self::get_async_file_read(self.semaphore, &self.path).await?;

        r.read_to_end(&mut data).await?;

        Ok(data)
    }

    /// Get an `AsyncRead` returning the data.
    pub async fn get_async_read(&self) -> std::io::Result<BoxAsyncRead> {
        let semaphore = self.semaphore.clone();
        let path = self.path.to_owned();

        Ok(Box::new(AsyncReadInit::new(async move {
            Self::get_async_file_read(semaphore, &path).await
        })))
    }
}
