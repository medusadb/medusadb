use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::AsyncReadExt;
use tokio::sync::Semaphore;
use tracing::trace;

use crate::{AsyncReadInit, BoxAsyncRead};

use super::AsyncFileRead;

/// A source file on disk.
#[derive(Debug)]
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
    pub(crate) async fn open(
        semaphore: Arc<Semaphore>,
        path: PathBuf,
    ) -> std::io::Result<Option<Self>> {
        match tokio::fs::metadata(&path).await {
            Ok(metadata) => {
                let size = metadata.len();

                Ok(Some(Self {
                    path,
                    size,
                    semaphore,
                }))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(std::io::Error::new(
                err.kind(),
                format!("failed to read metadata for `{}`: {err}", path.display()),
            )),
        }
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
    pub async fn read_all_into_memory(self) -> std::io::Result<Vec<u8>> {
        let data_size: usize = self
            .size
            .try_into()
            .expect("failed to convert u64 to usize");
        let mut data = Vec::with_capacity(data_size);

        trace!(
            "Reading file at `{}` into a memory buffer of {} bytes.",
            self.path.display(),
            self.size,
        );

        let mut r = Self::get_async_file_read(self.semaphore, &self.path).await?;

        r.read_to_end(&mut data).await?;

        debug_assert_eq!(data.len(), data_size);

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
