use std::{borrow::Cow, sync::Arc};

use async_compat::CompatExt;
use aws_config::SdkConfig;
use aws_sdk_s3::{operation::head_object::HeadObjectError, primitives::ByteStream};
use futures::{AsyncRead, AsyncReadExt};
use tokio::sync::Semaphore;

use super::{Error, Result};
use crate::{AsyncPermitRead, AsyncReadInit, BoxAsyncRead, RemoteRef};

#[derive(Debug)]
pub struct Storage {
    semaphore: Arc<Semaphore>,
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl Storage {
    const MAX_CONCURRENT_DOWNLOADS: usize = 4;

    /// Instantiate a new AWS S3 storage.
    pub fn new(sdk_config: &SdkConfig, bucket: impl Into<String>) -> Self {
        let semaphore = Arc::new(Semaphore::new(Self::MAX_CONCURRENT_DOWNLOADS));
        let client = aws_sdk_s3::Client::new(sdk_config);
        let bucket = bucket.into();

        Self {
            semaphore,
            client,
            bucket,
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => match err.into_service_error() {
                HeadObjectError::NotFound(_) => Ok(false),
                err => Err(Error::new(err)),
            },
        }
    }

    /// Retrieve a value from S3.
    pub async fn retrieve(self: &Arc<Self>, remote_ref: &RemoteRef) -> Result<Option<AsyncSource>> {
        let key = remote_ref.to_string();
        let name = format!("s3://{}/{key}", self.bucket);

        let _permit = self.semaphore.acquire().await.map_err(|err| {
            Error::from_string(format!(
                "failed to acquire semaphore for reading `{name}`: {err}"
            ))
        })?;

        Ok(if self.exists(&key).await? {
            Some(AsyncSource {
                size: remote_ref.ref_size(),
                storage: Arc::clone(self),
                key: remote_ref.to_string(),
            })
        } else {
            None
        })
    }

    /// Store a value in S3.
    ///
    /// If the blob already exists, the call does nothing and succeeds immediately.
    pub async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: impl Into<crate::AsyncSource<'_>>,
    ) -> Result<()> {
        let key = remote_ref.to_string();

        match self.exists(&key).await {
            Ok(true) => {
                tracing::debug!(
                    "Object `s3://{}{key}` already exists: skipping PutObject operation",
                    &self.bucket
                );

                return Ok(());
            }
            Ok(false) => {}
            Err(err) => {
                tracing::warn!("failed to assess S3 object existence for `s3://{}{key}` ({err}): assuming non-existence", &self.bucket);
            }
        }

        let source = source.into();
        let body: ByteStream = match source.into_static_data() {
            Ok(data) => ByteStream::from_static(data),
            Err(source) => match source.into_data() {
                Ok(data) => ByteStream::from(data),
                Err(source) => match source.path() {
                    Some(path) => ByteStream::from_path(path).await.map_err(Error::new)?,
                    None => match source.read_all_into_memory().await.map_err(Error::new)? {
                        Cow::Owned(v) => v.into(),
                        Cow::Borrowed(s) => s.to_owned().into(),
                    },
                },
            },
        };

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(Error::new)?;

        Ok(())
    }
}

/// An async source that retrieves its data on AWS S3.
#[derive(Debug, Clone)]
pub struct AsyncSource {
    size: u64,
    storage: Arc<Storage>,
    key: String,
}

impl AsyncSource {
    async fn get_async_read_impl(
        semaphore: Arc<Semaphore>,
        client: &aws_sdk_s3::Client,
        bucket: impl AsRef<str>,
        key: impl AsRef<str>,
    ) -> Result<AsyncPermitRead<impl AsyncRead + Send + Unpin + 'static>> {
        let bucket = bucket.as_ref();
        let key = key.as_ref();
        let name = format!("s3://{bucket}/{key}");

        tracing::debug!("Acquiring permit for reading `{name}`...");

        let permit = semaphore.acquire_owned().await.map_err(|err| {
            Error::from_string(format!(
                "failed to acquire semaphore for reading `{name}`: {err}",
            ))
        })?;

        tracing::debug!("Acquired permit for reading `{name}`.");

        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(Error::new)?;

        tracing::debug!("Got async reader for `{name}`.");

        Ok(AsyncPermitRead::new(
            name,
            permit,
            resp.body.into_async_read().compat(),
        ))
    }

    /// Get an `AsyncRead` instance from this source.
    ///
    /// This source will request a semaphore permit when it is first polled: depending on how many
    /// other AWS S3 sources are being read from, this might block the reader until other readers
    /// are either done reading or dropped.
    pub async fn get_async_read(&self) -> Result<BoxAsyncRead> {
        Ok(Box::new(AsyncReadInit::new(async move {
            Self::get_async_read_impl(
                self.storage.semaphore.clone(),
                &self.storage.client,
                &self.storage.bucket,
                &self.key,
            )
            .await
            .map_err(Error::into_io_error)
        })))
    }

    /// Read all the content from this source into a new buffer.
    pub async fn read_all_into_memory(self) -> Result<Vec<u8>> {
        let mut r = Self::get_async_read_impl(
            self.storage.semaphore.clone(),
            &self.storage.client,
            &self.storage.bucket,
            self.key,
        )
        .await?;

        let data_size: usize = self
            .size
            .try_into()
            .expect("failed to convert u64 to usize");
        let mut data = Vec::with_capacity(data_size);

        r.read_to_end(&mut data).await.map_err(Error::new)?;

        debug_assert_eq!(data.len(), data_size);

        Ok(data)
    }

    /// Get the size of the referenced data.
    pub fn size(&self) -> u64 {
        self.size
    }
}
