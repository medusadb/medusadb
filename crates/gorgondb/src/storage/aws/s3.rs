use std::sync::Arc;

use async_compat::CompatExt;
use aws_config::SdkConfig;
use aws_sdk_s3::{
    operation::{head_object::HeadObjectError, put_object::PutObjectError},
    primitives::{ByteStream, SdkBody},
};
use futures::{AsyncRead, AsyncReadExt};
use http::Response;
use tokio::sync::Semaphore;

use crate::{AsyncPermitRead, AsyncReadInit, BoxAsyncRead, RemoteRef};

/// AWS errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A S3 put object call failed.
    #[error("failed to execute PutObject")]
    S3PutObject(#[from] aws_sdk_dynamodb::error::SdkError<PutObjectError, Response<SdkBody>>),

    /// An I/O error occured.
    #[error("I/O error")]
    Io(#[from] std::io::Error),
}

/// A convenience result type for AWS S3 errors.
pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    /// Retrieve a value from S3.
    pub fn retrieve(self: &Arc<Self>, remote_ref: &RemoteRef) -> AsyncSource {
        AsyncSource {
            size: remote_ref.ref_size(),
            storage: Arc::clone(self),
            key: remote_ref.to_string(),
        }
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

        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => {
                tracing::debug!(
                    "Object `s3://{}{key}` already exists: skipping PutObject operation",
                    &self.bucket
                );

                return Ok(());
            }
            Err(err) => match err.into_service_error() {
                HeadObjectError::NotFound(_) => {}
                err => {
                    tracing::warn!("failed to assess S3 object existence for `s3://{}{key}` ({err}): assuming non-existence", &self.bucket);
                }
            },
        };

        let source = source.into();
        let body: ByteStream = match source.into_data() {
            Ok(data) => ByteStream::from(data),
            Err(source) => match source.path() {
                Some(path) => ByteStream::from_path(path).await.map_err(|err| {
                    Error::Io(std::io::Error::new(std::io::ErrorKind::Other, err))
                })?,
                None => source.read_all_into_vec().await?.into(),
            },
        };

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await?;

        Ok(())
    }
}

/// An async source that retrieves its data on AWS S3.
#[derive(Debug)]
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
    ) -> std::io::Result<AsyncPermitRead<impl AsyncRead + Send + Unpin + 'static>> {
        let bucket = bucket.as_ref();
        let key = key.as_ref();
        let name = format!("s3://{bucket}/{key}");

        tracing::debug!("Acquiring permit for reading `{name}`...");

        let permit = semaphore.acquire_owned().await.map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to acquire semaphore for reading `{name}`: {err}",),
            )
        })?;

        tracing::debug!("Acquired permit for reading `{name}`.");

        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| match err.into_service_error() {
                aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("S3 object `{name}` does not exist",),
                    )
                }
                err => std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("failed to get S3 object from `{name}`: {err}",),
                ),
            })?;

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
    pub async fn get_async_read(&self) -> std::io::Result<BoxAsyncRead> {
        Ok(Box::new(AsyncReadInit::new(async move {
            Self::get_async_read_impl(
                self.storage.semaphore.clone(),
                &self.storage.client,
                &self.storage.bucket,
                &self.key,
            )
            .await
        })))
    }

    /// Read all the content from this source into a new buffer.
    pub async fn read_all_into_vec(self) -> std::io::Result<Vec<u8>> {
        let mut r = Self::get_async_read_impl(
            self.storage.semaphore.clone(),
            &self.storage.client,
            &self.storage.bucket,
            self.key,
        )
        .await?;

        let mut data = vec![
            0;
            self.size
                .try_into()
                .expect("failed to convert u64 to usize")
        ];

        r.read_to_end(&mut data).await?;

        Ok(data)
    }

    /// Get the size of the referenced data.
    pub fn size(&self) -> u64 {
        self.size
    }
}
