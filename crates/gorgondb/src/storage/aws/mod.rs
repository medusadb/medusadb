use std::{borrow::Cow, fmt::Display, sync::Arc};

use aws_config::SdkConfig;
use aws_sdk_dynamodb::error::DisplayErrorContext;
use futures::future::BoxFuture;

use crate::{BoxAsyncRead, RemoteRef};

mod dynamodb;
mod s3;

pub use dynamodb::Storage as DynamoDbStorage;
pub use s3::{AsyncSource as S3AsyncSource, Storage as S3Storage};

/// AWS errors.
#[derive(Debug, thiserror::Error)]
pub struct Error(String);

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AWS: {}", self.0)
    }
}

impl Error {
    pub(crate) fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub(crate) fn new(value: impl std::error::Error) -> Self {
        Self(DisplayErrorContext(value).to_string())
    }

    pub(crate) fn into_io_error(self) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, self.to_string())
    }
}

/// A convenience result type for AWS errors.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A storage that uses AWS S3 and DynamoDB.
#[derive(Debug)]
pub struct AwsStorage {
    s3: Arc<S3Storage>,
    dynamodb: DynamoDbStorage,
}

impl AwsStorage {
    /// The threshold size is 128K.
    ///
    /// Storing anything smaller in AWS S3 will still be billed as something that is 128K in size,
    /// so we store such values in DynamoDB intead, which only accepts values of 400K at most.
    const THRESHOLD_SIZE: u64 = 128 * 1024;

    /// Instantiate a new AWS storage.
    pub fn new(
        sdk_config: &SdkConfig,
        s3_bucket_name: impl Into<String>,
        dynamodb_table_table: impl Into<String>,
    ) -> Self {
        let s3 = Arc::new(S3Storage::new(sdk_config, s3_bucket_name));
        let dynamodb = DynamoDbStorage::new(sdk_config, dynamodb_table_table);

        Self { s3, dynamodb }
    }

    /// Retrieve a value on AWS.
    pub async fn retrieve(&self, remote_ref: &RemoteRef) -> Result<Option<AsyncSource>> {
        if remote_ref.ref_size() < Self::THRESHOLD_SIZE {
            self.dynamodb
                .retrieve(remote_ref)
                .await
                .map(|o| o.map(Into::into))
        } else {
            self.s3
                .retrieve(remote_ref)
                .await
                .map(|o| o.map(Into::into))
        }
    }

    /// Store a value in AWS.
    ///
    /// If the blob already exists, the call does nothing and succeeds immediately.
    pub async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: impl Into<crate::AsyncSource<'_>>,
    ) -> Result<()> {
        if remote_ref.ref_size() < Self::THRESHOLD_SIZE {
            self.dynamodb.store(remote_ref, source).await?;
        } else {
            self.s3.store(remote_ref, source).await?;
        }

        Ok(())
    }
}

/// A source of data that comes from Aws.
#[derive(Debug, Clone)]
pub enum AsyncSource<'d> {
    /// The source comes from AWS DynamoDB.
    DynamoDb(Cow<'d, [u8]>),

    /// The source comes from AWS S3.
    S3(S3AsyncSource),
}

impl<'d> AsyncSource<'d> {
    /// Get the size of the data.
    pub fn size(&self) -> u64 {
        match self {
            Self::DynamoDb(buf) => buf.len().try_into().expect("buffer should fit in a u64"),
            Self::S3(source) => source.size(),
        }
    }

    /// Get a reference to the data, if it lives in memory.
    pub fn data(&self) -> Option<&[u8]> {
        match self {
            Self::DynamoDb(buf) => Some(buf),
            Self::S3(_) => None,
        }
    }

    /// Get the data, if it lives in memory.
    pub fn into_data(self) -> Result<Vec<u8>, Self> {
        match self {
            Self::DynamoDb(buf) => Ok(buf.to_vec()),
            Self::S3(_) => Err(self),
        }
    }

    /// Transform the source in a buffer in memory, reading the entirety of the data first if
    /// necessary.
    pub async fn read_all_into_memory(self) -> Result<Cow<'d, [u8]>> {
        match self {
            Self::DynamoDb(buf) => Ok(buf),
            Self::S3(source) => source.read_all_into_memory().await.map(Into::into),
        }
    }

    /// Get an asynchronous reader from this `AsyncSource`.
    pub fn get_async_read(&self) -> BoxFuture<'_, Result<BoxAsyncRead<'_>>> {
        match self {
            Self::DynamoDb(buf) => {
                Box::pin(async move { Ok(Box::new(futures::io::Cursor::new(buf)) as BoxAsyncRead) })
            }
            Self::S3(source) => Box::pin(async move {
                source
                    .get_async_read()
                    .await
                    .map(|r| Box::new(r) as BoxAsyncRead)
            }),
        }
    }
}

impl From<Vec<u8>> for AsyncSource<'_> {
    fn from(value: Vec<u8>) -> Self {
        Cow::Owned::<[u8]>(value).into()
    }
}

impl<'d> From<Cow<'d, [u8]>> for AsyncSource<'d> {
    fn from(value: Cow<'d, [u8]>) -> Self {
        Self::DynamoDb(value)
    }
}

impl From<S3AsyncSource> for AsyncSource<'_> {
    fn from(value: S3AsyncSource) -> Self {
        Self::S3(value)
    }
}
