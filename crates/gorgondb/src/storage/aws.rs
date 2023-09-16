#![cfg(feature = "aws")]

use std::sync::Arc;

use async_compat::CompatExt;
use aws_sdk_dynamodb::{
    error::SdkError, operation::get_item::GetItemError, primitives::Blob, types::AttributeValue,
};
use aws_sdk_s3::primitives::SdkBody;
use futures::{future::BoxFuture, AsyncRead};
use http::Response;
use tokio::sync::Semaphore;

use crate::{AsyncPermitRead, AsyncSource, BoxAsyncRead, RemoteRef};

/// AWS errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A DynamoDB get item called failed.
    #[error("DynamoDB GetItem: {0}")]
    DynamoDbGetItem(#[from] SdkError<GetItemError, Response<SdkBody>>),
}

/// A convenience result type for AWS errors.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A storage that uses AWS S3 and DynamoDB.
#[derive(Debug, Clone)]
pub struct AwsStorage {
    s3: Arc<AwsS3Storage>,
    dynamodb: Arc<AwsDynamoDbStorage>,
}

impl AwsStorage {
    /// The threshold size is 128K.
    ///
    /// Storing anything smaller in AWS S3 will still be billed as something that is 128K in size,
    /// so we store such values in DynamoDB intead, which only accepts values of 400K at most.
    const THRESHOLD_SIZE: u64 = 128 * 1024;

    /// Retrieve a value on AWS.
    pub async fn retrieve(&self, remote_ref: &RemoteRef) -> Result<AsyncAwsSource> {
        if remote_ref.ref_size() < Self::THRESHOLD_SIZE {
            self.dynamodb.retrieve(remote_ref).await.map(Into::into)
        } else {
            Ok(self.s3.retrieve(remote_ref).await.into())
        }
    }

    /// Store a value on disk.
    ///
    /// If the file already exists, it is assumed to exist and contain the expected value. As such,
    /// the function will return immediately, in success.
    pub async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: impl Into<AsyncSource<'_>>,
    ) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct AwsDynamoDbStorage {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
}

impl AwsDynamoDbStorage {
    const PK: &'static str = "remote_ref";

    /// Retrieve a value on DynamoDB.
    ///
    /// There is no streaming involved as values in DynamoDB are small by design.
    async fn retrieve(self: &Arc<Self>, remote_ref: &RemoteRef) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(Self::PK, AttributeValue::B(Blob::new(remote_ref.to_vec())))
            .send()
            .await?;

        unimplemented!();
    }
}

#[derive(Debug)]
struct AwsS3Storage {
    semaphore: Arc<Semaphore>,
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl AwsS3Storage {
    /// Retrieve a value on S3.
    async fn retrieve(self: &Arc<Self>, remote_ref: &RemoteRef) -> AsyncAwsS3Source {
        AsyncAwsS3Source {
            size: remote_ref.ref_size(),
            storage: Arc::clone(self),
            key: remote_ref.to_string(),
        }
    }
}

/// A source of data that comes from Aws.
pub enum AsyncAwsSource {
    /// The source comes from AWS DynamoDB.
    DynamoDb(Vec<u8>),
    /// The source comes from AWS S3.
    S3(AsyncAwsS3Source),
}

impl AsyncAwsSource {
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

    /// Transform the source in a buffer in memory, reading the entirety of the data first if
    /// necessary.
    pub async fn read_all_into_vec(self) -> std::io::Result<Vec<u8>> {
        unimplemented!()
    }

    /// Get an asynchronous reader from this `AsyncSource`.
    pub fn get_async_read(&self) -> BoxFuture<'_, std::io::Result<BoxAsyncRead<'_>>> {
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

impl From<Vec<u8>> for AsyncAwsSource {
    fn from(value: Vec<u8>) -> Self {
        Self::DynamoDb(value)
    }
}

impl From<AsyncAwsS3Source> for AsyncAwsSource {
    fn from(value: AsyncAwsS3Source) -> Self {
        Self::S3(value)
    }
}

/// An async source that retrieves its data on AWS S3.
#[derive(Debug)]
pub struct AsyncAwsS3Source {
    size: u64,
    storage: Arc<AwsS3Storage>,
    key: String,
}

impl AsyncAwsS3Source {
    async fn get_async_read(
        &self,
    ) -> std::io::Result<AsyncPermitRead<impl AsyncRead + Send + Unpin + 'static>> {
        let permit = self
            .storage
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "failed to acquire semaphore for reading `s3://{}/{}`: {err}",
                        self.storage.bucket, self.key,
                    ),
                )
            })?;

        let resp = self
            .storage
            .client
            .get_object()
            .bucket(&self.storage.bucket)
            .key(&self.key)
            .send()
            .await
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "failed to get S3 object from `s3://{}/{}`: {err}",
                        self.storage.bucket, self.key
                    ),
                )
            })?;

        Ok(AsyncPermitRead::new(
            permit,
            resp.body.into_async_read().compat(),
        ))
    }

    /// Get the size of the referenced data.
    pub fn size(&self) -> u64 {
        self.size
    }
}
