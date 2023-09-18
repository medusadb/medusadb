use aws_config::SdkConfig;
use aws_sdk_dynamodb::{
    operation::{get_item::GetItemError, put_item::PutItemError},
    primitives::Blob,
    types::AttributeValue,
};
use aws_sdk_s3::primitives::SdkBody;
use http::Response;
use tracing::debug;

use crate::RemoteRef;

/// AWS errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A AWS DynamoDB get item callfailed.
    #[error("failed to execute GetItem")]
    GetItem(#[from] aws_sdk_dynamodb::error::SdkError<GetItemError, Response<SdkBody>>),

    /// A AWS DynamoDB put item callfailed.
    #[error("failed to execute PutItem")]
    PutItem(#[from] aws_sdk_dynamodb::error::SdkError<PutItemError, Response<SdkBody>>),

    /// The AWS DynamoDB item was not found...
    #[error("item was not found")]
    NotFound,

    /// The AWS DynamoDB item has an incorrect structure.
    #[error("data in DynamoDB is corrupted: {0}")]
    CorruptedData(String),

    /// An I/O error occured.
    #[error("I/O error")]
    Io(#[from] std::io::Error),
}

/// A convenience result type for AWS errors.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A storage that uses AWS DynamoDB.
#[derive(Debug, Clone)]
pub struct Storage {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
}

impl Storage {
    const PK: &'static str = "remote_ref";
    const DATA: &'static str = "data";

    /// Instantiate a new AWS DynamoDB storage.
    pub fn new(sdk_config: &SdkConfig, table_name: impl Into<String>) -> Self {
        let client = aws_sdk_dynamodb::Client::new(sdk_config);
        let table_name = table_name.into();

        Self { client, table_name }
    }

    /// Retrieve a value from DynamoDB.
    ///
    /// There is no streaming involved as values in DynamoDB are small by design.
    pub async fn retrieve(&self, remote_ref: &RemoteRef) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(Self::PK, AttributeValue::B(Blob::new(remote_ref.to_vec())))
            .send()
            .await?;

        if let Some(consumed_capacity) = resp.consumed_capacity() {
            debug!(
                "AWS DynamoDB: consumed {} read capacity units.",
                consumed_capacity.capacity_units().unwrap_or_default()
            );
        }

        let mut item = resp.item.ok_or_else(|| Error::NotFound)?;

        match item
            .remove(Self::DATA)
            .ok_or_else(|| Error::CorruptedData("missing data attribute".to_owned()))?
        {
            AttributeValue::B(blob) => Ok(blob.into_inner()),
            _ => Err(Error::CorruptedData(
                "data attribute is not binary".to_owned(),
            )),
        }
    }

    /// Store a value in DynamoDB.
    ///
    /// If the blob already exists, the call does nothing and succeeds immediately.
    pub async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: impl Into<crate::AsyncSource<'_>>,
    ) -> Result<()> {
        let attributes = [
            (Self::PK, remote_ref.to_vec()),
            (Self::DATA, source.into().read_all_into_vec().await?),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), AttributeValue::B(Blob::new(v))))
        .collect();

        match self
            .client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(attributes))
            .condition_expression("attribute_not_exists(#pk)")
            .expression_attribute_names("#pk", Self::PK)
            .send()
            .await
        {
            Ok(resp) => {
                if let Some(consumed_capacity) = resp.consumed_capacity() {
                    debug!(
                        "AWS DynamoDB: consumed {} write capacity units.",
                        consumed_capacity.capacity_units().unwrap_or_default()
                    );
                }

                Ok(())
            }
            Err(aws_sdk_dynamodb::error::SdkError::ServiceError(err))
                if err.err().is_conditional_check_failed_exception() =>
            {
                debug!("AWS DynamoDB key for `{remote_ref}` already exists: assuming it has the appropriate value.");

                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }
}
