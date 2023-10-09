use std::borrow::Cow;

use aws_config::SdkConfig;
use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};
use tracing::debug;

use super::{Error, Result};
use crate::RemoteRef;

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
    ///
    /// If the value does not exist, `Ok(None)` is returned.
    pub async fn retrieve(&self, remote_ref: &RemoteRef) -> Result<Option<Vec<u8>>> {
        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(Self::PK, AttributeValue::B(Blob::new(remote_ref.to_vec())))
            .send()
            .await
            .map_err(Error::new)?;

        if let Some(consumed_capacity) = resp.consumed_capacity() {
            debug!(
                "AWS DynamoDB: consumed {} read capacity units.",
                consumed_capacity.capacity_units().unwrap_or_default()
            );
        }

        match resp.item {
            Some(mut item) => match item
                .remove(Self::DATA)
                .ok_or_else(|| Error::from_string("missing data attribute"))?
            {
                AttributeValue::B(blob) => Ok(Some(blob.into_inner())),
                _ => Err(Error::from_string("data attribute is not binary")),
            },
            None => Ok(None),
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
            (
                Self::DATA,
                match source
                    .into()
                    .read_all_into_memory()
                    .await
                    .map_err(Error::new)?
                {
                    Cow::Owned(b) => b,
                    Cow::Borrowed(s) => s.to_vec(),
                },
            ),
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
            Err(err) => Err(Error::new(err)),
        }
    }
}
