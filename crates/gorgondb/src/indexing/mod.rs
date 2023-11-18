//! Indexing facilities.

mod fixed_size;
mod tree;

pub use fixed_size::FixedSizeIndex;
pub use tree::{
    BinaryTreePath, BinaryTreePathElement, Tree, TreeBranch, TreePath, TreeSearchResult,
    TreeSearchStack,
};

/// An error type.
#[derive(Debug, thiserror::Error)]
pub enum Error<KeyElem> {
    /// An error happened at the transaction level.
    #[error("transaction: {0}")]
    TransactionError(#[from] crate::gorgon::Error),

    /// The tree is corrupted.
    #[error("tree is corrupted at `{path}`: {err}")]
    CorruptedTree {
        path: TreePath<KeyElem>,
        err: String,
    },
}

#[cfg(test)]
pub(super) mod tests {
    macro_rules! blob_id {
        ($data:literal) => {
            BlobId::self_contained($data.as_bytes()).unwrap()
        };
    }

    macro_rules! assert_empty {
        ($index:ident) => {
            tracing::debug!("assert_empty()");

            assert!($index.is_empty());
            assert_eq!($index.len(), 0);
        };
    }

    macro_rules! assert_len {
        ($index:ident, $len:expr) => {
            tracing::debug!("assert_len({})", $len);

            assert_eq!($index.len(), $len);
        };
    }

    macro_rules! assert_total_size {
        ($index:ident, $total_size:expr) => {
            tracing::debug!("assert_total_size({})", $total_size);

            assert_eq!($index.total_size(), $total_size);
        };
    }

    macro_rules! assert_key_missing {
        ($index:ident, $key:expr) => {
            tracing::debug!("assert_key_missing({})", $key);

            assert!($index.get(&$key).await.unwrap().is_none());
        };
    }

    macro_rules! assert_key_exists {
        ($index:ident, $key:expr, $data:literal) => {
            tracing::debug!("assert_key_exists({}, {:?})", $key, $data);

            assert_eq!($index.get(&$key).await.unwrap(), Some(blob_id!($data)));
        };
    }

    macro_rules! assert_insert_new {
        ($index:ident, $key:expr, $data:literal) => {
            tracing::debug!("assert_insert_new({}, {:?})", $key, $data);

            let blob_id = blob_id!($data);
            assert_eq!($index.insert(&$key, blob_id).await.unwrap(), None);
        };
    }

    macro_rules! assert_insert_update {
        ($index:ident, $key:expr, $data:literal, $old_data:literal) => {
            tracing::debug!(
                "assert_insert_update({}, {:?}, {:?})",
                $key,
                $data,
                $old_data
            );

            let blob_id = blob_id!($data);
            let old_blob_id = blob_id!($old_data);
            assert_eq!(
                $index.insert(&$key, blob_id).await.unwrap(),
                Some(old_blob_id)
            );
        };
    }

    macro_rules! assert_remove_missing {
        ($index:ident, $key:expr) => {
            tracing::debug!("assert_remove_missing({})", $key);

            assert!($index.remove(&$key).await.unwrap().is_none());
        };
    }

    macro_rules! assert_remove_exists {
        ($index:ident, $key:expr, $old_data:literal) => {
            tracing::debug!("assert_remove_exists({}, {})", $key, $old_data);

            let old_blob_id = blob_id!($old_data);
            assert_eq!($index.remove(&$key).await.unwrap(), Some(old_blob_id));
        };
    }

    macro_rules! assert_transaction_empty {
        ($tx:ident) => {
            let blobs: std::collections::HashSet<_> = $tx.get_blobs().await.collect();

            assert_eq!(blobs, Default::default());
        };
    }

    pub(super) use {
        assert_empty, assert_insert_new, assert_insert_update, assert_key_exists,
        assert_key_missing, assert_len, assert_remove_exists, assert_remove_missing,
        assert_total_size, assert_transaction_empty, blob_id,
    };
}
