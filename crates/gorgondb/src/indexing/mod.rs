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
