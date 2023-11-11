//! Indexing facilities.

mod fixed_size;
mod tree;

pub use fixed_size::FixedSizeIndex;
pub use tree::{
    BinaryTreePath, BinaryTreePathElement, Tree, TreeBranch, TreePath, TreeSearchResult,
    TreeSearchStack,
};
