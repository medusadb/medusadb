//! Tree-type structure for indexes.

use std::fmt::Display;

use bytes::Bytes;
use itertools::Itertools;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::BlobId;

/// A tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Tree<Meta> {
    /// A branch.
    Branch(TreeBranch<Meta>),

    /// A leaf.
    Leaf(BlobId),
}

impl<Meta: Default> Default for Tree<Meta> {
    fn default() -> Self {
        Self::Branch(TreeBranch::default())
    }
}

impl<Meta: DeserializeOwned> Tree<Meta> {
    /// Deserialize the tree from a slice of bytes.
    pub fn from_slice(input: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(input)
    }
}

impl<Meta: Serialize> Tree<Meta> {
    /// Serialize the tree as a vector of bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).expect("serialization should never fail")
    }
}

/// A tree branch.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TreeBranch<Meta> {
    pub(crate) meta: Meta,

    children: Vec<TreeItem>,
}

impl<Meta: DeserializeOwned> TreeBranch<Meta> {
    /// Deserialize the tree from a slice of bytes.
    pub fn from_slice(input: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(input)
    }
}

impl<Meta: Serialize> TreeBranch<Meta> {
    /// Serialize the tree as a vector of bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).expect("serialization should never fail")
    }
}

impl<Meta> TreeBranch<Meta> {
    /// Find the children with the specified key.
    ///
    /// If the children is not found, an error is returned containing the index at which it should
    /// have been found.
    pub fn entry(&'_ mut self, key: Bytes) -> TreeBranchEntry<'_, Meta> {
        match self.children.binary_search_by(|item| item.0.cmp(&key)) {
            Ok(idx) => TreeBranchEntry::Occupied(TreeBranchOccupiedEntry {
                branch: self,
                idx,
                key,
            }),
            Err(idx) => TreeBranchEntry::Vacant(TreeBranchVacantEntry {
                branch: self,
                idx,
                key,
            }),
        }
    }
}

#[derive(Debug)]
pub enum TreeBranchEntry<'b, Meta> {
    /// The result was found.
    Occupied(TreeBranchOccupiedEntry<'b, Meta>),

    /// The result was not found.
    Vacant(TreeBranchVacantEntry<'b, Meta>),
}

#[derive(Debug)]
pub struct TreeBranchOccupiedEntry<'b, Meta> {
    branch: &'b mut TreeBranch<Meta>,
    idx: usize,
    key: Bytes,
}

impl<'b, Meta> TreeBranchOccupiedEntry<'b, Meta> {
    /// Transform the entry into its key.
    pub fn into_key(self) -> Bytes {
        self.key
    }

    /// Get the value associated to the entry.
    pub fn get(&self) -> &BlobId {
        &self
            .branch
            .children
            .get(self.idx)
            .expect("index should be valid")
            .1
    }

    /// Insert a new value in the occupied entry, returning the previous one.
    pub fn insert(self, mut id: BlobId) -> BlobId {
        let new_id = &mut self
            .branch
            .children
            .get_mut(self.idx)
            .expect("index should be valid")
            .1;

        std::mem::swap(new_id, &mut id);

        id
    }

    /// Remove the value.
    pub fn remove(self) -> (Bytes, BlobId) {
        let TreeItem(key, id) = self.branch.children.remove(self.idx);

        (key, id)
    }
}

#[derive(Debug)]
pub struct TreeBranchVacantEntry<'b, Meta> {
    branch: &'b mut TreeBranch<Meta>,
    idx: usize,
    key: Bytes,
}

impl<'b, Meta> TreeBranchVacantEntry<'b, Meta> {
    /// Turn the entry into its contained key.
    pub fn into_key(self) -> Bytes {
        self.key
    }

    /// Insert a value in the vacant entry.
    pub fn insert(self, id: BlobId) {
        self.branch
            .children
            .insert(self.idx, TreeItem(self.key, id))
    }
}

/// A tree item.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TreeItem(#[serde(with = "serde_tree_item_key")] Bytes, BlobId);

mod serde_tree_item_key {
    use bytes::Bytes;
    use serde::{Deserializer, Serialize, Serializer};
    use serde_bytes::Deserialize;

    pub fn serialize<S>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::Bytes::new(value).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b = serde_bytes::ByteBuf::deserialize(deserializer)?;

        Ok(b.to_vec().into())
    }
}

/// A path in a tree.
#[derive(Debug, Clone)]
pub struct TreePath<Key>(Vec<Key>);

impl<T> Default for TreePath<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

/// An alias for a binary tree path.
pub type BinaryTreePath = TreePath<BinaryTreePathElement>;

impl<Elem: Display> Display for TreePath<Elem> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            std::iter::once("$".to_string())
                .chain(self.0.iter().map(|elem| elem.to_string()))
                .join(".")
        )
    }
}

impl<Elem> TreePath<Elem> {
    /// Get the root path.
    pub fn root() -> Self {
        Self(Vec::default())
    }

    /// Push a new element to the path.
    pub fn push(&mut self, path: impl Into<Elem>) {
        self.0.push(path.into());
    }
}

/// A tree path element that has a binary value.
#[derive(Debug, Clone)]
pub struct BinaryTreePathElement(pub Bytes);

impl Display for BinaryTreePathElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl From<Bytes> for BinaryTreePathElement {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<BinaryTreePathElement> for Bytes {
    fn from(value: BinaryTreePathElement) -> Self {
        value.0
    }
}

/// A tree search result.
#[derive(Debug)]
pub enum TreeSearchResult<Elem, Meta> {
    /// The value was found in the tree, at the specified path.
    Found {
        /// The path of the found entry.
        path: TreePath<Elem>,

        /// The last parent.
        parent: TreeBranch<Meta>,

        /// The blob ID of the found entry.
        id: BlobId,
    },
    Missing {
        /// The path of the last existing parent.
        parent_path: TreePath<Elem>,

        /// The last parent.
        parent: TreeBranch<Meta>,

        /// The local key that was missing.
        local_key: Elem,

        /// The next key.
        next_key: Elem,
    },
}
