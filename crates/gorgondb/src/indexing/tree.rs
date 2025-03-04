//! Tree-type structure for indexes.

use std::{fmt::Display, num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::BlobId;

use super::Error;

/// A trait for types that can be serialized using MessagePack.
pub trait ByteSerialize: Serialize {
    /// Serialize the tree as a vector of bytes.
    fn to_vec(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).expect("serialization should never fail")
    }
}

/// A trait for types that can be deserialized using MessagePack.
pub trait ByteDeserialize: for<'a> Deserialize<'a> {
    /// Deserialize the tree from a slice of bytes.
    fn from_slice(input: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(input)
    }
}

/// A tree branch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeBranch<KeyElem, Meta> {
    pub(crate) meta: Meta,
    pub(crate) children: Vec<TreeItem<KeyElem>>,
}

impl<KeyElem, Meta: Default> Default for TreeBranch<KeyElem, Meta> {
    fn default() -> Self {
        Self {
            meta: Default::default(),
            children: Default::default(),
        }
    }
}

impl<KeyElem: DeserializeOwned, Meta: DeserializeOwned> ByteDeserialize
    for TreeBranch<KeyElem, Meta>
{
}

impl<KeyElem: Serialize, Meta: Serialize> ByteSerialize for TreeBranch<KeyElem, Meta> {}

impl<KeyElem: Ord, Meta> TreeBranch<KeyElem, Meta> {
    /// Create a new tree branch with no children.
    pub fn new(meta: Meta) -> Self {
        Self {
            meta,
            children: Default::default(),
        }
    }

    /// Create a new tree branch with a single child.
    pub fn new_with_single_child(meta: Meta, key: KeyElem, id: BlobId) -> Self {
        Self {
            meta,
            children: vec![TreeItem(key, id)],
        }
    }

    /// Find the occupied entry for the children with the specified key.
    pub(crate) fn get(&self, key: &KeyElem) -> Option<&TreeItem<KeyElem>> {
        self.children
            .binary_search_by(|item| item.0.cmp(key))
            .ok()
            .and_then(|idx| self.children.get(idx))
    }

    /// Find the entry for the children with the specified key.
    pub fn entry(&mut self, key: KeyElem) -> TreeBranchEntry<'_, KeyElem, Meta> {
        match self.children.binary_search_by(|item| item.0.cmp(&key)) {
            Ok(idx) => TreeBranchEntry::Occupied(TreeBranchOccupiedEntry { branch: self, idx }),
            Err(idx) => TreeBranchEntry::Vacant(TreeBranchVacantEntry {
                branch: self,
                idx,
                key,
            }),
        }
    }

    /// Find the occupied entry for the children with the specified key.
    pub fn occupied_entry(&mut self, key: &KeyElem) -> TreeBranchOccupiedEntry<'_, KeyElem, Meta> {
        let idx = self
            .children
            .binary_search_by(|item| item.0.cmp(key))
            .expect("the entry should exist");

        TreeBranchOccupiedEntry { branch: self, idx }
    }

    /// Replace a child into the branch, that is guaranteed to already be present.
    ///
    /// Returns the previous value.
    pub fn replace_existing(&mut self, key: &KeyElem, id: BlobId) -> BlobId {
        self.occupied_entry(key).replace(id)
    }

    /// Remove a child from the branch, that is guaranteed to already be present.
    ///
    /// Returns the previous value.
    pub fn remove_existing(&mut self, key: &KeyElem) -> BlobId {
        self.occupied_entry(key).remove()
    }

    /// Insert a child into the branch, that is guaranteed to not exist yet.
    pub fn insert_non_existing(&mut self, key: KeyElem, id: BlobId) {
        match self.entry(key) {
            TreeBranchEntry::Occupied(_) => unreachable!("key should not exist"),
            TreeBranchEntry::Vacant(entry) => entry.insert(id),
        };
    }
}

#[derive(Debug)]
pub enum TreeBranchEntry<'b, KeyElem, Meta> {
    /// The result was found.
    Occupied(TreeBranchOccupiedEntry<'b, KeyElem, Meta>),

    /// The result was not found.
    Vacant(TreeBranchVacantEntry<'b, KeyElem, Meta>),
}

#[derive(Debug)]
pub struct TreeBranchOccupiedEntry<'b, KeyElem, Meta> {
    branch: &'b mut TreeBranch<KeyElem, Meta>,
    idx: usize,
}

impl<KeyElem, Meta> TreeBranchOccupiedEntry<'_, KeyElem, Meta> {
    /// Get the value associated to the entry.
    pub fn value(&self) -> &BlobId {
        &self
            .branch
            .children
            .get(self.idx)
            .expect("index should be valid")
            .1
    }

    /// Replace the value in the occupied entry, returning the previous one.
    pub fn replace(&mut self, mut id: BlobId) -> BlobId {
        let new_id = &mut self
            .branch
            .children
            .get_mut(self.idx)
            .expect("index should be valid")
            .1;

        std::mem::swap(new_id, &mut id);

        id
    }

    /// Remove the value in the occupied entry, returning the it.
    pub fn remove(&mut self) -> BlobId {
        self.branch.children.remove(self.idx).1
    }
}

#[derive(Debug)]
pub struct TreeBranchVacantEntry<'b, KeyElem, Meta> {
    branch: &'b mut TreeBranch<KeyElem, Meta>,
    idx: usize,
    key: KeyElem,
}

impl<KeyElem, Meta> TreeBranchVacantEntry<'_, KeyElem, Meta> {
    /// Insert a value in the vacant entry.
    pub fn insert(self, id: BlobId) {
        self.branch
            .children
            .insert(self.idx, TreeItem(self.key, id));
    }
}

/// A tree item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TreeItem<KeyElem>(pub(crate) KeyElem, pub(crate) BlobId);

/// A trait for key elements that have a size.
pub trait SizedKeyElement {
    /// Get the size of the key element, in bytes.
    fn size(&self) -> NonZeroU64;
}

/// A path in a tree.
#[derive(Debug, Clone)]
pub struct TreePath<KeyElem>(Vec<KeyElem>);

impl<T> Default for TreePath<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<KeyElem: Display> Display for TreePath<KeyElem> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            std::iter::once("#".to_string())
                .chain(self.0.iter().map(|elem| elem.to_string()))
                .join(".")
        )
    }
}

impl<KeyElem> TreePath<KeyElem> {
    /// Get the root path.
    pub fn root() -> Self {
        Self(Vec::default())
    }

    /// Push a new element to the path.
    pub fn push(&mut self, path: impl Into<KeyElem>) {
        self.0.push(path.into());
    }

    /// Pop the last element from the path, if there is one, and returns it.
    pub fn pop(&mut self) -> Option<KeyElem> {
        self.0.pop()
    }
}

impl<KeyElem: SizedKeyElement> TreePath<KeyElem> {
    pub fn size(&self) -> u64 {
        self.0
            .iter()
            .map(|item| item.size().get())
            .reduce(|acc, v| acc + v)
            .unwrap_or_default()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TreePathElementError {
    #[error("the path element cannot be empty")]
    Empty,
}

/// A tree path element that has a binary value with a striclty positive size.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BinaryTreePathElement(#[serde(with = "serde_binary_tree_path_elem")] pub Bytes);

mod serde_binary_tree_path_elem {
    use bytes::Bytes;
    use serde::{Deserializer, Serialize, Serializer, de::Error};
    use serde_bytes::Deserialize;

    use super::TreePathElementError;

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

        let b: Bytes = b.to_vec().into();

        if b.is_empty() {
            Err(D::Error::custom(TreePathElementError::Empty))
        } else {
            Ok(b)
        }
    }
}

impl Display for BinaryTreePathElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl From<u8> for BinaryTreePathElement {
    fn from(value: u8) -> Self {
        Self(vec![value].into())
    }
}

impl From<Vec<u8>> for BinaryTreePathElement {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into())
    }
}

impl TryFrom<Bytes> for BinaryTreePathElement {
    type Error = TreePathElementError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(TreePathElementError::Empty)
        } else {
            Ok(Self(value))
        }
    }
}

impl From<BinaryTreePathElement> for Bytes {
    fn from(value: BinaryTreePathElement) -> Self {
        value.0
    }
}

impl SizedKeyElement for BinaryTreePathElement {
    /// Get the size of the binary tree path element.
    fn size(&self) -> NonZeroU64 {
        NonZeroU64::new(
            self.0
                .len()
                .try_into()
                .expect("could not convert usize to u64"),
        )
        .expect("binary tree path element should never be empty")
    }
}

impl BinaryTreePathElement {
    /// Check wether the binary path element has the specified prefix.
    pub fn has_prefix(&self, other: &Self) -> bool {
        if other.0.len() > self.0.len() {
            return false;
        }

        self.0[0..other.0.len()] == other.0
    }
}

/// A search stack.
#[derive(Debug, Clone)]
pub struct TreeSearchStack<KeyElem, Meta>(Vec<(Arc<TreeBranch<KeyElem, Meta>>, KeyElem)>);

impl<KeyElem, Meta> Default for TreeSearchStack<KeyElem, Meta> {
    fn default() -> Self {
        Self(Default::default())
    }
}

/// A search stack result.
pub type TreeSearchStackResult<T, KeyElem> = Result<T, Error<KeyElem>>;
pub type TreeSearchStackItem<KeyElem, Meta> = (Arc<TreeBranch<KeyElem, Meta>>, KeyElem);

impl<KeyElem, Meta> TreeSearchStack<KeyElem, Meta> {
    /// Instantiate a new stack starting from the specified node.
    pub fn new(root: Arc<TreeBranch<KeyElem, Meta>>, child_key: KeyElem) -> Self {
        Self(vec![(root, child_key)])
    }

    /// Push a new item to the stack.
    pub fn push(
        &mut self,
        branch: Arc<TreeBranch<KeyElem, Meta>>,
        elem: impl Into<KeyElem>,
    ) -> &mut Self {
        self.0.push((branch, elem.into()));

        self
    }

    /// Pop an item from the stack.
    pub fn pop(&mut self) -> Option<TreeSearchStackItem<KeyElem, Meta>> {
        self.0.pop()
    }

    /// Pop an item from the stack, returning an error is the stack is empty.
    pub fn pop_non_empty(
        mut self,
    ) -> TreeSearchStackResult<(Self, TreeSearchStackItem<KeyElem, Meta>), KeyElem> {
        match self.pop() {
            Some(v) => Ok((self, v)),
            None => Err(super::Error::CorruptedTree {
                path: self.into_path(),
                err: "stack should not be empty at that point".to_owned(),
            }),
        }
    }

    /// Convert the stack into a path.
    pub fn into_path(self) -> TreePath<KeyElem> {
        TreePath(self.0.into_iter().map(|(_, k)| k).collect())
    }
}

impl<KeyElem, Meta> Iterator for TreeSearchStack<KeyElem, Meta> {
    type Item = TreeSearchStackItem<KeyElem, Meta>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop()
    }
}

impl<KeyElem: Ord, Meta> TreeSearchStack<KeyElem, Meta> {
    /// Get the top occupied entry.
    pub fn top_value(&self) -> Option<&BlobId> {
        match self.0.last() {
            None => None,
            Some(top) => top.0.get(&top.1).map(|item| &item.1),
        }
    }
}

impl<KeyElem: Clone, Meta> TreeSearchStack<KeyElem, Meta> {
    /// Convert the stack into a path.
    pub fn as_path(&self) -> TreePath<KeyElem> {
        TreePath(self.0.iter().map(|(_, k)| k).cloned().collect())
    }
}

/// A tree search result.
#[derive(Debug)]
pub enum TreeSearchResult<KeyElem, Meta> {
    /// The value was found in the tree, at the specified path.
    Found {
        /// The stack that led to the found entry.
        stack: TreeSearchStack<KeyElem, Meta>,
    },
    Missing {
        /// The stack up to the missing entry.
        stack: TreeSearchStack<KeyElem, Meta>,

        /// The next key, if there is one.
        next_key: Option<KeyElem>,
    },
}

/// Represents a tree difference.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TreeDiff<Key> {
    /// The key is only present on the left tree.
    LeftOnly { key: Key, value: BlobId },

    /// The key is only present on the right tree.
    RightOnly { key: Key, value: BlobId },

    /// The key exists in both trees with a different value.
    Diff {
        key: Key,
        left: BlobId,
        right: BlobId,
    },
}
