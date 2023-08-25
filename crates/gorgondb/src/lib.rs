//! GorgonDB is the lower-layer database that powers MedusaDB.
//!
//! [Gorgons](`https://en.wikipedia.org/wiki/Gorgon`) are a mythological creature than turn their
//! victims to stone, leaving them forever in that immutable state.
//!
//! GorgonDB turns data into immutable blobs and generates [`identifiers`](`crate::Cairn`) for
//! them.
//!
//! # Deduplication
//!
//! One particular aspect of GorgonDB is that a given blob of data will - in the common case -
//! always yield the same identifier. This allows some interesting optimizations (for for files
//! that contain repeated segments or files which share the same subset of data) as well as
//! allowing high-duration - or even eternal - caching of data blobs.
//!
//! GorgonDB is not a standalone database server running somewhere in the cloud: it's an ecosystem
//! of various storage engines behind several, distributed, layers of in-memory and on-disk local
//! and remote caches.

pub(crate) mod buf_utils;
pub mod cairn;
pub mod fragmentation;
pub mod hash_algorithm;
pub mod remote_ref;

pub use cairn::Cairn;
pub use fragmentation::FragmentationMethod;
pub use hash_algorithm::HashAlgorithm;
pub use remote_ref::RemoteRef;
