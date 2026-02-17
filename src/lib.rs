#![cfg_attr(feature = "docs", doc = include_utils::include_md!("README.md:intro"))]
#![cfg_attr(feature = "docs", doc = include_utils::include_md!("README.md:design"))]
#![cfg_attr(feature = "docs", doc = include_utils::include_md!("README.md:usage"))]
#![cfg_attr(feature = "docs", doc = include_utils::include_md!("README.md:devnote"))]
#![cfg_attr(not(test), deny(clippy::cast_possible_truncation))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(unused_crate_dependencies)]
#![deny(warnings)]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(not(feature = "std"))))]
extern crate alloc;

// only used in benches
#[cfg(test)]
use {
    crabtime as _,
    divan as _,
    proptest as _,
    tempfile as _,
};

mod error;
mod hash;
mod proof;
mod tree;

pub mod adapters;

#[cfg(feature = "storage")]
#[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
pub mod storage;

pub use error::TreeError;
pub use hash::{
    Hash,
    Hasher,
};
pub use proof::{
    NaryProof,
    ProofLevel,
};
pub use tree::{
    LeanIMT,
    TreeSnapshot,
};

#[cfg(feature = "blake3")]
#[cfg_attr(docsrs, doc(cfg(feature = "blake3")))]
pub use adapters::blake3::Blake3Hasher;

#[cfg(feature = "storage")]
#[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
pub use storage::{
    DurabilityToken,
    FlushPolicy,
    RotorTree,
    RotorTreeConfig,
    RotorTreeError,
    StorageError,
};
