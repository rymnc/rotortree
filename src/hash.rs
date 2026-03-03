#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

pub type Hash = [u8; 32];

/// Trait for raw hashing of arbitrary bytes into a 32-byte digest.
///
/// Implementations provide only the raw hash primitive.
/// Domain separation (leaf vs internal node) is enforced by
/// [`TreeHasher`], which wraps any `Hasher`
pub trait Hasher: Clone + Send + Sync + 'static {
    /// Hash arbitrary bytes into a 32-byte digest
    fn hash_bytes(&self, data: &[u8]) -> Hash;
}

/// Domain-separated hasher for Merkle tree operations.
///
/// Wraps any [`Hasher`] and enforces leaf/internal-node domain
///
/// - Leaves are hashed with a `0x00` prefix tag.
/// - Internal nodes are hashed with a `0x01` prefix tag + child count.
#[derive(Clone, Debug)]
pub struct TreeHasher<H>(H);

impl<H: Hasher> TreeHasher<H> {
    /// Wrap a raw hasher in domain-separated tree hashing.
    pub fn new(hasher: H) -> Self {
        Self(hasher)
    }

    /// Access the inner raw hasher.
    pub fn inner(&self) -> &H {
        &self.0
    }

    /// Hash a leaf value with `0x00` domain tag.
    ///
    /// Output is provably disjoint from [`hash_children`](Self::hash_children)
    /// (which uses `0x01`), preventing second-preimage attacks.
    #[inline]
    pub fn hash_leaf(&self, leaf: &Hash) -> Hash {
        let mut buf = [0u8; 1 + 32];
        buf[0] = 0x00;
        buf[1..].copy_from_slice(leaf);
        self.0.hash_bytes(&buf)
    }

    /// Hash 2..=N children with `0x01` domain tag and child count.
    #[inline]
    pub fn hash_children(&self, children: &[Hash]) -> Hash {
        debug_assert!(children.len() <= u8::MAX as usize);
        let mut buf = Vec::with_capacity(2 + children.len() * 32);
        buf.push(0x01);
        #[allow(clippy::cast_possible_truncation)]
        buf.push(children.len() as u8);
        buf.extend_from_slice(children.as_flattened());
        self.0.hash_bytes(&buf)
    }
}
