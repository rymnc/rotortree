pub type Hash = [u8; 32];

/// Streaming hash state
pub trait HashState {
    /// Feed bytes into the state
    fn update(&mut self, data: &[u8]);

    /// Finalize and return the digest
    fn finalize(self) -> Hash;
}

/// Raw hash primitive supporting streaming
pub trait Hasher: Clone + Send + Sync + 'static {
    type State: HashState;

    /// Create a fresh hashing state
    fn new_state(&self) -> Self::State;
}

/// Domain-separated hasher for Merkle tree operations.
///
/// Wraps any [`Hasher`] and enforces leaf/internal-node domain
///
/// - Leaves are hashed with a `0x00` prefix tag
/// - Internal nodes are hashed with a `0x01` prefix tag + child count
#[derive(Clone, Debug)]
pub struct TreeHasher<H>(H);

impl<H: Hasher> TreeHasher<H> {
    #[inline(always)]
    pub fn new(hasher: H) -> Self {
        Self(hasher)
    }

    pub fn inner(&self) -> &H {
        &self.0
    }

    #[inline(always)]
    pub fn hash_leaf(&self, leaf: &Hash) -> Hash {
        let mut buf = [0u8; 33];
        buf[1..].copy_from_slice(leaf);
        let mut state = self.0.new_state();
        state.update(&buf);
        state.finalize()
    }

    #[inline(always)]
    pub fn hash_children(&self, children: &[Hash]) -> Hash {
        let len = children.len();
        debug_assert!(len <= u8::MAX as usize);

        let mut state = self.0.new_state();
        #[allow(clippy::cast_possible_truncation)]
        state.update(&[0x01, len as u8]);
        state.update(children.as_flattened());
        state.finalize()
    }
}
