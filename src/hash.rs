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

    /// Hash the concatenation of child node hashes into their parent.
    ///
    /// Children are hashed verbatim, with no domain tag and no length
    /// prefix, so an internal node is `hash(child_0 || child_1 || ...)`.
    /// This matches the canonical Lean IMT node hash and keeps the
    /// digest compatible with other implementations.
    #[inline]
    fn hash_children(&self, children: &[Hash]) -> Hash {
        let mut state = self.new_state();
        state.update(children.as_flattened());
        state.finalize()
    }
}
