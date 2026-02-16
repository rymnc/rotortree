pub type Hash = [u8; 32];

/// Trait for hashing N-ary groups of children into a parent node.
pub trait Hasher: Clone + Send + Sync + 'static {
    /// Hash 2..=N children into a parent.
    ///
    /// The input slice length is always in `[2, N]`.
    fn hash_children(&self, children: &[Hash]) -> Hash;
}
