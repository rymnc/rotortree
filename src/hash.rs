pub type Hash = [u8; 32];

/// Trait for hashing N-ary groups of children into a parent node.
///
/// Implementations **must** incorporate [`Self::DOMAIN_SEPARATOR`] into
/// [`hash_children`](Hasher::hash_children) so that internal-node hashes
/// are domain-separated from raw leaf values.
pub trait Hasher: Clone + Send + Sync + 'static {
    /// A fixed 32-byte domain separator
    const DOMAIN_SEPARATOR: Hash;

    /// Hash 2..=N children into a parent.
    ///
    /// The input slice length is always in `[2, N]`.
    /// Implementations must prepend or otherwise incorporate
    /// [`Self::DOMAIN_SEPARATOR`] into the hash computation.
    fn hash_children(&self, children: &[Hash]) -> Hash;
}
