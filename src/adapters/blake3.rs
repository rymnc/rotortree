use crate::{
    Hash,
    Hasher,
};

/// Blake3Hasher newtype
#[derive(Debug, Clone, Copy, Default)]
pub struct Blake3Hasher;

impl Blake3Hasher {
    pub fn new() -> Self {
        Self
    }
}

impl Hasher for Blake3Hasher {
    const DOMAIN_SEPARATOR: Hash = *b"rotortree-internal-v1\0\0\0\0\0\0\0\0\0\0\0";

    #[inline]
    fn hash_children(&self, children: &[Hash]) -> Hash {
        let mut hasher = ::blake3::Hasher::new();
        hasher.update(&Self::DOMAIN_SEPARATOR);
        hasher.update(children.as_flattened());
        *hasher.finalize().as_bytes()
    }
}
