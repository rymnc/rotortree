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
    #[inline]
    fn hash_children(&self, children: &[Hash]) -> Hash {
        *::blake3::hash(children.as_flattened()).as_bytes()
    }
}
