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
    fn hash_bytes(&self, data: &[u8]) -> Hash {
        *::blake3::hash(data).as_bytes()
    }
}
