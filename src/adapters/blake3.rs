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
    fn hash_children(&self, children: &[Hash]) -> Hash {
        let mut hasher = ::blake3::Hasher::new();
        for child in children {
            hasher.update(child);
        }
        *hasher.finalize().as_bytes()
    }
}
