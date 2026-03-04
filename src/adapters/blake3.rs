use crate::{
    Hash,
    HashState,
    Hasher,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct Blake3Hasher;

impl Blake3Hasher {
    pub fn new() -> Self {
        Self
    }
}

impl HashState for blake3::Hasher {
    #[inline]
    fn update(&mut self, data: &[u8]) {
        blake3::Hasher::update(self, data);
    }

    #[inline]
    fn finalize(self) -> Hash {
        *blake3::Hasher::finalize(&self).as_bytes()
    }
}

impl Hasher for Blake3Hasher {
    type State = blake3::Hasher;

    #[inline]
    fn new_state(&self) -> Self::State {
        blake3::Hasher::new()
    }
}
