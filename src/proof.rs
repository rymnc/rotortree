use crate::{
    Hash,
    Hasher,
    TreeError,
    tree::TreeSnapshot,
};

/// One level of an N-ary Merkle proof.
///
/// Uses a fixed-size `[Hash; N]` array (at most `N-1` siblings
/// are valid, indicated by `sibling_count`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(wincode::SchemaWrite, wincode::SchemaRead))]
pub struct ProofLevel<const N: usize> {
    /// Child position within the group (`0..N-1`)
    pub position: u8,
    /// Number of valid siblings in `siblings` (0 for lifted)
    pub sibling_count: u8,
    /// Sibling hashes
    pub siblings: [Hash; N],
}

impl<const N: usize> ProofLevel<N> {
    const EMPTY: Self = Self {
        position: 0,
        sibling_count: 0,
        siblings: [[0u8; 32]; N],
    };
}

/// An N-ary Merkle inclusion proof
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(wincode::SchemaWrite, wincode::SchemaRead))]
pub struct NaryProof<const N: usize, const MAX_DEPTH: usize> {
    /// The Merkle root this proof verifies against
    pub root: Hash,
    /// The leaf hash being proved
    pub leaf: Hash,
    /// The 0-based index of the leaf
    pub leaf_index: u64,
    /// Number of valid levels in `levels`
    pub level_count: usize,
    /// Proof levels from leaf to root
    pub levels: [ProofLevel<N>; MAX_DEPTH],
}

impl<const N: usize, const MAX_DEPTH: usize> NaryProof<N, MAX_DEPTH> {
    /// Verify this proof against the given hasher.
    pub fn verify<H: Hasher>(&self, hasher: &H) -> Result<bool, TreeError> {
        if self.level_count > MAX_DEPTH {
            return Err(TreeError::MathError);
        }

        let mut current = self.leaf;

        for level in &self.levels[..self.level_count] {
            if level.sibling_count == 0 {
                continue;
            }

            let total = (level.sibling_count as usize) + 1;
            let pos = level.position as usize;
            if total > N || pos >= total {
                return Err(TreeError::MathError);
            }
            let mut children = [[0u8; 32]; N];
            children[..pos].copy_from_slice(&level.siblings[..pos]);
            children[pos] = current;
            let rest = total - pos - 1;
            children[pos + 1..total].copy_from_slice(&level.siblings[pos..pos + rest]);
            current = hasher.hash_children(&children[..total]);
        }

        Ok(current == self.root)
    }
}

impl<const N: usize, const MAX_DEPTH: usize> TreeSnapshot<N, MAX_DEPTH> {
    /// Generate an inclusion proof for the leaf at `leaf_index`
    pub fn generate_proof(
        &self,
        leaf_index: u64,
    ) -> Result<NaryProof<N, MAX_DEPTH>, TreeError> {
        if leaf_index >= self.size {
            return Err(TreeError::IndexOutOfRange {
                index: leaf_index,
                size: self.size,
            });
        }

        let idx = usize::try_from(leaf_index).map_err(|_| TreeError::CapacityExceeded)?;
        let mut index = idx;
        let mut levels = [ProofLevel::<N>::EMPTY; MAX_DEPTH];

        #[allow(clippy::needless_range_loop)]
        for level in 0..self.depth {
            let child_pos = index % N;
            let group_start = index - child_pos;
            let group_end = core::cmp::min(group_start + N, self.levels[level].len());
            let group_size = group_end - group_start;

            if group_size == 1 {
                levels[level] = ProofLevel {
                    position: 0,
                    sibling_count: 0,
                    siblings: [[0u8; 32]; N],
                };
            } else {
                let mut group = [[0u8; 32]; N];
                self.levels[level].get_group(group_start, group_size, &mut group);
                let mut siblings = [[0u8; 32]; N];
                let mut sib_idx = 0usize;
                for i in 0..group_size {
                    if i != child_pos {
                        siblings[sib_idx] = group[i];
                        sib_idx += 1;
                    }
                }
                levels[level] = ProofLevel {
                    position: u8::try_from(child_pos)
                        .map_err(|_| TreeError::MathError)?,
                    sibling_count: u8::try_from(sib_idx)
                        .map_err(|_| TreeError::MathError)?,
                    siblings,
                };
            }

            index /= N;
        }

        Ok(NaryProof {
            root: self.root.expect("set prev; qed"),
            leaf: self.levels[0].get(idx)?,
            leaf_index,
            level_count: self.depth,
            levels,
        })
    }
}

#[cfg(test)]
#[cfg_attr(feature = "concurrent", allow(unused_mut))]
mod tests {
    #[cfg(not(feature = "std"))]
    use alloc::vec::Vec;
    #[cfg(feature = "std")]
    use std::vec::Vec;

    use super::*;
    use crate::LeanIMT;

    #[derive(Clone)]
    struct XorHasher;

    impl crate::Hasher for XorHasher {
        fn hash_children(&self, children: &[Hash]) -> Hash {
            let mut result = [0u8; 32];
            for child in children {
                for (r, c) in result.iter_mut().zip(child.iter()) {
                    *r ^= c;
                }
            }
            result
        }
    }

    fn leaf(n: u8) -> Hash {
        let mut h = [0u8; 32];
        h[0] = n;
        h
    }

    #[test]
    fn proof_single_leaf() {
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        let l = leaf(1);
        tree.insert(l).unwrap();
        let snap = tree.snapshot();

        let proof = snap.generate_proof(0).unwrap();
        assert_eq!(proof.leaf, l);
        assert_eq!(proof.root, l);
        assert_eq!(proof.leaf_index, 0);
        assert_eq!(proof.level_count, 0); // depth=0
        assert!(proof.verify(&XorHasher).unwrap());
    }

    #[test]
    fn proof_two_leaves_binary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        let l0 = leaf(1);
        let l1 = leaf(2);
        tree.insert(l0).unwrap();
        tree.insert(l1).unwrap();
        let snap = tree.snapshot();

        let p0 = snap.generate_proof(0).unwrap();
        assert_eq!(p0.leaf, l0);
        assert_eq!(p0.level_count, 1);
        assert_eq!(p0.levels[0].position, 0);
        assert_eq!(p0.levels[0].sibling_count, 1);
        assert_eq!(p0.levels[0].siblings[0], l1);
        assert!(p0.verify(&h).unwrap());

        let p1 = snap.generate_proof(1).unwrap();
        assert_eq!(p1.leaf, l1);
        assert_eq!(p1.levels[0].position, 1);
        assert_eq!(p1.levels[0].sibling_count, 1);
        assert_eq!(p1.levels[0].siblings[0], l0);
        assert!(p1.verify(&h).unwrap());
    }

    #[test]
    fn proof_three_leaves_binary_lifted() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        let l0 = leaf(1);
        let l1 = leaf(2);
        let l2 = leaf(3);
        tree.insert(l0).unwrap();
        tree.insert(l1).unwrap();
        tree.insert(l2).unwrap();
        let snap = tree.snapshot();

        let p = snap.generate_proof(2).unwrap();
        assert_eq!(p.level_count, 2);
        assert_eq!(p.levels[0].sibling_count, 0);
        let h01 = h.hash_children(&[l0, l1]);
        assert_eq!(p.levels[1].position, 1);
        assert_eq!(p.levels[1].sibling_count, 1);
        assert_eq!(p.levels[1].siblings[0], h01);
        assert!(p.verify(&h).unwrap());
    }

    #[test]
    fn proof_four_leaves_binary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        let leaves: Vec<Hash> = (1..=4).map(leaf).collect();
        for &l in &leaves {
            tree.insert(l).unwrap();
        }
        let snap = tree.snapshot();

        for i in 0..4u64 {
            let p = snap.generate_proof(i).unwrap();
            assert!(p.verify(&h).unwrap());
            assert_eq!(p.leaf, leaves[i as usize]);
        }
    }

    #[test]
    fn proof_ternary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 3, 32>::new(h.clone());
        let leaves: Vec<Hash> = (1..=4).map(leaf).collect();
        for &l in &leaves {
            tree.insert(l).unwrap();
        }
        let snap = tree.snapshot();

        for i in 0..4u64 {
            let p = snap.generate_proof(i).unwrap();
            assert!(p.verify(&h).unwrap());
        }
    }

    #[test]
    fn proof_quaternary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 4, 32>::new(h.clone());
        let leaves: Vec<Hash> = (1..=5).map(leaf).collect();
        for &l in &leaves {
            tree.insert(l).unwrap();
        }
        let snap = tree.snapshot();

        for i in 0..5u64 {
            let p = snap.generate_proof(i).unwrap();
            assert!(p.verify(&h).unwrap());
        }
    }

    #[test]
    fn verify_rejects_wrong_leaf() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        tree.insert(leaf(1)).unwrap();
        tree.insert(leaf(2)).unwrap();
        let snap = tree.snapshot();

        let mut proof = snap.generate_proof(0).unwrap();
        proof.leaf = leaf(99);
        assert!(!proof.verify(&h).unwrap());
    }

    #[test]
    fn verify_rejects_wrong_root() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        tree.insert(leaf(1)).unwrap();
        tree.insert(leaf(2)).unwrap();
        let snap = tree.snapshot();

        let mut proof = snap.generate_proof(0).unwrap();
        proof.root = [0xFF; 32];
        assert!(!proof.verify(&h).unwrap());
    }

    #[test]
    fn verify_rejects_wrong_sibling() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        tree.insert(leaf(1)).unwrap();
        tree.insert(leaf(2)).unwrap();
        let snap = tree.snapshot();

        let mut proof = snap.generate_proof(0).unwrap();
        proof.levels[0].siblings[0] = [0xFF; 32];
        assert!(!proof.verify(&h).unwrap());
    }

    #[test]
    fn proof_index_out_of_range() {
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        tree.insert(leaf(1)).unwrap();
        let snap = tree.snapshot();

        let err = snap.generate_proof(1).unwrap_err();
        assert_eq!(err, TreeError::IndexOutOfRange { index: 1, size: 1 });
    }

    #[cfg(feature = "blake3")]
    #[test]
    fn proof_blake3_round_trip() {
        use crate::Blake3Hasher;
        let h = Blake3Hasher;
        let mut tree = LeanIMT::<Blake3Hasher, 2, 32>::new(h);

        for i in 0u8..20 {
            tree.insert(*::blake3::hash(&[i]).as_bytes()).unwrap();
        }
        let snap = tree.snapshot();

        for i in 0..20u64 {
            let proof = snap.generate_proof(i).unwrap();
            assert!(proof.verify(&h).unwrap());
        }
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn wincode_round_trip_binary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        for i in 1..=10u8 {
            tree.insert(leaf(i)).unwrap();
        }
        let snap = tree.snapshot();

        for i in 0..10u64 {
            let proof = snap.generate_proof(i).unwrap();
            let bytes = wincode::serialize(&proof).unwrap();
            let decoded: NaryProof<2, 32> = wincode::deserialize(&bytes).unwrap();
            assert_eq!(decoded, proof);
            assert!(decoded.verify(&h).unwrap());
        }
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn wincode_round_trip_ternary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 3, 32>::new(h.clone());
        for i in 1..=10u8 {
            tree.insert(leaf(i)).unwrap();
        }
        let snap = tree.snapshot();

        for i in 0..10u64 {
            let proof = snap.generate_proof(i).unwrap();
            let bytes = wincode::serialize(&proof).unwrap();
            let decoded: NaryProof<3, 32> = wincode::deserialize(&bytes).unwrap();
            assert_eq!(decoded, proof);
            assert!(decoded.verify(&h).unwrap());
        }
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn wincode_round_trip_quaternary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 4, 32>::new(h.clone());
        for i in 1..=10u8 {
            tree.insert(leaf(i)).unwrap();
        }
        let snap = tree.snapshot();

        for i in 0..10u64 {
            let proof = snap.generate_proof(i).unwrap();
            let bytes = wincode::serialize(&proof).unwrap();
            let decoded: NaryProof<4, 32> = wincode::deserialize(&bytes).unwrap();
            assert_eq!(decoded, proof);
            assert!(decoded.verify(&h).unwrap());
        }
    }
}
