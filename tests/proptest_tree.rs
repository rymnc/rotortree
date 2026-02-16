#![cfg_attr(feature = "concurrent", allow(unused_mut))]

use proptest::prelude::*;
use rotortree::{
    Hash,
    Hasher,
    LeanIMT,
};

#[derive(Clone)]
struct XorHasher;

impl Hasher for XorHasher {
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

fn leaves_strategy(max: usize) -> impl Strategy<Value = Vec<Hash>> {
    prop::collection::vec(prop::array::uniform32(any::<u8>()), 1..=max)
}

proptest! {
    #[test]
    fn root_deterministic_binary(
        leaves in leaves_strategy(100)
    ) {
        let mut t1 =
            LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        let mut t2 =
            LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &leaf in &leaves {
            t1.insert(leaf).unwrap();
            t2.insert(leaf).unwrap();
        }
        prop_assert_eq!(t1.root(), t2.root());
    }

    #[test]
    fn proof_round_trip_binary(
        leaves in leaves_strategy(100)
    ) {
        let mut tree =
            LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &leaf in &leaves {
            tree.insert(leaf).unwrap();
        }
        let snap = tree.snapshot();
        for i in 0..leaves.len() as u64 {
            let proof = snap.generate_proof(i).unwrap();
            prop_assert!(proof.verify(&XorHasher).unwrap());
        }
    }

    #[test]
    fn insert_many_equivalence_binary(
        leaves in leaves_strategy(200)
    ) {
        let mut seq =
            LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &leaf in &leaves {
            seq.insert(leaf).unwrap();
        }

        let mut batch =
            LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        batch.insert_many(&leaves).unwrap();

        prop_assert_eq!(seq.root(), batch.root());
    }
}

proptest! {
    #[test]
    fn root_deterministic_ternary(
        leaves in leaves_strategy(100)
    ) {
        let mut t1 =
            LeanIMT::<XorHasher, 3, 32>::new(XorHasher);
        let mut t2 =
            LeanIMT::<XorHasher, 3, 32>::new(XorHasher);
        for &leaf in &leaves {
            t1.insert(leaf).unwrap();
            t2.insert(leaf).unwrap();
        }
        prop_assert_eq!(t1.root(), t2.root());
    }

    #[test]
    fn proof_round_trip_ternary(
        leaves in leaves_strategy(100)
    ) {
        let mut tree =
            LeanIMT::<XorHasher, 3, 32>::new(XorHasher);
        for &leaf in &leaves {
            tree.insert(leaf).unwrap();
        }
        let snap = tree.snapshot();
        for i in 0..leaves.len() as u64 {
            let proof = snap.generate_proof(i).unwrap();
            prop_assert!(proof.verify(&XorHasher).unwrap());
        }
    }

    #[test]
    fn insert_many_equivalence_ternary(
        leaves in leaves_strategy(200)
    ) {
        let mut seq =
            LeanIMT::<XorHasher, 3, 32>::new(XorHasher);
        for &leaf in &leaves {
            seq.insert(leaf).unwrap();
        }

        let mut batch =
            LeanIMT::<XorHasher, 3, 32>::new(XorHasher);
        batch.insert_many(&leaves).unwrap();

        prop_assert_eq!(seq.root(), batch.root());
    }
}

proptest! {
    #[test]
    fn root_deterministic_quaternary(
        leaves in leaves_strategy(100)
    ) {
        let mut t1 =
            LeanIMT::<XorHasher, 4, 32>::new(XorHasher);
        let mut t2 =
            LeanIMT::<XorHasher, 4, 32>::new(XorHasher);
        for &leaf in &leaves {
            t1.insert(leaf).unwrap();
            t2.insert(leaf).unwrap();
        }
        prop_assert_eq!(t1.root(), t2.root());
    }

    #[test]
    fn proof_round_trip_quaternary(
        leaves in leaves_strategy(100)
    ) {
        let mut tree =
            LeanIMT::<XorHasher, 4, 32>::new(XorHasher);
        for &leaf in &leaves {
            tree.insert(leaf).unwrap();
        }
        let snap = tree.snapshot();
        for i in 0..leaves.len() as u64 {
            let proof = snap.generate_proof(i).unwrap();
            prop_assert!(proof.verify(&XorHasher).unwrap());
        }
    }

    #[test]
    fn insert_many_equivalence_quaternary(
        leaves in leaves_strategy(200)
    ) {
        let mut seq =
            LeanIMT::<XorHasher, 4, 32>::new(XorHasher);
        for &leaf in &leaves {
            seq.insert(leaf).unwrap();
        }

        let mut batch =
            LeanIMT::<XorHasher, 4, 32>::new(XorHasher);
        batch.insert_many(&leaves).unwrap();

        prop_assert_eq!(seq.root(), batch.root());
    }
}

proptest! {
    #[test]
    fn insert_many_incremental_binary(
        first in leaves_strategy(50),
        second in leaves_strategy(50),
    ) {
        let mut seq =
            LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &leaf in first.iter().chain(second.iter()) {
            seq.insert(leaf).unwrap();
        }

        let mut mixed =
            LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &leaf in &first {
            mixed.insert(leaf).unwrap();
        }
        mixed.insert_many(&second).unwrap();

        prop_assert_eq!(seq.root(), mixed.root());
    }
}
