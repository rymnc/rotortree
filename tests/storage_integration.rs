#![cfg(feature = "storage")]

use std::sync::Arc;

use rotortree::{
    FlushPolicy,
    Hash,
    Hasher,
    RotorTree,
    RotorTreeConfig,
    RotorTreeError,
    StorageError,
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

fn leaf(n: u8) -> Hash {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

fn manual_config(dir: &std::path::Path) -> RotorTreeConfig {
    RotorTreeConfig {
        path: dir.to_path_buf(),
        flush_policy: FlushPolicy::Manual,
    }
}

#[test]
fn open_empty_close_reopen() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.root(), None);
    assert_eq!(tree.size(), 0);
    tree.close().unwrap();

    // when
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();

    // then
    assert_eq!(tree.root(), None);
    assert_eq!(tree.size(), 0);
    tree.close().unwrap();
}

#[test]
fn insert_close_reopen() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();

    let (root, _token) = tree.insert(leaf(1)).unwrap();
    tree.flush().unwrap();
    let size = tree.size();
    // when
    tree.close().unwrap();

    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();

    // then
    assert_eq!(tree.root(), Some(root));
    assert_eq!(tree.size(), size);
    tree.close().unwrap();
}

#[test]
fn insert_many_close_reopen() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let leaves: Vec<Hash> = (0..10).map(leaf).collect();

    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    let (root, _token) = tree.insert_many(&leaves).unwrap();
    tree.flush().unwrap();
    // when
    tree.close().unwrap();

    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();

    // then
    assert_eq!(tree.root(), Some(root));
    assert_eq!(tree.size(), 10);
    tree.close().unwrap();
}

#[test]
fn many_inserts_close_reopen() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let n = 50u8;

    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    for i in 0..n {
        tree.insert(leaf(i)).unwrap();
    }
    tree.flush().unwrap();
    let root = tree.root();
    let snap = tree.snapshot();
    // Verify all proofs before close.
    for i in 0..n as u64 {
        let proof = snap.generate_proof(i).unwrap();
        assert!(proof.verify(&XorHasher).unwrap());
    }
    // when
    tree.close().unwrap();

    // Reopen and verify.
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.root(), root);
    assert_eq!(tree.size(), n as u64);
    let snap = tree.snapshot();
    for i in 0..n as u64 {
        let proof = snap.generate_proof(i).unwrap();
        // then
        assert!(proof.verify(&XorHasher).unwrap());
    }
    tree.close().unwrap();
}

#[test]
fn truncated_tail_recovery() {
    // given
    let dir = tempfile::tempdir().unwrap();

    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    for i in 0..5u8 {
        tree.insert(leaf(i)).unwrap();
    }
    tree.flush().unwrap();
    let root_after_5 = tree.root().unwrap();
    let size_after_5 = tree.size();

    tree.insert(leaf(5)).unwrap();
    tree.flush().unwrap();
    tree.close().unwrap();

    let wal_path = dir.path().join("wal");
    let data = std::fs::read(&wal_path).unwrap();
    // when: remove last few bytes
    std::fs::write(&wal_path, &data[..data.len() - 10]).unwrap();

    // then: recovery
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.size(), size_after_5);
    assert_eq!(tree.root(), Some(root_after_5));
    tree.close().unwrap();
}

#[test]
fn crc_corruption_mid_file() {
    // given
    let dir = tempfile::tempdir().unwrap();

    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    for i in 0..3u8 {
        tree.insert(leaf(i)).unwrap();
    }
    tree.flush().unwrap();
    tree.close().unwrap();

    let wal_path = dir.path().join("wal");
    let mut data = std::fs::read(&wal_path).unwrap();
    // when
    let corrupt_offset = data.len() / 2;
    data[corrupt_offset] ^= 0xFF;
    std::fs::write(&wal_path, &data).unwrap();

    // then: error
    let result =
        RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()));
    assert!(result.is_err());
}

#[test]
fn config_mismatch() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    tree.close().unwrap();

    // when, then
    let result =
        RotorTree::<XorHasher, 4, 32>::open(XorHasher, manual_config(dir.path()));
    match result {
        Err(RotorTreeError::Storage(StorageError::ConfigMismatch { .. })) => {}
        Err(e) => panic!("expected ConfigMismatch, got {e:?}"),
        Ok(_) => panic!("expected ConfigMismatch, got Ok"),
    }
}

#[test]
fn durability_token_lifecycle() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();

    let (_root, token) = tree.insert(leaf(1)).unwrap();

    assert!(!token.is_durable());

    // when
    tree.flush().unwrap();

    // then
    assert!(token.is_durable());

    tree.close().unwrap();
}

#[test]
fn concurrent_insert_recover() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let n_threads = 4;
    let inserts_per_thread = 25u8;
    let total = n_threads * inserts_per_thread as usize;

    let tree = Arc::new(
        RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
            .unwrap(),
    );

    std::thread::scope(|s| {
        for t in 0..n_threads {
            let tree = Arc::clone(&tree);
            // when
            s.spawn(move || {
                for i in 0..inserts_per_thread {
                    let val = (t as u8) * inserts_per_thread + i;
                    tree.insert(leaf(val)).unwrap();
                }
            });
        }
    });

    tree.flush().unwrap();
    // then
    assert_eq!(tree.size(), total as u64);

    let snap = tree.snapshot();
    for i in 0..total as u64 {
        let proof = snap.generate_proof(i).unwrap();
        assert!(proof.verify(&XorHasher).unwrap());
    }

    let root = tree.root();
    let tree = Arc::try_unwrap(tree)
        .ok()
        .expect("other Arc references still held");
    tree.close().unwrap();

    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.size(), total as u64);
    assert_eq!(tree.root(), root);
    tree.close().unwrap();
}

#[test]
fn recovery_continuation() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    for i in 0..5u8 {
        tree.insert(leaf(i)).unwrap();
    }
    tree.flush().unwrap();
    tree.close().unwrap();

    // when
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.size(), 5);
    for i in 5..10u8 {
        tree.insert(leaf(i)).unwrap();
    }
    tree.flush().unwrap();
    tree.close().unwrap();

    // then
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.size(), 10);
    let snap = tree.snapshot();
    for i in 0..10u64 {
        let proof = snap.generate_proof(i).unwrap();
        assert!(proof.verify(&XorHasher).unwrap());
    }
    tree.close().unwrap();
}

#[test]
fn depth_change_persistence() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();

    // when
    for i in 0..5u8 {
        tree.insert(leaf(i)).unwrap();
    }
    tree.flush().unwrap();
    let depth = tree.depth();
    let root = tree.root();
    assert!(
        depth >= 2,
        "depth should be at least 2 for 5 leaves with N=2"
    );
    tree.close().unwrap();

    // then
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.depth(), depth);
    assert_eq!(tree.root(), root);
    tree.close().unwrap();
}

#[test]
fn interleaved_single_and_batch() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();

    // when
    tree.insert(leaf(0)).unwrap();
    tree.insert(leaf(1)).unwrap();
    let batch: Vec<Hash> = (2..7).map(leaf).collect();
    tree.insert_many(&batch).unwrap();
    tree.insert(leaf(7)).unwrap();
    tree.insert(leaf(8)).unwrap();
    let batch2: Vec<Hash> = (9..15).map(leaf).collect();
    tree.insert_many(&batch2).unwrap();

    tree.flush().unwrap();
    let root = tree.root();
    let size = tree.size();
    assert_eq!(size, 15);
    tree.close().unwrap();

    // then
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.size(), size);
    assert_eq!(tree.root(), root);

    let snap = tree.snapshot();
    for i in 0..size {
        let proof = snap.generate_proof(i).unwrap();
        assert!(proof.verify(&XorHasher).unwrap());
    }
    tree.close().unwrap();
}

#[test]
fn first_insert_after_recovery() {
    // given
    let dir = tempfile::tempdir().unwrap();

    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    for i in 0..3u8 {
        tree.insert(leaf(i)).unwrap();
    }
    tree.flush().unwrap();
    tree.close().unwrap();

    // when
    let tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();
    assert_eq!(tree.size(), 3);

    let (root, _) = tree.insert(leaf(3)).unwrap();
    tree.flush().unwrap();
    assert_eq!(tree.size(), 4);
    assert_eq!(tree.root(), Some(root));

    // then
    let snap = tree.snapshot();
    for i in 0..4u64 {
        let proof = snap.generate_proof(i).unwrap();
        assert!(proof.verify(&XorHasher).unwrap());
    }
    tree.close().unwrap();
}

#[test]
fn file_locking() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let _tree = RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()))
        .unwrap();

    // when, then
    let result =
        RotorTree::<XorHasher, 2, 10>::open(XorHasher, manual_config(dir.path()));
    match result {
        Err(RotorTreeError::Storage(StorageError::FileLocked)) => {}
        Err(e) => panic!("expected FileLocked, got {e:?}"),
        Ok(_) => panic!("expected FileLocked, got Ok"),
    }
}
