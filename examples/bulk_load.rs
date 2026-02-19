use rotortree::{
    Blake3Hasher,
    CheckpointPolicy,
    FlushPolicy,
    RotorTree,
    RotorTreeConfig,
    TieringConfig,
};
use std::{
    path::PathBuf,
    time::Instant,
};

fn main() {
    let config = RotorTreeConfig {
        path: PathBuf::from(".db"),
        flush_policy: FlushPolicy::Interval(std::time::Duration::from_millis(10)),
        checkpoint_policy: CheckpointPolicy::MemoryThreshold(1024 * 1024),
        tiering: TieringConfig::default(),
    };

    // N=4, MAX_DEPTH=12 ~16.6M leaves
    let tree = RotorTree::<Blake3Hasher, 4, 12>::open(Blake3Hasher, config).unwrap();

    // generate 1M leaves
    let leaves: Vec<[u8; 32]> = (0..1_000_000u32)
        .map(|i| {
            let mut h = [0u8; 32];
            h[..4].copy_from_slice(&i.to_le_bytes());
            h
        })
        .collect();

    // parallel batch insert, checkpoints run in the background
    let start = Instant::now();
    let (root, token) = tree.insert_many(&leaves).unwrap();
    let insert_elapsed = start.elapsed();
    token.wait();
    let durable_elapsed = start.elapsed();

    println!("inserted 1M leaves in {insert_elapsed:?} (durable in {durable_elapsed:?})");
    println!("root: {}", hex(&root));
    println!("size: {}", tree.size());

    // proofs work over mmap'd chunks after checkpoint
    let snap = tree.snapshot();

    let start = Instant::now();
    let proof = snap.generate_proof(0).unwrap();
    assert!(proof.verify(&Blake3Hasher).unwrap());
    println!("proof for leaf 0 verified in {:?}", start.elapsed());

    let start = Instant::now();
    let proof = snap.generate_proof(999_999).unwrap();
    assert!(proof.verify(&Blake3Hasher).unwrap());
    println!("proof for leaf 999999 verified in {:?}", start.elapsed());

    tree.close().unwrap();

    // reopen from checkpoint + wal replay
    let config = RotorTreeConfig {
        path: PathBuf::from(".db"),
        flush_policy: FlushPolicy::Manual,
        checkpoint_policy: CheckpointPolicy::default(),
        tiering: TieringConfig::default(),
    };
    let start = Instant::now();
    let tree = RotorTree::<Blake3Hasher, 4, 12>::open(Blake3Hasher, config).unwrap();
    println!(
        "reopened in {:?}, root: {}",
        start.elapsed(),
        hex(&tree.root().unwrap())
    );

    tree.close().unwrap();

    // cleanup
    std::fs::remove_dir_all(".db").unwrap();
}

fn hex(h: &[u8; 32]) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}
