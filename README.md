# rotortree ~ nullifier databases done right*

<p align="center">
  <img src="./assets/title.png" alt="title" width="100%">
</p>


brrrrrr

<!-- ANCHOR: intro --> 
> [!NOTE]
> this database is a research experiment building on first principles of architecting and specializing a database, inspired by a few techniques mentioned in [Database Internals: A Deep Dive Into How Distributed Data Systems Work](https://www.oreilly.com/library/view/database-internals/9781492040330/) 

_Most_ of the privacy protocols reuse popular/production ready databases, such as rocksdb, postgres, etc, however, they may not be suitable for high performance use-cases, which is also why layerzero created [qmdb](https://arxiv.org/pdf/2501.05262) to achieve high throughput. 
their design is different than _rotortree_, because we focus on append only merkle trees here, and we do not support updating any leaves in-place.

<!-- ANCHOR_END: intro --> 

> [!WARNING]
> this approach makes **MANY** tradeoffs, and is not suitable for production **AT ALL!!!**

the tree design itself is heavily inspired by [lean-imt](https://zkkit.org/leanimt-paper.pdf) based on the great work by cedoor & vivian @ [PSE](https://pse.dev), this design was chosen so that it can have functional equivalents in zk dsls' and solidity. however, the main deviation is that here we implement an n-ary leanimt :) the intuition here is to reduce the depth, but maintain the same amount of total leaves. this also allows us to efficiently make use of on-disk storage blocks by grouping leaves together. 

<!-- ANCHOR: design --> 

## Design decisions

- you should have a k-v database in tandem with this to ensure you don't insert the same nullifier twice. 
- you should constrain node values to the finite field you're using before insertion
- generic hasher, blake3 default
- batteries included for playing with different branching factors and max depths
- wal for persistence and recovery, with checkpointing to prevent unbounded wal growth
- [wincode](https://github.com/anza-xyz/wincode) for fast serde
- no_std by default, persistence requires std
- benchmarks driven and configured by divan + crabtime
- by default your tree lives in memory, but with the `storage` feature you can tier cold levels to mmap'd data files via `TieringConfig::pin_above_level`
  - with N=4, MAX_DEPTH=16, you can fit ~4.3B nullifiers in 41 GiB
  - with N=8, MAX_DEPTH=10, you can fit ~1B nullifiers in 37 GiB
  - which are quite feasible, but expensive. just use a new tree per generation and encode your nullifiers with the generation pls
  - in most cases, you just need the tree in memory without crash persistence (as long as there is a bootstrapping sync mechanism), just use the single threaded variant, its _MUCH_ better if you have a low number of insertions
  - writes go to the wal + memory, reads are always from memory or mmap. one cannot do an apples to apples comparison with other merkle tree dbs that read from disk on every query
- few dependencies ~ 65 (active + transitive, excluding dev deps)

<!-- ANCHOR_END: design --> 

<!-- ANCHOR: usage --> 

## Usage

### In-memory (no persistence) `default-features = false`

use `LeanIMT` 

```rust
use rotortree::{LeanIMT, Blake3Hasher};

// N=4 branching factor, MAX_DEPTH=20
let mut tree = LeanIMT::<Blake3Hasher, 4, 20>::new(Blake3Hasher);

// single insert
let leaf = [1u8; 32];
let root = tree.insert(leaf).unwrap();

// batch insert
let leaves: Vec<[u8; 32]> = (0..1000u32)
    .map(|i| {
        let mut h = [0u8; 32];
        h[..4].copy_from_slice(&i.to_le_bytes());
        h
    })
    .collect();
let root = tree.insert_many(&leaves).unwrap();

// proof generation & verification
let snap = tree.snapshot();
let proof = snap.generate_proof(0).unwrap();
assert!(proof.verify(&Blake3Hasher).unwrap());
```

optional feature flags for the in-memory mode:
- `concurrent`: switches to `&self` methods with internal `Mutex` + lock-free snapshot reads via `ArcSwap`
- `parallel`: enables rayon-parallelized `insert_many` for large batches (this works really well)
- `wincode`: adds wincode serde derives to proof types

### With WAL persistence (`storage` feature)

```rust
use rotortree::{
    Blake3Hasher, RotorTree, RotorTreeConfig,
    FlushPolicy, CheckpointPolicy, TieringConfig,
};
use std::path::PathBuf;

let config = RotorTreeConfig {
    path: PathBuf::from("/tmp/my-tree"),
    flush_policy: FlushPolicy::default(), // fsync every 10ms
    checkpoint_policy: CheckpointPolicy::default(), // manual
    tiering: TieringConfig::default(), // all in memory
    verify_checkpoint: true, // recompute root on recovery
};

// opens existing WAL or creates a new one
let tree = RotorTree::<Blake3Hasher, 4, 20>::open(Blake3Hasher, config).unwrap();

// insert: returns root + a durability token
let (root, token) = tree.insert([42u8; 32]).unwrap();
// token.wait() blocks until the entry is fsynced

// or insert + wait for fsync in one call
let root = tree.insert_durable([43u8; 32]).unwrap();

// batch insert
let leaves = vec![[1u8; 32]; 500];
let (root, token) = tree.insert_many(&leaves).unwrap();

// lock-free snapshot for proof generation (same as in-memory)
let snap = tree.snapshot();
let proof = snap.generate_proof(0).unwrap();
assert!(proof.verify(&Blake3Hasher).unwrap());

// explicit flush & close
tree.flush().unwrap();
tree.close().unwrap();
// (also flushes + releases file lock on drop)
```

`FlushPolicy` options:
- `Interval(Duration)`: background thread fsyncs periodically
- `Manual`: caller controls flushing via `tree.flush()` (works well if you're following a blockchain as the canonical source of state transitions)

`CheckpointPolicy` options (materializes wal state to data files, allowing wal truncation):
- `Manual`: caller calls `checkpoint()` explicitly
- `EveryNEntries(n)`: auto-checkpoint after every `n` wal entries
- `MemoryThreshold(bytes)`: auto-checkpoint when uncheckpointed memory exceeds threshold
- `OnClose`: checkpoint only on graceful close

`TieringConfig` controls which levels stay in memory vs get mmap'd after checkpoint:
- `pin_above_level`: levels below this value have their committed chunks mmap'd from data files after checkpoint. set to `0` to keep everything in memory (default: `usize::MAX`, all checkpointed chunks get mmap'd)

<!-- ANCHOR_END: usage --> 

## Development

### Prerequisites

- [cargo-hack](https://github.com/taiki-e/cargo-hack?tab=readme-ov-file#installation): to test all combinations of feature flags
- [cargo-nextest](https://nexte.st/): rust test runner

### Check

```
cargo hack check --feature-powerset
```

### Clippy

```
cargo hack clippy --feature-powerset
```

### Format

```
cargo +nightly fmt
```

😉 if you know where i grabbed the [.rustfmt.toml](.rustfmt.toml) from

## Testing

```
cargo hack nextest run --feature-powerset
```

## Benchmarks

```
cargo bench -- --list
```

there are some feature flagged benchmarks, refer to the [Cargo.toml entry](Cargo.toml) for more details

Head over to https://rymnc.github.io/rotortree/ which has the latest benchmark results (~380 benchmarks)

<!-- ANCHOR: devnote --> 

there seems to be some performance variance with the storage feature enabled, assume due to some contention / outliers. the pure in-memory benchmark (tree_bench_parallel) exhibits much lesser variance, and achieves peak throughput upto ~140M leaves/sec; why would anyone need this much? i do not know myself. single threaded by far has the best performance characteristic in terms of variance though, useful to keep in mind if that is a constraint; trading off performance for predictability under load.

<!-- ANCHOR_END: devnote --> 

> [!NOTE]
> There are more realistic benchmarks that simulate performance under load, i.e concurrent reads / proof generation + insertions 

#### Proof Latency vs Tree size

![Proof Latency vs Tree size](./assets/proof_latency_vs_tree_size.png)

#### Throughput vs Tree size

![Throughput vs Tree size](./assets/throughput_vs_tree_size.png)

The dips noticed here are during checkpoints


## Future work

1. optimize `ceil_log_n` by precomputing the table
2. run benchmarks in an isolated environment for better estimations
