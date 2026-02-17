# rotortree ~ nullifier databases done right*

<p align="center">
  <img src="./assets/title.png" alt="title" width="30%">
</p>


brrrrrr

> [!NOTE]
> this database is a research experiment building on first principles of architecting and specializing a database, inspired by a few techniques mentioned in [Database Internals: A Deep Dive Into How Distributed Data Systems Work](https://www.oreilly.com/library/view/database-internals/9781492040330/) 

_Most_ of the privacy protocols reuse popular/production ready databases, such as rocksdb, postgres, etc, however, they may not be suitable for high performance use-cases, which is also why layerzero created [qmdb](https://arxiv.org/pdf/2501.05262) to achieve high throughput. 
their design is different than _rotortree_, because we focus on append only merkle trees here, and we do not support updating any leaves in-place.

> [!WARNING]
> this approach makes **MANY** tradeoffs, and is not suitable for production **AT ALL!!!**

the tree design itself is heavily inspired by [lean-imt](https://zkkit.org/leanimt-paper.pdf) based on the great work by cedoor & vivian @ [PSE](https://pse.dev), this design was chosen so that it can have functional equivalents in zk dsls' and solidity. however, the main deviation is that here we implement an n-ary leanimt :) the intuition here is to reduce the depth, but maintain the same amount of total leaves. this also allows us to efficiently make use of on-disk storage blocks by grouping leaves together. 

## Design decisions

- you should have a k-v database in tandem with this to ensure you don't insert the same nullifier twice. 
- you should constrain node values to the finite field you're using before insertion
- generic hasher, blake3 default
- batteries included for playing with different branching factors and max depths
- wal for persistence and recovery. see [#future-work](#future-work) for checkpoint mechanism details
- [wincode](https://github.com/anza-xyz/wincode) for fast serde
- no_std by default, persistence requires std
- benchmarks driven and configured by divan + crabtime
- your tree _should_ fit in memory :)
  - with N=4, MAX_DEPTH=16, you can fit ~4.3B nullifiers in 41 GiB
  - with N=8, MAX_DEPTH=10, you can fit ~1B nullifiers in 37 GiB
  - which are quite feasible, but expensive. just use a new tree per generation and encode your nullifiers with the generation pls
  - in most cases, you just need the tree in memory without crash persistence (as long as there is a bootstrapping sync mechanism), just use the single threaded variant, its _MUCH_ better if you have a low number of insertions
  - for this reason, one cannot do an apples to apples comparison of this impl with other merkle tree dbs which write and read from the disk. here the writes are to the wal + memory and reads are only from memory
- few dependencies ~ 65 (active + transitive, excluding dev deps)

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
    Blake3Hasher, RotorTree, RotorTreeConfig, FlushPolicy,
};
use std::path::PathBuf;

let config = RotorTreeConfig {
    path: PathBuf::from("/tmp/my-tree"),
    flush_policy: FlushPolicy::default(), // fsync every 10ms
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
- `BatchSize(n)`: fsync after every `n` buffered entries
- `Manual`: caller controls flushing via `tree.flush()` (works well if you're following a blockchain as the canonical source of state transitions)

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

### Sample benchmark (insert_many)

Test bench: M4 pro, 14c 48g 

```
RUSTFLAGS="-C target-cpu=native" cargo bench tree_bench_storage::insert_many --features storage,parallel
```

| Benchmark | Fastest | Slowest | Median | Mean | Samples | Iters |
|---|---|---|---|---|---|---|
| insert_many_n2_1000 | 130.1 µs (7.682 Mitem/s) | 494.9 µs (2.02 Mitem/s) | 223 µs (4.483 Mitem/s) | 232.8 µs (4.293 Mitem/s) | 100 | 100 |
| insert_many_n2_10000 | 704.6 µs (14.19 Mitem/s) | 1.839 ms (5.436 Mitem/s) | 907.7 µs (11.01 Mitem/s) | 945 µs (10.58 Mitem/s) | 100 | 100 |
| insert_many_n2_100000 | 3.153 ms (31.7 Mitem/s) | 5.393 ms (18.53 Mitem/s) | 3.996 ms (25.02 Mitem/s) | 4.105 ms (24.35 Mitem/s) | 100 | 100 |
| insert_many_n2_1000000 | 22.8 ms (43.84 Mitem/s) | 49.19 ms (20.32 Mitem/s) | 32.16 ms (31.09 Mitem/s) | 32.21 ms (31.03 Mitem/s) | 100 | 100 |
| insert_many_n4_1000 | 62.74 µs (15.93 Mitem/s) | 382.6 µs (2.613 Mitem/s) | 120.3 µs (8.305 Mitem/s) | 135.4 µs (7.384 Mitem/s) | 100 | 100 |
| insert_many_n4_10000 | 351.4 µs (28.44 Mitem/s) | 1.445 ms (6.917 Mitem/s) | 496.4 µs (20.14 Mitem/s) | 545.6 µs (18.32 Mitem/s) | 100 | 100 |
| insert_many_n4_100000 | 1.78 ms (56.15 Mitem/s) | 4.148 ms (24.1 Mitem/s) | 2.379 ms (42.01 Mitem/s) | 2.406 ms (41.54 Mitem/s) | 100 | 100 |
| insert_many_n4_1000000 | 14.37 ms (69.57 Mitem/s) | 31.68 ms (31.56 Mitem/s) | 20.84 ms (47.96 Mitem/s) | 20.63 ms (48.45 Mitem/s) | 100 | 100 |
| insert_many_n8_1000 | 51.58 µs (19.38 Mitem/s) | 346.4 µs (2.886 Mitem/s) | 95.45 µs (10.47 Mitem/s) | 113 µs (8.842 Mitem/s) | 100 | 100 |
| insert_many_n8_10000 | 183.3 µs (54.53 Mitem/s) | 1.04 ms (9.608 Mitem/s) | 365 µs (27.39 Mitem/s) | 392.3 µs (25.49 Mitem/s) | 100 | 100 |
| insert_many_n8_100000 | 1.362 ms (73.37 Mitem/s) | 2.631 ms (37.99 Mitem/s) | 1.861 ms (53.72 Mitem/s) | 1.879 ms (53.19 Mitem/s) | 100 | 100 |
| insert_many_n8_1000000 | 11.87 ms (84.24 Mitem/s) | 29.15 ms (34.29 Mitem/s) | 17.28 ms (57.86 Mitem/s) | 17.15 ms (58.27 Mitem/s) | 100 | 100 |
| insert_many_n16_1000 | 42.04 µs (23.78 Mitem/s) | 57.2 µs (17.48 Mitem/s) | 44.04 µs (22.7 Mitem/s) | 44.12 µs (22.66 Mitem/s) | 100 | 100 |
| insert_many_n16_10000 | 248.9 µs (40.16 Mitem/s) | 809.5 µs (12.35 Mitem/s) | 310.4 µs (32.21 Mitem/s) | 334.3 µs (29.9 Mitem/s) | 100 | 100 |
| insert_many_n16_100000 | 1.28 ms (78.1 Mitem/s) | 2.039 ms (49.03 Mitem/s) | 1.632 ms (61.26 Mitem/s) | 1.633 ms (61.2 Mitem/s) | 100 | 100 |
| insert_many_n16_1000000 | **10.85 ms (92.1 Mitem/s)** | 22.16 ms (45.11 Mitem/s) | 15.97 ms (62.57 Mitem/s) | 15.8 ms (63.28 Mitem/s) | 100 | 100 |

peak insertions/sec is ~ 92.1 Million leaves/sec this gives us quite some room for a read-through cache incase we want to make use of larger trees

> [!NOTE]
> There are more realistic benchmarks that simulate performance under load, i.e concurrent reads / proof generation + insertions 

## Future work

1. optimize `ceil_log_n` by precomputing the table; 
2. currently working on the design for the checkpointing mechanism to prevent unbounded growth of the wal
3. reduce performance variance across benchmark samples
