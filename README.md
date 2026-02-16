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
RUSTFLAGS="-C target-cpu=native" cargo bench tree_bench_storage::insert_many --features storage
```

| Benchmark | Fastest | Slowest | Median | Mean | Samples | Iters |
|---|---|---|---|---|---|---|
| insert_many_n2_1000 | 84.91 µs (11.77 Mitem/s) | 107.5 µs (9.295 Mitem/s) | 94.31 µs (10.6 Mitem/s) | 94.28 µs (10.6 Mitem/s) | 100 | 100 |
| insert_many_n2_10000 | 802.2 µs (12.46 Mitem/s) | 1.059 ms (9.442 Mitem/s) | 847 µs (11.8 Mitem/s) | 846.8 µs (11.8 Mitem/s) | 100 | 100 |
| insert_many_n2_100000 | 7.822 ms (12.78 Mitem/s) | 9.328 ms (10.71 Mitem/s) | 8.451 ms (11.83 Mitem/s) | 8.537 ms (11.71 Mitem/s) | 100 | 100 |
| insert_many_n2_1000000 | 80.59 ms (12.4 Mitem/s) | 99.05 ms (10.09 Mitem/s) | 90.02 ms (11.1 Mitem/s) | 90 ms (11.11 Mitem/s) | 100 | 100 |
| insert_many_n4_1000 | 52.2 µs (19.15 Mitem/s) | 103.9 µs (9.623 Mitem/s) | 61.12 µs (16.35 Mitem/s) | 59.8 µs (16.72 Mitem/s) | 100 | 100 |
| insert_many_n4_10000 | 461.8 µs (21.65 Mitem/s) | 939.3 µs (10.64 Mitem/s) | 528.9 µs (18.9 Mitem/s) | 532.6 µs (18.77 Mitem/s) | 100 | 100 |
| insert_many_n4_100000 | 4.808 ms (20.79 Mitem/s) | 5.899 ms (16.95 Mitem/s) | 5.272 ms (18.96 Mitem/s) | 5.274 ms (18.95 Mitem/s) | 100 | 100 |
| insert_many_n4_1000000 | 49.72 ms (20.11 Mitem/s) | 62.7 ms (15.94 Mitem/s) | 56.5 ms (17.69 Mitem/s) | 56.24 ms (17.77 Mitem/s) | 100 | 100 |
| insert_many_n8_1000 | 44.83 µs (22.3 Mitem/s) | 64.12 µs (15.59 Mitem/s) | 51.08 µs (19.57 Mitem/s) | 50.31 µs (19.87 Mitem/s) | 100 | 100 |
| insert_many_n8_10000 | 376.2 µs (26.58 Mitem/s) | 807.9 µs (12.37 Mitem/s) | 442.5 µs (22.59 Mitem/s) | 442.7 µs (22.58 Mitem/s) | 100 | 100 |
| insert_many_n8_100000 | 4.031 ms (24.8 Mitem/s) | 4.854 ms (20.6 Mitem/s) | 4.412 ms (22.66 Mitem/s) | 4.419 ms (22.62 Mitem/s) | 100 | 100 |
| insert_many_n8_1000000 | 41.44 ms (24.12 Mitem/s) | 55.65 ms (17.96 Mitem/s) | 47.64 ms (20.98 Mitem/s) | 47.52 ms (21.04 Mitem/s) | 100 | 100 |
| insert_many_n16_1000 | 41.91 µs (23.85 Mitem/s) | 257.4 µs (3.884 Mitem/s) | 48.33 µs (20.68 Mitem/s) | 50.37 µs (19.85 Mitem/s) | 100 | 100 |
| insert_many_n16_10000 | 362.5 µs (27.58 Mitem/s) | 784.8 µs (12.74 Mitem/s) | 413.6 µs (24.17 Mitem/s) | 420.4 µs (23.78 Mitem/s) | 100 | 100 |
| insert_many_n16_100000 | 3.767 ms (26.54 Mitem/s) | 4.567 ms (21.89 Mitem/s) | 4.087 ms (24.46 Mitem/s) | 4.098 ms (24.39 Mitem/s) | 100 | 100 |
| insert_many_n16_1000000 | 38.34 ms (26.07 Mitem/s) | 49.58 ms (20.16 Mitem/s) | 44.12 ms (22.66 Mitem/s) | 43.86 ms (22.79 Mitem/s) | 100 | 100 |

this gives us quite some room for a read-through cache incase we want to make use of larger trees

## Future work

1. optimize `ceil_log_n` by precomputing the table; 
2. currently working on the design for the checkpointing mechanism to prevent unbounded growth of the wal
