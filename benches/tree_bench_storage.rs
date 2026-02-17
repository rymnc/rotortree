use rotortree::{
    Blake3Hasher,
    FlushPolicy,
    RotorTree,
    RotorTreeConfig,
};

mod common;
use common::generate_leaves;

fn main() {
    divan::main();
}

#[crabtime::function]
fn bench_insert_single(n_values: Vec<usize>) {
    let counts = [1_000usize, 10_000, 100_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn insert_single_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let leaves = generate_leaves({{count}});
                    let dir = tempfile::tempdir().unwrap();
                    let config = RotorTreeConfig {
                        path: dir.path().to_path_buf(),
                        flush_policy: FlushPolicy::Manual,
                    };
                    let tree = RotorTree::<Blake3Hasher, {{n}}, 32>::open(Blake3Hasher, config).unwrap();
                    bencher
                        .counter(divan::counter::ItemsCount::new({{count}} as usize))
                        .bench_local(|| {
                            for &leaf in &leaves {
                                divan::black_box(tree.insert(leaf).unwrap());
                            }
                        });
                    tree.close().unwrap();
                }
            }
        }
    }
}

bench_insert_single!([2, 4, 8, 16]);

#[crabtime::function]
fn bench_insert_many(n_values: Vec<usize>) {
    let counts = [1_000usize, 10_000, 100_000, 1_000_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn insert_many_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let leaves = generate_leaves({{count}});
                    let dir = tempfile::tempdir().unwrap();
                    let config = RotorTreeConfig {
                        path: dir.path().to_path_buf(),
                        flush_policy: FlushPolicy::Manual,
                    };
                    let tree = RotorTree::<Blake3Hasher, {{n}}, 32>::open(Blake3Hasher, config).unwrap();
                    bencher
                        .counter(divan::counter::ItemsCount::new({{count}} as usize))
                        .bench_local(|| {
                            divan::black_box(tree.insert_many(&leaves).unwrap());
                        });
                    tree.close().unwrap();
                }
            }
        }
    }
}

bench_insert_many!([2, 4, 8, 16]);

#[crabtime::function]
fn bench_flush(n_values: Vec<usize>) {
    let counts = [1_000usize, 10_000, 100_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn flush_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let leaves = generate_leaves({{count}});
                    let dir = tempfile::tempdir().unwrap();
                    let config = RotorTreeConfig {
                        path: dir.path().to_path_buf(),
                        flush_policy: FlushPolicy::Manual,
                    };
                    let tree = RotorTree::<Blake3Hasher, {{n}}, 32>::open(Blake3Hasher, config).unwrap();
                    bencher
                        .counter(divan::counter::ItemsCount::new({{count}} as usize))
                        .bench_local(|| {
                            tree.insert_many(&leaves).unwrap();
                            divan::black_box(tree.flush().unwrap());
                        });
                    tree.close().unwrap();
                }
            }
        }
    }
}

bench_flush!([2, 4, 8, 16]);

#[crabtime::function]
fn bench_open_recover(n_values: Vec<usize>) {
    let counts = [1_000usize, 10_000, 100_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn open_recover_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let leaves = generate_leaves({{count}});
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().to_path_buf();
                    {
                        let config = RotorTreeConfig {
                            path: path.clone(),
                            flush_policy: FlushPolicy::Manual,
                        };
                        let tree = RotorTree::<Blake3Hasher, {{n}}, 32>::open(Blake3Hasher, config).unwrap();
                        tree.insert_many(&leaves).unwrap();
                        tree.flush().unwrap();
                        tree.close().unwrap();
                    }
                    bencher
                        .counter(divan::counter::ItemsCount::new({{count}} as usize))
                        .bench_local(|| {
                            let config = RotorTreeConfig {
                                path: path.clone(),
                                flush_policy: FlushPolicy::Manual,
                            };
                            let tree = RotorTree::<Blake3Hasher, {{n}}, 32>::open(Blake3Hasher, config).unwrap();
                            divan::black_box(tree.root());
                            tree.close().unwrap();
                        });
                }
            }
        }
    }
}

bench_open_recover!([2, 4, 8, 16]);

#[crabtime::function]
fn bench_mixed_workload(n_values: Vec<usize>) {
    let tick_sizes = [100usize, 1_000, 10_000, 100_000];
    for n in n_values {
        for tick in tick_sizes {
            crabtime::output! {
                #[divan::bench]
                fn mixed_workload_n{{n}}_{{tick}}(bencher: divan::Bencher) {
                    let prepop_leaves = generate_leaves(10_000);
                    let tick_leaves = generate_leaves({{tick}});

                    let dir = tempfile::tempdir().unwrap();
                    let config = RotorTreeConfig {
                        path: dir.path().to_path_buf(),
                        flush_policy: FlushPolicy::Manual,
                    };
                    let tree = RotorTree::<Blake3Hasher, {{n}}, 32>::open(Blake3Hasher, config).unwrap();
                    tree.insert_many(&prepop_leaves).unwrap();
                    tree.flush().unwrap();

                    let hasher = Blake3Hasher;

                    bencher
                        .counter(divan::counter::ItemsCount::new({{tick}} as usize))
                        .bench_local(|| {
                            let (root, _token) = tree.insert_many(&tick_leaves).unwrap();
                            divan::black_box(root);

                            divan::black_box(tree.root());

                            let snap = tree.snapshot();
                            let proof_index = snap.size() / 2;
                            let proof = snap.generate_proof(proof_index).unwrap();
                            divan::black_box(&proof);

                            divan::black_box(proof.verify(&hasher).unwrap());
                        });
                    tree.close().unwrap();
                }
            }
        }
    }
}

bench_mixed_workload!([2, 4, 8, 16]);
