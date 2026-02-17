use rotortree::{
    Blake3Hasher,
    LeanIMT,
};
use std::sync::Arc;

mod common;
use common::generate_leaves;

fn main() {
    divan::main();
}

#[crabtime::function]
fn bench_insert_many(n_values: Vec<usize>) {
    let counts = [1_000usize, 10_000, 100_000, 1_000_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn insert_many_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let leaves = generate_leaves({{count}});
                    bencher
                        .counter(divan::counter::ItemsCount::new({{count}} as usize))
                        .with_inputs(|| LeanIMT::<Blake3Hasher, {{n}}, 32>::new(Blake3Hasher))
                        .bench_local_refs(|tree| {
                            divan::black_box(tree.insert_many(&leaves).unwrap());
                        });
                }
            }
        }
    }
}

bench_insert_many!([2, 4, 8, 16]);

#[crabtime::function]
fn bench_insert_incremental(n_values: Vec<usize>) {
    let counts = [10_000usize, 100_000, 1_000_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn insert_incremental_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let all_leaves = generate_leaves({{count}});
                    let half = {{count}} / 2;
                    let (first_half, second_half) = all_leaves.split_at(half);
                    bencher
                        .counter(divan::counter::ItemsCount::new(second_half.len()))
                        .with_inputs(|| {
                            let tree = LeanIMT::<Blake3Hasher, {{n}}, 32>::new(Blake3Hasher);
                            tree.insert_many(first_half).unwrap();
                            tree
                        })
                        .bench_local_refs(|tree| {
                            divan::black_box(tree.insert_many(second_half).unwrap());
                        });
                }
            }
        }
    }
}

bench_insert_incremental!([2, 4, 8, 16]);

#[crabtime::function]
fn bench_concurrent_contention(n_values: Vec<usize>) {
    let counts = [10_000usize, 100_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn concurrent_contention_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let leaves = generate_leaves({{count}});
                    bencher
                        .counter(divan::counter::ItemsCount::new({{count}} as usize))
                        .with_inputs(|| Arc::new(LeanIMT::<Blake3Hasher, {{n}}, 32>::new(Blake3Hasher)))
                        .bench_local_values(|tree| {
                            std::thread::scope(|s| {
                                for _ in 0..4 {
                                    let tree = Arc::clone(&tree);
                                    s.spawn(move || {
                                        loop {
                                            let snap = tree.snapshot();
                                            let size = snap.size();
                                            if size > 0 {
                                                let _ = divan::black_box(
                                                    snap.generate_proof(size / 2)
                                                );
                                            }
                                            if size >= {{count}} as u64 {
                                                break;
                                            }
                                        }
                                    });
                                }
                                tree.insert_many(&leaves).unwrap();
                            });
                        });
                }
            }
        }
    }
}

bench_concurrent_contention!([2, 4, 8, 16]);
