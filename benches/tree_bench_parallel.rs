use rotortree::{
    Blake3Hasher,
    LeanIMT,
};

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
                        .with_inputs(|| {
                            let tree = LeanIMT::<Blake3Hasher, {{n}}, 32>::new(Blake3Hasher);
                            rayon::broadcast(|_| {});
                            tree
                        })
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
fn bench_insert_many_chunked_100(n_values: Vec<usize>) {
    let counts = [10_000usize, 100_000, 1_000_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn insert_many_chunked_100_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let leaves = generate_leaves({{count}});
                    bencher
                        .counter(divan::counter::ItemsCount::new({{count}} as usize))
                        .with_inputs(|| {
                            let tree = LeanIMT::<Blake3Hasher, {{n}}, 32>::new(Blake3Hasher);
                            rayon::broadcast(|_| {});
                            tree
                        })
                        .bench_local_refs(|tree| {
                            for chunk in leaves.chunks(100) {
                                divan::black_box(tree.insert_many(chunk).unwrap());
                            }
                        });
                }
            }
        }
    }
}

bench_insert_many_chunked_100!([2, 4, 8, 16]);

#[crabtime::function]
fn bench_insert_many_chunked_1000(n_values: Vec<usize>) {
    let counts = [10_000usize, 100_000, 1_000_000];
    for n in n_values {
        for count in counts {
            crabtime::output! {
                #[divan::bench]
                fn insert_many_chunked_1000_n{{n}}_{{count}}(bencher: divan::Bencher) {
                    let leaves = generate_leaves({{count}});
                    bencher
                        .counter(divan::counter::ItemsCount::new({{count}} as usize))
                        .with_inputs(|| {
                            let tree = LeanIMT::<Blake3Hasher, {{n}}, 32>::new(Blake3Hasher);
                            rayon::broadcast(|_| {});
                            tree
                        })
                        .bench_local_refs(|tree| {
                            for chunk in leaves.chunks(1000) {
                                divan::black_box(tree.insert_many(chunk).unwrap());
                            }
                        });
                }
            }
        }
    }
}

bench_insert_many_chunked_1000!([2, 4, 8, 16]);

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
                            let mut tree = LeanIMT::<Blake3Hasher, {{n}}, 32>::new(Blake3Hasher);
                            tree.insert_many(first_half).unwrap();
                            rayon::broadcast(|_| {});
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
