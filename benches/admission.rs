use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use admission::{ClusterConfig, ClusterPool, PoolConfig};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// A pool with 8 `trino-*` clusters in `analytics`, plus 20 unrelated groups so
/// the group lookup is not a single-entry hash map in the benchmark.
fn pool(analytics: Vec<ClusterConfig>) -> Arc<ClusterPool> {
    let mut config = PoolConfig::default().group("analytics", analytics);
    for i in 0..20 {
        config = config.group(
            format!("tenant-{i}"),
            vec![ClusterConfig::new(format!("t{i}"), 1_000_000)],
        );
    }
    ClusterPool::new(config)
}

/// Equal capacity, the shape the starter was measured on.
fn equal() -> Arc<ClusterPool> {
    pool(
        (0..8)
            .map(|i| ClusterConfig::new(format!("trino-{i}"), 1_000_000))
            .collect(),
    )
}

/// Unequal capacity: the selection policy has to follow free slots, not just
/// round-robin the group.
fn mixed_caps() -> Arc<ClusterPool> {
    pool(
        (0..8)
            .map(|i| ClusterConfig::new(format!("trino-{i}"), 1_000_000 >> i))
            .collect(),
    )
}

/// One acquire + release, i.e. the admission overhead added to every query.
///
/// `held` permits per thread are taken before the timed region and released after
/// it, so `running` is realistic (a production gateway holds one permit per
/// in-flight query) instead of bouncing around zero.
fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("acquire_release");
    for threads in [1usize, 4, 16] {
        group.throughput(Throughput::Elements(threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = equal();
                b.iter_custom(|iters| {
                    let barrier = Barrier::new(threads + 1);
                    thread::scope(|s| {
                        let workers: Vec<_> = (0..threads)
                            .map(|_| {
                                s.spawn(|| {
                                    barrier.wait();
                                    for _ in 0..iters {
                                        if let Ok(permit) = pool.try_acquire("analytics") {
                                            permit.release();
                                        }
                                    }
                                })
                            })
                            .collect();
                        barrier.wait();
                        let start = Instant::now();
                        for w in workers {
                            w.join().unwrap();
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }
    group.finish();
}

/// Same, but each caller holds permits while it churns, so `available()` is not
/// simply `max_running` and the per-cluster counters are under write pressure
/// from several cores.
fn bench_with_held_permits(c: &mut Criterion) {
    const HELD: usize = 100;
    let mut group = c.benchmark_group("acquire_release_held");
    for threads in [1usize, 4, 16] {
        group.throughput(Throughput::Elements(threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = equal();
                b.iter_custom(|iters| {
                    let barrier = Barrier::new(threads + 1);
                    thread::scope(|s| {
                        let workers: Vec<_> = (0..threads)
                            .map(|_| {
                                s.spawn(|| {
                                    let held: Vec<_> = (0..HELD)
                                        .filter_map(|_| pool.try_acquire("analytics").ok())
                                        .collect();
                                    barrier.wait();
                                    for _ in 0..iters {
                                        if let Ok(permit) = pool.try_acquire("analytics") {
                                            permit.release();
                                        }
                                    }
                                    drop(held);
                                })
                            })
                            .collect();
                        barrier.wait();
                        let start = Instant::now();
                        for w in workers {
                            w.join().unwrap();
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }
    group.finish();
}

/// Unequal capacities: same hot path, but the sampled clusters differ in free
/// slots, so selection has to compare them.
fn bench_mixed_capacities(c: &mut Criterion) {
    let mut group = c.benchmark_group("acquire_release_mixed");
    for threads in [1usize, 4, 16] {
        group.throughput(Throughput::Elements(threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = mixed_caps();
                b.iter_custom(|iters| {
                    let barrier = Barrier::new(threads + 1);
                    thread::scope(|s| {
                        let workers: Vec<_> = (0..threads)
                            .map(|_| {
                                s.spawn(|| {
                                    barrier.wait();
                                    for _ in 0..iters {
                                        if let Ok(permit) = pool.try_acquire("analytics") {
                                            permit.release();
                                        }
                                    }
                                })
                            })
                            .collect();
                        barrier.wait();
                        let start = Instant::now();
                        for w in workers {
                            w.join().unwrap();
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(3));
    targets = bench, bench_with_held_permits, bench_mixed_capacities
}
criterion_main!(benches);
