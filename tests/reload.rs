//! Reload must preserve cluster identity.
//!
//! Part 2 moved the admission metric onto the cluster state, so `reload` had to
//! stop rebuilding state or every reload would reset the dashboards to zero. This
//! is the reload half of INC-6: a reload must never let a cluster run above its
//! cap, and must never reset the counts an operator is watching.
//!
//! `try_acquire` reads a per-thread cached snapshot of the topology, so these also
//! cover that a reload reaches threads that already have a cached copy.

use std::sync::Arc;

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn config(clusters: &[(&str, u64)]) -> PoolConfig {
    PoolConfig::default().group(
        "g",
        clusters
            .iter()
            .map(|(name, max)| ClusterConfig::new(*name, *max))
            .collect(),
    )
}

fn pool(clusters: &[(&str, u64)]) -> Arc<ClusterPool> {
    ClusterPool::new(config(clusters))
}

#[test]
fn reload_does_not_reset_running_counts() {
    let pool = pool(&[("a", 10)]);
    let held: Vec<_> = (0..8).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.reload(config(&[("a", 10)]));
    assert_eq!(
        pool.running("g", "a"),
        Some(8),
        "reload reset the running count"
    );

    // Only two slots are left, so a reload must not hand out a fresh batch.
    let extra: Vec<_> = (0..2).map(|_| pool.try_acquire("g").unwrap()).collect();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    assert_eq!(pool.running("g", "a"), Some(10));

    // Permits admitted before the reload still release the same counter.
    drop(held);
    drop(extra);
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[test]
fn reload_keeps_the_admissions_metric() {
    let pool = pool(&[("a", 10)]);
    pool.try_acquire("g").unwrap().release();
    pool.try_acquire("g").unwrap().release();
    assert_eq!(pool.admissions("g", "a"), 2);

    pool.reload(config(&[("a", 10)]));
    assert_eq!(pool.admissions("g", "a"), 2, "reload reset the metric");
}

#[test]
fn reload_can_shrink_a_cap_below_running() {
    let pool = pool(&[("a", 10)]);
    let held: Vec<_> = (0..5).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.reload(config(&[("a", 3)]));
    assert_eq!(
        pool.running("g", "a"),
        Some(5),
        "running queries are not revoked"
    );
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    drop(held);
    assert_eq!(pool.running("g", "a"), Some(0));
    assert!(
        pool.try_acquire("g").is_ok(),
        "capacity returns once it drains"
    );
}

#[test]
fn reload_adds_and_removes_clusters() {
    let pool = pool(&[("a", 1)]);
    let permit = pool.try_acquire("g").unwrap();

    pool.reload(config(&[("b", 1)]));
    assert_eq!(pool.running("g", "a"), None, "`a` left the config");
    let on_b = pool
        .try_acquire("g")
        .expect("a new cluster takes traffic at once");
    assert_eq!(on_b.cluster(), "b");

    // The permit for the removed cluster must release safely: it holds its own
    // state, so it cannot corrupt whatever now has that name.
    permit.release();
    assert_eq!(pool.running("g", "b"), Some(1));
    drop(on_b);

    // Re-adding `a` starts it fresh, like any newly configured cluster.
    pool.reload(config(&[("a", 1)]));
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.admissions("g", "a"), 0);
}

#[test]
fn reload_updates_capacity_in_both_directions() {
    let pool = pool(&[("a", 1)]);
    let _first = pool.try_acquire("g").unwrap();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    pool.reload(config(&[("a", 4)]));
    let more: Vec<_> = (0..3)
        .map(|_| pool.try_acquire("g").expect("cap was raised to 4"))
        .collect();
    assert_eq!(pool.running("g", "a"), Some(4));
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    drop(more);
}

#[test]
fn reload_reaches_threads_that_cached_the_old_topology() {
    let pool = pool(&[("a", 10)]);
    let barrier = std::sync::Barrier::new(2);

    std::thread::scope(|s| {
        let worker = s.spawn(|| {
            // Warm this thread's snapshot cache against the old topology.
            let first = pool.try_acquire("g").unwrap();
            assert_eq!(first.cluster(), "a");
            drop(first);

            barrier.wait(); // main thread reloads now
            barrier.wait(); // and signals the reload is done

            // `a` is gone from the config: a stale cache would still use it.
            let after = pool.try_acquire("g").unwrap();
            assert_eq!(
                after.cluster(),
                "b",
                "thread kept using its cached topology"
            );
        });

        barrier.wait();
        pool.reload(config(&[("b", 10)]));
        barrier.wait();
        worker.join().unwrap();
    });
}

#[test]
fn concurrent_reloads_do_not_lose_state() {
    let pool = pool(&[("a", 1000)]);
    let held: Vec<_> = (0..100).map(|_| pool.try_acquire("g").unwrap()).collect();

    let reloaders: Vec<_> = (0..4)
        .map(|_| {
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || {
                for _ in 0..200 {
                    pool.reload(config(&[("a", 1000)]));
                }
            })
        })
        .collect();
    for r in reloaders {
        r.join().unwrap();
    }

    assert_eq!(
        pool.running("g", "a"),
        Some(100),
        "a concurrent reload lost state"
    );
    drop(held);
    assert_eq!(pool.running("g", "a"), Some(0));
}
