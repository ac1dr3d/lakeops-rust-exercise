//! Part 2 selection policy: "roughly least-loaded".
//!
//! These check the spread, not exact counts: the policy samples two clusters, so
//! the split is statistical. Bounds are loose enough to never flake and tight
//! enough to catch a cluster being starved or a capacity being ignored.

use std::sync::Arc;
use std::thread;

use admission::{ClusterConfig, ClusterPool, PoolConfig};

fn pool(clusters: &[(&str, u64)]) -> Arc<ClusterPool> {
    ClusterPool::new(
        PoolConfig::default().group(
            "g",
            clusters
                .iter()
                .map(|(name, max)| ClusterConfig::new(*name, *max))
                .collect(),
        ),
    )
}

fn names(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("c{i}")).collect()
}

/// Sequential acquire + release: every cluster is always tied at full capacity,
/// so this measures tie-breaking alone.
#[test]
fn equal_clusters_share_sequential_traffic() {
    for n in [2usize, 3, 4, 8] {
        let names = names(n);
        let pool = pool(&names.iter().map(|c| (c.as_str(), 100)).collect::<Vec<_>>());
        let total = 8_000;
        for _ in 0..total {
            pool.try_acquire("g").unwrap().release();
        }
        let expected = total as u64 / n as u64;
        for c in &names {
            let got = pool.admissions("g", c);
            assert!(
                got > expected / 2 && got < expected * 3 / 2,
                "n={n}: {c} got {got} of {total} admissions, expected about {expected}"
            );
        }
    }
}

/// Every thread starts with a fresh thread-local; they must not all pick the
/// same cluster on their first call.
#[test]
fn fresh_threads_do_not_all_pick_the_same_cluster() {
    let names = names(8);
    let pool = pool(&names.iter().map(|c| (c.as_str(), 1000)).collect::<Vec<_>>());

    let picks: Vec<String> = (0..64)
        .map(|_| {
            let pool = Arc::clone(&pool);
            thread::spawn(move || pool.try_acquire("g").unwrap().cluster().to_string())
                .join()
                .unwrap()
        })
        .collect();

    let distinct: std::collections::HashSet<_> = picks.iter().collect();
    assert!(
        distinct.len() >= 4,
        "64 fresh threads used only {} of 8 clusters: {distinct:?}",
        distinct.len()
    );
}

/// With held permits, a cluster with ten times the capacity must take most of
/// the load. This is the "by how much capacity each has free" requirement.
#[test]
fn bigger_clusters_take_more_of_the_load() {
    let pool = pool(&[("big", 100), ("s1", 10), ("s2", 10), ("s3", 10)]);
    let held: Vec<_> = (0..60).map(|_| pool.try_acquire("g").unwrap()).collect();

    let big = pool.running("g", "big").unwrap();
    let small: Vec<u64> = ["s1", "s2", "s3"]
        .iter()
        .map(|c| pool.running("g", c).unwrap())
        .collect();
    assert!(
        big >= 30,
        "big cluster took only {big} of 60, small ones {small:?}"
    );
    for (c, r) in ["s1", "s2", "s3"].iter().zip(&small) {
        assert!(*r <= 10, "{c} over its cap: {r}");
    }
    drop(held);
}

/// Nothing is starved under load: with all slots held, every cluster is full.
#[test]
fn filling_a_group_uses_every_cluster() {
    let names = names(8);
    let pool = pool(&names.iter().map(|c| (c.as_str(), 5)).collect::<Vec<_>>());
    let held: Vec<_> = (0..40).map(|_| pool.try_acquire("g").unwrap()).collect();
    for c in &names {
        assert_eq!(pool.running("g", c), Some(5), "{c} was not filled");
    }
    assert!(pool.try_acquire("g").is_err());
    drop(held);
}
