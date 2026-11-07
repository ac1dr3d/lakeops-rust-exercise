//! Part 1: one or more reproducing tests per incident. Each test fails on the
//! starter code and passes after the fix. See NOTES.md for the root causes.

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

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

// ---------------------------------------------------------------------------
// INC-1: Cap exceeded, then wedged.
//
// Root cause: `try_increment` was `load` + check + `store`, not one atomic
// read-modify-write. Concurrent admissions lost increments (over-admission), and
// the `store` could also erase a concurrent release. Releases then drove the
// counter below zero, `fetch_sub` wrapped it to ~u64::MAX, and every later
// `try_acquire` on that cluster failed.
// ---------------------------------------------------------------------------

#[test]
fn inc1_concurrent_admission_never_exceeds_cap_and_does_not_wedge() {
    const CAP: u64 = 4;
    const THREADS: usize = 8;
    const ITERS: usize = 20_000;

    let pool = pool(&[("a", CAP)]);
    // Lower bound on permits alive at once: incremented after acquire and
    // decremented before release, so `peak > CAP` is a real violation.
    let live = AtomicU64::new(0);
    let peak = AtomicU64::new(0);

    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for _ in 0..ITERS {
                    if let Ok(permit) = pool.try_acquire("g") {
                        let now = live.fetch_add(1, SeqCst) + 1;
                        peak.fetch_max(now, SeqCst);
                        live.fetch_sub(1, SeqCst);
                        permit.release();
                    }
                }
            });
        }
    });

    let peak = peak.load(SeqCst);
    assert!(
        peak <= CAP,
        "cap exceeded: {peak} permits alive at once, cap is {CAP}"
    );
    assert_eq!(pool.running("g", "a"), Some(0), "counter drifted");

    // Not wedged: the full capacity is usable again, and no more than that.
    let held: Vec<_> = (0..CAP)
        .map(|i| {
            pool.try_acquire("g")
                .unwrap_or_else(|e| panic!("wedged at slot {i}: {e:?}"))
        })
        .collect();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    drop(held);
}

// ---------------------------------------------------------------------------
// INC-2: Capacity cut.
//
// Root cause: `available()` was `max_running - running` with no guard. After a
// cut below `running` it panicked in debug builds and wrapped to a huge value in
// release, which made the over-capacity cluster look the *least* loaded. `pick`
// chose it, the increment failed, and `try_acquire` returned `Busy` without
// trying the group's other clusters.
// ---------------------------------------------------------------------------

#[test]
fn inc2_cap_cut_below_running_does_not_block_the_rest_of_the_group() {
    let pool = pool(&[("a", 40), ("b", 40)]);

    // Put 35 queries on `a`.
    pool.set_max_running("g", "b", 0);
    let _on_a: Vec<_> = (0..35).map(|_| pool.try_acquire("g").unwrap()).collect();
    pool.set_max_running("g", "b", 40);

    // Operator cuts `a` from 40 to 10 while it is busy.
    pool.set_max_running("g", "a", 10);

    let permit = pool
        .try_acquire("g")
        .expect("`b` is idle, so the group must not be Busy");
    assert_eq!(permit.cluster(), "b");
    assert_eq!(
        pool.running("g", "a"),
        Some(35),
        "running queries are not revoked"
    );
}

#[test]
fn inc2_cluster_over_its_new_cap_takes_no_traffic_until_it_drains() {
    let pool = pool(&[("a", 40)]);
    let mut held: Vec<_> = (0..35).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.set_max_running("g", "a", 10);
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    // Drain to 9 running: exactly one slot under the new cap.
    for permit in held.drain(9..) {
        permit.release(); // explicit, so this test does not depend on the INC-4 fix
    }
    assert_eq!(pool.running("g", "a"), Some(9));
    let _tenth = pool
        .try_acquire("g")
        .expect("one slot is free under the new cap");
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
}

// ---------------------------------------------------------------------------
// INC-3: Busy with free capacity.
//
// Root cause: `try_acquire` snapshotted every cluster, picked one (ties went to
// the *last* cluster, so concurrent callers all picked the same one), and made a
// single attempt. A caller that lost the race for that cluster got `Busy` even
// though other clusters had free slots.
// ---------------------------------------------------------------------------

#[test]
fn inc3_concurrent_callers_are_not_rejected_while_capacity_is_free() {
    const CLUSTERS: usize = 4;
    const MAX: u64 = 2;
    const THREADS: usize = CLUSTERS * MAX as usize; // exactly the group's capacity
    const ROUNDS: usize = 2_000;

    let names: Vec<String> = (0..CLUSTERS).map(|i| format!("c{i}")).collect();
    let pool = pool(&names.iter().map(|n| (n.as_str(), MAX)).collect::<Vec<_>>());

    let barrier = Barrier::new(THREADS);
    let busy = AtomicU64::new(0);
    let over_cap = Mutex::new(Vec::new());

    thread::scope(|s| {
        for t in 0..THREADS {
            let (pool, names, barrier, busy, over_cap) =
                (&pool, &names, &barrier, &busy, &over_cap);
            s.spawn(move || {
                for round in 0..ROUNDS {
                    barrier.wait(); // everyone starts the round together
                    let permit = pool.try_acquire("g");
                    if permit.is_err() {
                        busy.fetch_add(1, SeqCst);
                    }
                    barrier.wait(); // all attempts done, permits still held
                    if t == 0 {
                        for n in names {
                            let running = pool.running("g", n).unwrap();
                            if running > MAX {
                                over_cap.lock().unwrap().push((round, n.clone(), running));
                            }
                        }
                    }
                    barrier.wait(); // check done
                                    // Explicit release, so this test does not depend on the INC-4 fix.
                    if let Ok(permit) = permit {
                        permit.release();
                    }
                }
            });
        }
    });

    let over_cap = over_cap.into_inner().unwrap();
    assert!(
        over_cap.is_empty(),
        "clusters ran over their cap: {:?}",
        &over_cap[..over_cap.len().min(5)]
    );
    let busy = busy.load(SeqCst);
    assert_eq!(
        busy, 0,
        "{busy} callers got Busy while the group had room for all of them"
    );
}

// ---------------------------------------------------------------------------
// INC-4: Slots never recovered.
//
// Root cause: `Permit` had no `Drop`; only an explicit `release()` returned the
// slot. When a client timed out or disconnected, the request future was dropped
// with the permit inside it, and the slot leaked until restart.
// ---------------------------------------------------------------------------

#[test]
fn inc4_dropped_permit_returns_its_slot() {
    let pool = pool(&[("a", 1)]);
    {
        let _permit = pool.try_acquire("g").unwrap();
        // Dropped here without calling `release()`.
    }
    assert_eq!(pool.running("g", "a"), Some(0));
    assert!(pool.try_acquire("g").is_ok(), "slot leaked");
}

#[tokio::test(start_paused = true)]
async fn inc4_client_timeouts_during_engine_outage_do_not_leak_slots() {
    let pool = pool(&[("a", 2)]);

    // A request that is admitted, then hangs because the engine is down.
    async fn run_query(pool: Arc<ClusterPool>) {
        let _permit = pool.try_acquire("g").unwrap();
        std::future::pending::<()>().await;
    }

    // Client timeouts drop the request future, permit included.
    for _ in 0..2 {
        let timed_out =
            tokio::time::timeout(Duration::from_secs(30), run_query(Arc::clone(&pool))).await;
        assert!(timed_out.is_err());
    }
    assert_eq!(
        pool.running("g", "a"),
        Some(0),
        "client timeouts leaked slots"
    );

    // Client disconnects abort the request task.
    let task = tokio::spawn(run_query(Arc::clone(&pool)));
    tokio::task::yield_now().await;
    assert_eq!(
        pool.running("g", "a"),
        Some(1),
        "the request is admitted and hanging"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        pool.running("g", "a"),
        Some(0),
        "client disconnect leaked a slot"
    );

    // Engine is back: full capacity is available.
    assert_eq!(pool.running("g", "a"), Some(0));
    let _a = pool.try_acquire("g").expect("slot 1 leaked");
    let _b = pool.try_acquire("g").expect("slot 2 leaked");
}

#[test]
fn explicit_release_returns_the_slot_exactly_once() {
    let pool = pool(&[("a", 2)]);
    let first = pool.try_acquire("g").unwrap();
    let second = pool.try_acquire("g").unwrap();

    first.release();
    assert_eq!(
        pool.running("g", "a"),
        Some(1),
        "release() must not also decrement on drop"
    );
    drop(second);
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.admissions("g", "a"), 2);
}
