//! INC-5: `reconcile` made the counters *less* accurate under load.
//!
//! These are deterministic interleaving tests. The engine is a pair of channels:
//! the fetch closure signals `started` the moment `reconcile` calls it, then waits
//! for the test to send the engine's count. That pins the test's traffic to
//! "while the slow fetch is in flight", which is exactly the window the bug lives
//! in, without sleeps or retries.

use std::sync::Arc;
use std::time::Duration;

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

fn config(clusters: &[(&str, u64)]) -> PoolConfig {
    PoolConfig::default().group(
        "g",
        clusters
            .iter()
            .map(|(name, max)| ClusterConfig::new(*name, *max))
            .collect(),
    )
}

fn pool(max_running: u64) -> Arc<ClusterPool> {
    ClusterPool::new(config(&[("a", max_running)]))
}

/// The test's side of a fake engine for one `reconcile` call.
struct Engine {
    /// Fires when `reconcile` calls `fetch`, i.e. once it has taken its snapshot
    /// and is about to wait on the engine.
    started: oneshot::Receiver<()>,
    /// The engine's answer: how many queries it is running.
    answer: oneshot::Sender<u64>,
}

/// Start `reconcile("g", "a")` on a task, with a fetch the test controls.
fn spawn_reconcile(pool: &Arc<ClusterPool>) -> (JoinHandle<()>, Engine) {
    let (started_tx, started) = oneshot::channel();
    let (answer, answer_rx) = oneshot::channel();
    let pool = Arc::clone(pool);
    let task = tokio::spawn(async move {
        pool.reconcile("g", "a", move || {
            let _ = started_tx.send(());
            async move { answer_rx.await.expect("test dropped the engine") }
        })
        .await;
    });
    (task, Engine { started, answer })
}

/// Leak `n` permits: the counter keeps them, nothing will ever release them.
/// This is the INC-1-style drift `reconcile` exists to correct.
fn leak(pool: &Arc<ClusterPool>, n: usize) {
    for _ in 0..n {
        std::mem::forget(pool.try_acquire("g").unwrap());
    }
}

#[tokio::test]
async fn reconcile_keeps_admissions_and_releases_made_during_the_fetch() {
    let pool = pool(100);
    let mut held: Vec<_> = (0..10).map(|_| pool.try_acquire("g").unwrap()).collect();

    let (task, engine) = spawn_reconcile(&pool);
    engine.started.await.unwrap(); // reconcile is now waiting on the slow engine

    // Traffic continues while the fetch is in flight: 5 admitted, 3 finish.
    held.extend((0..5).map(|_| pool.try_acquire("g").unwrap()));
    held.drain(..3);
    assert_eq!(pool.running("g", "a"), Some(12));

    // The engine counted the 10 queries that were running when it was asked.
    engine.answer.send(10).unwrap();
    task.await.unwrap();

    assert_eq!(
        pool.running("g", "a"),
        Some(12),
        "reconcile threw away the admissions and releases made during the fetch"
    );
    drop(held);
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[tokio::test]
async fn reconcile_corrects_drift_while_traffic_continues() {
    let pool = pool(100);
    leak(&pool, 3); // counter says 3 too many
    let mut held: Vec<_> = (0..10).map(|_| pool.try_acquire("g").unwrap()).collect();
    assert_eq!(pool.running("g", "a"), Some(13));

    let (task, engine) = spawn_reconcile(&pool);
    engine.started.await.unwrap();

    held.extend((0..5).map(|_| pool.try_acquire("g").unwrap()));
    held.drain(..2);

    // The engine sees the 10 real queries from when it was asked.
    engine.answer.send(10).unwrap();
    task.await.unwrap();

    assert_eq!(
        pool.running("g", "a"),
        Some(13),
        "expected the 3 leaked slots removed and the 13 live permits kept"
    );
    drop(held);
    assert_eq!(pool.running("g", "a"), Some(0), "the drift came back");
}

#[tokio::test]
async fn reconcile_still_corrects_drift_on_a_quiet_cluster() {
    let pool = pool(10);
    leak(&pool, 4);
    let _held: Vec<_> = (0..2).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.reconcile("g", "a", || async { 2 }).await;

    assert_eq!(pool.running("g", "a"), Some(2));
    let more: Vec<_> = (0..8)
        .map(|_| pool.try_acquire("g").expect("leaked slots were recovered"))
        .collect();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    drop(more);
}

#[tokio::test]
async fn reconcile_counts_queries_the_engine_runs_for_someone_else() {
    let pool = pool(10);
    let held: Vec<_> = (0..2).map(|_| pool.try_acquire("g").unwrap()).collect();

    // The engine runs 3 queries this gateway did not admit.
    pool.reconcile("g", "a", || async { 5 }).await;
    assert_eq!(pool.running("g", "a"), Some(5));

    let more: Vec<_> = (0..5).map(|_| pool.try_acquire("g").unwrap()).collect();
    assert_eq!(
        pool.try_acquire("g").err(),
        Some(AcquireError::Busy),
        "the cap must include the engine's other queries"
    );

    // Our queries finish; the engine's other 3 are assumed to run until the next
    // reconcile says otherwise.
    drop(held);
    drop(more);
    assert_eq!(pool.running("g", "a"), Some(3));
    pool.reconcile("g", "a", || async { 0 }).await;
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[tokio::test]
async fn concurrent_reconciles_apply_the_correction_once() {
    let pool = pool(100);
    leak(&pool, 4);
    let held: Vec<_> = (0..6).map(|_| pool.try_acquire("g").unwrap()).collect();

    let (first, first_engine) = spawn_reconcile(&pool);
    first_engine.started.await.unwrap();

    // A second reconcile for the same cluster starts while the first is waiting.
    let (second, second_engine) = spawn_reconcile(&pool);
    for _ in 0..10 {
        tokio::task::yield_now().await; // let it run as far as it can
    }

    first_engine.answer.send(6).unwrap();
    first.await.unwrap();
    second_engine.started.await.unwrap();
    second_engine.answer.send(6).unwrap();
    second.await.unwrap();

    assert_eq!(
        pool.running("g", "a"),
        Some(6),
        "the 4 leaked slots were corrected twice"
    );
    drop(held);
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[tokio::test]
async fn cancelled_reconcile_changes_nothing_and_does_not_block_the_next() {
    let pool = pool(100);
    let _held: Vec<_> = (0..5).map(|_| pool.try_acquire("g").unwrap()).collect();

    let (task, engine) = spawn_reconcile(&pool);
    engine.started.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(pool.running("g", "a"), Some(5));

    let (next, next_engine) = spawn_reconcile(&pool);
    tokio::time::timeout(Duration::from_secs(5), next_engine.started)
        .await
        .expect("a cancelled reconcile left the cluster locked")
        .unwrap();
    next_engine.answer.send(5).unwrap();
    next.await.unwrap();
    assert_eq!(pool.running("g", "a"), Some(5));
}

#[tokio::test]
async fn reconcile_for_an_unknown_cluster_is_a_no_op() {
    let pool = pool(10);
    let _held = pool.try_acquire("g").unwrap();

    pool.reconcile("g", "nope", || async { 7 }).await;
    pool.reconcile("nope", "a", || async { 7 }).await;

    assert_eq!(pool.running("g", "a"), Some(1));
}

#[tokio::test]
async fn reconcile_spanning_a_reload_never_touches_the_new_cluster() {
    let pool = pool(10);
    let held = pool.try_acquire("g").unwrap();

    let (task, engine) = spawn_reconcile(&pool);
    engine.started.await.unwrap();

    // `a` is removed while the fetch is in flight, and `b` is added.
    pool.reload(config(&[("b", 10)]));
    let on_b = pool.try_acquire("g").unwrap();
    assert_eq!(on_b.cluster(), "b");

    engine.answer.send(0).unwrap();
    task.await.unwrap();

    assert_eq!(pool.running("g", "a"), None);
    assert_eq!(
        pool.running("g", "b"),
        Some(1),
        "a correction meant for `a` landed on `b`"
    );
    drop(held);
    drop(on_b);
}

/// The brief's constraint for the bonus: `try_acquire` must not take an exclusive
/// lock. `reconcile` now holds one (to serialize corrections), so prove the hot
/// path is not blocked by it: a reconcile parked on a slow engine holds the
/// per-cluster mutex for the whole time, and admissions keep working.
#[tokio::test]
async fn try_acquire_is_not_blocked_by_a_reconcile_holding_its_lock() {
    let pool = pool(100);
    let mut held: Vec<_> = (0..3).map(|_| pool.try_acquire("g").unwrap()).collect();

    let (task, engine) = spawn_reconcile(&pool);
    engine.started.await.unwrap(); // mutex held, fetch in flight

    // Hot path still works while the exclusive lock is held. The reconcile
    // snapshotted 3 before the fetch, so its answer is 3 and the delta is zero.
    let admitted = tokio::time::timeout(Duration::from_secs(1), async {
        for _ in 0..50 {
            held.push(pool.try_acquire("g").unwrap());
        }
    })
    .await;
    assert!(
        admitted.is_ok(),
        "try_acquire blocked behind the reconcile lock"
    );
    assert_eq!(pool.running("g", "a"), Some(53));

    engine.answer.send(3).unwrap();
    task.await.unwrap();
    assert_eq!(
        pool.running("g", "a"),
        Some(53),
        "the correction changed nothing"
    );
}
