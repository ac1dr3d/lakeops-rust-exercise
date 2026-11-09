//! A `loom` model of the counter protocol in `admission::protocol`.
//!
//! Run it with:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test --release --test loom_model
//! ```
//!
//! The `#![cfg(loom)]` below is why a normal `cargo test` compiles this file to an
//! empty crate: `loom` is a `[target.'cfg(loom)'.dev-dependencies]` entry, so without
//! the flag it is not in the dependency graph at all (`cargo tree` does not list it),
//! and keeping it out is what saves every normal build ~39 transitive crates.
//!
//! What is modelled: the three transitions in `protocol` (`admit`, `release`,
//! `correct`) and the CAS wrapper that applies them, interleaved across real
//! scheduling decisions, exhaustively for the small thread counts used here.
//!
//! What is not: `tokio::sync::Mutex` (so `reconcile`'s one-at-a-time serialization is
//! proven by `tests/reconcile.rs` instead), the per-thread topology cache
//! (`thread_local!` is not modelled by loom), and `Arc` refcounts.
//!
//! Two threads, not three: with three, CAS retry loops make the state space explode
//! (a three-thread version did not finish in ten minutes). A one-word CAS protocol is
//! exhausted by two threads plus loom's own preemption.
#![cfg(loom)]

use admission::protocol::{self, Counter};
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

/// `protocol::Counter` for loom's atomic. The orphan rule forbids implementing a
/// foreign trait for a foreign type, hence the wrapper; this is the only piece of
/// the protocol that exists in the model rather than in `src/`.
struct LoomCounter(AtomicU64);

impl Counter for LoomCounter {
    fn update<F>(&self, f: F) -> Result<u64, u64>
    where
        F: FnMut(u64) -> Option<u64>,
    {
        self.0.try_update(Ordering::Relaxed, Ordering::Relaxed, f)
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn counter(start: u64) -> Arc<LoomCounter> {
    Arc::new(LoomCounter(AtomicU64::new(start)))
}

/// INC-1: concurrent admission must never hand out more than the cap, and the
/// counter must end up agreeing with the permits actually granted.
#[test]
fn concurrent_admission_respects_the_cap() {
    const MAX: u64 = 2;
    loom::model(|| {
        let counter = counter(0);
        let mut handles = Vec::new();
        for _ in 0..2 {
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                let mut granted = 0;
                for _ in 0..2 {
                    if protocol::try_admit(&*counter, MAX) {
                        granted += 1;
                    }
                }
                granted
            }));
        }
        let granted: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();

        assert!(
            granted <= MAX,
            "{granted} permits granted for a cap of {MAX}"
        );
        assert_eq!(
            counter.get(),
            granted,
            "counter disagrees with the permits granted"
        );
    });
}

/// INC-1: one permit, one release and one downward correction. Both want to
/// decrement, and the second must find nothing to take rather than wrapping to
/// ~`u64::MAX` and wedging the cluster.
#[test]
fn release_racing_a_correction_does_not_wrap() {
    loom::model(|| {
        let counter = counter(1);

        let releasing = {
            let counter = counter.clone();
            thread::spawn(move || protocol::release_one(&*counter))
        };
        let correcting = {
            let counter = counter.clone();
            // The engine saw this one query; by the time it answers there are none.
            thread::spawn(move || protocol::correct_by(&*counter, 1, 0))
        };
        releasing.join().unwrap();
        correcting.join().unwrap();

        assert_eq!(
            counter.get(),
            0,
            "one permit taken twice must saturate at zero, not wrap"
        );
    });
}

/// INC-5: a correction whose answer matches its own snapshot must change nothing,
/// whatever the interleaving. Storing the engine's answer (instead of applying the
/// delta) wipes out an admission that landed after the snapshot, and fails here.
#[test]
fn correction_keeps_admissions_made_after_the_snapshot() {
    loom::model(|| {
        let counter = counter(0);

        let correcting = {
            let counter = counter.clone();
            thread::spawn(move || {
                // Whatever the engine would report is what the snapshot sees.
                let before = counter.get();
                protocol::correct_by(&*counter, before, before);
            })
        };
        let admitting = {
            let counter = counter.clone();
            thread::spawn(move || assert!(protocol::try_admit(&*counter, 10)))
        };
        correcting.join().unwrap();
        admitting.join().unwrap();

        assert_eq!(
            counter.get(),
            1,
            "the correction lost an admission made during the fetch"
        );
    });
}
