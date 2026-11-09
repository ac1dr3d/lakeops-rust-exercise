//! The counter protocol: the only three ways `running` ever changes.
//!
//! This exists as a separate, generic seam so the `loom` model in
//! `tests/loom_model.rs` can drive **this** code with loom's atomics, instead of
//! reimplementing the transitions and drifting from what ships.
//!
//! Marked `#[doc(hidden)]` on the module declaration in `src/lib.rs`: it is a
//! test seam, not public API. `Counter` is implemented for `std`'s `AtomicU64`
//! below and for a thin loom wrapper in `tests/loom_model.rs`.

use std::sync::atomic::{AtomicU64, Ordering};

/// A counter that can be updated atomically. `update` is the compare-and-swap
/// loop; it must never be split into a load and a store (see INC-1).
pub trait Counter {
    /// Atomically apply `f` to the current value. `Ok(previous)` if `f` returned
    /// `Some`, `Err(current)` if it returned `None` (no change made).
    fn update<F>(&self, f: F) -> Result<u64, u64>
    where
        F: FnMut(u64) -> Option<u64>;

    /// Current value.
    fn get(&self) -> u64;
}

impl Counter for AtomicU64 {
    #[inline]
    fn update<F>(&self, f: F) -> Result<u64, u64>
    where
        F: FnMut(u64) -> Option<u64>,
    {
        self.try_update(Ordering::Relaxed, Ordering::Relaxed, f)
    }

    #[inline]
    fn get(&self) -> u64 {
        self.load(Ordering::Relaxed)
    }
}

/// Admission transition: `Some(next)` while under the cap, `None` at it.
#[inline]
pub fn admit(current: u64, max: u64) -> Option<u64> {
    (current < max).then_some(current + 1)
}

/// Release transition: `Some(next)`, or `None` at zero, so it never wraps.
#[inline]
pub fn release(current: u64) -> Option<u64> {
    current.checked_sub(1)
}

/// Reconcile transition: shift `current` by `engine - before`, saturating at zero.
#[inline]
pub fn correct(current: u64, before: u64, engine: u64) -> u64 {
    if engine >= before {
        current.saturating_add(engine - before)
    } else {
        current.saturating_sub(before - engine)
    }
}

/// Take one slot if the cluster is under its cap (INC-1: one RMW, not two steps).
#[inline]
pub fn try_admit<C: Counter + ?Sized>(counter: &C, max: u64) -> bool {
    counter.update(|current| admit(current, max)).is_ok()
}

/// Return one slot; never wraps below zero (INC-1).
#[inline]
pub fn release_one<C: Counter + ?Sized>(counter: &C) {
    let _ = counter.update(release);
}

/// Apply a reconcile delta as one read-modify-write, so traffic during the
/// fetch is kept (INC-5).
#[inline]
pub fn correct_by<C: Counter + ?Sized>(counter: &C, before: u64, engine: u64) {
    let _ = counter.update(|current| Some(correct(current, before, engine)));
}
