# Notes

## Part 1: Incidents

| Incident | Root cause                                                                                                                                                                                                                                                                                                                                                        | Reproducing test                                                                                                                    | Fix                                                                                                                                                                   |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| INC-1    | `try_increment` was `load`, check, `store`: three separate atomic steps. Concurrent callers lost increments (more permits than the counter showed, so the cap was exceeded), and the `store` could erase a concurrent release. Releases then drove the counter below zero, `fetch_sub` wrapped it to ~`u64::MAX`, and every later acquire on that cluster failed. | `inc1_concurrent_admission_never_exceeds_cap_and_does_not_wedge`                                                                    | Check and increment in one `fetch_update` (a compare-and-swap loop). The decrement uses `checked_sub` so it can never wrap.                                           |
| INC-2    | `available()` was `max_running - running` with no guard. After a cut below `running` it panicked in debug builds (the staging crash) and wrapped to a huge value in release, so the over-capacity cluster looked the least loaded. It was picked, the increment failed, and `try_acquire` returned `Busy` without trying the other clusters.                      | `inc2_cap_cut_below_running_does_not_block_the_rest_of_the_group`, `inc2_cluster_over_its_new_cap_takes_no_traffic_until_it_drains` | `saturating_sub`. `try_acquire` falls back to the next cluster (see INC-3).                                                                                           |
| INC-3    | `try_acquire` made one attempt on one cluster. Ties went to the *last* cluster, so concurrent callers piled onto the same one, and the losers got `Busy` while other clusters had free slots.                                                                                                                                                                     | `inc3_concurrent_callers_are_not_rejected_while_capacity_is_free`                                                                   | Try every cluster, most free slots first. `Busy` only after all of them refuse.                                                                                       |
| INC-4    | `Permit` had no `Drop`; only an explicit `release()` returned the slot. Client timeouts and disconnects dropped the request future with the permit inside it, leaking the slot until restart. The racy `store` from INC-1 could also erase releases.                                                                                                              | `inc4_dropped_permit_returns_its_slot`, `inc4_client_timeouts_during_engine_outage_do_not_leak_slots`                               | `impl Drop for Permit`; `release()` just drops it. The permit holds the `Arc<ClusterState>` it was admitted to, instead of names that `release` had to look up again. |

All tests are in `tests/incidents.rs`. I ran each one against the original `src/lib.rs` (in a separate git worktree), in debug and release, and every test file fails there (7 of 7 in `incidents.rs`). After the fix, the whole file passed 30/30 runs in debug and 30/30 in release.

Notes on the tests:

- **INC-1 and INC-3 share code.** Both come from the racy admission path. On the original code, `inc3_…` usually hits the INC-1 counter corruption first (debug: overflow panic; release: a cluster reading `u64::MAX` running) rather than a clean `Busy`. It still fails before the fix and checks "no `Busy` while there is room" after it.
- **Debug vs release.** `inc2_cluster_over_its_new_cap…` reproduces only the debug-build panic. In release, a single-cluster group behaves correctly by accident. The production symptom (the whole group `Busy`) is reproduced by `inc2_cap_cut_below_running…` in both modes.
- **Tests are isolated.** The INC-1, INC-2 and INC-3 tests call `release()` explicitly, so they don't depend on the INC-4 fix and each fails for its own reason. `explicit_release_returns_the_slot_exactly_once` guards against a double decrement (explicit `release()` followed by `Drop`).
- **Concurrency tests are probabilistic.** They hammer the pool (8 threads, 20k iterations; 2,000 synchronized rounds) and failed every time on the original code. They can't prove the absence of a race; a `loom` model would.

Known limits after Part 1:

- **Ties go to the first cluster.** Equal free capacity still resolves to the first cluster in config order (stable sort), so concurrent callers still collide on it. Correctness is fine thanks to the fallback; spreading ties out is a Part 2 concern.
- **`Busy` is not decided at a single instant.** It means each cluster was full when we tried it, which is the honest guarantee without a group-wide lock.
- **Metrics key collision.** `admissions` used a global `Mutex` and `"{group}/{cluster}"` keys, so group `a/b` + cluster `c` and group `a` + cluster `b/c` shared a counter. Fixed in Part 2 by keeping the counter on the cluster state.

## Part 2: Performance

Machine: Intel i5-1135G7, 4 cores / 8 hardware threads (so `acquire_release/16` is
oversubscribed). Baseline saved from the untouched starter with
`cargo bench -- --save-baseline starter`; compare with `--baseline starter`.

| Bench | Before | After | Change |
|-------|--------|-------|--------|
| acquire_release/1  | 1.95 M/s (513 ns) | 16.3 M/s (61 ns) | 8.4x |
| acquire_release/4  | 3.71 M/s (1.08 µs) | 23.2 M/s (173 ns) | 6.2x |
| acquire_release/16 | 4.74 M/s (3.38 µs) | **29.3 M/s (546 ns)** | **6.2x** (target was 3x) |

Absolute numbers move 5-10% between runs on this laptop, so the deltas below were
each measured back to back. `p = 0.00` on the starter comparison.

I added two benchmarks because the starter's `acquire + release` loop keeps
`running` near zero, which is not what a gateway looks like:

| Bench | `/1` | `/4` | `/16` |
|-------|------|------|-------|
| acquire_release (starter shape) | 16.6 M/s | 23.4 M/s | 30.0 M/s |
| acquire_release_held (100 permits held per thread) | 16.7 M/s | 21.5 M/s | 25.9 M/s |
| acquire_release_mixed (capacities 1M >> i, eight clusters) | 16.2 M/s | 19.1 M/s | 20.5 M/s |

`mixed` is the slowest and that is expected: with wildly unequal capacities, a
capacity-following policy sends nearly all traffic to the biggest cluster, so every
core writes the same counter. It is still 4.3x the starter on that shape. In
production groups the capacities are within a small factor of each other, which is
the `acquire_release` shape.

### Changes, and why each one helps (or didn't)

Each row is one change and one bench run, in the order I made them. Rows 1-6 and
7-8 are two separate lineages (`6a` was later replaced by `6b`), so only compare
within a row.

| # | Change | `/1` | `/16` | Why |
|---|--------|------|-------|-----|
| 0 | starter | 513 ns | 4.74 M/s | baseline |
| 1 | Part 1 correctness fixes | 228 ns | 2.57 M/s | **Faster single-threaded, slower under load.** A CAS loop retries where the old `store` never did, and the mutex it replaced *queued* threads while the CAS loop makes them fight over one cache line. I did not profile this; it is the most likely reading of the numbers. Correctness first, and the rest of the list is what recovers the throughput. |
| 2 | spread the tie-break (`ROTATION`) | 233 ns | 2.55 M/s | **Measured: no effect.** A global mutex was still serializing the call, so nothing about selection could show up. Kept because it is what stops callers converging on one cluster once the shared writes are gone. |
| 3 | per-cluster `admissions: AtomicU64`, drop the global `Mutex<HashMap>` + `format!` | 161 ns | 4.70 M/s | Removes a lock acquisition, a `String` allocation and a `HashMap` lookup per admission. Also fixes the key collision where group `a/b` + cluster `c` and group `a` + cluster `b/c` shared one counter. |
| 4 | `Arc<[Arc<ClusterState>]>` per group + lookup by `&str` | 69 ns | 8.31 M/s | Removed 2 heap allocations (`group.to_string()`, `Vec` clone) and 16 refcount operations on cluster state per cycle (8 clone + 8 drop), at the cost of 2 on the group slice: 14 net. Each bump is a write to a shared cache line. Largest single-threaded win. |
| 5 | power of two choices instead of a full scan | 59 ns | 25.6 M/s | The scan read both counters of all 8 clusters: 16 loads from lines other cores were writing. Two samples need 4. |
| 6a | `ArcSwap` for the topology | 54 ns | 25.4 M/s | Read the topology without taking a lock. `RwLock::read` is a read-modify-write on a shared word, and cloning the group `Arc` per call was another shared write. **`ArcSwap::load` is not a plain load**: it swaps the pointer into a per-thread hazard slot (`SeqCst`), which is why it is fast — the write is to memory no other core shares. |
| 6b | per-thread topology snapshot, std only (replaces 6a) | 43 ns | 27.0 M/s | Same idea without the dependency: a thread-local `Arc` plus a version counter that only `reload` writes. A hit reads one word that every core keeps cached and writes nothing; a miss takes the `RwLock` once. Measured faster than 6a in the same session (27.0 vs 25.4 M/s). |
| 7 | fix the tie-break pairing bug | 60 ns | 30.0 M/s | Final run, with the bug below fixed. |

Two things worth keeping:

- **Dropping a capability probe entirely is faster and wrong.** Replacing selection
  with pure rotation (no capacity comparison at all) measured **37 M/s** at `/16`.
  That is the fastest number I saw, but it ignores free capacity, so it is not a
  policy I would ship. It is how I learned the scan, not the atomics, was the
  remaining limit.
- **The `admissions` counter shares a cache line with `running`.** They are written
  by the same caller in the same call, so I did not pad it; I have not measured
  whether padding helps.

### A bug the review caught, and the test that now guards it

My first tie-break drew `start` and `second` from a per-thread counter that
advanced by one **per draw, two draws per call**. So `start` was always
`odd % n`. For any even group size that means even-indexed clusters were never
preferred on a tie: with 8 clusters and one query in flight, clusters 0, 2, 4 and 6
got **zero** admissions out of 8,000, and 64 freshly spawned threads all picked the
same cluster on their first call.

`tests/selection.rs` now checks that sequential traffic on 2, 3, 4 and 8 equal
clusters spreads within a factor of two, that fresh threads fan out, that a cluster
with 10x the capacity takes most of the load, and that filling a group uses every
cluster. The fix is one SplitMix64 draw per call, seed per thread, low 32 bits for
the first sample and high 32 bits for the second, so both are independent and
uniform, which is what the power-of-two-choices bound actually assumes.

### Selection policy, and why

1. **Fast path: power of two choices.** Sample two clusters uniformly at random and
   try the one with more free slots. P2C is the standard approximation of
   least-loaded: capacity-aware, provably close to optimal spread, two probes
   instead of a scan of every cluster.
2. **The atomic increment decides, not the sample.** Free-slot counts only choose
   what to try, so a stale read costs a retry, never a cap violation.
3. **Slow path, only when the sampled cluster is full: try every other cluster
   once.** This keeps the INC-3 guarantee: `Busy` means every cluster in the group
   refused, never "the two I sampled were full". It costs a scan, but only on the
   path that was about to return `Busy`.
4. **Ties are uniform per call**, not per thread, so callers fan out instead of
   queueing behind one cluster.

Known trade-off: following capacity means unequal capacities concentrate traffic on
the largest cluster (see `acquire_release_mixed`), which costs throughput under
contention. The alternative is a fixed round-robin, which is faster per call but
ignores capacity.

### No dependency added

I tried `arc-swap` first (row 6a) and then replaced it with the std-only per-thread
snapshot above, which measured faster and removed the dependency. `reload` now takes
the write lock, builds the new topology once and bumps the version while holding the
lock. That is also why there is no read-copy-update retry closure: the earlier
`ArcSwap::rcu` version updated live caps *inside* a closure that can re-run on
contention, which could clobber a concurrent `set_max_running`. The cost of the std
version is that a thread caches one pool, so a process serving two pools refreshes on
every call (still correct, just slower); a gateway has one pool.

### Reload preserves cluster identity

Moving the metric onto the cluster state made `reload` a correctness problem: it
used to rebuild every `ClusterState`, which would have reset both `running` and
`admissions` on every config reload (counts to zero, and a cluster with in-flight
queries advertising full capacity again). `build_groups` now reuses the existing
state for each `(group, name)` and only updates the cap. `tests/reload.rs` covers
that, cap changes in both directions, added/removed clusters, concurrent reloads, and
that a thread which already cached the old topology picks up a reload on its next
call. Six of the seven tests fail on the previous implementation; the metric test
passes there by accident, because that version kept metrics in a map that survived
reload.

Numbers to re-check before shipping: measured on a 4-core laptop with 16 threads
oversubscribed, so the ceiling on a 16-core pod is higher, but the ordering should
hold. Everything is `Relaxed` atomics in CAS loops, which is the cheapest correct
choice for a counter; if a profile ever shows the per-cluster line as the limit, the
next step is to shard the counter, not to add fences.

## Part 3: Waiting for a slot

## Part 4: Global admission across 6 pods

## Bonus: INC-5 and INC-6

### INC-6: reload (fixed)

Two symptoms: clusters briefly ran above their caps, and dashboards showed running
counts reset to zero.

Both came from `reload` rebuilding every `ClusterState` with `running = 0`:
- a cluster with 35 queries in flight came back advertising 40 free slots, and the
  next 40 admittances were granted on top of the 35 already running;
- every counter an operator was watching went to zero;
- in-flight `Permit`s then released against the *new* counters, driving them below
  zero and wrapping them to ~`u64::MAX` (the INC-1 wedge, reachable from a plain
  config reload).

What it does now: a cluster is identified by `(group, name)` across reloads, so the
existing state object is reused and only `max_running` changes. Removed clusters
leave the config and take no new traffic; a `Permit` for one keeps its own state
alive, so its release can never touch a different cluster. Concurrent reloads are
serialized by the write lock, so the build happens once and cannot lose a swap.

Guarantees: a reload never changes `running`, never resets `admissions`, never
admits past the new cap, and never writes a cap that a concurrent
`set_max_running` did not ask for.

Not done: a cluster removed from the config drops out immediately even if it still
has queries in flight, so a re-added cluster starts from zero while old queries may
still be running. Draining a removed cluster (keep it in a "closed" state until its
permits return) is the honest fix; I would want it before shipping.

### INC-5: reconcile (fixed)

Symptom: the job meant to correct drift made the counters *less* accurate under load.

Root cause: `reconcile` stored the engine's answer after awaiting it.

```rust
let target = fetch().await;                                // slow: seconds, under load
if let Some(state) = self.find(group, cluster) {
    state.running.store(target, Ordering::Relaxed);        // overwrites everything since
}
```

`fetch` can take a long time, and the gateway keeps admitting and releasing while it
waits. The answer describes the engine at the moment it was asked, but `store`
replaced the current value with it, discarding every admission and release in
between. The more traffic, the more work there is to throw away, which is why it got
worse exactly when it mattered. If the engine reported fewer queries than had been
admitted since the snapshot, the next admits overshot the cap; if it reported more,
the cluster looked busy with free slots.

Fix: apply the engine's answer as a **delta**, captured before the fetch.

```rust
let Some(state) = self.find(group, cluster) else { return };
let _serialized = state.reconcile.lock().await;   // one reconcile per cluster
let before = state.running();                     // snapshot before asking
let engine = fetch().await;
state.correct(before, engine);                    // atomic: current + (engine - before)
```

`correct` is a single `try_update`: if the engine's count went up, it adds
`engine - before` to whatever the counter is *now*; if it went down, it subtracts
`saturating_sub`, so it can never wrap below zero. Traffic that happened during the
fetch is preserved because only the difference is applied.

Four further decisions in that function:

- **The cluster is resolved before the fetch, not after.** A correction can only
  land on the counter it was measured against, even if a reload removes or re-adds
  that cluster while the engine is being asked. (Fixing this is why the test that
  spans a reload passes.)
- **One reconcile per cluster at a time** (`tokio::sync::Mutex` on the cluster
  state). Two overlapping runs would each apply the same drift, so the second waits.
  The lock is only taken by `reconcile`; `try_acquire` never touches it, so the hot
  path is unaffected as the brief requires, and it is an async mutex, so waiting does
  not block a runtime worker.
- **Cancelling is harmless.** A timeout around a hung engine drops the future before
  the adjustment; the counter is untouched and the lock is released.
- **Saturation, not clamping.** The counter may end up above `max_running` (the
  engine may be running queries this gateway did not admit, e.g. a second pod);
  the cluster then takes no new queries until it drains. It cannot go below zero.

What the guarantee still is *not*: the delta assumes `engine - before` stayed
constant while the answer was in flight. It did not have to. Any change in the
engine's own in-flight count during that window is still lost — a query that
finished at the engine before it answered, or one the engine started for someone
else, is not visible in either number. So the residual error is bounded by the
number of queries the engine itself started or finished inside one fetch round trip,
per reconcile, and under sustained load where queries finish faster than the fetch
that error can persist rather than vanish. Removing it entirely needs the engine to
report identifiably-versioned counts, or a read that is atomic with respect to the
hot path, which the brief rules out for `try_acquire`. Being bounded and
self-correcting on the next pass is the honest guarantee; "exact" is not available
here.

Tests: `tests/reconcile.rs`, 8 tests, all deterministic interleavings rather than
sleeps. The engine is a pair of channels: the fetch closure signals the test the
moment it is called, then waits for the test to send its answer, so the test's
traffic lands exactly inside the fetch window.

| Test | On the old code |
|------|-----------------|
| `reconcile_keeps_admissions_and_releases_made_during_the_fetch` | **fails** (12 vs 10) |
| `reconcile_corrects_drift_while_traffic_continues` | **fails** (drift reapplied) |
| `reconcile_still_corrects_drift_on_a_quiet_cluster` | passes (the easy case) |
| `reconcile_counts_queries_the_engine_runs_for_someone_else` | passes |
| `concurrent_reconciles_apply_the_correction_once` | passes; **fails if the per-cluster lock is removed** ("corrected twice") |
| `cancelled_reconcile_changes_nothing_and_does_not_block_the_next` | passes |
| `reconcile_for_an_unknown_cluster_is_a_no_op` | passes |
| `reconcile_spanning_a_reload_never_touches_the_new_cluster` | passes |

The last five are behaviour I chose and want to keep; they passed on the old code
too, so I check them by breaking the new code on purpose (removing the lock, or the
resolve-before-fetch ordering) rather than by comparing against the starter.

Not done: a `loom` model. The correction is a single CAS on one word, so there is no
interleaving left in it to model; the interesting interleaving is *between* two
reconciles, which the per-cluster mutex serializes by construction.

## Before shipping, I would still want

-
