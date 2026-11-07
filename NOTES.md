# Notes

## Part 1: Incidents

| Incident | Root cause                                                                                                                                                                                                                                                                                                                                                        | Reproducing test                                                                                                                    | Fix                                                                                                                                                                   |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| INC-1    | `try_increment` was `load`, check, `store`: three separate atomic steps. Concurrent callers lost increments (more permits than the counter showed, so the cap was exceeded), and the `store` could erase a concurrent release. Releases then drove the counter below zero, `fetch_sub` wrapped it to ~`u64::MAX`, and every later acquire on that cluster failed. | `inc1_concurrent_admission_never_exceeds_cap_and_does_not_wedge`                                                                    | Check and increment in one `fetch_update` (a compare-and-swap loop). The decrement uses `checked_sub` so it can never wrap.                                           |
| INC-2    | `available()` was `max_running - running` with no guard. After a cut below `running` it panicked in debug builds (the staging crash) and wrapped to a huge value in release, so the over-capacity cluster looked the least loaded. It was picked, the increment failed, and `try_acquire` returned `Busy` without trying the other clusters.                      | `inc2_cap_cut_below_running_does_not_block_the_rest_of_the_group`, `inc2_cluster_over_its_new_cap_takes_no_traffic_until_it_drains` | `saturating_sub`. `try_acquire` falls back to the next cluster (see INC-3).                                                                                           |
| INC-3    | `try_acquire` made one attempt on one cluster. Ties went to the *last* cluster, so concurrent callers piled onto the same one, and the losers got `Busy` while other clusters had free slots.                                                                                                                                                                     | `inc3_concurrent_callers_are_not_rejected_while_capacity_is_free`                                                                   | Try every cluster, most free slots first. `Busy` only after all of them refuse.                                                                                       |
| INC-4    | `Permit` had no `Drop`; only an explicit `release()` returned the slot. Client timeouts and disconnects dropped the request future with the permit inside it, leaking the slot until restart. The racy `store` from INC-1 could also erase releases.                                                                                                              | `inc4_dropped_permit_returns_its_slot`, `inc4_client_timeouts_during_engine_outage_do_not_leak_slots`                               | `impl Drop for Permit`; `release()` just drops it. The permit holds the `Arc<ClusterState>` it was admitted to, instead of names that `release` had to look up again. |

All tests are in `tests/incidents.rs`. I ran each one against the original `src/lib.rs` (in a separate git worktree), in debug and release, and each fails there. After the fix, the whole file passed 30/30 runs in debug and 30/30 in release.

Notes on the tests:

- **INC-1 and INC-3 share code.** Both come from the racy admission path. On the original code, `inc3_…` usually hits the INC-1 counter corruption first (debug: overflow panic; release: a cluster reading `u64::MAX` running) rather than a clean `Busy`. It still fails before the fix and checks "no `Busy` while there is room" after it.
- **Debug vs release.** `inc2_cluster_over_its_new_cap…` reproduces only the debug-build panic. In release, a single-cluster group behaves correctly by accident. The production symptom (the whole group `Busy`) is reproduced by `inc2_cap_cut_below_running…` in both modes.
- **Tests are isolated.** The INC-1, INC-2 and INC-3 tests call `release()` explicitly, so they don't depend on the INC-4 fix and each fails for its own reason. `explicit_release_returns_the_slot_exactly_once` guards against a double decrement (explicit `release()` followed by `Drop`).
- **Concurrency tests are probabilistic.** They hammer the pool (8 threads, 20k iterations; 2,000 synchronized rounds) and failed every time on the original code. They can't prove the absence of a race; a `loom` model would.

Known limits after Part 1:

- **Ties go to the first cluster.** Equal free capacity still resolves to the first cluster in config order (stable sort), so concurrent callers still collide on it. Correctness is fine thanks to the fallback; spreading ties out is a Part 2 concern.
- **`Busy` is not decided at a single instant.** It means each cluster was full when we tried it, which is the honest guarantee without a group-wide lock.
- **Metrics key collision.** `admissions` still uses a global `Mutex` and `"{group}/{cluster}"` keys, so group `a/b` + cluster `c` and group `a` + cluster `b/c` share a counter. To fix in Part 2.

## Part 2: Performance

Machine (CPU and core count):

| Bench | Before | After |
|-------|--------|-------|
| acquire_release/1  | | |
| acquire_release/4  | | |
| acquire_release/16 | | |

Changes, and why each one helps (or didn't):

-

Selection policy, and why:

## Part 3: Waiting for a slot

## Part 4: Global admission across 6 pods

## Bonus: INC-5 and INC-6

## Before shipping, I would still want

-
