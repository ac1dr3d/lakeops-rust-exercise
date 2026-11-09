//! Cluster admission control for a SQL gateway.
//!
//! A gateway fronts several *groups* of engine clusters (Trino, StarRocks, ...).
//! Every incoming query must be admitted to one cluster of its target group
//! before it is forwarded, and each cluster accepts at most `max_running`
//! concurrent queries. The gateway handles thousands of queries per second,
//! so `try_acquire` sits on the hot path of every request.
//!
//! See README.md for the tasks.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub name: String,
    pub max_running: u64,
}

impl ClusterConfig {
    pub fn new(name: impl Into<String>, max_running: u64) -> Self {
        Self {
            name: name.into(),
            max_running,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolConfig {
    pub groups: HashMap<String, Vec<ClusterConfig>>,
}

impl PoolConfig {
    pub fn group(mut self, name: impl Into<String>, clusters: Vec<ClusterConfig>) -> Self {
        self.groups.insert(name.into(), clusters);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// The group does not exist in the current config.
    UnknownGroup,
    /// Every cluster in the group is at capacity.
    Busy,
    /// `acquire_timeout` waited for its full duration without getting a slot.
    Timeout,
}

#[doc(hidden)]
pub mod protocol;

/// Wakes callers parked in `acquire_timeout` for one group.
///
/// One per group, shared by every `ClusterState` in it, so a releasing permit can
/// wake a waiter without a lookup, an allocation or an extra reference on the hot
/// path. Identity follows the group name across `reload`, which is what keeps a
/// waiter reachable after the config swaps underneath it.
#[derive(Debug, Default)]
struct GroupWake {
    notify: tokio::sync::Notify,
    queued: AtomicU64,
}

impl GroupWake {
    /// A release happened: wake one waiter, if anyone is waiting.
    ///
    /// Reading `queued` is a plain load of a word nobody writes while the group is
    /// idle, so this costs a load and no invalidation on the hot path.
    fn wake_one(&self) {
        if self.queued.load(Ordering::Relaxed) > 0 {
            self.notify.notify_one();
        }
    }

    /// Capacity was added or recovered by an admin path (`set_max_running`,
    /// `reload`, or a downward `reconcile`), which can open several slots at once:
    /// wake every parked waiter so they can compete for the new slots immediately.
    fn wake_all(&self) {
        if self.queued.load(Ordering::Relaxed) > 0 {
            self.notify.notify_waiters();
        }
    }

    fn queued(&self) -> u64 {
        self.queued.load(Ordering::Relaxed)
    }
}

/// Counts a caller as queued for as long as it lives, including on cancellation,
/// which is what makes `queued()` honest for a dropped wait future.
struct Queued<'a> {
    wake: &'a GroupWake,
}

impl<'a> Queued<'a> {
    fn new(wake: &'a GroupWake) -> Self {
        wake.queued.fetch_add(1, Ordering::Relaxed);
        Self { wake }
    }
}

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        self.wake.queued.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ClusterState {
    name: String,
    max_running: AtomicU64,
    running: AtomicU64,
    /// Queries ever admitted to this cluster (a Prometheus metric). Per cluster,
    /// so the hot path never touches a shared lock (see `admissions()`).
    admissions: AtomicU64,
    /// Serializes `reconcile` for this cluster (INC-5). Never taken on the hot path.
    reconcile: tokio::sync::Mutex<()>,
    /// The group's wake object, so `Permit::drop` can wake a waiter (Part 3).
    wake: Arc<GroupWake>,
}

impl ClusterState {
    fn new(config: &ClusterConfig, wake: Arc<GroupWake>) -> Self {
        Self {
            name: config.name.clone(),
            max_running: AtomicU64::new(config.max_running),
            running: AtomicU64::new(0),
            admissions: AtomicU64::new(0),
            reconcile: tokio::sync::Mutex::new(()),
            wake,
        }
    }

    fn running(&self) -> u64 {
        self.running.load(Ordering::Relaxed)
    }

    fn max_running(&self) -> u64 {
        self.max_running.load(Ordering::Relaxed)
    }

    fn set_max_running(&self, max_running: u64) {
        let previous = self.max_running.swap(max_running, Ordering::Relaxed);
        if max_running > previous {
            self.wake.wake_all();
        }
    }

    /// Free slots. `running` can legitimately exceed `max_running` after a cap
    /// cut (running queries are not revoked), so this saturates at zero (INC-2).
    fn available(&self) -> u64 {
        self.max_running().saturating_sub(self.running())
    }

    /// Take one slot if the cluster is under its cap.
    ///
    /// The check and the increment are one atomic read-modify-write, so two
    /// callers can never both take the last slot, and a concurrent release can
    /// never be overwritten (INC-1).
    fn try_increment(&self) -> bool {
        protocol::try_admit(&self.running, self.max_running())
    }

    /// Return one slot. Never wraps below zero, so a counter that was corrected
    /// downwards (e.g. by `reconcile`) cannot wedge the cluster (INC-1).
    fn decrement(&self) {
        protocol::release_one(&self.running);
    }

    /// Shift the counter by `engine - before`, saturating at zero.
    ///
    /// A read-modify-write, so admissions and releases racing with it are kept.
    /// A downward correction recovers slots that had drifted, so any parked
    /// waiters are woken to take them.
    fn correct(&self, before: u64, engine: u64) {
        protocol::correct_by(&self.running, before, engine);
        if engine < before {
            self.wake.wake_all();
        }
    }
}

/// Seeds each thread's generator differently, so threads that start at the same
/// time do not all sample the same clusters on their first call.
static NEXT_SEED: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

thread_local! {
    /// Per-thread random state for cluster sampling. Thread-local, so drawing a
    /// number never touches shared memory.
    static RNG: Cell<u64> =
        Cell::new(splitmix64(NEXT_SEED.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)));
}

/// SplitMix64 output step: a fast, well-mixed 64-bit generator. Not
/// cryptographic; it only has to spread load evenly.
fn splitmix64(x: u64) -> u64 {
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// One random 64-bit draw from this thread's generator.
fn random() -> u64 {
    RNG.with(|state| {
        let next = state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        state.set(next);
        splitmix64(next)
    })
}

/// One group's clusters. An `Arc<[..]>` so that a reader clones one pointer
/// instead of copying the whole list and bumping a refcount per cluster.
type Clusters = Arc<[Arc<ClusterState>]>;

/// One group: its clusters and the object that wakes its waiters.
struct Group {
    clusters: Clusters,
    wake: Arc<GroupWake>,
}

type Topology = HashMap<String, Arc<Group>>;

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

/// A thread's copy of one pool's topology, valid while `version` is current.
struct CachedTopology {
    pool: u64,
    version: u64,
    topology: Arc<Topology>,
}

thread_local! {
    static TOPOLOGY: RefCell<Option<CachedTopology>> = const { RefCell::new(None) };
}

pub struct ClusterPool {
    /// Distinguishes pools in the per-thread cache.
    id: u64,
    /// Bumped by every `reload`, while holding the write lock.
    version: AtomicU64,
    /// The topology. Only `reload` and per-thread cache misses take this lock;
    /// `try_acquire` normally reads its thread's cached copy (`with_topology`).
    topology: RwLock<Arc<Topology>>,
}

impl ClusterPool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            version: AtomicU64::new(0),
            topology: RwLock::new(Arc::new(build_groups(&config, &Topology::new()))),
        })
    }

    /// Run `f` against this thread's cached snapshot of the topology.
    ///
    /// The hit path reads one shared word (`version`) that only `reload` writes, so
    /// it stays in every core's cache: no lock, no refcount, no shared write. On a
    /// miss (first call on this thread, or a reload happened) it takes the read lock
    /// once and caches a fresh `Arc`.
    ///
    /// Cloning the `Arc` out for `f` would cost two refcount writes per call (they
    /// land on a line every thread shares), so `f` runs while this thread's cache is
    /// borrowed. `f` must therefore not re-enter `with_topology` on the same thread,
    /// which would panic on a double borrow. `admit_to` takes no pool reference, so
    /// the only path that can reach here cannot recurse.
    fn with_topology<R>(&self, f: impl FnOnce(&Topology) -> R) -> R {
        let version = self.version.load(Ordering::Acquire);
        TOPOLOGY.with(|cache| {
            let mut cache = cache.borrow_mut();
            let fresh = matches!(&*cache, Some(c) if c.pool == self.id && c.version == version);
            if !fresh {
                let topology = self.topology.read().unwrap();
                *cache = Some(CachedTopology {
                    pool: self.id,
                    // Read under the lock: `reload` bumps it while holding the write
                    // lock, so this pair is consistent.
                    version: self.version.load(Ordering::Acquire),
                    topology: Arc::clone(&topology),
                });
            }
            f(&cache.as_ref().expect("cache was just filled").topology)
        })
    }

    /// Lookup is by `&str` directly: `HashMap<String, _>` borrows its keys, so no
    /// per-call `String` allocation.
    fn clusters(&self, group: &str) -> Option<Clusters> {
        self.topology
            .read()
            .unwrap()
            .get(group)
            .map(|g| Arc::clone(&g.clusters))
    }

    fn find(&self, group: &str, cluster: &str) -> Option<Arc<ClusterState>> {
        self.clusters(group)?
            .iter()
            .find(|c| c.name == cluster)
            .cloned()
    }

    /// Admit one query to the least-loaded cluster of `group`, or fail fast with `Busy`.
    ///
    /// "Least-loaded" is approximated with power of two choices: sample two
    /// clusters at random and try the one with more free slots. If that one is
    /// full, every other cluster is tried once, so `Busy` means every cluster in
    /// the group refused (INC-2, INC-3). Free-slot counts only choose what to try;
    /// the atomic increment decides admission, so a stale read costs a retry,
    /// never a cap violation.
    pub fn try_acquire(self: &Arc<Self>, group: &str) -> Result<Permit, AcquireError> {
        self.with_topology(|topology| {
            let group = topology.get(group).ok_or(AcquireError::UnknownGroup)?;
            admit_to(&group.clusters)
        })
    }

    /// Part 3: like `try_acquire`, but wait up to `wait` for a slot to free up.
    pub async fn acquire_timeout(
        self: &Arc<Self>,
        group: &str,
        wait: Duration,
    ) -> Result<Permit, AcquireError> {
        // Resolve the group once, before waiting: a reload must not redirect this
        // caller to a different group's waiters half-way through.
        let wake = self
            .with_topology(|topology| {
                topology
                    .get(group)
                    .map(|g| (Arc::clone(&g.wake), g.clusters.is_empty()))
            })
            .ok_or(AcquireError::UnknownGroup)?;
        let (wake, empty) = wake;
        if empty {
            // Nothing will ever free a slot in a group with no clusters.
            return Err(AcquireError::Busy);
        }

        let _queued = Queued::new(&wake);
        let deadline = tokio::time::Instant::now() + wait;

        loop {
            // Fast path: a slot is free right now.
            if let Ok(permit) = self.try_acquire(group) {
                return Ok(permit);
            }

            // Register interest *before* the second check, so a release that lands
            // between the check and the await is not lost (tokio's documented
            // enable-then-check ordering).
            let notified = wake.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Ok(permit) = self.try_acquire(group) {
                return Ok(permit);
            }

            match tokio::time::timeout_at(deadline, notified).await {
                Ok(()) => continue, // a slot probably freed; re-check
                Err(_) => return Err(AcquireError::Timeout),
            }
        }
    }

    /// Current running count for a cluster, or `None` if it is not in the current config.
    pub fn running(&self, group: &str, cluster: &str) -> Option<u64> {
        self.find(group, cluster).map(|c| c.running())
    }

    /// Number of callers currently waiting inside `acquire_timeout` for `group`.
    pub fn queued(&self, group: &str) -> u64 {
        self.topology
            .read()
            .unwrap()
            .get(group)
            .map(|g| g.wake.queued())
            .unwrap_or(0)
    }

    /// Total queries ever admitted to a cluster (a metric scraped by Prometheus).
    pub fn admissions(&self, group: &str, cluster: &str) -> u64 {
        self.find(group, cluster)
            .map(|state| state.admissions.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Admin API: change a cluster's capacity at runtime.
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        if let Some(state) = self.find(group, cluster) {
            state.set_max_running(max_running);
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    ///
    /// A cluster keeps its identity across reloads by (group, name). Clusters removed
    /// from the config take no new queries; clusters added to it take traffic at once.
    pub fn reload(&self, config: PoolConfig) {
        // Reloads are rare and serialized by the write lock, so `build_groups` runs
        // exactly once per reload (it updates live caps as it goes). Readers keep
        // using their cached snapshot until they see the new version.
        let next = {
            let mut topology = self.topology.write().unwrap();
            let next = Arc::new(build_groups(&config, &topology));
            *topology = Arc::clone(&next);
            self.version.fetch_add(1, Ordering::Release);
            next
        };
        // Wake after publishing `version` and dropping the write lock, so woken
        // waiters see the new topology (e.g. a newly added cluster or raised cap).
        for group in next.values() {
            group.wake.wake_all();
        }
    }

    /// Bonus: correct a cluster's running counter against the engine's own count.
    /// `fetch` asks the engine how many queries it is running; it is slow.
    ///
    /// The engine's answer describes the moment it was asked, but the gateway kept
    /// admitting and releasing while it waited. So this does not store the answer.
    /// It snapshots the counter right before asking, and applies only the
    /// difference, `engine - snapshot`, as one atomic adjustment once the answer
    /// arrives. Everything that happened during the fetch is kept (INC-5).
    ///
    /// - At most one reconcile per cluster runs at a time. Two overlapping ones
    ///   would each correct the same drift, so the second waits for the first.
    /// - The cluster is resolved once, before the fetch. A correction always lands
    ///   on the counter it was measured against, even if a reload happens meanwhile.
    /// - Cancelling it (e.g. a timeout around a hung engine) changes nothing: the
    ///   adjustment is applied in one synchronous step after the fetch returns.
    /// - The counter saturates at zero and is allowed to exceed `max_running`, like
    ///   after a cap cut: the cluster just takes no new queries until it drains.
    ///
    /// See NOTES.md (INC-5) for the error that remains and why it is bounded.
    pub async fn reconcile<F, Fut>(&self, group: &str, cluster: &str, fetch: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = u64>,
    {
        let Some(state) = self.find(group, cluster) else {
            return;
        };
        let _serialized = state.reconcile.lock().await;

        let before = state.running();
        let engine = fetch().await;
        state.correct(before, engine);
    }
}

/// Resolve a config into runtime state.
///
/// A cluster keeps its identity across reloads by (group, name): its existing
/// state is reused and only the cap is updated. That keeps `running`,
/// `admissions` and every in-flight `Permit` pointing at the same counters
/// (INC-6), so a reload cannot reset counts or let a cluster exceed its cap.
fn build_groups(config: &PoolConfig, previous: &Topology) -> Topology {
    config
        .groups
        .iter()
        .map(|(group, clusters)| {
            let old = previous.get(group);
            // The wake object follows the group *name*, so a caller parked on it
            // stays reachable across a reload even if every cluster is replaced.
            let wake = match old {
                Some(existing) => Arc::clone(&existing.wake),
                None => Arc::new(GroupWake::default()),
            };
            let states: Clusters = clusters
                .iter()
                .map(
                    |c| match old.and_then(|s| s.clusters.iter().find(|st| st.name == c.name)) {
                        Some(existing) => {
                            existing.set_max_running(c.max_running);
                            Arc::clone(existing)
                        }
                        None => Arc::new(ClusterState::new(c, Arc::clone(&wake))),
                    },
                )
                .collect();
            (
                group.clone(),
                Arc::new(Group {
                    clusters: states,
                    wake,
                }),
            )
        })
        .collect()
}

/// Proof of admission. The holder may run one query on `cluster()`.
///
/// The slot is returned when the permit is dropped, including when the request
/// future holding it is cancelled (INC-4). The permit holds the cluster state it
/// was admitted to, so the release always reaches the counter it incremented.
pub struct Permit {
    state: Arc<ClusterState>,
}

impl Permit {
    /// Record the admission and take ownership of the slot.
    fn admit(state: &Arc<ClusterState>) -> Self {
        state.admissions.fetch_add(1, Ordering::Relaxed);
        Self {
            state: Arc::clone(state),
        }
    }

    pub fn cluster(&self) -> &str {
        &self.state.name
    }

    /// Give the slot back once the query has finished. Same as dropping the permit.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.state.decrement();
        // Part 3: hand the freed slot to a caller parked in `acquire_timeout`.
        //
        // This must happen on *every* release, not only when the cluster was full:
        // two concurrent releases can free two slots, and only the first looks like
        // it came from a full cluster. Wake-one-on-every-release with waiters
        // present is what makes progress, and `wake_one` returns after one plain
        // load when nobody is waiting.
        self.state.wake.wake_one();
    }
}

/// Pick a cluster and take a slot on it. Policy: see `ClusterPool::try_acquire`.
fn admit_to(clusters: &[Arc<ClusterState>]) -> Result<Permit, AcquireError> {
    let n = clusters.len();
    if n == 0 {
        return Err(AcquireError::Busy);
    }

    // One draw gives both samples: the low half picks the first cluster, the high
    // half picks the second among the other n - 1. Independent and uniform, which
    // is what the power-of-two-choices spread guarantee assumes.
    let draw = random();
    let start = (draw as u32 as usize) % n;

    // Fast path: take the freer of two sampled clusters. Scanning every cluster
    // re-reads counters that other cores are writing; on 16 threads that cost
    // about 2x the throughput of this, for a worse spread.
    if n == 1 {
        let state = &clusters[0];
        return if state.try_increment() {
            Ok(Permit::admit(state))
        } else {
            Err(AcquireError::Busy)
        };
    }

    let second = (start + 1 + ((draw >> 32) as usize) % (n - 1)) % n;
    // Ties keep `start`, which is already uniformly random.
    let best = if clusters[second].available() > clusters[start].available() {
        second
    } else {
        start
    };
    if clusters[best].try_increment() {
        return Ok(Permit::admit(&clusters[best]));
    }

    // Slow path: the sampled clusters were full. Try every other cluster once
    // before reporting Busy, so a half-empty group is never reported as Busy.
    if n <= 64 {
        let mut tried: u64 = 1 << best;
        while (tried.count_ones() as usize) < n {
            let mut pick = usize::MAX;
            for k in 0..n {
                let j = start + k;
                let i = if j >= n { j - n } else { j };
                if tried & (1 << i) == 0 {
                    pick = i;
                    break;
                }
            }
            if clusters[pick].try_increment() {
                return Ok(Permit::admit(&clusters[pick]));
            }
            tried |= 1 << pick;
        }
    } else {
        for k in 0..n {
            let j = start + k;
            let i = if j >= n { j - n } else { j };
            if i != best && clusters[i].try_increment() {
                return Ok(Permit::admit(&clusters[i]));
            }
        }
    }
    Err(AcquireError::Busy)
}
