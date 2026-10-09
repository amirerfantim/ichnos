//! The worker: claim shards, drain them, fold, write.
//!
//! There is no scheduler. A worker takes work rather than being given it, which
//! is what removes the single process whose death would stop the pipeline. It
//! claims whatever shards are free, renews them while it lives, and hands them
//! back when it stops. Run one worker or twelve; the shards redistribute
//! themselves.
//!
//! Because a shard has exactly one owner, nothing here takes a lock. That is
//! the whole reason the ownership dance is worth its complexity.

use crate::config::Config;
use crate::geo::distance;
use crate::ingest::elide::{self, Write};
use crate::ingest::fold::{self, Action};
use crate::ingest::lease::{fair_share, rank, Leases};
use crate::ingest::queue::{Entry, Queue};
use crate::metrics::{self, Timer};
use crate::model::{Station, StationKey};
use crate::store::{Store, StoreError};
use chrono::Utc;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

/// What one pass over one shard did.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ShardOutcome {
    pub observations: usize,
    pub stations: usize,
    pub written: usize,
    pub skipped: usize,
    pub blocked: usize,
    pub created: usize,
}

impl ShardOutcome {
    /// Observations per station: how much the fold saved.
    pub fn fold_ratio(&self) -> f64 {
        fold::fold_ratio(self.observations, self.stations)
    }

    /// Share of station updates that needed no write.
    pub fn elision_rate(&self) -> f64 {
        if self.stations == 0 {
            return 0.0;
        }
        self.skipped as f64 / self.stations as f64
    }
}

/// Drain one shard and write what it produced.
///
/// Keeps pulling while the shard still has work, rather than taking one batch
/// and waiting for the next window: throughput would otherwise be capped at one
/// batch per shard per window no matter how much was waiting, and the queue
/// would grow without limit under any real load.
///
/// The batch cap stops one busy shard starving the others a worker also holds.
pub async fn process_shard<Q: Queue + ?Sized, S: Store + ?Sized>(
    queue: &Q,
    store: &S,
    shard: u32,
    cfg: &Config,
) -> Result<ShardOutcome, WorkerError> {
    let mut total = ShardOutcome::default();
    for _ in 0..cfg.ingest.max_drain_batches {
        let outcome = drain_once(queue, store, shard, cfg).await?;
        let drained = outcome.observations;
        total.observations += outcome.observations;
        total.stations += outcome.stations;
        total.written += outcome.written;
        total.skipped += outcome.skipped;
        total.blocked += outcome.blocked;
        total.created += outcome.created;
        // A short read means the shard is empty, so there is nothing to come
        // back for.
        if drained < cfg.ingest.read_batch {
            break;
        }
    }
    Ok(total)
}

/// Take one batch from a shard, fold it, and write the result.
async fn drain_once<Q: Queue + ?Sized, S: Store + ?Sized>(
    queue: &Q,
    store: &S,
    shard: u32,
    cfg: &Config,
) -> Result<ShardOutcome, WorkerError> {
    let m = metrics::metrics();
    let drain = Timer::start();

    let entries: Vec<Entry> = queue.read(shard, cfg.ingest.read_batch).await.map_err(|e| {
        m.worker_errors.with_label_values(&["queue_read"]).inc();
        m.queue_errors.with_label_values(&["read"]).inc();
        queue_error(e)
    })?;

    if entries.is_empty() {
        return Ok(ShardOutcome::default());
    }

    let ids: Vec<String> = entries.iter().map(|e| e.id.clone()).collect();
    record_queue_lag(&entries);
    let observations: Vec<_> = entries.into_iter().map(|e| e.observation).collect();
    let observation_count = observations.len();
    m.shard_batch_observations.observe(observation_count as f64);
    record_report_age(&observations);

    let grouped = fold::group(observations);
    let keys: Vec<StationKey> = grouped.keys().copied().collect();

    // One read for the whole batch, not one per station.
    let read = Timer::start();
    let existing: BTreeMap<StationKey, Station> = store
        .get_stations(&keys)
        .await
        .map_err(|e| {
            m.worker_errors.with_label_values(&["store_read"]).inc();
            store_error(e)
        })?
        .into_iter()
        .map(|s| (s.key, s))
        .collect();
    read.observe(&m.db_operation_seconds, "fold_read");

    let now = Utc::now();
    let today = now.date_naive();
    let mut outcome = ShardOutcome {
        observations: observation_count,
        stations: grouped.len(),
        ..Default::default()
    };
    let mut to_write: Vec<Station> = Vec::with_capacity(grouped.len());

    for (key, observations) in grouped {
        let previous = existing.get(&key);
        let kind = match key.kind() {
            crate::model::key::StationKind::Wifi => "wifi",
            crate::model::key::StationKind::Cell => "cell",
        };
        // A blocked station is not learned from. The block exists because its
        // evidence disagreed with itself or with what was known; folding more
        // of that evidence in would lift the block's effect at once, and count
        // a fresh block against the station every time it was reported.
        if previous.is_some_and(|p| p.is_blocked(today, &cfg.block)) {
            m.station_writes.with_label_values(&[kind, "ignored_blocked"]).inc();
            continue;
        }
        let update = fold::fold_station(key, &observations, previous, cfg, now);
        m.station_writes.with_label_values(&[kind, update.action.as_str()]).inc();

        match update.action {
            Action::Block | Action::NewBlock => outcome.blocked += 1,
            Action::New => outcome.created += 1,
            _ => {}
        }
        record_movement(previous, &update.station);

        match elide::decide(&update, previous, today, &cfg.elision) {
            Write::Full => {
                m.elision.with_label_values(&["write"]).inc();
                to_write.push(update.station);
            }
            Write::Skip => {
                m.elision.with_label_values(&["skip"]).inc();
                outcome.skipped += 1;
            }
        }
    }

    outcome.written = to_write.len();
    if !to_write.is_empty() {
        let write = Timer::start();
        let result = store.put_stations(&to_write).await;
        write.observe_on(&m.db_batch_seconds);
        result.map_err(|e| {
            m.worker_errors.with_label_values(&["store_write"]).inc();
            store_error(e)
        })?;
    }

    // Acknowledge only after the write succeeded. A crash before this point
    // leaves the batch to be redelivered, which at worst folds it twice, and
    // folding the same evidence twice is harmless.
    queue.ack(shard, &ids).await.map_err(|e| {
        m.worker_errors.with_label_values(&["queue_ack"]).inc();
        m.queue_errors.with_label_values(&["ack"]).inc();
        queue_error(e)
    })?;

    let ratio = outcome.fold_ratio();
    m.fold_ratio.set(ratio);
    m.fold_ratio_hist.observe(ratio);
    drain.observe_on(&m.shard_drain_seconds);

    Ok(outcome)
}

/// How long the oldest entry in this batch waited in the queue.
///
/// Depth says how much is waiting; this says how long it has been waiting,
/// which is the number that tells you whether a backlog is actually hurting.
fn record_queue_lag(entries: &[Entry]) {
    let Some(oldest) = entries.iter().filter_map(|e| e.queued_ms).min() else {
        return;
    };
    let lag_ms = Utc::now().timestamp_millis() - oldest;
    if lag_ms > 0 {
        metrics::metrics().queue_lag_seconds.observe(lag_ms as f64 / 1000.0);
    }
}

/// How old the oldest report in this batch is, counted from its scan.
///
/// It includes how late the device uploaded, which the queue has no part in.
/// A report from the future records nothing.
fn record_report_age(observations: &[crate::model::Observation]) {
    let Some(oldest) = observations.iter().map(|o| o.timestamp).min() else {
        return;
    };
    let age_ms = Utc::now().timestamp_millis() - oldest;
    if age_ms > 0 {
        metrics::metrics().report_age_seconds.observe(age_ms as f64 / 1000.0);
    }
}

/// A queue failure, kept apart when the queue server is gone.
fn queue_error(e: crate::ingest::queue::QueueError) -> WorkerError {
    match e {
        crate::ingest::queue::QueueError::Unavailable(m) => {
            WorkerError::Unavailable(format!("queue: {m}"))
        }
        other => WorkerError::Queue(other.to_string()),
    }
}

/// A store failure, kept apart when the database is gone.
fn store_error(e: StoreError) -> WorkerError {
    match e {
        StoreError::Unavailable(m) => WorkerError::Unavailable(format!("store: {m}")),
        other => WorkerError::Store(other.to_string()),
    }
}

/// How far a station's recorded position moved in this update.
///
/// A settled station moves centimetres. A histogram that suddenly grows a tail
/// at kilometres means either the area genuinely changed or something is
/// feeding the service positions it should not trust.
fn record_movement(previous: Option<&Station>, updated: &Station) {
    let Some(previous) = previous else { return };
    let (Some(old_lat), Some(old_lon)) = (previous.lat, previous.lon) else { return };
    let (Some(new_lat), Some(new_lon)) = (updated.lat, updated.lon) else { return };
    metrics::metrics().station_moved.observe(distance(old_lat, old_lon, new_lat, new_lon));
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("queue: {0}")]
    Queue(String),
    #[error("store: {0}")]
    Store(String),
    #[error("leases: {0}")]
    Lease(String),
    /// The database or the queue server is gone: every other shard would
    /// only wait out the same timeout.
    #[error("unavailable: {0}")]
    Unavailable(String),
}

/// A running worker.
pub struct Worker<Q: Queue + ?Sized, S: Store + ?Sized, L: Leases + ?Sized> {
    queue: Arc<Q>,
    store: Arc<S>,
    leases: Arc<L>,
    cfg: Config,
    /// Distinguishes this worker from its siblings in the lease registry.
    identity: String,
    held: HashSet<u32>,
    /// When the leases were last renewed. A drain checks it between shards,
    /// so a slow pass cannot outlive the leases it works under.
    renewed: Option<Instant>,
}

impl<Q: Queue + ?Sized, S: Store + ?Sized, L: Leases + ?Sized> Worker<Q, S, L> {
    pub fn new(
        queue: Arc<Q>,
        store: Arc<S>,
        leases: Arc<L>,
        cfg: Config,
        identity: String,
    ) -> Self {
        Self { queue, store, leases, cfg, identity, held: HashSet::new(), renewed: None }
    }

    pub fn held_shards(&self) -> Vec<u32> {
        let mut shards: Vec<u32> = self.held.iter().copied().collect();
        shards.sort_unstable();
        shards
    }

    /// How many shards this worker should be holding right now.
    ///
    /// An explicit limit wins. Otherwise it is an even split across the
    /// workers that have announced themselves (see [`fair_share`]), which is
    /// what lets a new worker take up work without waiting for an old one to
    /// die.
    pub async fn target_shards(&self) -> Result<u32, WorkerError> {
        if self.cfg.lease.max_shards_per_worker > 0 {
            return Ok(self.cfg.lease.max_shards_per_worker);
        }
        let ttl = Duration::from_secs(self.cfg.lease.ttl_secs);
        let live = self.leases.live_workers(ttl).await.map_err(|e| {
            metrics::metrics().worker_errors.with_label_values(&["lease_count"]).inc();
            WorkerError::Lease(e.to_string())
        })?;
        // Published here rather than in the run loop: this is the only place
        // that knows how many siblings answered, and a worker that cannot see
        // its siblings takes every shard, which is worth being able to see.
        let m = metrics::metrics();
        m.workers_live.set(live.len() as i64);
        let share = fair_share(self.cfg.ingest.shards, live.len(), rank(&live, &self.identity));
        m.leases_target.set(share as i64);
        Ok(share)
    }

    /// Announce that this worker is alive, so its siblings can count it.
    pub async fn announce(&self) -> Result<(), WorkerError> {
        let ttl = Duration::from_secs(self.cfg.lease.ttl_secs);
        self.leases
            .heartbeat(&self.identity, ttl)
            .await
            .map_err(|e| WorkerError::Lease(e.to_string()))
    }

    /// Give back any shards beyond this worker's fair share.
    ///
    /// Without this the first worker to start would hold everything and keep
    /// it: a second worker would sit idle until the first one died.
    pub async fn release_excess(&mut self) -> Result<usize, WorkerError> {
        let target = self.target_shards().await? as usize;
        if self.held.len() <= target {
            return Ok(0);
        }
        // Release from the top, so workers tend to settle on contiguous runs
        // and the churn stays small.
        let mut excess: Vec<u32> = self.held.iter().copied().collect();
        excess.sort_unstable();
        let to_release: Vec<u32> = excess.split_off(target);

        for shard in &to_release {
            if let Err(e) = self.leases.release(*shard, &self.identity).await {
                warn!(shard, error = %e, "could not release a shard");
            }
            self.held.remove(shard);
        }
        metrics::metrics()
            .lease_events
            .with_label_values(&["released"])
            .inc_by(to_release.len() as u64);
        if !to_release.is_empty() {
            info!(count = to_release.len(), target, "gave back shards to share the work");
        }
        Ok(to_release.len())
    }

    /// Claim any shard nobody holds, up to this worker's fair share.
    pub async fn claim_shards(&mut self) -> Result<usize, WorkerError> {
        let ttl = Duration::from_secs(self.cfg.lease.ttl_secs);
        let limit = self.target_shards().await?;

        let mut claimed = 0;
        for shard in 0..self.cfg.ingest.shards {
            if self.held.len() >= limit as usize {
                break;
            }
            if self.held.contains(&shard) {
                continue;
            }
            match self.leases.acquire(shard, &self.identity, ttl).await {
                Ok(true) => {
                    self.held.insert(shard);
                    claimed += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    metrics::metrics().worker_errors.with_label_values(&["lease_acquire"]).inc();
                    return Err(WorkerError::Lease(e.to_string()));
                }
            }
        }
        metrics::metrics().lease_events.with_label_values(&["claimed"]).inc_by(claimed as u64);
        Ok(claimed)
    }

    /// Extend every lease this worker holds, dropping any it has lost.
    ///
    /// Losing a lease means another worker may already own the shard, so the
    /// only safe response is to stop touching it at once.
    pub async fn renew_leases(&mut self) -> Result<(), WorkerError> {
        let ttl = Duration::from_secs(self.cfg.lease.ttl_secs);
        let mut lost = Vec::new();
        let mut renewed = 0u64;
        for &shard in &self.held {
            match self.leases.renew(shard, &self.identity, ttl).await {
                Ok(true) => renewed += 1,
                Ok(false) => lost.push(shard),
                Err(e) => {
                    metrics::metrics().worker_errors.with_label_values(&["lease_renew"]).inc();
                    return Err(WorkerError::Lease(e.to_string()));
                }
            }
        }
        self.renewed = Some(Instant::now());
        let m = metrics::metrics();
        m.lease_events.with_label_values(&["renewed"]).inc_by(renewed);
        // A lost lease means a sibling already owns the shard. It is the only
        // event here that says a takeover happened, so it is worth alerting on.
        m.lease_events.with_label_values(&["lost"]).inc_by(lost.len() as u64);
        for shard in lost {
            warn!(shard, "lost the lease on a shard; releasing it");
            self.held.remove(&shard);
        }
        Ok(())
    }

    /// Drain every shard this worker holds, once.
    ///
    /// Two things keep a slow pass from losing the leases it works under. The
    /// leases are renewed between shards whenever a renewal is due, rather
    /// than only from the run loop, which waits for the whole pass; and a pass
    /// stops at the first shard that finds the database or the queue gone,
    /// since every shard after it would only wait out the same timeout.
    pub async fn process_held(&mut self) -> ShardOutcome {
        let renew_every = Duration::from_secs(self.cfg.lease.renew_secs);
        let mut total = ShardOutcome::default();
        for shard in self.held_shards() {
            if self.renewed.is_none_or(|at| at.elapsed() >= renew_every) {
                if let Err(e) = self.renew_leases().await {
                    error!(error = %e, "could not renew leases during a pass");
                }
            }
            if !self.held.contains(&shard) {
                continue; // lost while the pass ran: a sibling may own it now
            }
            match process_shard(&*self.queue, &*self.store, shard, &self.cfg).await {
                Ok(outcome) => {
                    total.observations += outcome.observations;
                    total.stations += outcome.stations;
                    total.written += outcome.written;
                    total.skipped += outcome.skipped;
                    total.blocked += outcome.blocked;
                    total.created += outcome.created;
                }
                Err(e) => {
                    // One shard failing must not stop the others. The batch was
                    // not acknowledged, so it comes back.
                    metrics::metrics().worker_errors.with_label_values(&["shard"]).inc();
                    error!(shard, error = %e, "shard failed; its batch will be redelivered");
                    if matches!(e, WorkerError::Unavailable(_)) {
                        warn!("a backend is unavailable; ending this pass early");
                        break;
                    }
                }
            }
        }
        total
    }

    /// Hand every shard back, so a restart is picked up in seconds rather than
    /// after the leases expire.
    pub async fn release_all(&mut self) {
        for &shard in &self.held {
            if let Err(e) = self.leases.release(shard, &self.identity).await {
                warn!(shard, error = %e, "could not release a shard cleanly");
            }
        }
        let m = metrics::metrics();
        m.lease_events.with_label_values(&["released"]).inc_by(self.held.len() as u64);
        self.held.clear();
        m.leases_held.set(0);
    }

    /// Run until told to stop.
    pub async fn run(&mut self, mut shutdown: watch::Receiver<bool>) {
        let fold_window = Duration::from_secs(self.cfg.ingest.fold_window_secs);
        let renew_every = Duration::from_secs(self.cfg.lease.renew_secs);

        let mut work = tokio::time::interval(fold_window);
        let mut renew = tokio::time::interval(renew_every);
        work.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        info!(identity = %self.identity, shards = self.cfg.ingest.shards, "worker starting");

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("shutting down; handing shards back");
                        self.release_all().await;
                        return;
                    }
                }
                _ = renew.tick() => {
                    if let Err(e) = self.announce().await {
                        metrics::metrics().worker_errors.with_label_values(&["announce"]).inc();
                        error!(error = %e, "could not announce this worker");
                    }
                    if let Err(e) = self.renew_leases().await {
                        error!(error = %e, "could not renew leases");
                    }
                    // Give back anything beyond a fair share before claiming,
                    // so a newly started sibling has something to pick up.
                    if let Err(e) = self.release_excess().await {
                        error!(error = %e, "could not release excess shards");
                    }
                    if let Err(e) = self.claim_shards().await {
                        error!(error = %e, "could not claim shards");
                    }
                    metrics::metrics().leases_held.set(self.held.len() as i64);
                }
                _ = work.tick() => {
                    let outcome = self.process_held().await;
                    if outcome.observations > 0 {
                        debug!(
                            observations = outcome.observations,
                            stations = outcome.stations,
                            written = outcome.written,
                            skipped = outcome.skipped,
                            fold_ratio = outcome.fold_ratio(),
                            "drained held shards"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::lease::MemoryLeases;
    use crate::ingest::queue::MemoryQueue;
    use crate::model::key::WifiKey;
    use crate::model::observation::ReportSource;
    use crate::model::Observation;
    use crate::store::memory::MemoryStore;

    fn cfg() -> Config {
        let mut c = Config::for_tests();
        c.ingest.shards = 4;
        c
    }

    fn observation(id: u64, lat: f64, lon: f64) -> Observation {
        Observation {
            key: StationKey::Wifi(WifiKey(id)),
            lat,
            lon,
            accuracy: Some(10.0),
            age_ms: Some(0.0),
            speed: Some(0.0),
            signal: Some(-70),
            source: ReportSource::Gnss,
            timestamp: 1_700_000_000_000,
        }
    }

    async fn publish(queue: &MemoryQueue, cfg: &Config, observations: Vec<Observation>) {
        let routed: Vec<(u32, Observation)> =
            observations.into_iter().map(|o| (o.key.shard(cfg.ingest.shards), o)).collect();
        queue.publish(&routed).await.unwrap();
    }

    #[tokio::test]
    async fn a_drained_shard_writes_the_stations_it_learned() {
        let cfg = cfg();
        let queue = MemoryQueue::new(cfg.ingest.shards, 1_000);
        let store = MemoryStore::new();
        let observations = vec![observation(1, 35.7219, 51.3347)];
        let shard = observations[0].key.shard(cfg.ingest.shards);
        publish(&queue, &cfg, observations).await;

        let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();
        assert_eq!(outcome.observations, 1);
        assert_eq!(outcome.stations, 1);
        assert_eq!(outcome.created, 1);
        assert_eq!(outcome.written, 1);
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn repeated_sightings_of_one_station_cost_a_single_write() {
        let cfg = cfg();
        let queue = MemoryQueue::new(cfg.ingest.shards, 1_000);
        let store = MemoryStore::new();
        let observations: Vec<Observation> =
            (0..20).map(|_| observation(1, 35.7219, 51.3347)).collect();
        let shard = observations[0].key.shard(cfg.ingest.shards);
        publish(&queue, &cfg, observations).await;

        let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();
        assert_eq!(outcome.observations, 20);
        assert_eq!(outcome.stations, 1);
        assert_eq!(outcome.written, 1, "twenty sightings, one write");
        assert_eq!(outcome.fold_ratio(), 20.0);
        assert_eq!(store.write_count(), 1);
    }

    #[tokio::test]
    async fn a_second_unmoved_pass_writes_nothing() {
        // The elision path, end to end.
        let cfg = cfg();
        let queue = MemoryQueue::new(cfg.ingest.shards, 1_000);
        let store = MemoryStore::new();
        let shard = observation(1, 35.7219, 51.3347).key.shard(cfg.ingest.shards);

        publish(&queue, &cfg, vec![observation(1, 35.7219, 51.3347)]).await;
        process_shard(&queue, &store, shard, &cfg).await.unwrap();
        let writes_after_first = store.write_count();

        publish(&queue, &cfg, vec![observation(1, 35.7219, 51.3347)]).await;
        let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();

        assert_eq!(outcome.skipped, 1);
        assert_eq!(outcome.written, 0);
        assert_eq!(outcome.elision_rate(), 1.0);
        assert_eq!(store.write_count(), writes_after_first, "no second write");
    }

    #[tokio::test]
    async fn a_station_reported_from_far_away_is_blocked() {
        let cfg = cfg();
        let queue = MemoryQueue::new(cfg.ingest.shards, 1_000);
        let store = MemoryStore::new();
        let shard = observation(1, 35.7219, 51.3347).key.shard(cfg.ingest.shards);

        publish(&queue, &cfg, vec![observation(1, 35.7219, 51.3347)]).await;
        process_shard(&queue, &store, shard, &cfg).await.unwrap();

        publish(&queue, &cfg, vec![observation(1, 36.5000, 52.0000)]).await;
        let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();

        assert_eq!(outcome.blocked, 1);
        let station = store.get_station(&StationKey::Wifi(WifiKey(1))).await.unwrap().unwrap();
        assert!(!station.has_position());
        assert_eq!(station.block_count, 1);
    }

    #[tokio::test]
    async fn a_blocked_station_is_not_learned_from_or_blocked_again() {
        let cfg = cfg();
        let queue = MemoryQueue::new(cfg.ingest.shards, 1_000);
        let store = MemoryStore::new();
        let key = StationKey::Wifi(WifiKey(1));
        let shard = key.shard(cfg.ingest.shards);

        publish(&queue, &cfg, vec![observation(1, 35.7219, 51.3347)]).await;
        process_shard(&queue, &store, shard, &cfg).await.unwrap();
        publish(&queue, &cfg, vec![observation(1, 36.5000, 52.0000)]).await;
        process_shard(&queue, &store, shard, &cfg).await.unwrap();
        let blocked = store.get_station(&key).await.unwrap().unwrap();
        assert_eq!(blocked.block_count, 1);

        // More reports, from anywhere, the same day: the station is blocked,
        // so none of it counts.
        for (lat, lon) in [(35.7219, 51.3347), (36.5000, 52.0000), (35.7220, 51.3348)] {
            publish(&queue, &cfg, vec![observation(1, lat, lon)]).await;
            let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();
            assert_eq!((outcome.written, outcome.blocked), (0, 0));
        }
        let after = store.get_station(&key).await.unwrap().unwrap();
        assert_eq!(after, blocked, "nothing about the station changed");
    }

    #[tokio::test]
    async fn a_station_whose_block_has_passed_is_learned_from_again() {
        let mut cfg = cfg();
        cfg.block.temporary_days = 1;
        cfg.block.budget_days = 1.0;
        let queue = MemoryQueue::new(cfg.ingest.shards, 1_000);
        let store = MemoryStore::new();
        let key = StationKey::Wifi(WifiKey(1));
        let shard = key.shard(cfg.ingest.shards);

        // A station known for ages, blocked once, last blocked long ago.
        let mut old = Station::new(key, Utc::now() - chrono::Duration::days(400));
        old.block_count = 1;
        old.block_first = Some(Utc::now().date_naive() - chrono::Duration::days(300));
        old.block_last = old.block_first;
        store.put_stations(&[old]).await.unwrap();

        publish(&queue, &cfg, vec![observation(1, 35.7219, 51.3347)]).await;
        let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();
        assert_eq!(outcome.written, 1);
        assert!(store.get_station(&key).await.unwrap().unwrap().has_position());
    }

    #[tokio::test]
    async fn a_shard_is_drained_until_it_is_empty_not_one_batch_at_a_time() {
        // The bug this pins: draining one batch per window caps throughput at
        // read_batch per shard per window no matter how much is waiting, so the
        // queue grows without limit under any real load.
        let mut cfg = cfg();
        cfg.ingest.read_batch = 10;
        cfg.ingest.max_drain_batches = 50;

        let queue = MemoryQueue::new(cfg.ingest.shards, 10_000);
        let store = MemoryStore::new();
        let shard = observation(1, 35.7219, 51.3347).key.shard(cfg.ingest.shards);

        // Far more than one batch, all for one station so they fold together.
        let observations: Vec<Observation> =
            (0..250).map(|_| observation(1, 35.7219, 51.3347)).collect();
        publish(&queue, &cfg, observations).await;

        let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();
        assert_eq!(outcome.observations, 250, "one pass should have taken all of it");
        assert_eq!(queue.depth(shard).await.unwrap(), 0, "and left the shard empty");
    }

    #[tokio::test]
    async fn one_busy_shard_cannot_be_drained_for_ever() {
        // The cap is what stops a flooded shard starving the others a worker
        // also holds.
        let mut cfg = cfg();
        cfg.ingest.read_batch = 10;
        cfg.ingest.max_drain_batches = 3;

        let queue = MemoryQueue::new(cfg.ingest.shards, 10_000);
        let store = MemoryStore::new();
        let shard = observation(1, 35.7219, 51.3347).key.shard(cfg.ingest.shards);
        publish(&queue, &cfg, (0..250).map(|_| observation(1, 35.7219, 51.3347)).collect()).await;

        let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();
        assert_eq!(outcome.observations, 30, "three batches of ten, then it moves on");
        assert!(queue.depth(shard).await.unwrap() > 0, "the rest waits for the next pass");
    }

    #[tokio::test]
    async fn an_empty_shard_is_a_no_op() {
        let cfg = cfg();
        let queue = MemoryQueue::new(cfg.ingest.shards, 1_000);
        let store = MemoryStore::new();
        let outcome = process_shard(&queue, &store, 0, &cfg).await.unwrap();
        assert_eq!(outcome, ShardOutcome::default());
        assert_eq!(store.write_count(), 0);
    }

    #[tokio::test]
    async fn a_batch_is_acknowledged_only_after_it_is_written() {
        let cfg = cfg();
        let queue = MemoryQueue::new(cfg.ingest.shards, 1_000).with_redeliver_after(Duration::ZERO);
        let store = MemoryStore::new();
        let shard = observation(1, 35.7219, 51.3347).key.shard(cfg.ingest.shards);
        publish(&queue, &cfg, vec![observation(1, 35.7219, 51.3347)]).await;

        store.set_failing(true);
        assert!(process_shard(&queue, &store, shard, &cfg).await.is_err());
        assert_eq!(queue.pending_count(shard), 1, "still unacknowledged");

        // Which means the next pass takes it again once the store is back,
        // with no one having to put it back by hand.
        store.set_failing(false);
        let outcome = process_shard(&queue, &store, shard, &cfg).await.unwrap();
        assert_eq!(outcome.written, 1, "nothing was lost");
        assert_eq!(queue.pending_count(shard), 0);
    }

    #[tokio::test]
    async fn a_batch_taken_by_a_worker_that_died_is_written_by_the_one_that_takes_over() {
        let cfg = cfg();
        let queue = Arc::new(
            MemoryQueue::new(cfg.ingest.shards, 1_000).with_redeliver_after(Duration::ZERO),
        );
        let store = Arc::new(MemoryStore::new());
        let leases = Arc::new(MemoryLeases::new());
        let obs = observation(7, 35.7219, 51.3347);
        let shard = obs.key.shard(cfg.ingest.shards);
        publish(&queue, &cfg, vec![obs]).await;

        // The first worker takes the batch and dies before writing it.
        let mut dead =
            Worker::new(queue.clone(), store.clone(), leases.clone(), cfg.clone(), "a".into());
        dead.announce().await.unwrap();
        dead.claim_shards().await.unwrap();
        assert_eq!(queue.read(shard, 100).await.unwrap().len(), 1);
        for held in dead.held_shards() {
            leases.expire(held);
        }
        leases.forget_worker("a");
        assert_eq!(store.len(), 0);

        // The next worker to hold the shard finds the batch and writes it.
        let mut next = Worker::new(queue.clone(), store.clone(), leases, cfg, "b".into());
        next.announce().await.unwrap();
        next.claim_shards().await.unwrap();
        assert!(next.held_shards().contains(&shard));
        let outcome = next.process_held().await;

        assert_eq!(outcome.written, 1, "the dead worker's batch was not lost");
        assert_eq!(store.len(), 1);
        assert_eq!(queue.pending_count(shard), 0, "and it is acknowledged now");
    }

    #[tokio::test]
    async fn a_worker_claims_every_free_shard() {
        let cfg = cfg();
        let mut worker = Worker::new(
            Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000)),
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryLeases::new()),
            cfg.clone(),
            "worker-a".into(),
        );
        assert_eq!(worker.claim_shards().await.unwrap(), 4);
        assert_eq!(worker.held_shards(), vec![0, 1, 2, 3]);
        assert_eq!(worker.claim_shards().await.unwrap(), 0, "and does not double-claim");
    }

    #[tokio::test]
    async fn a_second_worker_takes_half_the_shards_without_the_first_dying() {
        // The first worker starts alone and takes everything. When a second
        // appears, the first must give half back rather than holding on until
        // it dies.
        let cfg = cfg();
        let queue = Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000));
        let store = Arc::new(MemoryStore::new());
        let leases = Arc::new(MemoryLeases::new());

        let mut first =
            Worker::new(queue.clone(), store.clone(), leases.clone(), cfg.clone(), "a".into());
        first.announce().await.unwrap();
        first.claim_shards().await.unwrap();
        assert_eq!(first.held_shards().len(), 4, "alone, it takes them all");

        let mut second = Worker::new(queue, store, leases, cfg, "b".into());
        second.announce().await.unwrap();

        first.release_excess().await.unwrap();
        second.claim_shards().await.unwrap();

        assert_eq!(first.held_shards().len(), 2);
        assert_eq!(second.held_shards().len(), 2);
        assert!(first.held_shards().iter().all(|s| !second.held_shards().contains(s)));
    }

    #[tokio::test]
    async fn a_worker_reclaims_everything_when_its_sibling_disappears() {
        let cfg = cfg();
        let leases = Arc::new(MemoryLeases::new());
        let mut first = Worker::new(
            Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000)),
            Arc::new(MemoryStore::new()),
            leases.clone(),
            cfg.clone(),
            "a".into(),
        );
        first.announce().await.unwrap();
        leases.heartbeat("b", Duration::from_secs(15)).await.unwrap();
        first.claim_shards().await.unwrap();
        assert_eq!(first.held_shards().len(), 2, "it shares while the sibling lives");

        leases.forget_worker("b");
        for shard in 0..cfg.ingest.shards {
            leases.expire(shard);
        }
        first.claim_shards().await.unwrap();
        assert_eq!(first.held_shards().len(), 4, "and takes over when it does not");
    }

    #[tokio::test]
    async fn two_workers_split_the_shards_between_them() {
        let mut cfg = cfg();
        cfg.lease.max_shards_per_worker = 2;
        let queue = Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000));
        let store = Arc::new(MemoryStore::new());
        let leases = Arc::new(MemoryLeases::new());

        let mut a =
            Worker::new(queue.clone(), store.clone(), leases.clone(), cfg.clone(), "a".into());
        let mut b = Worker::new(queue, store, leases, cfg, "b".into());

        a.claim_shards().await.unwrap();
        b.claim_shards().await.unwrap();

        assert_eq!(a.held_shards(), vec![0, 1]);
        assert_eq!(b.held_shards(), vec![2, 3]);
        // No shard is held twice, which is what makes the lock-free path safe.
        assert!(a.held_shards().iter().all(|s| !b.held_shards().contains(s)));
    }

    #[tokio::test]
    async fn a_worker_that_loses_a_lease_stops_touching_that_shard() {
        let cfg = cfg();
        let leases = Arc::new(MemoryLeases::new());
        let mut worker = Worker::new(
            Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000)),
            Arc::new(MemoryStore::new()),
            leases.clone(),
            cfg,
            "worker-a".into(),
        );
        worker.claim_shards().await.unwrap();
        assert_eq!(worker.held_shards().len(), 4);

        leases.expire(2); // the lease lapsed and someone else may hold it now
        worker.renew_leases().await.unwrap();

        assert_eq!(worker.held_shards(), vec![0, 1, 3]);
    }

    #[tokio::test]
    async fn releasing_hands_shards_over_at_once() {
        let cfg = cfg();
        let leases = Arc::new(MemoryLeases::new());
        let mut worker = Worker::new(
            Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000)),
            Arc::new(MemoryStore::new()),
            leases.clone(),
            cfg.clone(),
            "worker-a".into(),
        );
        worker.claim_shards().await.unwrap();
        worker.release_all().await;

        assert!(worker.held_shards().is_empty());
        let mut other = Worker::new(
            Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000)),
            Arc::new(MemoryStore::new()),
            leases,
            cfg,
            "worker-b".into(),
        );
        assert_eq!(other.claim_shards().await.unwrap(), 4, "picked up without waiting");
    }

    #[tokio::test]
    async fn one_failing_shard_does_not_stop_the_others() {
        let cfg = cfg();
        let queue = Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000));
        let store = Arc::new(MemoryStore::new());
        let mut worker = Worker::new(
            queue.clone(),
            store.clone(),
            Arc::new(MemoryLeases::new()),
            cfg.clone(),
            "worker-a".into(),
        );
        worker.claim_shards().await.unwrap();

        // Spread stations over several shards.
        let observations: Vec<Observation> =
            (1..=20).map(|i| observation(i, 35.7219, 51.3347)).collect();
        publish(&queue, &cfg, observations).await;

        let outcome = worker.process_held().await;
        assert_eq!(outcome.stations, 20, "every shard was drained");
        assert_eq!(outcome.written, 20);
    }

    #[tokio::test]
    async fn a_pass_ends_at_the_first_shard_that_finds_the_database_gone() {
        let cfg = cfg();
        let queue = Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000));
        let store = Arc::new(MemoryStore::new());
        let mut worker = Worker::new(
            queue.clone(),
            store.clone(),
            Arc::new(MemoryLeases::new()),
            cfg.clone(),
            "worker-a".into(),
        );
        worker.claim_shards().await.unwrap();
        let observations: Vec<Observation> =
            (1..=20).map(|i| observation(i, 35.7219, 51.3347)).collect();
        publish(&queue, &cfg, observations).await;

        store.set_failing(true);
        worker.process_held().await;
        let taken = (0..cfg.ingest.shards).filter(|&s| queue.pending_count(s) > 0).count();
        assert_eq!(taken, 1, "every shard after the first would only wait out the same timeout");
        assert_eq!(worker.held_shards().len() as u32, cfg.ingest.shards, "and the leases are kept");
    }

    #[tokio::test]
    async fn a_worker_stops_when_told_and_hands_its_shards_back() {
        let mut cfg = cfg();
        cfg.ingest.fold_window_secs = 1;
        cfg.lease.renew_secs = 1;
        cfg.lease.ttl_secs = 5;
        let leases = Arc::new(MemoryLeases::new());
        let mut worker = Worker::new(
            Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000)),
            Arc::new(MemoryStore::new()),
            leases.clone(),
            cfg,
            "worker-a".into(),
        );

        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            worker.run(rx).await;
        });

        tokio::time::sleep(Duration::from_millis(1_200)).await;
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), handle).await.unwrap().unwrap();

        assert_eq!(leases.holder(0).await.unwrap(), None, "shards were handed back");
    }
}
