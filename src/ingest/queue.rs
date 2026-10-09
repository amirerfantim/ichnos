//! The queue between accepting a report and writing it down.
//!
//! Observations are routed by `hash(station key) mod shards`, so every sighting
//! of one station always lands in the same shard. That is what lets a single
//! worker own a shard and update its stations without taking any lock.
//!
//! The queue is capped rather than unbounded. When it fills, the oldest entries
//! are dropped and counted. That is a deliberate, visible loss of a little
//! history, and it is affordable because a device re-reports the same stations
//! within seconds. An unbounded queue instead trades a small loss for the
//! chance of exhausting memory and taking the read path down with it.

use crate::model::Observation;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("queue unavailable: {0}")]
    Unavailable(String),
    #[error("queue operation failed: {0}")]
    Failed(String),
}

pub type Result<T> = std::result::Result<T, QueueError>;

/// One queued observation, with the handle needed to acknowledge it.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub id: String,
    pub observation: Observation,
    /// When the entry joined the queue, in Unix milliseconds, if known.
    pub queued_ms: Option<i64>,
}

#[async_trait]
pub trait Queue: Send + Sync + 'static {
    /// Add observations to their shards. Returns how many were accepted; the
    /// difference from the input length is what the cap discarded.
    async fn publish(&self, entries: &[(u32, Observation)]) -> Result<usize>;

    /// Take up to `max` entries from one shard.
    ///
    /// Entries stay pending until acknowledged, so a worker that dies mid-batch
    /// leaves its work to be redelivered rather than losing it.
    async fn read(&self, shard: u32, max: usize) -> Result<Vec<Entry>>;

    /// Mark entries as done and remove them. Unacknowledged entries come back.
    async fn ack(&self, shard: u32, ids: &[String]) -> Result<()>;

    /// How many entries in a shard are not yet done: waiting to be read, or
    /// read and not yet acknowledged.
    async fn depth(&self, shard: u32) -> Result<usize>;

    /// How many entries the cap has discarded, across every shard, since this
    /// handle was opened.
    async fn trimmed(&self) -> Result<u64>;

    async fn healthy(&self) -> bool;
}

/// Sample queue depth into the metrics, for ever, until told to stop.
///
/// Depth cannot be recorded where entries are written, because nobody writes
/// to a shard often enough to notice it filling, and a shard nobody writes to
/// is exactly the one a dead worker leaves behind. So it is sampled instead.
///
/// The fill ratio is taken from the fullest shard rather than the average: one
/// stuck shard is losing data even while the other thirty-one look idle.
pub async fn poll_depth<Q: Queue + ?Sized>(
    queue: std::sync::Arc<Q>,
    shards: u32,
    maxlen: usize,
    every: std::time::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let m = crate::metrics::metrics();
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The queue counts trims cumulatively; the metric is a counter, so only
    // the step since the last sample may be added.
    let mut reported_trims = 0u64;

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
            }
            _ = ticker.tick() => {
                let mut total = 0usize;
                let mut fullest = 0usize;
                for shard in 0..shards {
                    match queue.depth(shard).await {
                        Ok(depth) => {
                            m.stream_depth
                                .with_label_values(&[&shard.to_string()])
                                .set(depth as i64);
                            total += depth;
                            fullest = fullest.max(depth);
                        }
                        Err(_) => {
                            m.queue_errors.with_label_values(&["depth"]).inc();
                        }
                    }
                }
                m.stream_depth_total.set(total as i64);
                if maxlen > 0 {
                    m.stream_fill_ratio.set(fullest as f64 / maxlen as f64);
                }

                match queue.trimmed().await {
                    Ok(trims) if trims > reported_trims => {
                        m.stream_trimmed.inc_by(trims - reported_trims);
                        reported_trims = trims;
                    }
                    Ok(_) => {}
                    Err(_) => {
                        m.queue_errors.with_label_values(&["trimmed"]).inc();
                    }
                }
            }
        }
    }
}

/// An entry handed out and not yet acknowledged, and when it was handed out.
type Pending = (Entry, Instant);

/// An in-process queue for tests and single-process runs.
///
/// Behaves as the Redis queue does: an entry taken and never acknowledged is
/// handed out again once it has waited `redeliver_after`.
#[derive(Clone)]
pub struct MemoryQueue {
    shards: Arc<Mutex<Vec<VecDeque<Entry>>>>,
    /// Taken but not acknowledged, with when each was last handed out.
    pending: Arc<Mutex<Vec<Vec<Pending>>>>,
    maxlen: usize,
    redeliver_after: Duration,
    trimmed: Arc<AtomicU64>,
    next_id: Arc<AtomicU64>,
    broken: Arc<std::sync::atomic::AtomicBool>,
}

impl MemoryQueue {
    pub fn new(shards: u32, maxlen: usize) -> Self {
        Self {
            shards: Arc::new(Mutex::new(vec![VecDeque::new(); shards as usize])),
            pending: Arc::new(Mutex::new(vec![Vec::new(); shards as usize])),
            maxlen,
            redeliver_after: Duration::from_secs(
                crate::config::Ingest::default().redeliver_after_secs,
            ),
            trimmed: Arc::new(AtomicU64::new(0)),
            next_id: Arc::new(AtomicU64::new(1)),
            broken: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// How long an unacknowledged entry waits before it is handed out again.
    pub fn with_redeliver_after(mut self, after: Duration) -> Self {
        self.redeliver_after = after;
        self
    }

    pub fn set_broken(&self, broken: bool) {
        self.broken.store(broken, Ordering::SeqCst);
    }

    /// Entries taken but not yet acknowledged, which is what would be
    /// redelivered if the worker died now.
    pub fn pending_count(&self, shard: u32) -> usize {
        self.pending.lock().expect("queue lock poisoned")[shard as usize].len()
    }

    fn check(&self) -> Result<()> {
        if self.broken.load(Ordering::SeqCst) {
            return Err(QueueError::Unavailable("memory queue set to fail".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl Queue for MemoryQueue {
    async fn publish(&self, entries: &[(u32, Observation)]) -> Result<usize> {
        self.check()?;
        let mut shards = self.shards.lock().expect("queue lock poisoned");
        let mut accepted = 0;
        for (shard, observation) in entries {
            let queue = &mut shards[*shard as usize];
            let id = self.next_id.fetch_add(1, Ordering::SeqCst).to_string();
            let queued_ms = Some(chrono::Utc::now().timestamp_millis());
            queue.push_back(Entry { id, observation: observation.clone(), queued_ms });
            // Drop from the front: the oldest history is the least missed,
            // because the same stations are reported again within seconds.
            while queue.len() > self.maxlen {
                queue.pop_front();
                self.trimmed.fetch_add(1, Ordering::Relaxed);
            }
            accepted += 1;
        }
        Ok(accepted)
    }

    async fn read(&self, shard: u32, max: usize) -> Result<Vec<Entry>> {
        self.check()?;

        // Entries left unacknowledged for long enough come first, as they do
        // from Redis: their owner is gone or failed, and they are the oldest.
        let now = Instant::now();
        let mut pending = self.pending.lock().expect("queue lock poisoned");
        let mut stale = Vec::new();
        for (entry, taken) in pending[shard as usize].iter_mut() {
            if stale.len() >= max {
                break;
            }
            if now.duration_since(*taken) >= self.redeliver_after {
                *taken = now;
                stale.push(entry.clone());
            }
        }
        if !stale.is_empty() {
            crate::metrics::metrics().queue_redelivered.inc_by(stale.len() as u64);
            return Ok(stale);
        }

        let mut shards = self.shards.lock().expect("queue lock poisoned");
        let queue = &mut shards[shard as usize];
        let taken: Vec<Entry> =
            (0..max.min(queue.len())).filter_map(|_| queue.pop_front()).collect();
        drop(shards);

        pending[shard as usize].extend(taken.iter().cloned().map(|e| (e, now)));
        Ok(taken)
    }

    async fn ack(&self, shard: u32, ids: &[String]) -> Result<()> {
        self.check()?;
        let mut pending = self.pending.lock().expect("queue lock poisoned");
        pending[shard as usize].retain(|(e, _)| !ids.contains(&e.id));
        Ok(())
    }

    async fn depth(&self, shard: u32) -> Result<usize> {
        self.check()?;
        let waiting = self.shards.lock().expect("queue lock poisoned")[shard as usize].len();
        Ok(waiting + self.pending_count(shard))
    }

    async fn trimmed(&self) -> Result<u64> {
        self.check()?;
        Ok(self.trimmed.load(Ordering::Relaxed))
    }

    async fn healthy(&self) -> bool {
        !self.broken.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::{StationKey, WifiKey};
    use crate::model::observation::ReportSource;

    fn observation(id: u64) -> Observation {
        Observation {
            key: StationKey::Wifi(WifiKey(id)),
            lat: 35.7219,
            lon: 51.3347,
            accuracy: Some(10.0),
            age_ms: Some(0.0),
            speed: Some(0.0),
            signal: Some(-70),
            source: ReportSource::Gnss,
            timestamp: 1_700_000_000_000,
        }
    }

    #[tokio::test]
    async fn entries_come_back_in_the_order_they_went_in() {
        let queue = MemoryQueue::new(4, 100);
        queue
            .publish(&[(0, observation(1)), (0, observation(2)), (0, observation(3))])
            .await
            .unwrap();

        let read = queue.read(0, 10).await.unwrap();
        let keys: Vec<StationKey> = read.iter().map(|e| e.observation.key).collect();
        assert_eq!(
            keys,
            vec![
                StationKey::Wifi(WifiKey(1)),
                StationKey::Wifi(WifiKey(2)),
                StationKey::Wifi(WifiKey(3))
            ]
        );
    }

    #[tokio::test]
    async fn shards_are_independent() {
        let queue = MemoryQueue::new(4, 100);
        queue.publish(&[(0, observation(1)), (2, observation(2))]).await.unwrap();

        assert_eq!(queue.depth(0).await.unwrap(), 1);
        assert_eq!(queue.depth(1).await.unwrap(), 0);
        assert_eq!(queue.depth(2).await.unwrap(), 1);
        assert_eq!(queue.read(1, 10).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn reading_takes_at_most_what_was_asked_for() {
        let queue = MemoryQueue::new(1, 100);
        for i in 0..10 {
            queue.publish(&[(0, observation(i))]).await.unwrap();
        }
        let taken = queue.read(0, 4).await.unwrap();
        assert_eq!(taken.len(), 4);
        assert_eq!(
            queue.depth(0).await.unwrap(),
            10,
            "taken is not done: six waiting, four in flight"
        );
        let ids: Vec<String> = taken.iter().map(|e| e.id.clone()).collect();
        queue.ack(0, &ids).await.unwrap();
        assert_eq!(queue.depth(0).await.unwrap(), 6, "finished work leaves the depth");
    }

    #[tokio::test]
    async fn a_full_shard_drops_its_oldest_entries_and_counts_them() {
        let queue = MemoryQueue::new(1, 3);
        for i in 0..5 {
            queue.publish(&[(0, observation(i))]).await.unwrap();
        }

        assert_eq!(queue.depth(0).await.unwrap(), 3, "the cap holds");
        assert_eq!(queue.trimmed().await.unwrap(), 2, "and the loss is counted, not silent");

        // The survivors are the newest, which is what makes the loss cheap.
        let keys: Vec<StationKey> =
            queue.read(0, 10).await.unwrap().iter().map(|e| e.observation.key).collect();
        assert_eq!(
            keys,
            vec![
                StationKey::Wifi(WifiKey(2)),
                StationKey::Wifi(WifiKey(3)),
                StationKey::Wifi(WifiKey(4))
            ]
        );
    }

    #[tokio::test]
    async fn read_entries_stay_pending_until_acknowledged() {
        let queue = MemoryQueue::new(1, 100);
        queue.publish(&[(0, observation(1)), (0, observation(2))]).await.unwrap();

        let read = queue.read(0, 10).await.unwrap();
        assert_eq!(queue.pending_count(0), 2);

        let ids: Vec<String> = read.iter().map(|e| e.id.clone()).collect();
        queue.ack(0, &ids).await.unwrap();
        assert_eq!(queue.pending_count(0), 0);
    }

    #[tokio::test]
    async fn work_a_dead_worker_never_acknowledged_comes_back() {
        let queue = MemoryQueue::new(1, 100).with_redeliver_after(Duration::ZERO);
        queue.publish(&[(0, observation(1)), (0, observation(2))]).await.unwrap();

        let first = queue.read(0, 10).await.unwrap(); // taken, then the worker dies
        assert_eq!(queue.depth(0).await.unwrap(), 2, "still not done");

        let again = queue.read(0, 10).await.unwrap();
        let ids = |entries: &[Entry]| entries.iter().map(|e| e.id.clone()).collect::<Vec<_>>();
        assert_eq!(ids(&again), ids(&first), "the same entries, nothing lost");

        queue.ack(0, &ids(&again)).await.unwrap();
        assert_eq!(queue.pending_count(0), 0);
        assert!(queue.read(0, 10).await.unwrap().is_empty(), "and once done, done");
    }

    #[tokio::test]
    async fn an_unacknowledged_entry_waits_before_it_is_handed_out_again() {
        // A worker still writing its batch must not have it taken away.
        let queue = MemoryQueue::new(1, 100).with_redeliver_after(Duration::from_secs(3600));
        queue.publish(&[(0, observation(1))]).await.unwrap();
        queue.read(0, 10).await.unwrap();

        assert!(queue.read(0, 10).await.unwrap().is_empty(), "too soon to take it back");
        assert_eq!(queue.pending_count(0), 1);
    }

    #[tokio::test]
    async fn redelivered_entries_come_before_new_ones() {
        let queue = MemoryQueue::new(1, 100).with_redeliver_after(Duration::ZERO);
        queue.publish(&[(0, observation(1))]).await.unwrap();
        let old = queue.read(0, 10).await.unwrap();
        queue.publish(&[(0, observation(2))]).await.unwrap();

        let next = queue.read(0, 10).await.unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].id, old[0].id, "the stuck entry is the oldest, so it goes first");
        queue.ack(0, &[next[0].id.clone()]).await.unwrap();

        let fresh = queue.read(0, 10).await.unwrap();
        assert_eq!(fresh.len(), 1);
        assert_ne!(fresh[0].id, old[0].id);
    }

    #[tokio::test]
    async fn redelivery_respects_the_read_limit() {
        let queue = MemoryQueue::new(1, 100).with_redeliver_after(Duration::ZERO);
        queue
            .publish(&[(0, observation(1)), (0, observation(2)), (0, observation(3))])
            .await
            .unwrap();
        queue.read(0, 10).await.unwrap();
        assert_eq!(queue.read(0, 2).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_partial_acknowledgement_leaves_the_rest_pending() {
        let queue = MemoryQueue::new(1, 100).with_redeliver_after(Duration::ZERO);
        queue.publish(&[(0, observation(1)), (0, observation(2))]).await.unwrap();
        let read = queue.read(0, 10).await.unwrap();

        queue.ack(0, &[read[0].id.clone()]).await.unwrap();
        assert_eq!(queue.pending_count(0), 1);

        let again = queue.read(0, 10).await.unwrap();
        assert_eq!(again.len(), 1, "only the unacknowledged one returns");
        assert_eq!(again[0].id, read[1].id);
    }

    #[tokio::test]
    async fn an_empty_shard_reads_as_empty_rather_than_failing() {
        let queue = MemoryQueue::new(2, 100);
        assert!(queue.read(0, 10).await.unwrap().is_empty());
        assert_eq!(queue.depth(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn publishing_nothing_is_not_an_error() {
        let queue = MemoryQueue::new(2, 100);
        assert_eq!(queue.publish(&[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_broken_queue_reports_itself_rather_than_pretending() {
        let queue = MemoryQueue::new(1, 100);
        queue.set_broken(true);
        assert!(queue.publish(&[(0, observation(1))]).await.is_err());
        assert!(queue.read(0, 1).await.is_err());
        assert!(!queue.healthy().await);
    }

    #[tokio::test]
    async fn every_entry_carries_a_distinct_handle() {
        let queue = MemoryQueue::new(1, 100);
        queue.publish(&[(0, observation(1)), (0, observation(1))]).await.unwrap();
        let read = queue.read(0, 10).await.unwrap();
        assert_ne!(read[0].id, read[1].id, "identical observations still need separate handles");
    }
}
