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

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("queue unavailable: {0}")]
    Unavailable(String),
    #[error("queue operation failed: {0}")]
    Failed(String),
}

pub type Result<T> = std::result::Result<T, QueueError>;

/// One queued observation, with the handle needed to acknowledge it.
#[derive(Debug, Clone)]
pub struct Entry {
    pub id: String,
    pub observation: Observation,
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

    /// Mark entries as done. Unacknowledged entries come back.
    async fn ack(&self, shard: u32, ids: &[String]) -> Result<()>;

    /// How many entries are waiting in a shard.
    async fn depth(&self, shard: u32) -> Result<usize>;

    /// How many entries have been discarded by the cap since start-up.
    fn trimmed(&self) -> u64;

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

                let trims = queue.trimmed();
                if trims > reported_trims {
                    m.stream_trimmed.inc_by(trims - reported_trims);
                    reported_trims = trims;
                }
            }
        }
    }
}

/// An in-process queue for tests and single-process runs.
#[derive(Clone)]
pub struct MemoryQueue {
    shards: Arc<Mutex<Vec<VecDeque<Entry>>>>,
    pending: Arc<Mutex<Vec<Vec<Entry>>>>,
    maxlen: usize,
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
            trimmed: Arc::new(AtomicU64::new(0)),
            next_id: Arc::new(AtomicU64::new(1)),
            broken: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn set_broken(&self, broken: bool) {
        self.broken.store(broken, Ordering::SeqCst);
    }

    /// Entries taken but not yet acknowledged, which is what would be
    /// redelivered if the worker died now.
    pub fn pending_count(&self, shard: u32) -> usize {
        self.pending.lock().expect("queue lock poisoned")[shard as usize].len()
    }

    /// Put unacknowledged entries back at the front, as a redelivery would.
    pub fn redeliver(&self, shard: u32) {
        let mut pending = self.pending.lock().expect("queue lock poisoned");
        let mut shards = self.shards.lock().expect("queue lock poisoned");
        for entry in pending[shard as usize].drain(..).rev() {
            shards[shard as usize].push_front(entry);
        }
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
            queue.push_back(Entry { id, observation: observation.clone() });
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
        let mut shards = self.shards.lock().expect("queue lock poisoned");
        let queue = &mut shards[shard as usize];
        let taken: Vec<Entry> =
            (0..max.min(queue.len())).filter_map(|_| queue.pop_front()).collect();
        drop(shards);

        self.pending.lock().expect("queue lock poisoned")[shard as usize]
            .extend(taken.iter().cloned());
        Ok(taken)
    }

    async fn ack(&self, shard: u32, ids: &[String]) -> Result<()> {
        self.check()?;
        let mut pending = self.pending.lock().expect("queue lock poisoned");
        pending[shard as usize].retain(|e| !ids.contains(&e.id));
        Ok(())
    }

    async fn depth(&self, shard: u32) -> Result<usize> {
        self.check()?;
        Ok(self.shards.lock().expect("queue lock poisoned")[shard as usize].len())
    }

    fn trimmed(&self) -> u64 {
        self.trimmed.load(Ordering::Relaxed)
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
        assert_eq!(queue.read(0, 4).await.unwrap().len(), 4);
        assert_eq!(queue.depth(0).await.unwrap(), 6);
    }

    #[tokio::test]
    async fn a_full_shard_drops_its_oldest_entries_and_counts_them() {
        let queue = MemoryQueue::new(1, 3);
        for i in 0..5 {
            queue.publish(&[(0, observation(i))]).await.unwrap();
        }

        assert_eq!(queue.depth(0).await.unwrap(), 3, "the cap holds");
        assert_eq!(queue.trimmed(), 2, "and the loss is counted, not silent");

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
        let queue = MemoryQueue::new(1, 100);
        queue.publish(&[(0, observation(1)), (0, observation(2))]).await.unwrap();

        queue.read(0, 10).await.unwrap(); // taken, then the worker dies
        assert_eq!(queue.depth(0).await.unwrap(), 0);

        queue.redeliver(0);
        assert_eq!(queue.depth(0).await.unwrap(), 2, "nothing was lost");
        assert_eq!(queue.pending_count(0), 0);
    }

    #[tokio::test]
    async fn a_partial_acknowledgement_leaves_the_rest_pending() {
        let queue = MemoryQueue::new(1, 100);
        queue.publish(&[(0, observation(1)), (0, observation(2))]).await.unwrap();
        let read = queue.read(0, 10).await.unwrap();

        queue.ack(0, &[read[0].id.clone()]).await.unwrap();
        assert_eq!(queue.pending_count(0), 1);

        queue.redeliver(0);
        assert_eq!(queue.depth(0).await.unwrap(), 1, "only the unacknowledged one returns");
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
