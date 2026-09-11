//! Deciding which worker owns which shard.
//!
//! This is what replaces a scheduler. A worker claims free shards, renews them
//! on a timer, and blocks on the ones it holds. Nothing hands out work; workers
//! take it. Scale the number of workers from one to many and the leases
//! redistribute on their own, with no configuration to change and no single
//! process whose death stops the pipeline.
//!
//! Ownership is what makes the write path lock-free: exactly one worker ever
//! folds a given station, so there is no contention to arbitrate.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("lease store unavailable: {0}")]
    Unavailable(String),
}

pub type Result<T> = std::result::Result<T, LeaseError>;

#[async_trait]
pub trait Leases: Send + Sync + 'static {
    /// Claim a shard if nobody holds it. Returns whether the claim succeeded.
    async fn acquire(&self, shard: u32, owner: &str, ttl: Duration) -> Result<bool>;

    /// Extend a lease this worker already holds.
    ///
    /// Returns false when the lease has been lost, which is the signal to stop
    /// working that shard immediately: somebody else may already own it.
    async fn renew(&self, shard: u32, owner: &str, ttl: Duration) -> Result<bool>;

    /// Give a shard up on a clean shutdown, so it is picked up in seconds
    /// rather than after the lease expires.
    async fn release(&self, shard: u32, owner: &str) -> Result<()>;

    /// Who holds a shard, if anyone.
    async fn holder(&self, shard: u32) -> Result<Option<String>>;

    /// Announce that this worker is alive, and refresh that announcement.
    ///
    /// Without it a worker cannot know how many siblings it has, and the first
    /// one to start would take every shard and keep them: adding a second
    /// worker would change nothing until the first one died.
    async fn heartbeat(&self, owner: &str, ttl: Duration) -> Result<()>;

    /// How many workers have announced themselves recently.
    async fn live_workers(&self, ttl: Duration) -> Result<usize>;
}

/// How many shards one worker should hold, given how many workers there are.
///
/// Rounded up, so every shard has an owner even when the division is uneven.
/// A worker holding more than this releases the excess, which is what lets a
/// new worker pick up work without waiting for an old one to die.
pub fn fair_share(shards: u32, workers: usize) -> u32 {
    let workers = workers.max(1) as u32;
    shards.div_ceil(workers)
}

/// An in-process lease registry for tests and single-process runs.
#[derive(Clone, Default)]
pub struct MemoryLeases {
    held: Arc<Mutex<HashMap<u32, (String, Instant)>>>,
    workers: Arc<Mutex<HashMap<String, Instant>>>,
}

impl MemoryLeases {
    pub fn new() -> Self {
        Self::default()
    }

    /// Expire a lease immediately, standing in for a worker that stopped
    /// renewing.
    pub fn expire(&self, shard: u32) {
        self.held.lock().expect("lease lock poisoned").remove(&shard);
    }

    /// Forget a worker immediately, standing in for one that died.
    pub fn forget_worker(&self, owner: &str) {
        self.workers.lock().expect("lease lock poisoned").remove(owner);
    }

    fn live(&self, shard: u32) -> Option<(String, Instant)> {
        let mut held = self.held.lock().expect("lease lock poisoned");
        match held.get(&shard) {
            Some((owner, until)) if *until > Instant::now() => Some((owner.clone(), *until)),
            Some(_) => {
                held.remove(&shard);
                None
            }
            None => None,
        }
    }
}

#[async_trait]
impl Leases for MemoryLeases {
    async fn acquire(&self, shard: u32, owner: &str, ttl: Duration) -> Result<bool> {
        if let Some((holder, _)) = self.live(shard) {
            // Re-acquiring a shard this worker already holds is a renewal, not
            // a conflict.
            if holder != owner {
                return Ok(false);
            }
        }
        self.held
            .lock()
            .expect("lease lock poisoned")
            .insert(shard, (owner.to_string(), Instant::now() + ttl));
        Ok(true)
    }

    async fn renew(&self, shard: u32, owner: &str, ttl: Duration) -> Result<bool> {
        match self.live(shard) {
            Some((holder, _)) if holder == owner => {
                self.held
                    .lock()
                    .expect("lease lock poisoned")
                    .insert(shard, (owner.to_string(), Instant::now() + ttl));
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn release(&self, shard: u32, owner: &str) -> Result<()> {
        let mut held = self.held.lock().expect("lease lock poisoned");
        if let Some((holder, _)) = held.get(&shard) {
            // Only the holder may release, or a slow worker could drop a lease
            // that has since passed to somebody else.
            if holder == owner {
                held.remove(&shard);
            }
        }
        Ok(())
    }

    async fn holder(&self, shard: u32) -> Result<Option<String>> {
        Ok(self.live(shard).map(|(owner, _)| owner))
    }

    async fn heartbeat(&self, owner: &str, ttl: Duration) -> Result<()> {
        self.workers
            .lock()
            .expect("lease lock poisoned")
            .insert(owner.to_string(), Instant::now() + ttl);
        Ok(())
    }

    async fn live_workers(&self, _ttl: Duration) -> Result<usize> {
        let mut workers = self.workers.lock().expect("lease lock poisoned");
        let now = Instant::now();
        workers.retain(|_, until| *until > now);
        Ok(workers.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(15);

    #[tokio::test]
    async fn a_free_shard_can_be_claimed() {
        let leases = MemoryLeases::new();
        assert!(leases.acquire(0, "worker-a", TTL).await.unwrap());
        assert_eq!(leases.holder(0).await.unwrap().as_deref(), Some("worker-a"));
    }

    #[tokio::test]
    async fn two_workers_cannot_hold_the_same_shard() {
        // This is the property the whole lock-free write path rests on.
        let leases = MemoryLeases::new();
        assert!(leases.acquire(0, "worker-a", TTL).await.unwrap());
        assert!(!leases.acquire(0, "worker-b", TTL).await.unwrap());
        assert_eq!(leases.holder(0).await.unwrap().as_deref(), Some("worker-a"));
    }

    #[tokio::test]
    async fn a_worker_can_reclaim_a_shard_it_already_holds() {
        let leases = MemoryLeases::new();
        assert!(leases.acquire(0, "worker-a", TTL).await.unwrap());
        assert!(leases.acquire(0, "worker-a", TTL).await.unwrap());
    }

    #[tokio::test]
    async fn different_shards_go_to_different_workers() {
        let leases = MemoryLeases::new();
        assert!(leases.acquire(0, "worker-a", TTL).await.unwrap());
        assert!(leases.acquire(1, "worker-b", TTL).await.unwrap());
        assert_eq!(leases.holder(1).await.unwrap().as_deref(), Some("worker-b"));
    }

    #[tokio::test]
    async fn the_holder_can_renew() {
        let leases = MemoryLeases::new();
        leases.acquire(0, "worker-a", TTL).await.unwrap();
        assert!(leases.renew(0, "worker-a", TTL).await.unwrap());
    }

    #[tokio::test]
    async fn renewing_a_lease_you_do_not_hold_fails() {
        // The false answer is the signal to stop working the shard at once,
        // because somebody else may already own it.
        let leases = MemoryLeases::new();
        leases.acquire(0, "worker-a", TTL).await.unwrap();
        assert!(!leases.renew(0, "worker-b", TTL).await.unwrap());
    }

    #[tokio::test]
    async fn renewing_a_shard_nobody_holds_fails() {
        let leases = MemoryLeases::new();
        assert!(!leases.renew(0, "worker-a", TTL).await.unwrap());
    }

    #[tokio::test]
    async fn a_dead_workers_shard_is_taken_over_once_its_lease_lapses() {
        let leases = MemoryLeases::new();
        leases.acquire(0, "worker-a", TTL).await.unwrap();
        assert!(!leases.acquire(0, "worker-b", TTL).await.unwrap());

        leases.expire(0); // worker-a stopped renewing

        assert!(leases.acquire(0, "worker-b", TTL).await.unwrap());
        assert_eq!(leases.holder(0).await.unwrap().as_deref(), Some("worker-b"));
    }

    #[tokio::test]
    async fn an_expired_lease_reports_no_holder() {
        let leases = MemoryLeases::new();
        leases.acquire(0, "worker-a", Duration::from_millis(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(leases.holder(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn releasing_hands_the_shard_over_at_once() {
        let leases = MemoryLeases::new();
        leases.acquire(0, "worker-a", TTL).await.unwrap();
        leases.release(0, "worker-a").await.unwrap();
        assert_eq!(leases.holder(0).await.unwrap(), None);
        assert!(leases.acquire(0, "worker-b", TTL).await.unwrap());
    }

    #[tokio::test]
    async fn a_worker_cannot_release_someone_elses_shard() {
        // A slow worker waking up after losing its lease must not drop the
        // lease that has since passed to another worker.
        let leases = MemoryLeases::new();
        leases.acquire(0, "worker-a", TTL).await.unwrap();
        leases.release(0, "worker-b").await.unwrap();
        assert_eq!(leases.holder(0).await.unwrap().as_deref(), Some("worker-a"));
    }

    #[tokio::test]
    async fn releasing_a_shard_nobody_holds_is_harmless() {
        let leases = MemoryLeases::new();
        leases.release(7, "worker-a").await.unwrap();
    }

    #[tokio::test]
    async fn a_fair_share_covers_every_shard_even_when_it_divides_unevenly() {
        assert_eq!(fair_share(8, 1), 8);
        assert_eq!(fair_share(8, 2), 4);
        assert_eq!(fair_share(8, 3), 3, "3 each covers 8 with one to spare");
        assert_eq!(fair_share(8, 8), 1);
        assert_eq!(fair_share(8, 20), 1, "more workers than shards is not an error");
        assert_eq!(fair_share(8, 0), 8, "and nor is claiming to have none");
    }

    #[tokio::test]
    async fn workers_are_counted_while_they_keep_announcing_themselves() {
        let leases = MemoryLeases::new();
        assert_eq!(leases.live_workers(TTL).await.unwrap(), 0);

        leases.heartbeat("a", TTL).await.unwrap();
        leases.heartbeat("b", TTL).await.unwrap();
        assert_eq!(leases.live_workers(TTL).await.unwrap(), 2);

        leases.heartbeat("a", TTL).await.unwrap();
        assert_eq!(leases.live_workers(TTL).await.unwrap(), 2, "repeats do not double-count");

        leases.forget_worker("b");
        assert_eq!(leases.live_workers(TTL).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn a_worker_that_stops_announcing_itself_is_dropped_from_the_count() {
        let leases = MemoryLeases::new();
        leases.heartbeat("gone", Duration::from_millis(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(leases.live_workers(TTL).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn many_workers_racing_for_one_shard_produce_exactly_one_winner() {
        let leases = MemoryLeases::new();
        let mut wins = 0;
        for i in 0..50 {
            if leases.acquire(0, &format!("worker-{i}"), TTL).await.unwrap() {
                wins += 1;
            }
        }
        assert_eq!(wins, 1);
    }
}
