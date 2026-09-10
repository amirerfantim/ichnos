//! Counting requests against a key's daily allowance.
//!
//! Kept deliberately simple: a counter per key per day that expires on its own.
//! It is not a rate limiter and does not try to be smooth; it is a ceiling, and
//! the only question it answers is whether a caller has had its allowance for
//! today.
//!
//! A counting failure lets the request through. Refusing traffic because a
//! counter was unreachable would turn a small outage into a large one, and the
//! worst case is that a caller briefly exceeds a limit nobody was enforcing a
//! moment ago anyway.

use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Somewhere to keep per-day counters.
#[async_trait]
pub trait QuotaCounter: Send + Sync + 'static {
    /// Add one to today's count for a key and return the new total.
    ///
    /// `None` means the count could not be taken, which callers treat as
    /// "allow".
    async fn increment(&self, key: &str) -> Option<i64>;
}

/// Whether a request may proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaVerdict {
    Allowed,
    Exceeded,
}

/// Check and consume one unit of a key's daily allowance.
///
/// A quota of zero means unlimited, which is the default: a limit should be a
/// deliberate choice rather than something a key acquires by existing.
pub async fn consume<C: QuotaCounter + ?Sized>(
    counter: &C,
    key: &str,
    daily_quota: i64,
) -> QuotaVerdict {
    if daily_quota <= 0 {
        return QuotaVerdict::Allowed;
    }
    match counter.increment(key).await {
        Some(used) if used > daily_quota => QuotaVerdict::Exceeded,
        // Includes the counter being unavailable: allow rather than refuse.
        _ => QuotaVerdict::Allowed,
    }
}

/// The counter key for a key on a given day, in UTC.
pub fn counter_key(key: &str) -> String {
    format!("quota:{}:{}", key, Utc::now().format("%Y-%m-%d"))
}

/// An in-process counter, for tests and single-process runs.
///
/// Not shared between processes, so a multi-pod deployment using this would
/// enforce the quota per pod rather than per key. Real deployments use the
/// Redis-backed one.
#[derive(Clone, Default)]
pub struct MemoryQuota {
    counts: Arc<Mutex<HashMap<String, i64>>>,
    broken: Arc<std::sync::atomic::AtomicBool>,
}

impl MemoryQuota {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_broken(&self, broken: bool) {
        self.broken.store(broken, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn used(&self, key: &str) -> i64 {
        *self.counts.lock().expect("quota lock poisoned").get(&counter_key(key)).unwrap_or(&0)
    }
}

#[async_trait]
impl QuotaCounter for MemoryQuota {
    async fn increment(&self, key: &str) -> Option<i64> {
        if self.broken.load(std::sync::atomic::Ordering::SeqCst) {
            return None;
        }
        let mut counts = self.counts.lock().expect("quota lock poisoned");
        let entry = counts.entry(counter_key(key)).or_insert(0);
        *entry += 1;
        Some(*entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_quota_of_zero_means_unlimited() {
        let counter = MemoryQuota::new();
        for _ in 0..1_000 {
            assert_eq!(consume(&counter, "free", 0).await, QuotaVerdict::Allowed);
        }
        assert_eq!(counter.used("free"), 0, "and nothing is even counted");
    }

    #[tokio::test]
    async fn a_key_is_refused_once_it_passes_its_allowance() {
        let counter = MemoryQuota::new();
        for i in 1..=3 {
            assert_eq!(
                consume(&counter, "limited", 3).await,
                QuotaVerdict::Allowed,
                "request {i} of 3 should be allowed"
            );
        }
        assert_eq!(consume(&counter, "limited", 3).await, QuotaVerdict::Exceeded);
        assert_eq!(consume(&counter, "limited", 3).await, QuotaVerdict::Exceeded);
    }

    #[tokio::test]
    async fn keys_are_counted_separately() {
        let counter = MemoryQuota::new();
        consume(&counter, "one", 1).await;
        assert_eq!(consume(&counter, "one", 1).await, QuotaVerdict::Exceeded);
        assert_eq!(consume(&counter, "two", 1).await, QuotaVerdict::Allowed);
    }

    #[tokio::test]
    async fn a_broken_counter_lets_traffic_through() {
        // Refusing traffic because a counter was unreachable would turn a small
        // outage into a large one.
        let counter = MemoryQuota::new();
        counter.set_broken(true);
        for _ in 0..100 {
            assert_eq!(consume(&counter, "limited", 1).await, QuotaVerdict::Allowed);
        }
    }

    #[tokio::test]
    async fn counters_are_scoped_to_a_day() {
        let key = counter_key("abc");
        assert!(key.starts_with("quota:abc:"));
        assert_eq!(key.len(), "quota:abc:".len() + 10, "a YYYY-MM-DD suffix");
    }

    #[tokio::test]
    async fn a_negative_quota_is_treated_as_unlimited_not_as_zero() {
        // Otherwise a misconfigured key would refuse everything.
        let counter = MemoryQuota::new();
        assert_eq!(consume(&counter, "odd", -5).await, QuotaVerdict::Allowed);
    }
}
