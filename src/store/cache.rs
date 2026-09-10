//! A read-through cache that remembers misses as well as hits.
//!
//! Caching hits is obvious. Caching *misses* is the part that matters here: a
//! device usually reports several access points the service has never seen, and
//! without a negative entry each of those costs a database read on every single
//! request, forever. Remembering "we do not know this one" for a minute removes
//! most of that traffic, and a minute is short enough that a newly learned
//! station appears almost at once.
//!
//! A cache failure must never fail a request. Every error here is counted and
//! swallowed, and the call falls through to the real store.

use super::{ApiKey, Result, Station, Store};
use crate::config::Cache as CacheCfg;
use crate::model::{StationKey, StationKind};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Marks a key the service has looked up and does not have.
const NEGATIVE: &str = "\u{0}none";

/// Somewhere to keep cached entries. Implementations swallow their own errors:
/// returning `None` simply means "not cached", whatever the reason.
#[async_trait]
pub trait CacheBackend: Send + Sync + 'static {
    async fn get_many(&self, keys: &[String]) -> Vec<Option<String>>;
    async fn set_many(&self, entries: &[(String, String, u64)]);
    async fn delete_many(&self, keys: &[String]);
    /// Whether the backend is reachable, for reporting rather than for control
    /// flow.
    async fn healthy(&self) -> bool;
}

/// How the cache is performing. Exported as metrics and asserted in tests.
#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub negatives: AtomicU64,
    pub errors: AtomicU64,
}

impl CacheStats {
    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.negatives.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

/// Wraps any store with a cache.
pub struct CachedStore<S: Store, C: CacheBackend> {
    inner: S,
    backend: C,
    cfg: CacheCfg,
    stats: Arc<CacheStats>,
}

impl<S: Store, C: CacheBackend> CachedStore<S, C> {
    pub fn new(inner: S, backend: C, cfg: CacheCfg) -> Self {
        Self { inner, backend, cfg, stats: Arc::new(CacheStats::default()) }
    }

    pub fn stats(&self) -> Arc<CacheStats> {
        Arc::clone(&self.stats)
    }

    /// The store underneath, for the health endpoint to report on separately.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub async fn backend_healthy(&self) -> bool {
        self.backend.healthy().await
    }

    fn cache_key(key: &StationKey) -> String {
        format!("s:{}", key.to_token())
    }
}

#[async_trait]
impl<S: Store, C: CacheBackend> Store for CachedStore<S, C> {
    async fn get_stations(&self, keys: &[StationKey]) -> Result<Vec<Station>> {
        if !self.cfg.enabled || keys.is_empty() {
            return self.inner.get_stations(keys).await;
        }

        let cache_keys: Vec<String> = keys.iter().map(Self::cache_key).collect();
        let cached = self.backend.get_many(&cache_keys).await;

        let mut found: Vec<Station> = Vec::with_capacity(keys.len());
        let mut to_fetch: Vec<StationKey> = Vec::new();

        for (key, entry) in keys.iter().zip(cached) {
            match entry.as_deref() {
                Some(NEGATIVE) => {
                    // Known absent. No read, no result.
                    self.stats.negatives.fetch_add(1, Ordering::Relaxed);
                }
                Some(raw) => match serde_json::from_str::<Station>(raw) {
                    Ok(station) => {
                        self.stats.hits.fetch_add(1, Ordering::Relaxed);
                        found.push(station);
                    }
                    Err(_) => {
                        // A corrupt entry is treated as a miss rather than an
                        // error: the real store is still the truth.
                        self.stats.errors.fetch_add(1, Ordering::Relaxed);
                        to_fetch.push(*key);
                    }
                },
                None => {
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    to_fetch.push(*key);
                }
            }
        }

        if to_fetch.is_empty() {
            return Ok(found);
        }

        let fetched = self.inner.get_stations(&to_fetch).await?;

        // Write back both outcomes: what was found, and what turned out not to
        // exist. The second is the whole point of this cache.
        let by_key: HashMap<StationKey, &Station> =
            fetched.iter().map(|s| (s.key, s)).collect();
        let mut writes: Vec<(String, String, u64)> = Vec::with_capacity(to_fetch.len());
        for key in &to_fetch {
            match by_key.get(key) {
                Some(station) => {
                    if let Ok(json) = serde_json::to_string(station) {
                        writes.push((Self::cache_key(key), json, self.cfg.hit_ttl_secs));
                    }
                }
                None => {
                    writes.push((Self::cache_key(key), NEGATIVE.to_string(), self.cfg.miss_ttl_secs))
                }
            }
        }
        self.backend.set_many(&writes).await;

        found.extend(fetched);
        Ok(found)
    }

    async fn put_stations(&self, stations: &[Station]) -> Result<()> {
        self.inner.put_stations(stations).await?;

        if self.cfg.enabled && !stations.is_empty() {
            // Refresh rather than delete. The worker has just computed the new
            // row, so caching it costs nothing extra and saves the next reader
            // a round trip. This also replaces any negative entry, which is how
            // a newly learned station becomes visible before its negative would
            // have expired.
            let writes: Vec<(String, String, u64)> = stations
                .iter()
                .filter_map(|s| {
                    serde_json::to_string(s)
                        .ok()
                        .map(|json| (Self::cache_key(&s.key), json, self.cfg.hit_ttl_secs))
                })
                .collect();
            self.backend.set_many(&writes).await;
        }
        Ok(())
    }

    async fn delete_station(&self, key: &StationKey) -> Result<()> {
        self.inner.delete_station(key).await?;
        if self.cfg.enabled {
            self.backend.delete_many(&[Self::cache_key(key)]).await;
        }
        Ok(())
    }

    async fn stations_near(&self, h3: i64, kind: StationKind) -> Result<Vec<StationKey>> {
        // Spatial queries are for debugging and are not on any hot path, so
        // they always go to the real store.
        self.inner.stations_near(h3, kind).await
    }

    async fn get_api_key(&self, key: &str) -> Result<Option<ApiKey>> {
        self.inner.get_api_key(key).await
    }

    async fn put_api_key(&self, api_key: &ApiKey) -> Result<()> {
        self.inner.put_api_key(api_key).await
    }

    async fn health(&self) -> Result<()> {
        self.inner.health().await
    }
}

/// An in-process cache backend, used by tests and by single-process runs.
#[derive(Clone, Default)]
pub struct MemoryCache {
    entries: Arc<Mutex<HashMap<String, String>>>,
    broken: Arc<std::sync::atomic::AtomicBool>,
}

impl MemoryCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make the cache behave as if the server were unreachable.
    pub fn set_broken(&self, broken: bool) {
        self.broken.store(broken, Ordering::SeqCst);
    }

    pub fn len(&self) -> usize {
        self.entries.lock().expect("cache lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn down(&self) -> bool {
        self.broken.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl CacheBackend for MemoryCache {
    async fn get_many(&self, keys: &[String]) -> Vec<Option<String>> {
        if self.down() {
            return vec![None; keys.len()];
        }
        let entries = self.entries.lock().expect("cache lock poisoned");
        keys.iter().map(|k| entries.get(k).cloned()).collect()
    }

    async fn set_many(&self, writes: &[(String, String, u64)]) {
        if self.down() {
            return;
        }
        // Expiry is not simulated: tests that care about it set the entry
        // directly rather than waiting.
        let mut entries = self.entries.lock().expect("cache lock poisoned");
        for (key, value, _ttl) in writes {
            entries.insert(key.clone(), value.clone());
        }
    }

    async fn delete_many(&self, keys: &[String]) {
        if self.down() {
            return;
        }
        let mut entries = self.entries.lock().expect("cache lock poisoned");
        for key in keys {
            entries.remove(key);
        }
    }

    async fn healthy(&self) -> bool {
        !self.down()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::WifiKey;
    use crate::store::memory::MemoryStore;
    use chrono::Utc;

    fn station(id: u64) -> Station {
        let mut s = Station::new(StationKey::Wifi(WifiKey(id)), Utc::now());
        s.lat = Some(35.7219);
        s.lon = Some(51.3347);
        s
    }

    fn key(id: u64) -> StationKey {
        StationKey::Wifi(WifiKey(id))
    }

    fn cached() -> (CachedStore<MemoryStore, MemoryCache>, MemoryStore, MemoryCache) {
        let store = MemoryStore::new();
        let backend = MemoryCache::new();
        let cached = CachedStore::new(store.clone(), backend.clone(), CacheCfg::default());
        (cached, store, backend)
    }

    #[tokio::test]
    async fn a_second_read_of_the_same_station_never_reaches_the_store() {
        let (cached, store, _) = cached();
        store.put_stations(&[station(1)]).await.unwrap();
        let before = store.read_count();

        cached.get_stations(&[key(1)]).await.unwrap();
        let after_first = store.read_count();
        assert_eq!(after_first, before + 1, "the first read goes through");

        cached.get_stations(&[key(1)]).await.unwrap();
        assert_eq!(store.read_count(), after_first, "the second is served from cache");

        let (hits, misses, _, _) = cached.stats().snapshot();
        assert_eq!((hits, misses), (1, 1));
    }

    #[tokio::test]
    async fn an_unknown_station_is_only_looked_up_once() {
        // This is the case that matters: most reported access points are
        // unknown, and without negative caching each costs a read every time.
        let (cached, store, _) = cached();

        assert!(cached.get_stations(&[key(99)]).await.unwrap().is_empty());
        let after_first = store.read_count();

        for _ in 0..10 {
            assert!(cached.get_stations(&[key(99)]).await.unwrap().is_empty());
        }
        assert_eq!(store.read_count(), after_first, "ten more lookups, no further reads");

        let (_, _, negatives, _) = cached.stats().snapshot();
        assert_eq!(negatives, 10);
    }

    #[tokio::test]
    async fn learning_a_station_replaces_its_negative_entry_at_once() {
        let (cached, store, _) = cached();

        // Look it up while it does not exist, so a negative is cached.
        assert!(cached.get_stations(&[key(7)]).await.unwrap().is_empty());

        // The worker learns it.
        cached.put_stations(&[station(7)]).await.unwrap();

        // It must be visible immediately, not after the negative expires.
        let found = cached.get_stations(&[key(7)]).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(store.read_count(), 1, "and the read was served from cache");
    }

    #[tokio::test]
    async fn a_mixed_batch_fetches_only_what_is_missing() {
        let (cached, store, _) = cached();
        store.put_stations(&[station(1), station(2), station(3)]).await.unwrap();

        cached.get_stations(&[key(1), key(2)]).await.unwrap();
        let after_warm = store.read_count();

        let found = cached.get_stations(&[key(1), key(2), key(3)]).await.unwrap();
        assert_eq!(found.len(), 3);
        assert_eq!(store.read_count(), after_warm + 1, "one read, for the one uncached key");
    }

    #[tokio::test]
    async fn a_write_refreshes_the_cached_copy() {
        let (cached, _, _) = cached();
        cached.put_stations(&[station(1)]).await.unwrap();

        let mut updated = station(1);
        updated.samples = 99;
        cached.put_stations(&[updated]).await.unwrap();

        let found = cached.get_stations(&[key(1)]).await.unwrap();
        assert_eq!(found[0].samples, 99, "the cache must not serve the stale copy");
    }

    #[tokio::test]
    async fn deleting_a_station_removes_it_from_the_cache_too() {
        let (cached, _, backend) = cached();
        cached.put_stations(&[station(1)]).await.unwrap();
        assert_eq!(backend.len(), 1);

        cached.delete_station(&key(1)).await.unwrap();
        assert!(backend.is_empty());
        assert!(cached.get_stations(&[key(1)]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_broken_cache_slows_reads_but_never_fails_them() {
        let (cached, store, backend) = cached();
        store.put_stations(&[station(1)]).await.unwrap();
        backend.set_broken(true);

        for _ in 0..3 {
            let found = cached.get_stations(&[key(1)]).await.unwrap();
            assert_eq!(found.len(), 1, "answers still come back");
        }
        assert_eq!(store.read_count(), 3, "every read fell through to the store");
        assert!(!cached.backend_healthy().await);
    }

    #[tokio::test]
    async fn a_broken_cache_does_not_fail_writes() {
        let (cached, store, backend) = cached();
        backend.set_broken(true);
        cached.put_stations(&[station(1)]).await.unwrap();
        assert_eq!(store.len(), 1, "the write reached the store regardless");
    }

    #[tokio::test]
    async fn a_corrupt_cache_entry_is_ignored_rather_than_served() {
        let (cached, store, backend) = cached();
        store.put_stations(&[station(1)]).await.unwrap();
        backend
            .set_many(&[(format!("s:{}", key(1).to_token()), "not json at all".into(), 60)])
            .await;

        let found = cached.get_stations(&[key(1)]).await.unwrap();
        assert_eq!(found.len(), 1, "the real row is returned");
        let (_, _, _, errors) = cached.stats().snapshot();
        assert_eq!(errors, 1, "and the corruption is counted");
    }

    #[tokio::test]
    async fn a_store_failure_is_reported_rather_than_hidden_by_the_cache() {
        let (cached, store, _) = cached();
        store.set_failing(true);
        assert!(cached.get_stations(&[key(1)]).await.is_err());
        assert!(cached.health().await.is_err());
    }

    #[tokio::test]
    async fn caching_can_be_turned_off_entirely() {
        let store = MemoryStore::new();
        let cfg = CacheCfg { enabled: false, ..CacheCfg::default() };
        let cached = CachedStore::new(store.clone(), MemoryCache::new(), cfg);
        store.put_stations(&[station(1)]).await.unwrap();

        cached.get_stations(&[key(1)]).await.unwrap();
        cached.get_stations(&[key(1)]).await.unwrap();
        assert_eq!(store.read_count(), 2, "every read goes through when disabled");
    }

    #[tokio::test]
    async fn an_empty_request_does_no_work() {
        let (cached, store, _) = cached();
        assert!(cached.get_stations(&[]).await.unwrap().is_empty());
        assert_eq!(store.read_count(), 1, "delegated once, with nothing to do");
    }
}
