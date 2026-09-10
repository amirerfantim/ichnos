//! An in-process store for tests and for the replay harness.
//!
//! Deterministic and free of I/O, which is what makes replaying a week of
//! traffic take seconds. It is not a cache and not a fallback: nothing outside
//! tests and `ichnos replay` should reach for it.

use super::{ApiKey, Result, Store, StoreError};
use crate::model::{Station, StationKey, StationKind};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Default)]
struct Inner {
    stations: HashMap<StationKey, Station>,
    api_keys: HashMap<String, ApiKey>,
}

/// A store that keeps everything in memory.
#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<RwLock<Inner>>,
    /// Set to make every call fail, so callers' degraded paths can be tested
    /// without taking a real database away.
    failing: Arc<AtomicBool>,
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make every subsequent call return [`StoreError::Unavailable`].
    pub fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::SeqCst);
    }

    /// How many stations are held.
    pub fn len(&self) -> usize {
        self.inner.read().expect("store lock poisoned").stations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read calls served, for asserting that a cache actually prevented work.
    pub fn read_count(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    /// Write calls served.
    pub fn write_count(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    /// Every station held, ordered by key, for snapshotting a replay run.
    pub fn snapshot(&self) -> Vec<Station> {
        let inner = self.inner.read().expect("store lock poisoned");
        let mut all: Vec<Station> = inner.stations.values().cloned().collect();
        all.sort_by_key(|s| s.key);
        all
    }

    fn check(&self) -> Result<()> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable("memory store set to fail".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn get_stations(&self, keys: &[StationKey]) -> Result<Vec<Station>> {
        self.check()?;
        self.reads.fetch_add(1, Ordering::SeqCst);
        let inner = self.inner.read().expect("store lock poisoned");
        Ok(keys.iter().filter_map(|k| inner.stations.get(k).cloned()).collect())
    }

    async fn put_stations(&self, stations: &[Station]) -> Result<()> {
        self.check()?;
        self.writes.fetch_add(1, Ordering::SeqCst);
        let mut inner = self.inner.write().expect("store lock poisoned");
        for station in stations {
            inner.stations.insert(station.key, station.clone());
        }
        Ok(())
    }

    async fn delete_station(&self, key: &StationKey) -> Result<()> {
        self.check()?;
        self.inner.write().expect("store lock poisoned").stations.remove(key);
        Ok(())
    }

    async fn stations_near(&self, h3: i64, kind: StationKind) -> Result<Vec<StationKey>> {
        self.check()?;
        let inner = self.inner.read().expect("store lock poisoned");
        let mut keys: Vec<StationKey> = inner
            .stations
            .values()
            .filter(|s| s.h3 == Some(h3) && s.key.kind() == kind)
            .map(|s| s.key)
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn get_api_key(&self, key: &str) -> Result<Option<ApiKey>> {
        self.check()?;
        Ok(self.inner.read().expect("store lock poisoned").api_keys.get(key).cloned())
    }

    async fn put_api_key(&self, api_key: &ApiKey) -> Result<()> {
        self.check()?;
        self.inner
            .write()
            .expect("store lock poisoned")
            .api_keys
            .insert(api_key.key.clone(), api_key.clone());
        Ok(())
    }

    async fn health(&self) -> Result<()> {
        self.check()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::WifiKey;
    use chrono::Utc;

    fn station(id: u64) -> Station {
        Station::new(StationKey::Wifi(WifiKey(id)), Utc::now())
    }

    #[tokio::test]
    async fn a_station_can_be_written_and_read_back() {
        let store = MemoryStore::new();
        let mut s = station(1);
        s.lat = Some(35.7219);
        s.lon = Some(51.3347);
        store.put_stations(&[s.clone()]).await.unwrap();

        assert_eq!(store.get_station(&s.key).await.unwrap(), Some(s));
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn missing_keys_are_absent_rather_than_null() {
        let store = MemoryStore::new();
        store.put_stations(&[station(1)]).await.unwrap();

        let found = store
            .get_stations(&[
                StationKey::Wifi(WifiKey(1)),
                StationKey::Wifi(WifiKey(2)),
                StationKey::Wifi(WifiKey(3)),
            ])
            .await
            .unwrap();
        assert_eq!(found.len(), 1, "only the one that exists comes back");
    }

    #[tokio::test]
    async fn a_second_write_replaces_the_first() {
        let store = MemoryStore::new();
        store.put_stations(&[station(1)]).await.unwrap();

        let mut updated = station(1);
        updated.samples = 42;
        store.put_stations(&[updated]).await.unwrap();

        assert_eq!(store.len(), 1);
        assert_eq!(store.get_station(&StationKey::Wifi(WifiKey(1))).await.unwrap().unwrap().samples, 42);
    }

    #[tokio::test]
    async fn asking_for_nothing_returns_nothing() {
        let store = MemoryStore::new();
        assert!(store.get_stations(&[]).await.unwrap().is_empty());
        store.put_stations(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn a_station_can_be_deleted() {
        let store = MemoryStore::new();
        store.put_stations(&[station(1)]).await.unwrap();
        store.delete_station(&StationKey::Wifi(WifiKey(1))).await.unwrap();
        assert!(store.is_empty());
        // Deleting something absent is not an error.
        store.delete_station(&StationKey::Wifi(WifiKey(99))).await.unwrap();
    }

    #[tokio::test]
    async fn stations_can_be_found_by_spatial_cell() {
        let store = MemoryStore::new();
        let mut a = station(1);
        a.h3 = Some(1234);
        let mut b = station(2);
        b.h3 = Some(1234);
        let mut c = station(3);
        c.h3 = Some(9999);
        store.put_stations(&[a, b, c]).await.unwrap();

        let near = store.stations_near(1234, StationKind::Wifi).await.unwrap();
        assert_eq!(near.len(), 2);
        assert!(store.stations_near(5555, StationKind::Wifi).await.unwrap().is_empty());
        assert!(
            store.stations_near(1234, StationKind::Cell).await.unwrap().is_empty(),
            "the kind must match too"
        );
    }

    #[tokio::test]
    async fn api_keys_round_trip() {
        let store = MemoryStore::new();
        assert_eq!(store.get_api_key("missing").await.unwrap(), None);

        let key = ApiKey::full("dev-key");
        store.put_api_key(&key).await.unwrap();
        assert_eq!(store.get_api_key("dev-key").await.unwrap(), Some(key));
    }

    #[tokio::test]
    async fn a_failing_store_reports_itself_unavailable() {
        let store = MemoryStore::new();
        store.set_failing(true);

        assert!(store.health().await.is_err());
        assert!(store.get_stations(&[]).await.is_err());
        assert!(store.put_stations(&[station(1)]).await.is_err());

        store.set_failing(false);
        assert!(store.health().await.is_ok());
    }

    #[tokio::test]
    async fn call_counts_are_tracked_so_caching_can_be_proved() {
        let store = MemoryStore::new();
        assert_eq!(store.read_count(), 0);
        store.get_stations(&[]).await.unwrap();
        store.get_stations(&[]).await.unwrap();
        assert_eq!(store.read_count(), 2);
        store.put_stations(&[station(1)]).await.unwrap();
        assert_eq!(store.write_count(), 1);
    }

    #[tokio::test]
    async fn a_snapshot_is_ordered_so_replay_runs_can_be_compared() {
        let store = MemoryStore::new();
        store.put_stations(&[station(3), station(1), station(2)]).await.unwrap();
        let keys: Vec<StationKey> = store.snapshot().into_iter().map(|s| s.key).collect();
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
    async fn clones_share_one_underlying_store() {
        // Handing a clone to a worker must not give it a private copy.
        let store = MemoryStore::new();
        let clone = store.clone();
        clone.put_stations(&[station(1)]).await.unwrap();
        assert_eq!(store.len(), 1);
    }
}
