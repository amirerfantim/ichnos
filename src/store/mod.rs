//! Persistence, behind one trait.
//!
//! Three implementations, each with a different job:
//!
//! - [`cql::CqlStore`] is production. ScyllaDB, or Apache Cassandra, since the
//!   driver speaks to both.
//! - [`memory::MemoryStore`] backs the tests and the replay harness. It is
//!   deterministic and does no I/O, which makes a week of replayed traffic take
//!   seconds instead of hours.
//! - [`cache::CachedStore`] wraps any of the above with a Redis read-through
//!   cache that remembers misses as well as hits.
//!
//! The trait is deliberately narrow. Every access is by primary key, there are
//! no joins and no transactions, because a station update only ever reads and
//! writes one station and one worker owns each shard.

pub mod cache;
pub mod cql;
pub mod memory;

use crate::model::{Station, StationKey, StationKind};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database unavailable: {0}")]
    Unavailable(String),
    #[error("query failed: {0}")]
    Query(String),
    #[error("stored row is malformed: {0}")]
    Malformed(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// A caller's credentials and what they are allowed to do.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiKey {
    pub key: String,
    pub name: String,
    pub enabled: bool,
    pub allow_locate: bool,
    pub allow_submit: bool,
    /// Gates the debug routes, which expose one station's full record.
    pub allow_admin: bool,
    /// Requests per day. Zero means no limit.
    pub daily_quota: i64,
}

impl ApiKey {
    /// A key with every permission, for local development and tests.
    pub fn full(key: &str) -> Self {
        Self {
            key: key.to_string(),
            name: "development".into(),
            enabled: true,
            allow_locate: true,
            allow_submit: true,
            allow_admin: true,
            daily_quota: 0,
        }
    }
}

#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Fetch many stations at once. Missing keys are simply absent from the
    /// result, so the caller must not assume the order or the length matches.
    async fn get_stations(&self, keys: &[StationKey]) -> Result<Vec<Station>>;

    /// Write stations, replacing whatever was there.
    ///
    /// Every write carries the whole row on purpose. A partial write would
    /// leave the untouched columns ageing on their own expiry clock, and a
    /// station whose position expired while its timestamps survived is a
    /// corrupt row that nothing downstream expects.
    async fn put_stations(&self, stations: &[Station]) -> Result<()>;

    /// Fetch one station. A convenience over [`Store::get_stations`].
    async fn get_station(&self, key: &StationKey) -> Result<Option<Station>> {
        Ok(self.get_stations(std::slice::from_ref(key)).await?.into_iter().next())
    }

    /// Remove a station outright. Used by tests and by administrative repair,
    /// not by the ingest path, which lets rows expire instead.
    async fn delete_station(&self, key: &StationKey) -> Result<()>;

    /// Stations whose position falls in a spatial index cell.
    async fn stations_near(&self, h3: i64, kind: StationKind) -> Result<Vec<StationKey>>;

    async fn get_api_key(&self, key: &str) -> Result<Option<ApiKey>>;
    async fn put_api_key(&self, api_key: &ApiKey) -> Result<()>;

    /// Cheap liveness check, used by the health endpoint and to decide whether
    /// the read path should fall back to serving from cache alone.
    async fn health(&self) -> Result<()>;
}
