//! The production store, speaking CQL.
//!
//! Written against CQL rather than against one vendor, so the same code drives
//! ScyllaDB and Apache Cassandra. Two habits matter here:
//!
//! - **Every write writes the whole row.** A CQL write sets the expiry only on
//!   the columns it touches, so a partial write would leave the rest ageing on
//!   an older clock and eventually produce a half-expired row.
//! - **Reads are one single-partition query per key, run concurrently.** A
//!   multi-partition `IN` looks tidier and behaves worse: it parks a single
//!   coordinator on the slowest replica of every key in the list.

use super::{ApiKey, Result, Station, Store, StoreError};
use crate::config::Cql as CqlCfg;
use crate::model::key::{CellKey, Radio, StationKind, WifiKey};
use crate::model::observation::ReportSource;
use crate::model::StationKey;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use futures::stream::{self, StreamExt};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;
use scylla::{DeserializeRow, SerializeRow};
use std::sync::Arc;
use std::time::Duration;

/// Columns shared by both station tables, in a fixed order.
const STATION_COLUMNS: &str = "lat, lon, radius, max_lat, min_lat, max_lon, min_lon, h3, \
                               samples, weight, source, created, modified, last_seen, \
                               block_first, block_last, block_count";

#[derive(Debug, DeserializeRow)]
struct WifiRow {
    mac: i64,
    lat: Option<f64>,
    lon: Option<f64>,
    radius: Option<i32>,
    max_lat: Option<f64>,
    min_lat: Option<f64>,
    max_lon: Option<f64>,
    min_lon: Option<f64>,
    h3: Option<i64>,
    samples: Option<i32>,
    weight: Option<f64>,
    source: Option<i8>,
    created: Option<DateTime<Utc>>,
    modified: Option<DateTime<Utc>>,
    last_seen: Option<NaiveDate>,
    block_first: Option<NaiveDate>,
    block_last: Option<NaiveDate>,
    block_count: Option<i16>,
}

#[derive(Debug, DeserializeRow)]
struct CellRow {
    radio: i8,
    mcc: i32,
    mnc: i32,
    lac: i64,
    cid: i64,
    lat: Option<f64>,
    lon: Option<f64>,
    radius: Option<i32>,
    max_lat: Option<f64>,
    min_lat: Option<f64>,
    max_lon: Option<f64>,
    min_lon: Option<f64>,
    h3: Option<i64>,
    samples: Option<i32>,
    weight: Option<f64>,
    source: Option<i8>,
    created: Option<DateTime<Utc>>,
    modified: Option<DateTime<Utc>>,
    last_seen: Option<NaiveDate>,
    block_first: Option<NaiveDate>,
    block_last: Option<NaiveDate>,
    block_count: Option<i16>,
}

/// The station columns as they are bound on a write, in schema order.
#[derive(Debug, SerializeRow)]
struct StationValues {
    lat: Option<f64>,
    lon: Option<f64>,
    radius: Option<i32>,
    max_lat: Option<f64>,
    min_lat: Option<f64>,
    max_lon: Option<f64>,
    min_lon: Option<f64>,
    h3: Option<i64>,
    samples: i32,
    weight: f64,
    source: Option<i8>,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    last_seen: Option<NaiveDate>,
    block_first: Option<NaiveDate>,
    block_last: Option<NaiveDate>,
    block_count: i16,
}

/// A whole wifi row, key column first.
///
/// The key and the station columns cannot be bound as two separate values: a
/// prepared statement takes one flat row, so they live in one struct.
#[derive(Debug, SerializeRow)]
struct WifiWrite {
    mac: i64,
    lat: Option<f64>,
    lon: Option<f64>,
    radius: Option<i32>,
    max_lat: Option<f64>,
    min_lat: Option<f64>,
    max_lon: Option<f64>,
    min_lon: Option<f64>,
    h3: Option<i64>,
    samples: i32,
    weight: f64,
    source: Option<i8>,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    last_seen: Option<NaiveDate>,
    block_first: Option<NaiveDate>,
    block_last: Option<NaiveDate>,
    block_count: i16,
}

/// A whole cell row, key columns first.
#[derive(Debug, SerializeRow)]
struct CellWrite {
    radio: i8,
    mcc: i32,
    mnc: i32,
    lac: i64,
    cid: i64,
    lat: Option<f64>,
    lon: Option<f64>,
    radius: Option<i32>,
    max_lat: Option<f64>,
    min_lat: Option<f64>,
    max_lon: Option<f64>,
    min_lon: Option<f64>,
    h3: Option<i64>,
    samples: i32,
    weight: f64,
    source: Option<i8>,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    last_seen: Option<NaiveDate>,
    block_first: Option<NaiveDate>,
    block_last: Option<NaiveDate>,
    block_count: i16,
}

impl WifiWrite {
    fn new(mac: i64, v: StationValues) -> Self {
        Self {
            mac,
            lat: v.lat, lon: v.lon, radius: v.radius,
            max_lat: v.max_lat, min_lat: v.min_lat, max_lon: v.max_lon, min_lon: v.min_lon,
            h3: v.h3, samples: v.samples, weight: v.weight, source: v.source,
            created: v.created, modified: v.modified, last_seen: v.last_seen,
            block_first: v.block_first, block_last: v.block_last, block_count: v.block_count,
        }
    }
}

impl CellWrite {
    fn new(c: CellKey, v: StationValues) -> Self {
        Self {
            radio: c.radio.as_i8(), mcc: c.mcc, mnc: c.mnc, lac: c.lac, cid: c.cid,
            lat: v.lat, lon: v.lon, radius: v.radius,
            max_lat: v.max_lat, min_lat: v.min_lat, max_lon: v.max_lon, min_lon: v.min_lon,
            h3: v.h3, samples: v.samples, weight: v.weight, source: v.source,
            created: v.created, modified: v.modified, last_seen: v.last_seen,
            block_first: v.block_first, block_last: v.block_last, block_count: v.block_count,
        }
    }
}

impl StationValues {
    fn from(station: &Station) -> Self {
        Self {
            lat: station.lat,
            lon: station.lon,
            radius: station.radius,
            max_lat: station.max_lat,
            min_lat: station.min_lat,
            max_lon: station.max_lon,
            min_lon: station.min_lon,
            h3: station.h3,
            samples: station.samples,
            weight: station.weight,
            source: station.source.map(|s| s.as_i8()),
            created: station.created,
            modified: station.modified,
            last_seen: station.last_seen,
            block_first: station.block_first,
            block_last: station.block_last,
            block_count: station.block_count as i16,
        }
    }
}

#[derive(Debug, DeserializeRow)]
struct ApiKeyRow {
    key: String,
    name: Option<String>,
    enabled: Option<bool>,
    allow_locate: Option<bool>,
    allow_submit: Option<bool>,
    allow_admin: Option<bool>,
    daily_quota: Option<i64>,
}

/// Prepared statements, made once at startup.
///
/// Preparing on every call would cost a round trip and lose token awareness,
/// which is what lets the driver send a query straight to the replica that owns
/// the data instead of to an arbitrary coordinator.
struct Statements {
    select_wifi: PreparedStatement,
    select_cell: PreparedStatement,
    insert_wifi: PreparedStatement,
    insert_cell: PreparedStatement,
    delete_wifi: PreparedStatement,
    delete_cell: PreparedStatement,
    select_near: PreparedStatement,
    insert_near: PreparedStatement,
    select_api_key: PreparedStatement,
    insert_api_key: PreparedStatement,
}

pub struct CqlStore {
    session: Arc<Session>,
    statements: Arc<Statements>,
    cfg: CqlCfg,
    ttl_secs: i32,
}

impl CqlStore {
    /// Connect, prepare every statement, and check the keyspace is there.
    pub async fn connect(cfg: &CqlCfg) -> Result<Self> {
        let mut builder = SessionBuilder::new()
            .connection_timeout(Duration::from_millis(cfg.connect_timeout_ms))
            .use_keyspace(&cfg.keyspace, false);
        for node in &cfg.nodes {
            builder = builder.known_node(node);
        }
        if let (Some(user), Some(pass)) = (&cfg.username, &cfg.password) {
            builder = builder.user(user, pass);
        }

        let session =
            builder.build().await.map_err(|e| StoreError::Unavailable(e.to_string()))?;

        let ttl_secs = i32::try_from(cfg.ttl_days.saturating_mul(86_400))
            .map_err(|_| StoreError::Query("ttl_days is too large for CQL".into()))?;

        let statements = Self::prepare_all(&session, ttl_secs, cfg).await?;

        Ok(Self {
            session: Arc::new(session),
            statements: Arc::new(statements),
            cfg: cfg.clone(),
            ttl_secs,
        })
    }

    async fn prepare_all(
        session: &Session,
        ttl_secs: i32,
        cfg: &CqlCfg,
    ) -> Result<Statements> {
        let read = parse_consistency(&cfg.consistency_read)?;
        let write = parse_consistency(&cfg.consistency_write)?;

        let prepare = |cql: String, consistency: Consistency| async move {
            let mut stmt: PreparedStatement =
                session.prepare(cql).await.map_err(|e| StoreError::Query(e.to_string()))?;
            stmt.set_consistency(consistency);
            Ok::<PreparedStatement, StoreError>(stmt)
        };

        let wifi_write_cols = format!("mac, {STATION_COLUMNS}");
        let cell_write_cols = format!("radio, mcc, mnc, lac, cid, {STATION_COLUMNS}");

        Ok(Statements {
            select_wifi: prepare(
                format!("SELECT mac, {STATION_COLUMNS} FROM wifi WHERE mac = ?"),
                read,
            )
            .await?,
            select_cell: prepare(
                format!(
                    "SELECT radio, mcc, mnc, lac, cid, {STATION_COLUMNS} FROM cell \
                     WHERE radio = ? AND mcc = ? AND mnc = ? AND lac = ? AND cid = ?"
                ),
                read,
            )
            .await?,
            insert_wifi: prepare(
                format!(
                    "INSERT INTO wifi ({wifi_write_cols}) VALUES ({}) USING TTL {ttl_secs}",
                    placeholders(18)
                ),
                write,
            )
            .await?,
            insert_cell: prepare(
                format!(
                    "INSERT INTO cell ({cell_write_cols}) VALUES ({}) USING TTL {ttl_secs}",
                    placeholders(22)
                ),
                write,
            )
            .await?,
            delete_wifi: prepare("DELETE FROM wifi WHERE mac = ?".to_string(), write).await?,
            delete_cell: prepare(
                "DELETE FROM cell WHERE radio = ? AND mcc = ? AND mnc = ? AND lac = ? AND cid = ?"
                    .to_string(),
                write,
            )
            .await?,
            select_near: prepare(
                "SELECT key FROM station_by_h3 WHERE h3 = ? AND kind = ?".to_string(),
                read,
            )
            .await?,
            insert_near: prepare(
                format!(
                    "INSERT INTO station_by_h3 (h3, kind, key) VALUES (?, ?, ?) USING TTL {ttl_secs}"
                ),
                write,
            )
            .await?,
            select_api_key: prepare(
                "SELECT key, name, enabled, allow_locate, allow_submit, allow_admin, daily_quota \
                 FROM api_key WHERE key = ?"
                    .to_string(),
                read,
            )
            .await?,
            insert_api_key: prepare(
                "INSERT INTO api_key (key, name, enabled, allow_locate, allow_submit, \
                 allow_admin, daily_quota, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                    .to_string(),
                write,
            )
            .await?,
        })
    }

    /// Seconds each written row is given to live.
    pub fn ttl_secs(&self) -> i32 {
        self.ttl_secs
    }

    async fn get_one(&self, key: &StationKey) -> Result<Option<Station>> {
        match key {
            StationKey::Wifi(mac) => {
                let result = self
                    .session
                    .execute_unpaged(&self.statements.select_wifi, (mac.0 as i64,))
                    .await
                    .map_err(|e| StoreError::Query(e.to_string()))?;
                let rows = result
                    .into_rows_result()
                    .map_err(|e| StoreError::Malformed(e.to_string()))?;
                match rows.rows::<WifiRow>() {
                    Ok(mut iter) => match iter.next() {
                        Some(Ok(row)) => Ok(Some(wifi_row_to_station(row)?)),
                        Some(Err(e)) => Err(StoreError::Malformed(e.to_string())),
                        None => Ok(None),
                    },
                    Err(e) => Err(StoreError::Malformed(e.to_string())),
                }
            }
            StationKey::Cell(c) => {
                let result = self
                    .session
                    .execute_unpaged(
                        &self.statements.select_cell,
                        (c.radio.as_i8(), c.mcc, c.mnc, c.lac, c.cid),
                    )
                    .await
                    .map_err(|e| StoreError::Query(e.to_string()))?;
                let rows = result
                    .into_rows_result()
                    .map_err(|e| StoreError::Malformed(e.to_string()))?;
                match rows.rows::<CellRow>() {
                    Ok(mut iter) => match iter.next() {
                        Some(Ok(row)) => Ok(Some(cell_row_to_station(row)?)),
                        Some(Err(e)) => Err(StoreError::Malformed(e.to_string())),
                        None => Ok(None),
                    },
                    Err(e) => Err(StoreError::Malformed(e.to_string())),
                }
            }
        }
    }

    async fn put_one(&self, station: &Station) -> Result<()> {
        let values = StationValues::from(station);
        match station.key {
            StationKey::Wifi(mac) => {
                self.session
                    .execute_unpaged(
                        &self.statements.insert_wifi,
                        WifiWrite::new(mac.0 as i64, values),
                    )
                    .await
                    .map_err(|e| StoreError::Query(e.to_string()))?;
            }
            StationKey::Cell(c) => {
                self.session
                    .execute_unpaged(&self.statements.insert_cell, CellWrite::new(c, values))
                    .await
                    .map_err(|e| StoreError::Query(e.to_string()))?;
            }
        }

        // Mirror the spatial cell. Only worth doing when the station actually
        // has a position; a blocked station has none.
        if let Some(h3) = station.h3 {
            let kind = station.key.kind() as i8;
            let packed = pack_key(&station.key);
            self.session
                .execute_unpaged(&self.statements.insert_near, (h3, kind, packed))
                .await
                .map_err(|e| StoreError::Query(e.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Store for CqlStore {
    async fn get_stations(&self, keys: &[StationKey]) -> Result<Vec<Station>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let lookups: Vec<_> = keys.iter().map(|key| self.get_one(key)).collect();
        let found: Vec<Result<Option<Station>>> = stream::iter(lookups)
            .buffer_unordered(self.cfg.read_concurrency.max(1))
            .collect()
            .await;

        let mut out = Vec::with_capacity(keys.len());
        for entry in found {
            if let Some(station) = entry? {
                out.push(station);
            }
        }
        Ok(out)
    }

    async fn put_stations(&self, stations: &[Station]) -> Result<()> {
        if stations.is_empty() {
            return Ok(());
        }
        let writes: Vec<_> = stations.iter().map(|station| self.put_one(station)).collect();
        let results: Vec<Result<()>> = stream::iter(writes)
            .buffer_unordered(self.cfg.write_concurrency.max(1))
            .collect()
            .await;
        for result in results {
            result?;
        }
        Ok(())
    }

    async fn delete_station(&self, key: &StationKey) -> Result<()> {
        match key {
            StationKey::Wifi(mac) => {
                self.session
                    .execute_unpaged(&self.statements.delete_wifi, (mac.0 as i64,))
                    .await
                    .map_err(|e| StoreError::Query(e.to_string()))?;
            }
            StationKey::Cell(c) => {
                self.session
                    .execute_unpaged(
                        &self.statements.delete_cell,
                        (c.radio.as_i8(), c.mcc, c.mnc, c.lac, c.cid),
                    )
                    .await
                    .map_err(|e| StoreError::Query(e.to_string()))?;
            }
        }
        Ok(())
    }

    async fn stations_near(&self, h3: i64, kind: StationKind) -> Result<Vec<StationKey>> {
        let result = self
            .session
            .execute_unpaged(&self.statements.select_near, (h3, kind as i8))
            .await
            .map_err(|e| StoreError::Query(e.to_string()))?;
        let rows = result.into_rows_result().map_err(|e| StoreError::Malformed(e.to_string()))?;

        let mut out = Vec::new();
        for row in rows.rows::<(i64,)>().map_err(|e| StoreError::Malformed(e.to_string()))? {
            let (packed,) = row.map_err(|e| StoreError::Malformed(e.to_string()))?;
            // Only WiFi keys survive packing losslessly; cells are stored as a
            // hash and are resolved by the caller through the station table.
            if kind == StationKind::Wifi {
                out.push(StationKey::Wifi(WifiKey(packed as u64)));
            }
        }
        Ok(out)
    }

    async fn get_api_key(&self, key: &str) -> Result<Option<ApiKey>> {
        let result = self
            .session
            .execute_unpaged(&self.statements.select_api_key, (key,))
            .await
            .map_err(|e| StoreError::Query(e.to_string()))?;
        let rows = result.into_rows_result().map_err(|e| StoreError::Malformed(e.to_string()))?;
        let mut iter =
            rows.rows::<ApiKeyRow>().map_err(|e| StoreError::Malformed(e.to_string()))?;
        match iter.next() {
            Some(Ok(row)) => Ok(Some(ApiKey {
                key: row.key,
                name: row.name.unwrap_or_default(),
                enabled: row.enabled.unwrap_or(false),
                allow_locate: row.allow_locate.unwrap_or(false),
                allow_submit: row.allow_submit.unwrap_or(false),
                allow_admin: row.allow_admin.unwrap_or(false),
                daily_quota: row.daily_quota.unwrap_or(0),
            })),
            Some(Err(e)) => Err(StoreError::Malformed(e.to_string())),
            None => Ok(None),
        }
    }

    async fn put_api_key(&self, api_key: &ApiKey) -> Result<()> {
        self.session
            .execute_unpaged(
                &self.statements.insert_api_key,
                (
                    &api_key.key,
                    &api_key.name,
                    api_key.enabled,
                    api_key.allow_locate,
                    api_key.allow_submit,
                    api_key.allow_admin,
                    api_key.daily_quota,
                    Utc::now(),
                ),
            )
            .await
            .map_err(|e| StoreError::Query(e.to_string()))?;
        Ok(())
    }

    async fn health(&self) -> Result<()> {
        self.session
            .query_unpaged("SELECT key FROM api_key LIMIT 1", ())
            .await
            .map(|_| ())
            .map_err(|e| StoreError::Unavailable(e.to_string()))
    }
}

fn placeholders(n: usize) -> String {
    std::iter::repeat_n("?", n).collect::<Vec<_>>().join(", ")
}

fn parse_consistency(name: &str) -> Result<Consistency> {
    match name.to_ascii_uppercase().as_str() {
        "ONE" => Ok(Consistency::One),
        "TWO" => Ok(Consistency::Two),
        "THREE" => Ok(Consistency::Three),
        "QUORUM" => Ok(Consistency::Quorum),
        "ALL" => Ok(Consistency::All),
        "LOCAL_QUORUM" | "LOCALQUORUM" => Ok(Consistency::LocalQuorum),
        "EACH_QUORUM" | "EACHQUORUM" => Ok(Consistency::EachQuorum),
        "LOCAL_ONE" | "LOCALONE" => Ok(Consistency::LocalOne),
        "ANY" => Ok(Consistency::Any),
        other => Err(StoreError::Query(format!("unknown consistency level {other:?}"))),
    }
}

/// Fold a station key into one integer for the spatial mirror.
///
/// WiFi keys fit exactly. Cell keys do not, so they are hashed; the mirror is
/// only ever used to find candidates, which are then read from the station
/// table by their real key.
fn pack_key(key: &StationKey) -> i64 {
    match key {
        StationKey::Wifi(w) => w.0 as i64,
        StationKey::Cell(_) => {
            let token = key.to_token();
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in token.as_bytes() {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            hash as i64
        }
    }
}

fn wifi_row_to_station(row: WifiRow) -> Result<Station> {
    Ok(Station {
        key: StationKey::Wifi(WifiKey(row.mac as u64)),
        lat: row.lat,
        lon: row.lon,
        radius: row.radius,
        max_lat: row.max_lat,
        min_lat: row.min_lat,
        max_lon: row.max_lon,
        min_lon: row.min_lon,
        h3: row.h3,
        samples: row.samples.unwrap_or(0),
        weight: row.weight.unwrap_or(0.0),
        source: row.source.and_then(ReportSource::from_i8),
        created: row.created.unwrap_or_else(Utc::now),
        modified: row.modified.unwrap_or_else(Utc::now),
        last_seen: row.last_seen,
        block_first: row.block_first,
        block_last: row.block_last,
        block_count: i32::from(row.block_count.unwrap_or(0)),
    })
}

fn cell_row_to_station(row: CellRow) -> Result<Station> {
    let radio = Radio::from_i8(row.radio)
        .ok_or_else(|| StoreError::Malformed(format!("unknown radio {}", row.radio)))?;
    Ok(Station {
        key: StationKey::Cell(CellKey {
            radio,
            mcc: row.mcc,
            mnc: row.mnc,
            lac: row.lac,
            cid: row.cid,
        }),
        lat: row.lat,
        lon: row.lon,
        radius: row.radius,
        max_lat: row.max_lat,
        min_lat: row.min_lat,
        max_lon: row.max_lon,
        min_lon: row.min_lon,
        h3: row.h3,
        samples: row.samples.unwrap_or(0),
        weight: row.weight.unwrap_or(0.0),
        source: row.source.and_then(ReportSource::from_i8),
        created: row.created.unwrap_or_else(Utc::now),
        modified: row.modified.unwrap_or_else(Utc::now),
        last_seen: row.last_seen,
        block_first: row.block_first,
        block_last: row.block_last,
        block_count: i32::from(row.block_count.unwrap_or(0)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_lists_have_the_right_shape() {
        assert_eq!(placeholders(1), "?");
        assert_eq!(placeholders(3), "?, ?, ?");
    }

    #[test]
    fn consistency_levels_parse_in_the_spellings_people_write() {
        assert_eq!(parse_consistency("LOCAL_QUORUM").unwrap(), Consistency::LocalQuorum);
        assert_eq!(parse_consistency("local_one").unwrap(), Consistency::LocalOne);
        assert_eq!(parse_consistency("QUORUM").unwrap(), Consistency::Quorum);
        assert!(parse_consistency("PRETTY_SURE").is_err());
    }

    #[test]
    fn wifi_keys_pack_losslessly() {
        let key = StationKey::Wifi(WifiKey::parse("aabbccddeeff").unwrap());
        assert_eq!(pack_key(&key) as u64, 0xaabb_ccdd_eeff);
    }

    #[test]
    fn different_cell_keys_pack_to_different_values() {
        let a = StationKey::Cell(CellKey { radio: Radio::Lte, mcc: 432, mnc: 11, lac: 1, cid: 2 });
        let b = StationKey::Cell(CellKey { radio: Radio::Lte, mcc: 432, mnc: 11, lac: 1, cid: 3 });
        assert_ne!(pack_key(&a), pack_key(&b));
    }

    #[test]
    fn the_column_list_matches_the_write_binding() {
        // If these drift, every write silently lands in the wrong columns.
        assert_eq!(STATION_COLUMNS.split(',').count(), 17);
    }
}
