//! Every threshold, weight and limit the service uses.
//!
//! Nothing that shapes behaviour is a constant in the code. Defaults reproduce
//! Mozilla Ichnaea's behaviour, and the `source` noted on each field says which
//! upstream constant a default came from, so any deviation is deliberate and
//! traceable.

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: Server,
    pub locate: Locate,
    pub validation: Validation,
    pub station: StationCfg,
    pub block: Block,
    pub score: Score,
    pub ingest: Ingest,
    pub lease: Lease,
    pub elision: Elision,
    pub cache: Cache,
    pub cql: Cql,
    pub redis: RedisCfg,
    pub api: Api,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Server {
    pub bind: String,
    /// Whole-request timeout. A locate that cannot answer quickly is worth less
    /// than a fast "no".
    pub request_timeout_ms: u64,
}

impl Default for Server {
    fn default() -> Self {
        Self { bind: "0.0.0.0:8080".into(), request_timeout_ms: 2_000 }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Locate {
    pub wifi: LocateNetwork,
    pub cell: LocateNetwork,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct LocateNetwork {
    /// How far apart two stations may be and still count as one cluster.
    /// source: MAX_WIFI_CLUSTER_METERS
    pub max_cluster_meters: f64,
    /// Known stations a cluster needs before it may produce an answer. Two is
    /// what stops a single access point being located by anyone who asks.
    /// source: MIN_WIFIS_IN_QUERY
    pub min_in_cluster: usize,
    /// Best-scoring stations kept from one cluster for the fit.
    /// source: MAX_WIFIS_IN_CLUSTER
    pub max_in_cluster: usize,
    /// source: WIFI_MIN_ACCURACY / CELL_MIN_ACCURACY
    pub min_accuracy_m: f64,
    /// source: WIFI_MAX_ACCURACY / CELL_MAX_ACCURACY
    pub max_accuracy_m: f64,
    /// Signal assumed when a scan carries none. source: MIN_WIFI_SIGNAL
    pub min_signal_dbm: i32,
}

impl LocateNetwork {
    pub fn wifi_defaults() -> Self {
        Self {
            max_cluster_meters: 500.0,
            min_in_cluster: 2,
            max_in_cluster: 20,
            min_accuracy_m: 10.0,
            max_accuracy_m: 500.0,
            min_signal_dbm: -100,
        }
    }

    pub fn cell_defaults() -> Self {
        Self {
            // Cells are located one at a time rather than clustered, so the
            // cluster knobs only bound the loop.
            max_cluster_meters: 500.0,
            min_in_cluster: 1,
            max_in_cluster: 20,
            min_accuracy_m: 1_000.0,
            max_accuracy_m: 50_000.0,
            min_signal_dbm: -100,
        }
    }
}

impl Default for LocateNetwork {
    fn default() -> Self {
        Self::wifi_defaults()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Validation {
    /// A scan older than this is dropped. source: MAX_OBSERVATION_AGE
    pub max_observation_age_ms: f64,
    /// source: MAX_OBSERVATION_ACCURACY
    pub max_observation_accuracy_m: f64,
    /// source: WIFI_MAX_OBSERVATION_ACCURACY
    pub wifi_max_observation_accuracy_m: f64,
    /// source: CELL_MAX_OBSERVATION_ACCURACY
    pub cell_max_observation_accuracy_m: f64,
    /// source: MAX_OBSERVATION_SPEED
    pub max_observation_speed_mps: f64,
    /// source: MIN_AGE / MAX_AGE
    pub min_age_ms: i64,
    pub max_age_ms: i64,
    pub min_accuracy_m: f64,
    /// source: MAX_ACCURACY
    pub max_accuracy_m: f64,
    pub min_speed_mps: f64,
    /// source: MAX_SPEED
    pub max_speed_mps: f64,
    /// Web Mercator's usable range, which is what map tiles and every client
    /// assume. Anything outside it is a bad fix rather than a polar reading.
    pub lat_bounds: (f64, f64),
    pub lon_bounds: (f64, f64),
    pub wifi_frequency_mhz: (i32, i32),
    pub wifi_signal_dbm: (i32, i32),
    pub lac: (i64, i64),
    pub cid: (i64, i64),
    pub cid_gsm_max: i64,
    pub psc: (i32, i32),
    pub psc_lte_max: i32,
    pub cell_ta: (i32, i32),
    pub mcc: (i32, i32),
    pub mnc: (i32, i32),
    /// Empty means every mobile country code is accepted. Narrowing this to the
    /// countries actually served keeps foreign noise out of the dataset.
    pub mcc_allowlist: Vec<i32>,
    /// Placeholder addresses that are never stored. source: WIFI_TEST_MAC
    pub invalid_macs: Vec<String>,
}

impl Default for Validation {
    fn default() -> Self {
        Self {
            max_observation_age_ms: 20_000.0,
            max_observation_accuracy_m: 1_000.0,
            wifi_max_observation_accuracy_m: 200.0,
            cell_max_observation_accuracy_m: 1_000.0,
            max_observation_speed_mps: 50.0,
            min_age_ms: -3_600_000,
            max_age_ms: 3_600_000,
            min_accuracy_m: 0.0,
            max_accuracy_m: 1_000_000.0,
            min_speed_mps: 0.0,
            max_speed_mps: 300.0,
            lat_bounds: (-85.051, 85.051),
            lon_bounds: (-180.0, 180.0),
            wifi_frequency_mhz: (2_400, 5_999),
            wifi_signal_dbm: (-100, -10),
            lac: (1, 65_533),
            cid: (1, (1i64 << 28) - 1),
            cid_gsm_max: (1i64 << 16) - 1,
            psc: (0, 511),
            psc_lte_max: 503,
            cell_ta: (0, 63),
            mcc: (1, 999),
            mnc: (0, 999),
            mcc_allowlist: vec![],
            invalid_macs: vec![
                "000000000000".into(),
                "ffffffffffff".into(),
                "01005e901000".into(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct StationCfg {
    /// Observations further apart than this make a batch inconsistent, which is
    /// what triggers the move check. source: WIFI_MAX_RADIUS
    pub wifi_max_radius_m: f64,
    /// source: CELL_MAX_RADIUS
    pub cell_max_radius_m: f64,
    /// Ceiling on how much an existing position counts in the blend. Without it
    /// a long-established station could never move at all.
    /// source: StationState.MAX_OLD_WEIGHT
    pub max_old_weight: f64,
    /// Past this age a disagreeing report replaces the position instead of
    /// blocking it. source: StationState.MAX_OLD_DAYS
    pub old_position_days: i64,
}

impl Default for StationCfg {
    fn default() -> Self {
        Self {
            wifi_max_radius_m: 5_000.0,
            cell_max_radius_m: 100_000.0,
            max_old_weight: 10_000.0,
            old_position_days: 365,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Block {
    /// Days a freshly blocked station stays blocked.
    /// source: TEMPORARY_BLOCKLIST_DURATION, which is 2 upstream.
    pub temporary_days: i64,
    /// One forgiven block per this many days of known age. Upstream hardcodes
    /// 30, which puts recovery at 45 days; 10 puts it at 15.
    pub budget_days: f64,
}

impl Default for Block {
    fn default() -> Self {
        Self { temporary_days: 7, budget_days: 10.0 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Score {
    /// Ceiling on the sample term. source: station_score_samples
    pub max_sample_score: f64,
    /// Floor on the sample term.
    pub min_sample_score: f64,
    /// Days between first and last sighting for full collection weight.
    pub collection_days: f64,
}

impl Default for Score {
    fn default() -> Self {
        Self { max_sample_score: 10.0, min_sample_score: 0.5, collection_days: 10.0 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Ingest {
    /// Queue shards. One worker owns whole shards, which is what removes the
    /// need for row locks.
    pub shards: u32,
    /// How long a worker collects before folding and writing.
    pub fold_window_secs: u64,
    /// Cap per shard stream. At the cap the oldest entries drop, which is a
    /// counted decision rather than an out-of-memory surprise.
    pub stream_maxlen: usize,
    /// Entries read from one stream in a single call.
    pub read_batch: usize,
    /// Collapse repeats from one position bucket into a single weighted sample.
    /// Off by default, so counting matches Ichnaea exactly.
    pub collapse_by_position: bool,
    /// Bucket edge in metres, used only when the collapse above is on.
    pub collapse_bucket_m: f64,
}

impl Default for Ingest {
    fn default() -> Self {
        Self {
            shards: 32,
            fold_window_secs: 45,
            stream_maxlen: 200_000,
            read_batch: 5_000,
            collapse_by_position: false,
            collapse_bucket_m: 10.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Lease {
    /// How long a shard lease survives a worker that has stopped renewing it.
    pub ttl_secs: u64,
    /// Renewal interval. Must be comfortably below the TTL.
    pub renew_secs: u64,
    /// Shards one worker will hold at once. Zero means no limit.
    pub max_shards_per_worker: u32,
}

impl Default for Lease {
    fn default() -> Self {
        Self { ttl_secs: 15, renew_secs: 5, max_shards_per_worker: 0 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Elision {
    /// Below this movement, with last_seen already today, nothing is written.
    pub min_move_m: f64,
    /// Turning this off makes every folded batch write, which is what Ichnaea
    /// does for GNSS-sourced reports.
    pub enabled: bool,
}

impl Default for Elision {
    fn default() -> Self {
        Self { min_move_m: 1.0, enabled: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Cache {
    pub enabled: bool,
    /// How long a known station stays cached.
    pub hit_ttl_secs: u64,
    /// How long a key we have never seen stays cached as a negative. This is
    /// what stops unknown stations costing a database read every time.
    pub miss_ttl_secs: u64,
}

impl Default for Cache {
    fn default() -> Self {
        Self { enabled: true, hit_ttl_secs: 300, miss_ttl_secs: 60 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Cql {
    pub nodes: Vec<String>,
    pub keyspace: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Single-partition writes fired concurrently from one folded batch.
    pub write_concurrency: usize,
    /// Parallel single-partition reads per locate. A multi-partition IN would
    /// park one coordinator on the slowest replica of every key.
    pub read_concurrency: usize,
    pub consistency_write: String,
    pub consistency_read: String,
    /// Row time-to-live, refreshed by every write. This replaces a cleanup job.
    pub ttl_days: u32,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

impl Default for Cql {
    fn default() -> Self {
        Self {
            nodes: vec!["127.0.0.1:9042".into()],
            keyspace: "ichnos".into(),
            username: None,
            password: None,
            write_concurrency: 256,
            read_concurrency: 32,
            consistency_write: "LOCAL_QUORUM".into(),
            consistency_read: "LOCAL_ONE".into(),
            ttl_days: 180,
            connect_timeout_ms: 5_000,
            request_timeout_ms: 2_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct RedisCfg {
    /// Cache instance. Losing entries here is free, so it may run an eviction
    /// policy.
    pub cache_url: String,
    /// Queue instance. Losing entries here must be a counted decision, so it
    /// must not run an eviction policy; the streams cap themselves instead.
    pub queue_url: String,
    pub pool_timeout_ms: u64,
}

impl Default for RedisCfg {
    fn default() -> Self {
        Self {
            cache_url: "redis://127.0.0.1:6379/0".into(),
            queue_url: "redis://127.0.0.1:6380/0".into(),
            pool_timeout_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Api {
    /// Networks accepted per request. Beyond this the extras are ignored.
    pub max_networks: usize,
    /// Items accepted in one geosubmit body.
    pub max_items: usize,
    /// Largest request body accepted, in bytes.
    pub max_body_bytes: usize,
    /// Per-key requests per day. Zero means unlimited.
    pub default_daily_quota: i64,
    /// How long an API key stays cached in the process before it is re-read.
    pub key_cache_secs: u64,
    /// Refuse to start when no key is defined. Off makes local development
    /// easy; on is what you want anywhere real.
    pub require_keys: bool,
}

impl Default for Api {
    fn default() -> Self {
        Self {
            max_networks: 20,
            max_items: 100,
            max_body_bytes: 1024 * 1024,
            default_daily_quota: 0,
            key_cache_secs: 60,
            require_keys: true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to load configuration: {0}")]
    Load(#[from] config::ConfigError),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl Config {
    /// Load defaults, then an optional file, then `ICHNOS__` environment
    /// overrides (`ICHNOS__CQL__KEYSPACE=x`), then validate.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut builder = config::Config::builder();

        // Serialising the defaults first means a partial file only overrides
        // what it mentions, rather than blanking whole sections.
        let defaults = Config { locate: Locate::default_pair(), ..Default::default() };
        let defaults_toml =
            toml::to_string(&defaults).map_err(|e| ConfigError::Invalid(e.to_string()))?;
        builder = builder.add_source(config::File::from_str(
            &defaults_toml,
            config::FileFormat::Toml,
        ));

        if let Some(p) = path {
            builder = builder.add_source(config::File::from(p).required(true));
        }
        builder = builder.add_source(
            config::Environment::with_prefix("ICHNOS").separator("__").try_parsing(true),
        );

        let cfg: Config = builder.build()?.try_deserialize()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Defaults with no file and no environment, for tests.
    pub fn for_tests() -> Self {
        Config { locate: Locate::default_pair(), ..Default::default() }
    }

    /// Reject combinations that look healthy from outside but cannot work.
    /// Failing at startup beats failing quietly for a week.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));

        if self.ingest.shards == 0 {
            return bad("ingest.shards must be at least 1".into());
        }
        if self.ingest.fold_window_secs == 0 {
            return bad("ingest.fold_window_secs must be at least 1".into());
        }
        if self.ingest.stream_maxlen == 0 {
            return bad("ingest.stream_maxlen must be at least 1".into());
        }
        if self.lease.renew_secs == 0 || self.lease.renew_secs >= self.lease.ttl_secs {
            return bad(format!(
                "lease.renew_secs ({}) must be below lease.ttl_secs ({}), or a lease expires \
                 while its owner still believes it holds the shard",
                self.lease.renew_secs, self.lease.ttl_secs
            ));
        }
        if self.cql.nodes.is_empty() {
            return bad("cql.nodes must list at least one node".into());
        }
        if self.cql.ttl_days == 0 {
            return bad(
                "cql.ttl_days of 0 would write rows that never expire, which silently disables \
                 the only thing bounding the dataset"
                    .into(),
            );
        }
        for (name, n) in [("wifi", &self.locate.wifi), ("cell", &self.locate.cell)] {
            if n.min_in_cluster == 0 {
                return bad(format!("locate.{name}.min_in_cluster must be at least 1"));
            }
            if n.max_in_cluster < n.min_in_cluster {
                return bad(format!(
                    "locate.{name}.max_in_cluster ({}) is below min_in_cluster ({})",
                    n.max_in_cluster, n.min_in_cluster
                ));
            }
            if n.min_accuracy_m <= 0.0 || n.max_accuracy_m < n.min_accuracy_m {
                return bad(format!("locate.{name} accuracy bounds are not a valid range"));
            }
            if n.max_cluster_meters <= 0.0 {
                return bad(format!("locate.{name}.max_cluster_meters must be positive"));
            }
        }
        if self.block.budget_days <= 0.0 {
            return bad("block.budget_days must be positive".into());
        }
        if self.score.min_sample_score > self.score.max_sample_score {
            return bad("score.min_sample_score is above score.max_sample_score".into());
        }
        if self.api.max_networks == 0 {
            return bad("api.max_networks must be at least 1".into());
        }
        Ok(())
    }

    /// Time-to-live in seconds, as CQL wants it.
    pub fn ttl_secs(&self) -> u32 {
        self.cql.ttl_days.saturating_mul(86_400)
    }
}

impl Locate {
    /// WiFi and cell defaults differ, so `Default` alone would give cell the
    /// WiFi numbers.
    pub fn default_pair() -> Self {
        Self { wifi: LocateNetwork::wifi_defaults(), cell: LocateNetwork::cell_defaults() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        Config::for_tests().validate().unwrap();
    }

    #[test]
    fn defaults_reproduce_upstream_constants() {
        let c = Config::for_tests();
        assert_eq!(c.locate.wifi.max_cluster_meters, 500.0);
        assert_eq!(c.locate.wifi.min_in_cluster, 2);
        assert_eq!(c.locate.wifi.max_in_cluster, 20);
        assert_eq!(c.locate.wifi.min_accuracy_m, 10.0);
        assert_eq!(c.locate.wifi.max_accuracy_m, 500.0);
        assert_eq!(c.locate.cell.min_accuracy_m, 1_000.0);
        assert_eq!(c.locate.cell.max_accuracy_m, 50_000.0);
        assert_eq!(c.validation.max_observation_age_ms, 20_000.0);
        assert_eq!(c.validation.wifi_max_observation_accuracy_m, 200.0);
        assert_eq!(c.validation.max_observation_speed_mps, 50.0);
        assert_eq!(c.station.wifi_max_radius_m, 5_000.0);
        assert_eq!(c.station.cell_max_radius_m, 100_000.0);
        assert_eq!(c.station.max_old_weight, 10_000.0);
        assert_eq!(c.station.old_position_days, 365);
    }

    #[test]
    fn cell_defaults_are_not_the_wifi_defaults() {
        let c = Config::for_tests();
        assert_ne!(c.locate.cell.min_accuracy_m, c.locate.wifi.min_accuracy_m);
    }

    #[test]
    fn zero_shards_is_rejected() {
        let mut c = Config::for_tests();
        c.ingest.shards = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn a_lease_that_expires_before_renewal_is_rejected() {
        let mut c = Config::for_tests();
        c.lease.renew_secs = 20;
        c.lease.ttl_secs = 15;
        assert!(c.validate().is_err());
        c.lease.renew_secs = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn a_ttl_of_zero_is_rejected() {
        let mut c = Config::for_tests();
        c.cql.ttl_days = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn an_impossible_cluster_range_is_rejected() {
        let mut c = Config::for_tests();
        c.locate.wifi.max_in_cluster = 1;
        c.locate.wifi.min_in_cluster = 5;
        assert!(c.validate().is_err());
    }

    #[test]
    fn ttl_secs_is_days_times_a_day() {
        let mut c = Config::for_tests();
        c.cql.ttl_days = 180;
        assert_eq!(c.ttl_secs(), 15_552_000);
    }

    #[test]
    fn a_partial_file_only_overrides_what_it_mentions() {
        let dir = std::env::temp_dir().join(format!("ichnos-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("partial.toml");
        std::fs::write(&path, "[ingest]\nshards = 8\n").unwrap();

        let cfg = Config::load(Some(&path)).unwrap();
        assert_eq!(cfg.ingest.shards, 8, "the file's value should win");
        assert_eq!(cfg.ingest.fold_window_secs, 45, "unmentioned values keep their default");
        assert_eq!(cfg.locate.cell.max_accuracy_m, 50_000.0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn config_round_trips_through_toml() {
        let cfg = Config::for_tests();
        let text = toml::to_string(&cfg).unwrap();
        let back: Config = toml::from_str(&text).unwrap();
        assert_eq!(cfg, back);
    }
}
