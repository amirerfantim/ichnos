//! Every threshold, weight and limit the service uses.
//!
//! Nothing that shapes behaviour is a constant in the code. Defaults reproduce
//! Mozilla Ichnaea's behaviour, and the `source` noted on each field says which
//! upstream constant a default came from, so any deviation is deliberate and
//! traceable.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Where [`Config::find`] looks when no file is named: where a deployment
/// mounts it, then the repository's own config for local runs.
pub const DEFAULT_PATHS: [&str; 2] = ["/etc/ichnos/config.yaml", "config/config.yaml"];

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Config {
    pub log: Log,
    pub server: Server,
    pub locate: Locate,
    pub validation: Validation,
    pub station: StationCfg,
    pub block: Block,
    pub score: Score,
    pub ingest: Ingest,
    pub lease: Lease,
    pub elision: Elision,
    pub spatial: Spatial,
    pub cache: Cache,
    pub queue: QueueCfg,
    pub cql: Cql,
    pub redis: RedisCfg,
    pub api: Api,
    pub metrics: MetricsCfg,
    pub worker: WorkerCfg,
    pub import: Import,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Log {
    /// `debug`, `info`, `warn` or `error`.
    pub level: String,
    /// `json`, one object per line for machines, or `pretty` for a terminal.
    pub format: String,
}

impl Default for Log {
    fn default() -> Self {
        Self { level: "info".into(), format: "json".into() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Import {
    /// Stations per database write in `ichnos import`.
    pub batch: usize,
}

impl Default for Import {
    fn default() -> Self {
        Self { batch: 1_000 }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkerCfg {
    /// The name in the lease registry. Empty uses the host name, which in
    /// Kubernetes is the pod name: every worker needs a name of its own, so
    /// leave it empty in a Deployment.
    pub identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Locate {
    pub wifi: LocateNetwork,
    pub cell: LocateNetwork,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
    /// Report times outside this range, in Unix milliseconds, are not times
    /// at all (2001 to 2286 by default), and the clock is used instead.
    pub timestamp_ms: (i64, i64),
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
            invalid_macs: vec!["000000000000".into(), "ffffffffffff".into(), "01005e901000".into()],
            timestamp_ms: (1_000_000_000_000, 10_000_000_000_000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
    /// Batches a worker will drain from one shard before moving on.
    ///
    /// A worker keeps pulling while a shard still has work, rather than taking
    /// one batch and waiting for the next window. Without that, throughput is
    /// capped at read_batch per shard per window regardless of how much is
    /// waiting, and the queue grows without limit. The cap stops one busy
    /// shard starving the others.
    pub max_drain_batches: usize,
    /// How long an entry may sit taken but unacknowledged before any worker
    /// holding its shard takes it again.
    ///
    /// An entry is left unacknowledged when its worker died mid-batch, lost
    /// its lease, or failed to write. Without this it would stay pending for
    /// ever, because a normal read only returns entries never delivered. It
    /// must be at least the lease TTL: any shorter and a batch could be taken
    /// from a worker that still owns the shard and is still writing it.
    pub redeliver_after_secs: u64,
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
            max_drain_batches: 50,
            redeliver_after_secs: 30,
            collapse_by_position: false,
            collapse_bucket_m: 10.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Spatial {
    /// H3 resolution for the "what is known near here" index. Nine is about a
    /// 170 m cell, which is fine enough to be useful and coarse enough that a
    /// settled station almost never changes cell.
    pub h3_resolution: u8,
}

impl Default for Spatial {
    fn default() -> Self {
        Self { h3_resolution: 9 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct MetricsCfg {
    /// Sample queue depth in the background. Turning this off leaves the depth
    /// gauges empty, which costs the earliest warning the service has.
    pub poll_enabled: bool,
    /// How often to sample queue depth. Each sample costs one cheap call per
    /// shard, so this is a trade between resolution and chatter.
    pub poll_secs: u64,
    /// Record the shape of each kept observation: its weight, claimed
    /// accuracy, reporter speed and scan age.
    ///
    /// Four histogram writes per observation, on the hottest path there is.
    /// Worth it while tuning the weights or hunting stationary reporters, and
    /// worth turning off once the answers stop changing.
    pub observation_detail: bool,
}

impl Default for MetricsCfg {
    fn default() -> Self {
        Self { poll_enabled: true, poll_secs: 15, observation_detail: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Cache {
    pub enabled: bool,
    /// How long a known station stays cached.
    pub hit_ttl_secs: u64,
    /// How long a key we have never seen stays cached as a negative. This is
    /// what stops unknown stations costing a database read every time.
    pub miss_ttl_secs: u64,
    /// How long connecting to the cache server may take. The quota counters
    /// live there too and use the same limits.
    pub dial_timeout_ms: u64,
    /// How long one reply may take. A cache reply takes well under a
    /// millisecond.
    pub read_timeout_ms: u64,
    /// The most one cache call may take, all in: connecting, retrying,
    /// sending and the reply. The timeouts above bound one attempt; when a
    /// server vanishes mid-flight, a call can also wait on the reconnect and
    /// its retry, past a short caller's deadline.
    pub budget_ms: u64,
    /// How long the cache is skipped after it fails to answer, so requests go
    /// straight to the database instead of each waiting out its own timeout.
    pub pause_ms: u64,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            enabled: true,
            hit_ttl_secs: 300,
            miss_ttl_secs: 60,
            dial_timeout_ms: 200,
            read_timeout_ms: 100,
            budget_ms: 150,
            pause_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct QueueCfg {
    /// How long connecting to the queue server may take.
    pub dial_timeout_ms: u64,
    /// How long one reply may take.
    pub read_timeout_ms: u64,
    /// Retries of a command after a dropped connection.
    pub max_retries: usize,
    /// How long publishing is refused after the queue server fails to
    /// answer. A client that submitted can do nothing about a queue that is
    /// down: it gets its answer at once and the loss is counted, rather than
    /// every submit waiting out its own timeout.
    pub publish_pause_ms: u64,
    /// Every worker that ever read a shard stays in its group's consumer
    /// list, which grows with every restart. A read looks at most once per
    /// `pruneEverySecs` for consumers idle longer than this and holding no
    /// entries, and removes them.
    pub consumer_idle_secs: u64,
    pub prune_every_secs: u64,
    /// Calls taking back stuck entries in one read.
    pub max_reclaim_calls: usize,
    /// Entries per acknowledgement. At most 7000: Lua unpacks at most about
    /// 8000 arguments.
    pub ack_chunk: usize,
}

impl Default for QueueCfg {
    fn default() -> Self {
        Self {
            dial_timeout_ms: 200,
            read_timeout_ms: 500,
            max_retries: 1,
            publish_pause_ms: 1_000,
            consumer_idle_secs: 3_600,
            prune_every_secs: 600,
            max_reclaim_calls: 8,
            ack_chunk: 1_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
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
    /// Rewrite the addresses nodes advertise into ones this process can
    /// actually reach, as `"advertised" = "reachable"`.
    ///
    /// A node tells clients where to find it, and behind NAT, a load balancer
    /// or a forwarded port that address is not reachable from outside. Without
    /// a rewrite the driver connects once for metadata and then times out
    /// dialling an address it cannot route to.
    pub address_map: std::collections::BTreeMap<String, String>,
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
            address_map: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct RedisCfg {
    /// The cache's Redis Cluster, by one of its nodes. Losing entries here is
    /// free, so it may run an eviction policy.
    pub cache_url: String,
    /// The queue's and the leases' Redis Cluster, which may be the cache's.
    /// Losing entries here should be a counted decision, so it is best
    /// without an eviction policy; the streams cap themselves instead.
    pub queue_url: String,
    pub pool_timeout_ms: u64,
    /// How long a connection may keep its map of the cluster, which node
    /// holds which slots, after a node fails to answer, before it is made
    /// again with a fresh one. The map is otherwise read again only when a
    /// node redirects a command; a primary that dies cannot, so this bounds
    /// how long commands keep going to it after a replica has taken over.
    pub cluster_reload_ms: u64,
}

impl Default for RedisCfg {
    fn default() -> Self {
        Self {
            cache_url: "redis://127.0.0.1:6379/0".into(),
            queue_url: "redis://127.0.0.1:6380/0".into(),
            pool_timeout_ms: 1_000,
            cluster_reload_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Api {
    /// Networks accepted per request. Beyond this the extras are ignored.
    pub max_networks: usize,
    /// Reports accepted in one observations body.
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
    /// Scales every key's `store_sample_locate`, 0 to 100: the switch to
    /// shed the learning from locates when the write path is behind.
    /// source: global_locate_sample_rate
    pub locate_sample_percent: f64,
    /// How long a daily quota counter lives. More than a day, so a counter
    /// cannot expire before its day ends.
    pub quota_keep_hours: u64,
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
            locate_sample_percent: 100.0,
            quota_keep_hours: 48,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A file that could not be read at all: missing, broken YAML, an unknown
    /// key, or a value of the wrong type.
    #[error("failed to load configuration: {0}")]
    Load(String),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl Config {
    /// The file to read: `path` if one is named, else the first of
    /// [`DEFAULT_PATHS`] that exists, else none, for the defaults alone.
    pub fn find(path: Option<&Path>) -> Option<PathBuf> {
        if let Some(p) = path {
            return Some(p.to_path_buf());
        }
        DEFAULT_PATHS.iter().map(PathBuf::from).find(|p| p.exists())
    }

    /// Read the file at `path` over the defaults, then validate. No path is
    /// the defaults alone. Nothing is read from the environment, so the file
    /// is the whole truth about a deployment.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut cfg = Self::default();
        if let Some(p) = path {
            let raw = std::fs::read_to_string(p)
                .map_err(|e| ConfigError::Load(format!("{}: {e}", p.display())))?;
            cfg = Self::from_yaml(&raw)
                .map_err(|e| ConfigError::Load(format!("{}: {e}", p.display())))?;
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Decode a document over the defaults, refusing keys the service does not
    /// have. A misspelt key is almost always a setting someone believes is in
    /// effect, and silently ignoring it is how that belief survives for months.
    ///
    /// A file only needs the values it changes: it is laid over the defaults
    /// one mapping at a time, so anything it does not mention keeps its
    /// default, down to a single field of a nested section.
    pub fn from_yaml(raw: &str) -> Result<Self, String> {
        let file: serde_yaml_ng::Value = serde_yaml_ng::from_str(raw).map_err(|e| e.to_string())?;
        let mut merged = serde_yaml_ng::to_value(Self::default()).map_err(|e| e.to_string())?;
        overlay(&mut merged, file);
        serde_yaml_ng::from_value(merged).map_err(|e| e.to_string())
    }

    /// The configuration as YAML, as `ichnos show-config` prints it.
    pub fn to_yaml(&self) -> Result<String, String> {
        serde_yaml_ng::to_string(self).map_err(|e| e.to_string())
    }

    /// Defaults with no file, for tests.
    pub fn for_tests() -> Self {
        Self::default()
    }

    /// Reject combinations that look healthy from outside but cannot work.
    /// Failing at startup beats failing quietly for a week.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));

        if self.ingest.shards == 0 {
            return bad("ingest.shards must be at least 1".into());
        }
        if self.redis.cluster_reload_ms == 0 {
            return bad("redis.clusterReloadMs must be at least 1".into());
        }
        if !(0.0..=100.0).contains(&self.api.locate_sample_percent) {
            return bad(format!(
                "api.locateSamplePercent ({}) must be between 0 and 100",
                self.api.locate_sample_percent
            ));
        }
        if self.ingest.fold_window_secs == 0 {
            return bad("ingest.foldWindowSecs must be at least 1".into());
        }
        if self.ingest.stream_maxlen == 0 {
            return bad("ingest.streamMaxlen must be at least 1".into());
        }
        if self.ingest.read_batch == 0 {
            return bad("ingest.readBatch must be at least 1".into());
        }
        if self.ingest.max_drain_batches == 0 {
            return bad(
                "ingest.maxDrainBatches of 0 would stop the worker draining anything".into()
            );
        }
        if self.lease.renew_secs == 0 || self.lease.renew_secs >= self.lease.ttl_secs {
            return bad(format!(
                "lease.renewSecs ({}) must be below lease.ttlSecs ({}), or a lease expires \
                 while its owner still believes it holds the shard",
                self.lease.renew_secs, self.lease.ttl_secs
            ));
        }
        if self.ingest.redeliver_after_secs < self.lease.ttl_secs {
            return bad(format!(
                "ingest.redeliverAfterSecs ({}) must be at least lease.ttlSecs ({}), or a \
                 batch can be taken from a worker that still owns its shard",
                self.ingest.redeliver_after_secs, self.lease.ttl_secs
            ));
        }
        if self.metrics.poll_enabled && self.metrics.poll_secs == 0 {
            return bad("metrics.pollSecs must be at least 1 when polling is on".into());
        }
        if self.cql.nodes.is_empty() {
            return bad("cql.nodes must list at least one node".into());
        }
        if self.cql.ttl_days == 0 {
            return bad(
                "cql.ttlDays of 0 would write rows that never expire, which silently disables \
                 the only thing bounding the dataset"
                    .into(),
            );
        }
        for (name, n) in [("wifi", &self.locate.wifi), ("cell", &self.locate.cell)] {
            if n.min_in_cluster == 0 {
                return bad(format!("locate.{name}.minInCluster must be at least 1"));
            }
            if n.max_in_cluster < n.min_in_cluster {
                return bad(format!(
                    "locate.{name}.maxInCluster ({}) is below minInCluster ({})",
                    n.max_in_cluster, n.min_in_cluster
                ));
            }
            if n.min_accuracy_m <= 0.0 || n.max_accuracy_m < n.min_accuracy_m {
                return bad(format!("locate.{name} accuracy bounds are not a valid range"));
            }
            if n.max_cluster_meters <= 0.0 {
                return bad(format!("locate.{name}.maxClusterMeters must be positive"));
            }
        }
        if self.block.budget_days <= 0.0 {
            return bad("block.budgetDays must be positive".into());
        }
        if self.score.min_sample_score > self.score.max_sample_score {
            return bad("score.minSampleScore is above score.maxSampleScore".into());
        }
        if self.spatial.h3_resolution > 15 {
            return bad("spatial.h3Resolution must be 15 or less".into());
        }
        if self.api.max_networks == 0 {
            return bad("api.maxNetworks must be at least 1".into());
        }
        if !["debug", "info", "warn", "error"].contains(&self.log.level.as_str()) {
            return bad(format!(
                "log.level ({:?}) must be debug, info, warn or error",
                self.log.level
            ));
        }
        if !["json", "pretty"].contains(&self.log.format.as_str()) {
            return bad(format!("log.format ({:?}) must be json or pretty", self.log.format));
        }
        if self.cache.dial_timeout_ms == 0 || self.cache.read_timeout_ms == 0 {
            return bad("cache.dialTimeoutMs and readTimeoutMs must be at least 1".into());
        }
        if self.cache.budget_ms == 0 {
            return bad("cache.budgetMs must be at least 1".into());
        }
        if self.queue.dial_timeout_ms == 0 || self.queue.read_timeout_ms == 0 {
            return bad("queue.dialTimeoutMs and readTimeoutMs must be at least 1".into());
        }
        if self.queue.max_reclaim_calls == 0 {
            return bad("queue.maxReclaimCalls must be at least 1".into());
        }
        if !(1..=7_000).contains(&self.queue.ack_chunk) {
            return bad(
                "queue.ackChunk must be between 1 and 7000: Lua unpacks at most about 8000 \
                 arguments"
                    .into(),
            );
        }
        if self.api.quota_keep_hours <= 24 {
            return bad(
                "api.quotaKeepHours must be more than 24, or a counter can expire before its \
                 day ends"
                    .into(),
            );
        }
        if self.import.batch == 0 {
            return bad("import.batch must be at least 1".into());
        }
        if self.validation.timestamp_ms.0 > self.validation.timestamp_ms.1 {
            return bad("validation.timestampMs is not a valid range".into());
        }
        Ok(())
    }

    /// Time-to-live in seconds, as CQL wants it.
    pub fn ttl_secs(&self) -> u32 {
        self.cql.ttl_days.saturating_mul(86_400)
    }
}

impl Locate {
    /// WiFi and cell defaults differ, so `LocateNetwork::default` alone would
    /// give cell the WiFi numbers.
    pub fn default_pair() -> Self {
        Self { wifi: LocateNetwork::wifi_defaults(), cell: LocateNetwork::cell_defaults() }
    }
}

impl Default for Locate {
    fn default() -> Self {
        Self::default_pair()
    }
}

/// Lay `top` over `base`: mappings merge key by key, anything else replaces.
/// An empty document (null) changes nothing.
fn overlay(base: &mut serde_yaml_ng::Value, top: serde_yaml_ng::Value) {
    use serde_yaml_ng::Value;
    match (base, top) {
        (_, Value::Null) => {}
        (Value::Mapping(b), Value::Mapping(t)) => {
            for (k, v) in t {
                match b.get_mut(&k) {
                    Some(slot) if v.is_mapping() => overlay(slot, v),
                    _ => {
                        b.insert(k, v);
                    }
                }
            }
        }
        (b, t) => *b = t,
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
    fn a_locate_sample_outside_a_percentage_is_rejected() {
        for bad in [-1.0, 100.5, f64::NAN] {
            let mut c = Config::for_tests();
            c.api.locate_sample_percent = bad;
            assert!(c.validate().is_err(), "{bad}");
        }
        let mut c = Config::for_tests();
        c.api.locate_sample_percent = 0.0;
        c.validate().unwrap();
    }

    #[test]
    fn zero_shards_is_rejected() {
        let mut c = Config::for_tests();
        c.ingest.shards = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn a_cluster_map_that_is_never_read_again_is_rejected() {
        let mut c = Config::for_tests();
        c.redis.cluster_reload_ms = 0;
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("redis.clusterReloadMs"), "{err}");
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
    fn redelivery_sooner_than_a_lease_can_expire_is_rejected() {
        let mut c = Config::for_tests();
        c.lease.ttl_secs = 15;
        c.ingest.redeliver_after_secs = 14;
        assert!(c.validate().is_err(), "would take a batch from a live owner");
        c.ingest.redeliver_after_secs = 15;
        assert!(c.validate().is_ok(), "exactly one lease lifetime is enough");
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

    /// A file in a directory of its own, removed when dropped.
    struct TempFile {
        dir: PathBuf,
        path: PathBuf,
    }

    impl TempFile {
        fn new(name: &str, body: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("ichnos-cfg-{}-{name}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("config.yaml");
            std::fs::write(&path, body).unwrap();
            Self { dir, path }
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    #[test]
    fn a_partial_file_only_overrides_what_it_mentions() {
        let f = TempFile::new(
            "partial",
            "ingest:\n  shards: 8\nlocate:\n  cell:\n    minInCluster: 3\n",
        );
        let cfg = Config::load(Some(&f.path)).unwrap();
        assert_eq!(cfg.ingest.shards, 8, "the file's value should win");
        assert_eq!(cfg.ingest.fold_window_secs, 45, "unmentioned values keep their default");
        assert_eq!(cfg.locate.cell.min_in_cluster, 3);
        assert_eq!(
            cfg.locate.cell.max_accuracy_m, 50_000.0,
            "a field left out of a nested section keeps the cell default, not the WiFi one"
        );
        assert_eq!(cfg.locate.wifi, LocateNetwork::wifi_defaults());
    }

    #[test]
    fn keys_are_camel_case() {
        let cfg = Config::from_yaml("ingest:\n  foldWindowSecs: 1\nlog:\n  level: warn\n").unwrap();
        assert_eq!(cfg.ingest.fold_window_secs, 1);
        assert_eq!(cfg.log.level, "warn");
        assert!(Config::from_yaml("ingest:\n  fold_window_secs: 1\n").is_err());
    }

    #[test]
    fn an_unknown_key_is_refused() {
        for doc in ["ingest:\n  shardz: 8\n", "nosuch:\n  x: 1\n", "elision:\n  enabled: maybe\n"] {
            assert!(Config::from_yaml(doc).is_err(), "{doc}");
        }
        let f = TempFile::new("unknown", "ingest:\n  shardz: 8\n");
        assert!(matches!(Config::load(Some(&f.path)), Err(ConfigError::Load(_))));
    }

    #[test]
    fn an_empty_file_is_the_defaults() {
        assert_eq!(Config::from_yaml("").unwrap(), Config::default());
        assert_eq!(Config::from_yaml("# nothing\n").unwrap(), Config::default());
    }

    #[test]
    fn a_missing_file_is_an_error() {
        let path = std::env::temp_dir().join("ichnos-cfg-does-not-exist.yaml");
        assert!(matches!(Config::load(Some(&path)), Err(ConfigError::Load(_))));
    }

    #[test]
    fn an_invalid_file_fails_validation() {
        let f = TempFile::new("invalid", "ingest:\n  shards: 0\n");
        assert!(matches!(Config::load(Some(&f.path)), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn the_environment_is_not_read() {
        // Not set: a test must not change the environment of the others. A
        // load with no file is exactly the defaults, whatever is set.
        assert_eq!(Config::load(None).unwrap(), Config::default());
    }

    #[test]
    fn find_prefers_the_named_file() {
        let named = Path::new("/somewhere/else.yaml");
        assert_eq!(Config::find(Some(named)), Some(named.to_path_buf()));
    }

    #[test]
    fn a_bad_log_setting_is_rejected() {
        for (level, format) in [("loud", "json"), ("info", "xml"), ("", "json")] {
            let mut c = Config::for_tests();
            c.log.level = level.into();
            c.log.format = format.into();
            assert!(c.validate().is_err(), "{level} {format}");
        }
        let mut c = Config::for_tests();
        c.log.level = "debug".into();
        c.log.format = "pretty".into();
        c.validate().unwrap();
    }

    #[test]
    fn a_bad_limit_is_rejected() {
        let cases: [fn(&mut Config); 10] = [
            |c| c.cache.dial_timeout_ms = 0,
            |c| c.cache.read_timeout_ms = 0,
            |c| c.cache.budget_ms = 0,
            |c| c.queue.read_timeout_ms = 0,
            |c| c.queue.max_reclaim_calls = 0,
            |c| c.queue.ack_chunk = 0,
            |c| c.queue.ack_chunk = 7_001,
            |c| c.api.quota_keep_hours = 24,
            |c| c.import.batch = 0,
            |c| c.validation.timestamp_ms = (2, 1),
        ];
        for (i, set) in cases.iter().enumerate() {
            let mut c = Config::for_tests();
            set(&mut c);
            assert!(c.validate().is_err(), "case {i}");
        }
        let mut c = Config::for_tests();
        c.queue.ack_chunk = 7_000;
        c.api.quota_keep_hours = 25;
        c.validation.timestamp_ms = (5, 5);
        c.queue.max_retries = 0;
        c.cache.pause_ms = 0;
        c.validate().unwrap();
    }

    #[test]
    fn the_limits_keep_their_old_values() {
        let c = Config::for_tests();
        assert_eq!((c.cache.dial_timeout_ms, c.cache.read_timeout_ms), (200, 100));
        assert_eq!((c.cache.budget_ms, c.cache.pause_ms), (150, 1_000));
        assert_eq!(
            (c.queue.dial_timeout_ms, c.queue.read_timeout_ms, c.queue.max_retries),
            (200, 500, 1)
        );
        assert_eq!(c.queue.publish_pause_ms, 1_000);
        assert_eq!((c.queue.consumer_idle_secs, c.queue.prune_every_secs), (3_600, 600));
        assert_eq!((c.queue.max_reclaim_calls, c.queue.ack_chunk), (8, 1_000));
        assert_eq!(c.api.quota_keep_hours, 48);
        assert_eq!(c.import.batch, 1_000);
        assert_eq!(c.validation.timestamp_ms, (1_000_000_000_000, 10_000_000_000_000));
    }

    #[test]
    fn config_round_trips_through_yaml() {
        let mut cfg = Config::for_tests();
        cfg.cql.address_map.insert("10.0.0.5:9042".into(), "127.0.0.1:9042".into());
        cfg.validation.mcc_allowlist = vec![432];
        let text = cfg.to_yaml().unwrap();
        assert_eq!(Config::from_yaml(&text).unwrap(), cfg);
    }

    #[test]
    fn the_example_file_loads_and_is_complete() {
        // Every key, so a reader sees every setting in one place.
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/config.yaml");
        let raw = std::fs::read_to_string(&path).unwrap();
        Config::load(Some(&path)).unwrap();
        let file: serde_yaml_ng::Value = serde_yaml_ng::from_str(&raw).unwrap();
        let all = serde_yaml_ng::to_value(Config::default()).unwrap();
        let mut missing = vec![];
        keys_missing("", &all, &file, &mut missing);
        assert!(missing.is_empty(), "config/config.yaml lacks {missing:?}");
    }

    fn keys_missing(
        prefix: &str,
        all: &serde_yaml_ng::Value,
        file: &serde_yaml_ng::Value,
        out: &mut Vec<String>,
    ) {
        let Some(map) = all.as_mapping() else { return };
        for (k, v) in map {
            let name = format!("{prefix}{}", k.as_str().unwrap());
            match file.get(k) {
                None => out.push(name),
                Some(f) => keys_missing(&format!("{name}."), v, f, out),
            }
        }
    }
}
