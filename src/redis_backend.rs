//! Redis implementations of the cache, the queue and the shard leases.
//!
//! Two separate Redis instances, and the separation is not fussiness. Redis
//! applies its eviction policy per instance, so a cache configured to evict
//! under pressure would happily throw away queued work sitting in the same
//! server. Losing a cache entry costs a database read. Losing a queue entry
//! costs data. Those need different policies, so they need different servers:
//!
//! - the **cache** instance may evict freely,
//! - the **queue** instance must not evict at all, and bounds itself by capping
//!   each stream instead.

use crate::ingest::lease::{LeaseError, Leases};
use crate::ingest::queue::{Entry, Queue, QueueError};
use crate::model::Observation;
use crate::store::cache::CacheBackend;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Open a connection manager, which reconnects on its own after a drop.
async fn connect(url: &str) -> Result<ConnectionManager, String> {
    let client = Client::open(url).map_err(|e| e.to_string())?;
    ConnectionManager::new(client).await.map_err(|e| e.to_string())
}

// ---------------------------------------------------------------- cache -----

/// The cache backend. Every failure here is swallowed and counted: a request
/// must never fail because a cache was unreachable.
#[derive(Clone)]
pub struct RedisCache {
    conn: ConnectionManager,
    errors: Arc<AtomicU64>,
}

impl RedisCache {
    pub async fn connect(url: &str) -> Result<Self, String> {
        Ok(Self { conn: connect(url).await?, errors: Arc::new(AtomicU64::new(0)) })
    }

    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    fn note(&self, what: &str, error: redis::RedisError) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        warn!(operation = what, error = %error, "cache operation failed; falling through");
    }
}

#[async_trait]
impl CacheBackend for RedisCache {
    async fn get_many(&self, keys: &[String]) -> Vec<Option<String>> {
        if keys.is_empty() {
            return vec![];
        }
        let mut conn = self.conn.clone();
        // A single MGET, so one round trip covers every station in a request.
        match conn.mget::<_, Vec<Option<String>>>(keys).await {
            Ok(values) if values.len() == keys.len() => values,
            Ok(_) => vec![None; keys.len()],
            Err(e) => {
                self.note("mget", e);
                vec![None; keys.len()]
            }
        }
    }

    async fn set_many(&self, writes: &[(String, String, u64)]) {
        if writes.is_empty() {
            return;
        }
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for (key, value, ttl) in writes {
            pipe.set_ex(key, value, *ttl).ignore();
        }
        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
            self.note("set", e);
        }
    }

    async fn delete_many(&self, keys: &[String]) {
        if keys.is_empty() {
            return;
        }
        let mut conn = self.conn.clone();
        if let Err(e) = conn.del::<_, ()>(keys).await {
            self.note("del", e);
        }
    }

    async fn healthy(&self) -> bool {
        let mut conn = self.conn.clone();
        redis::cmd("PING").query_async::<String>(&mut conn).await.is_ok()
    }
}

// ---------------------------------------------------------------- queue -----

/// The queue backend: one capped stream per shard, with a consumer group.
///
/// Consumer groups are what make redelivery work. An entry stays pending until
/// acknowledged, so a worker that dies mid-batch leaves its work behind rather
/// than taking it to the grave.
#[derive(Clone)]
pub struct RedisQueue {
    conn: ConnectionManager,
    maxlen: usize,
    group: String,
    consumer: String,
    /// How long an entry sits unacknowledged before it is taken again.
    redeliver_after: std::time::Duration,
    trimmed: Arc<AtomicU64>,
}

impl RedisQueue {
    pub async fn connect(
        url: &str,
        shards: u32,
        maxlen: usize,
        consumer: String,
        redeliver_after: std::time::Duration,
    ) -> Result<Self, String> {
        let queue = Self {
            conn: connect(url).await?,
            maxlen,
            group: "ichnos".into(),
            consumer,
            redeliver_after,
            trimmed: Arc::new(AtomicU64::new(0)),
        };
        for shard in 0..shards {
            queue.ensure_group(shard).await?;
        }
        Ok(queue)
    }

    fn stream(shard: u32) -> String {
        format!("obs:{shard}")
    }

    /// Create the consumer group, tolerating the case where it already exists.
    async fn ensure_group(&self, shard: u32) -> Result<(), String> {
        let mut conn = self.conn.clone();
        let result: Result<String, redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(Self::stream(shard))
            .arg(&self.group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    /// Take over entries that have sat unacknowledged for too long, up to
    /// `max`, whichever consumer they were delivered to.
    ///
    /// Only the shard's lease holder reads the shard, and the wait is at least
    /// one lease lifetime, so an entry taken here belongs to a worker that is
    /// gone or has already given up on it. Returns the decoded entries and the
    /// ids of any that could not be decoded.
    async fn reclaim(
        &self,
        shard: u32,
        max: usize,
    ) -> Result<(Vec<Entry>, Vec<String>), QueueError> {
        let mut conn = self.conn.clone();
        let idle_ms = self.redeliver_after.as_millis() as u64;
        let mut cursor = "0-0".to_string();
        let mut entries = Vec::new();
        let mut malformed = Vec::new();

        // One call scans at most `count` pending entries, so a long pending
        // list takes a few calls. The bound keeps a huge one from stalling
        // the shard; what is left is picked up on the next read.
        for _ in 0..MAX_RECLAIM_CALLS {
            let wanted = max - entries.len();
            let reply: redis::Value = redis::cmd("XAUTOCLAIM")
                .arg(Self::stream(shard))
                .arg(&self.group)
                .arg(&self.consumer)
                .arg(idle_ms)
                .arg(&cursor)
                .arg("COUNT")
                .arg(wanted)
                .query_async(&mut conn)
                .await
                .map_err(|e| QueueError::Failed(e.to_string()))?;

            let redis::Value::Array(parts) = reply else { break };
            let next = parts.first().and_then(as_string).unwrap_or_else(|| "0-0".into());
            if let Some(redis::Value::Array(claimed)) = parts.get(1) {
                let (good, bad) = parse_entries(claimed);
                entries.extend(good);
                malformed.extend(bad);
            }
            if next == "0-0" || entries.len() >= max {
                break;
            }
            cursor = next;
        }

        if !entries.is_empty() {
            crate::metrics::metrics().queue_redelivered.inc_by(entries.len() as u64);
        }
        Ok((entries, malformed))
    }
}

/// Upper bound on XAUTOCLAIM calls in one read.
const MAX_RECLAIM_CALLS: usize = 8;

#[async_trait]
impl Queue for RedisQueue {
    async fn publish(&self, entries: &[(u32, Observation)]) -> Result<usize, QueueError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        let mut accepted = 0;

        for (shard, observation) in entries {
            let payload = serde_json::to_string(observation)
                .map_err(|e| QueueError::Failed(e.to_string()))?;
            // MAXLEN ~ is the approximate form: it trims at node boundaries,
            // which is far cheaper than exact trimming and just as effective at
            // bounding memory.
            pipe.cmd("XADD")
                .arg(Self::stream(*shard))
                .arg("MAXLEN")
                .arg("~")
                .arg(self.maxlen)
                .arg("*")
                .arg("o")
                .arg(payload)
                .ignore();
            accepted += 1;
        }

        pipe.query_async::<()>(&mut conn).await.map_err(|e| QueueError::Failed(e.to_string()))?;
        Ok(accepted)
    }

    async fn read(&self, shard: u32, max: usize) -> Result<Vec<Entry>, QueueError> {
        // Stuck entries first. `>` below only ever returns entries never
        // delivered to anyone, so without this an entry whose worker died,
        // lost the shard, or failed to write would stay pending for ever.
        let (mut entries, mut malformed) = self.reclaim(shard, max).await?;
        if entries.is_empty() {
            let mut conn = self.conn.clone();
            let reply: redis::Value = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&self.group)
                .arg(&self.consumer)
                .arg("COUNT")
                .arg(max)
                .arg("STREAMS")
                .arg(Self::stream(shard))
                .arg(">")
                .query_async(&mut conn)
                .await
                .map_err(|e| QueueError::Failed(e.to_string()))?;
            let (fresh, bad) = parse_stream_reply(&reply);
            entries = fresh;
            malformed.extend(bad);
        }

        // An entry that cannot be decoded never will be. Acknowledge it now,
        // or it would come back on every read for ever.
        if !malformed.is_empty() {
            crate::metrics::metrics()
                .queue_errors
                .with_label_values(&["decode"])
                .inc_by(malformed.len() as u64);
            self.ack(shard, &malformed).await?;
        }
        Ok(entries)
    }

    async fn ack(&self, shard: u32, ids: &[String]) -> Result<(), QueueError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.clone();
        redis::cmd("XACK")
            .arg(Self::stream(shard))
            .arg(&self.group)
            .arg(ids)
            .query_async::<i64>(&mut conn)
            .await
            .map_err(|e| QueueError::Failed(e.to_string()))?;
        Ok(())
    }

    async fn depth(&self, shard: u32) -> Result<usize, QueueError> {
        let mut conn = self.conn.clone();
        let len: i64 = redis::cmd("XLEN")
            .arg(Self::stream(shard))
            .query_async(&mut conn)
            .await
            .map_err(|e| QueueError::Failed(e.to_string()))?;
        Ok(len.max(0) as usize)
    }

    fn trimmed(&self) -> u64 {
        // Redis trims on its own and does not report how much, so this counts
        // only what this process knows about. Stream depth against the cap is
        // the signal to watch.
        self.trimmed.load(Ordering::Relaxed)
    }

    async fn healthy(&self) -> bool {
        let mut conn = self.conn.clone();
        redis::cmd("PING").query_async::<String>(&mut conn).await.is_ok()
    }
}

/// Pull entries out of an XREADGROUP reply.
///
/// The shape is streams, then entries. Returns the decoded entries and the ids
/// of those that did not decode: one malformed entry should not stop a shard,
/// but it must still be acknowledged, or it would come back for ever.
fn parse_stream_reply(reply: &redis::Value) -> (Vec<Entry>, Vec<String>) {
    let mut out = (Vec::new(), Vec::new());
    let redis::Value::Array(streams) = reply else {
        return out;
    };
    for stream in streams {
        let redis::Value::Array(parts) = stream else { continue };
        let Some(redis::Value::Array(entries)) = parts.get(1) else { continue };
        let (good, bad) = parse_entries(entries);
        out.0.extend(good);
        out.1.extend(bad);
    }
    out
}

/// Decode a list of stream entries, each an id and a flat field/value list.
///
/// An entry with no readable id cannot be acknowledged and is skipped. One
/// with an id but no decodable observation is returned as malformed.
fn parse_entries(entries: &[redis::Value]) -> (Vec<Entry>, Vec<String>) {
    let mut good = Vec::new();
    let mut bad = Vec::new();
    for entry in entries {
        let redis::Value::Array(fields) = entry else { continue };
        let Some(id) = fields.first().and_then(as_string) else { continue };
        let mut observation = None;
        if let Some(redis::Value::Array(pairs)) = fields.get(1) {
            // Fields arrive as name, value, name, value.
            for pair in pairs.chunks(2) {
                if let (Some(name), Some(value)) =
                    (pair.first().and_then(as_string), pair.get(1).and_then(as_string))
                {
                    if name == "o" {
                        observation = serde_json::from_str::<Observation>(&value).ok();
                    }
                }
            }
        }
        match observation {
            Some(observation) => good.push(Entry { id, observation }),
            None => bad.push(id),
        }
    }
    (good, bad)
}

fn as_string(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------- quota -----

/// Daily request counters in Redis, so the allowance is shared across every
/// pod rather than enforced separately by each one.
#[derive(Clone)]
pub struct RedisQuota {
    conn: ConnectionManager,
}

impl RedisQuota {
    pub async fn connect(url: &str) -> Result<Self, String> {
        Ok(Self { conn: connect(url).await? })
    }
}

#[async_trait]
impl crate::api::quota::QuotaCounter for RedisQuota {
    async fn increment(&self, key: &str) -> Option<i64> {
        let mut conn = self.conn.clone();
        let counter = crate::api::quota::counter_key(key);
        // Increment and set the expiry together, so a counter cannot survive
        // past the day it belongs to if the second command were to be lost.
        let (count,): (i64,) = redis::pipe()
            .atomic()
            .incr(&counter, 1)
            .expire(&counter, 172_800)
            .ignore()
            .query_async(&mut conn)
            .await
            .ok()?;
        Some(count)
    }
}

// --------------------------------------------------------------- leases -----

/// Shard leases in Redis.
///
/// Acquiring is `SET key owner NX EX ttl`, which is atomic, so two workers
/// racing for one shard cannot both win. Renewing and releasing compare the
/// owner first, in a script so the check and the action cannot be separated: a
/// slow worker waking after losing its lease must not extend or delete a lease
/// that now belongs to somebody else.
#[derive(Clone)]
pub struct RedisLeases {
    conn: ConnectionManager,
}

const RENEW_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
"#;

const RELEASE_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"#;

impl RedisLeases {
    pub async fn connect(url: &str) -> Result<Self, String> {
        Ok(Self { conn: connect(url).await? })
    }

    fn key(shard: u32) -> String {
        format!("lease:{shard}")
    }
}

#[async_trait]
impl Leases for RedisLeases {
    async fn acquire(&self, shard: u32, owner: &str, ttl: Duration) -> Result<bool, LeaseError> {
        let mut conn = self.conn.clone();
        // Re-acquiring a shard this worker already holds is a renewal, so try
        // that first rather than reporting a conflict with itself.
        if self.renew(shard, owner, ttl).await? {
            return Ok(true);
        }
        let reply: Option<String> = redis::cmd("SET")
            .arg(Self::key(shard))
            .arg(owner)
            .arg("NX")
            .arg("EX")
            .arg(ttl.as_secs().max(1))
            .query_async(&mut conn)
            .await
            .map_err(|e| LeaseError::Unavailable(e.to_string()))?;
        Ok(reply.is_some())
    }

    async fn renew(&self, shard: u32, owner: &str, ttl: Duration) -> Result<bool, LeaseError> {
        let mut conn = self.conn.clone();
        let changed: i64 = redis::Script::new(RENEW_SCRIPT)
            .key(Self::key(shard))
            .arg(owner)
            .arg(ttl.as_secs().max(1))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| LeaseError::Unavailable(e.to_string()))?;
        Ok(changed == 1)
    }

    async fn release(&self, shard: u32, owner: &str) -> Result<(), LeaseError> {
        let mut conn = self.conn.clone();
        redis::Script::new(RELEASE_SCRIPT)
            .key(Self::key(shard))
            .arg(owner)
            .invoke_async::<i64>(&mut conn)
            .await
            .map_err(|e| LeaseError::Unavailable(e.to_string()))?;
        Ok(())
    }

    async fn holder(&self, shard: u32) -> Result<Option<String>, LeaseError> {
        let mut conn = self.conn.clone();
        conn.get::<_, Option<String>>(Self::key(shard))
            .await
            .map_err(|e| LeaseError::Unavailable(e.to_string()))
    }

    async fn heartbeat(&self, owner: &str, _ttl: Duration) -> Result<(), LeaseError> {
        let mut conn = self.conn.clone();
        // A sorted set scored by time, so counting live workers is a range
        // query rather than a scan, and a worker that stops announcing itself
        // falls out on its own.
        let now = chrono::Utc::now().timestamp();
        redis::cmd("ZADD")
            .arg(WORKERS_KEY)
            .arg(now)
            .arg(owner)
            .query_async::<i64>(&mut conn)
            .await
            .map_err(|e| LeaseError::Unavailable(e.to_string()))?;
        Ok(())
    }

    async fn live_workers(&self, ttl: Duration) -> Result<usize, LeaseError> {
        let mut conn = self.conn.clone();
        let cutoff = chrono::Utc::now().timestamp() - ttl.as_secs().max(1) as i64;
        // Drop the ones that have gone quiet, then count what is left.
        let (_, count): (i64, i64) = redis::pipe()
            .atomic()
            .cmd("ZREMRANGEBYSCORE")
            .arg(WORKERS_KEY)
            .arg("-inf")
            .arg(cutoff)
            .cmd("ZCARD")
            .arg(WORKERS_KEY)
            .query_async(&mut conn)
            .await
            .map_err(|e| LeaseError::Unavailable(e.to_string()))?;
        Ok(count.max(0) as usize)
    }
}

/// Where workers announce themselves, so each can work out its fair share.
const WORKERS_KEY: &str = "workers";

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> redis::Value {
        redis::Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn stream_names_are_stable_per_shard() {
        assert_eq!(RedisQueue::stream(0), "obs:0");
        assert_eq!(RedisQueue::stream(31), "obs:31");
        assert_eq!(RedisLeases::key(7), "lease:7");
    }

    #[test]
    fn a_stream_reply_yields_its_entries() {
        let observation = crate::model::Observation {
            key: crate::model::StationKey::Wifi(crate::model::key::WifiKey(1)),
            lat: 35.7219,
            lon: 51.3347,
            accuracy: Some(10.0),
            age_ms: Some(0.0),
            speed: Some(0.0),
            signal: Some(-70),
            source: crate::model::ReportSource::Gnss,
            timestamp: 1_700_000_000_000,
        };
        let payload = serde_json::to_string(&observation).unwrap();

        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            bulk("obs:0"),
            redis::Value::Array(vec![redis::Value::Array(vec![
                bulk("1700000000000-0"),
                redis::Value::Array(vec![bulk("o"), bulk(&payload)]),
            ])]),
        ])]);

        let (entries, malformed) = parse_stream_reply(&reply);
        assert!(malformed.is_empty());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1700000000000-0");
        assert_eq!(entries[0].observation.lat, 35.7219);
    }

    #[test]
    fn an_unparsable_entry_is_set_aside_rather_than_failing_the_batch() {
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            bulk("obs:0"),
            redis::Value::Array(vec![redis::Value::Array(vec![
                bulk("1-0"),
                redis::Value::Array(vec![bulk("o"), bulk("not json")]),
            ])]),
        ])]);
        let (entries, malformed) = parse_stream_reply(&reply);
        assert!(entries.is_empty());
        assert_eq!(malformed, vec!["1-0".to_string()], "returned so it can be acknowledged");
    }

    #[test]
    fn an_entry_without_the_observation_field_is_malformed() {
        let entries = vec![
            redis::Value::Array(vec![bulk("2-0"), redis::Value::Array(vec![bulk("x"), bulk("1")])]),
            redis::Value::Array(vec![bulk("3-0")]),
        ];
        let (good, bad) = parse_entries(&entries);
        assert!(good.is_empty());
        assert_eq!(bad, vec!["2-0".to_string(), "3-0".to_string()]);
    }

    #[test]
    fn an_entry_without_a_readable_id_is_skipped_because_it_cannot_be_acknowledged() {
        let entries = vec![redis::Value::Array(vec![redis::Value::Nil]), redis::Value::Nil];
        let (good, bad) = parse_entries(&entries);
        assert!(good.is_empty() && bad.is_empty());
    }

    #[test]
    fn an_empty_reply_is_no_entries_rather_than_an_error() {
        assert_eq!(parse_stream_reply(&redis::Value::Nil), (vec![], vec![]));
        assert_eq!(parse_stream_reply(&redis::Value::Array(vec![])), (vec![], vec![]));
    }

    #[test]
    fn the_lease_scripts_compare_the_owner_before_acting() {
        // The comparison and the action must be in one script, or a worker that
        // lost its lease could extend somebody else's.
        assert!(RENEW_SCRIPT.contains("GET") && RENEW_SCRIPT.contains("EXPIRE"));
        assert!(RELEASE_SCRIPT.contains("GET") && RELEASE_SCRIPT.contains("DEL"));
    }
}
