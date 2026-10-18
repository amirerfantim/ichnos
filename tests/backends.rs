//! Tests against real ScyllaDB and Redis.
//!
//! Skipped unless the addresses are given, so `cargo test` still works with
//! nothing installed:
//!
//! ```bash
//! ICHNOS_TEST_CQL=127.0.0.1:9042 \
//! ICHNOS_TEST_REDIS=redis://127.0.0.1:6379/0 \
//! ICHNOS_TEST_REDIS_QUEUE=redis://127.0.0.1:6380/0 \
//!   cargo test --test backends
//! ```
//!
//! Everything else in the suite runs against in-memory implementations, which
//! is fast and deterministic but proves nothing about the drivers. These do the
//! opposite: they are slower and need a real server, and they are the only
//! place the CQL and Redis code is actually exercised.

use ichnos::config::{Cache as CacheCfg, Cql as CqlCfg, QueueCfg};
use ichnos::ingest::lease::Leases;
use ichnos::ingest::queue::Queue;
use ichnos::model::key::{CellKey, Radio, StationKind, WifiKey};
use ichnos::model::observation::ReportSource;
use ichnos::model::{Observation, Station, StationKey};
use ichnos::redis_backend::{RedisCache, RedisLeases, RedisQueue};
use ichnos::store::cache::CacheBackend;
use ichnos::store::{ApiKey, Store};
use std::time::Duration;

fn cql_address() -> Option<String> {
    std::env::var("ICHNOS_TEST_CQL").ok().filter(|s| !s.is_empty())
}

fn redis_cache_url() -> Option<String> {
    std::env::var("ICHNOS_TEST_REDIS").ok().filter(|s| !s.is_empty())
}

fn redis_queue_url() -> Option<String> {
    std::env::var("ICHNOS_TEST_REDIS_QUEUE").ok().filter(|s| !s.is_empty())
}

/// A key nothing else will touch, so parallel runs do not collide.
fn unique_mac() -> WifiKey {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    WifiKey((nanos as u64) & 0x0000_ffff_ffff_ffff)
}

async fn cql_store() -> Option<ichnos::store::cql::CqlStore> {
    let address = cql_address()?;
    let mut cfg = CqlCfg {
        nodes: vec![address],
        // A development cluster is a single node, so a quorum is unreachable.
        consistency_read: "ONE".into(),
        consistency_write: "ONE".into(),
        ..CqlCfg::default()
    };
    if let Ok(map) = std::env::var("ICHNOS_TEST_CQL_MAP") {
        if let Some((from, to)) = map.split_once('=') {
            cfg.address_map.insert(from.to_string(), to.to_string());
        }
    }
    ichnos::store::cql::CqlStore::connect(&cfg).await.ok()
}

#[tokio::test]
async fn a_station_survives_a_round_trip_through_the_database() {
    let Some(store) = cql_store().await else {
        eprintln!("skipped: ICHNOS_TEST_CQL is not set");
        return;
    };

    let key = StationKey::Wifi(unique_mac());
    let mut station = Station::new(key, chrono::Utc::now());
    station.lat = Some(35.7219);
    station.lon = Some(51.3347);
    station.radius = Some(84);
    station.max_lat = Some(35.7220);
    station.min_lat = Some(35.7218);
    station.max_lon = Some(51.3348);
    station.min_lon = Some(51.3346);
    station.h3 = Some(617_783_904_879_771_647);
    station.samples = 1_420;
    station.weight = 903.5;
    station.source = Some(ReportSource::Gnss);
    station.last_seen = Some(chrono::Utc::now().date_naive());
    station.block_count = 2;

    store.put_stations(&[station.clone()]).await.unwrap();
    let found = store.get_station(&key).await.unwrap().expect("the row should be there");

    // Every field, because a column bound in the wrong order is silent.
    assert_eq!(found.key, station.key);
    assert_eq!(found.lat, station.lat);
    assert_eq!(found.lon, station.lon);
    assert_eq!(found.radius, station.radius);
    assert_eq!(found.max_lat, station.max_lat);
    assert_eq!(found.min_lon, station.min_lon);
    assert_eq!(found.h3, station.h3);
    assert_eq!(found.samples, station.samples);
    assert_eq!(found.weight, station.weight);
    assert_eq!(found.source, station.source);
    assert_eq!(found.last_seen, station.last_seen);
    assert_eq!(found.block_count, station.block_count);

    store.delete_station(&key).await.unwrap();
    assert!(store.get_station(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn a_cell_survives_a_round_trip_through_the_database() {
    let Some(store) = cql_store().await else {
        eprintln!("skipped: ICHNOS_TEST_CQL is not set");
        return;
    };

    let key = StationKey::Cell(CellKey {
        radio: Radio::Lte,
        mcc: 432,
        mnc: 11,
        lac: 9_999,
        cid: (unique_mac().0 % 200_000) as i64 + 1,
    });
    let mut station = Station::new(key, chrono::Utc::now());
    station.lat = Some(35.70);
    station.lon = Some(51.30);
    station.radius = Some(3_000);
    station.samples = 7;

    store.put_stations(&[station.clone()]).await.unwrap();
    let found = store.get_station(&key).await.unwrap().expect("the row should be there");
    assert_eq!(found.key, key, "the composite key must survive the round trip");
    assert_eq!(found.radius, Some(3_000));
    assert_eq!(found.samples, 7);

    store.delete_station(&key).await.unwrap();
}

#[tokio::test]
async fn many_stations_are_written_and_read_in_one_call() {
    let Some(store) = cql_store().await else {
        eprintln!("skipped: ICHNOS_TEST_CQL is not set");
        return;
    };

    let base = unique_mac().0;
    let keys: Vec<StationKey> =
        (0..25).map(|i| StationKey::Wifi(WifiKey(base.wrapping_add(i)))).collect();
    let stations: Vec<Station> = keys
        .iter()
        .map(|k| {
            let mut s = Station::new(*k, chrono::Utc::now());
            s.lat = Some(35.72);
            s.lon = Some(51.33);
            s.samples = 1;
            s
        })
        .collect();

    store.put_stations(&stations).await.unwrap();
    let found = store.get_stations(&keys).await.unwrap();
    assert_eq!(found.len(), 25);

    // A key that does not exist is simply absent, not an error.
    let mut mixed = keys.clone();
    mixed.push(StationKey::Wifi(WifiKey(base.wrapping_add(9_999))));
    assert_eq!(store.get_stations(&mixed).await.unwrap().len(), 25);

    for key in &keys {
        store.delete_station(key).await.unwrap();
    }
}

#[tokio::test]
async fn a_written_row_carries_a_lifetime() {
    let Some(store) = cql_store().await else {
        eprintln!("skipped: ICHNOS_TEST_CQL is not set");
        return;
    };
    // The lifetime is what bounds the dataset, so a write that did not set one
    // would quietly turn expiry off.
    assert!(store.ttl_secs() > 0);
    assert_eq!(store.ttl_secs(), 180 * 86_400);
}

#[tokio::test]
async fn api_keys_survive_a_round_trip_through_the_database() {
    let Some(store) = cql_store().await else {
        eprintln!("skipped: ICHNOS_TEST_CQL is not set");
        return;
    };

    let name = format!("test-{}", unique_mac().0);
    let key = ApiKey {
        key: name.clone(),
        name: "round trip".into(),
        enabled: true,
        allow_locate: true,
        allow_submit: false,
        allow_admin: true,
        daily_quota: 500,
        store_sample_locate: 37,
    };
    store.put_api_key(&key).await.unwrap();

    let found = store.get_api_key(&name).await.unwrap().expect("the key should be there");
    assert_eq!(found, key, "permissions and quota must survive exactly");
    assert!(store.get_api_key("definitely-not-a-key").await.unwrap().is_none());
}

#[tokio::test]
async fn the_spatial_mirror_finds_stations_near_a_cell() {
    let Some(store) = cql_store().await else {
        eprintln!("skipped: ICHNOS_TEST_CQL is not set");
        return;
    };

    let cell = 617_783_904_879_771_600i64 + (unique_mac().0 % 1_000) as i64;
    let key = StationKey::Wifi(unique_mac());
    let mut station = Station::new(key, chrono::Utc::now());
    station.lat = Some(35.72);
    station.lon = Some(51.33);
    station.h3 = Some(cell);

    store.put_stations(&[station]).await.unwrap();
    let near = store.stations_near(cell, StationKind::Wifi).await.unwrap();
    assert!(near.contains(&key), "the station should be findable by its cell");

    store.delete_station(&key).await.unwrap();
}

#[tokio::test]
async fn the_database_reports_itself_healthy() {
    let Some(store) = cql_store().await else {
        eprintln!("skipped: ICHNOS_TEST_CQL is not set");
        return;
    };
    store.health().await.unwrap();
}

#[tokio::test]
async fn the_redis_cache_stores_reads_and_forgets() {
    let Some(url) = redis_cache_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS is not set");
        return;
    };
    let cache =
        RedisCache::connect(&url, &CacheCfg::default(), 1_000).await.expect("cache should connect");
    assert!(cache.healthy().await);

    let key = format!("test:{}", unique_mac().0);
    assert_eq!(cache.get_many(std::slice::from_ref(&key)).await, vec![None]);

    cache.set_many(&[(key.clone(), "a value".into(), 60)]).await;
    assert_eq!(cache.get_many(std::slice::from_ref(&key)).await, vec![Some("a value".to_string())]);

    cache.delete_many(std::slice::from_ref(&key)).await;
    assert_eq!(cache.get_many(&[key]).await, vec![None]);
    assert_eq!(cache.errors(), 0, "no operation should have failed");
}

#[tokio::test]
async fn a_multi_key_cache_read_keeps_its_order() {
    let Some(url) = redis_cache_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS is not set");
        return;
    };
    let cache = RedisCache::connect(&url, &CacheCfg::default(), 1_000).await.unwrap();
    let base = unique_mac().0;
    let (a, b, c) = (format!("t:{base}:a"), format!("t:{base}:b"), format!("t:{base}:c"));

    cache.set_many(&[(a.clone(), "first".into(), 60), (c.clone(), "third".into(), 60)]).await;

    // The middle key was never written. The answer must line up with the
    // request positionally, or the caller pairs values with the wrong keys.
    let got = cache.get_many(&[a.clone(), b.clone(), c.clone()]).await;
    assert_eq!(got, vec![Some("first".into()), None, Some("third".into())]);

    cache.delete_many(&[a, b, c]).await;
}

#[tokio::test]
async fn the_redis_queue_carries_observations_and_holds_them_until_acknowledged() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let consumer = format!("test-{}", unique_mac().0);
    let queue = RedisQueue::connect(
        &url,
        4,
        1_000,
        consumer,
        Duration::from_secs(30),
        &QueueCfg::default(),
        1_000,
    )
    .await
    .expect("queue should connect");
    assert!(queue.healthy().await);

    let shard = 3;
    let observation = Observation {
        key: StationKey::Wifi(unique_mac()),
        lat: 35.7219,
        lon: 51.3347,
        accuracy: Some(10.0),
        age_ms: Some(0.0),
        speed: Some(0.0),
        signal: Some(-70),
        source: ReportSource::Gnss,
        timestamp: 1_700_000_000_000,
    };

    let before = queue.depth(shard).await.unwrap();
    queue.publish(&[(shard, observation.clone())]).await.unwrap();
    assert_eq!(queue.depth(shard).await.unwrap(), before + 1);

    let entries = queue.read(shard, 100).await.unwrap();
    let mine = entries
        .iter()
        .find(|e| e.observation.key == observation.key)
        .expect("the observation should come back");
    assert_eq!(mine.observation.lat, observation.lat);
    assert_eq!(mine.observation.signal, observation.signal);

    let ids: Vec<String> = entries.iter().map(|e| e.id.clone()).collect();
    queue.ack(shard, &ids).await.unwrap();
}

/// A stream no other test uses, emptied first so earlier runs leave nothing.
///
/// Stream names are fixed per shard, so a test that counts pending entries
/// needs a shard to itself: the queues here are built with more shards than
/// any other test asks for, and each test picks its own.
/// A cluster connection, so a key on any node is reached, as the service
/// reaches it.
async fn private_shard(url: &str, shard: u32) -> redis::cluster_async::ClusterConnection {
    let client = redis::cluster::ClusterClient::new(vec![url]).unwrap();
    let mut conn = client.get_async_connection().await.unwrap();
    // One DEL with both keys: they share a slot, as the queue's do.
    let _: () = redis::cmd("DEL")
        .arg(format!("{{obs:{shard}}}"))
        .arg(format!("{{obs:{shard}}}:acked"))
        .query_async(&mut conn)
        .await
        .unwrap();
    conn
}

async fn stream_len(conn: &mut redis::cluster_async::ClusterConnection, shard: u32) -> u64 {
    redis::cmd("XLEN").arg(format!("{{obs:{shard}}}")).query_async(conn).await.unwrap()
}

async fn pending_in_group(conn: &mut redis::cluster_async::ClusterConnection, shard: u32) -> i64 {
    let reply: redis::Value = redis::cmd("XPENDING")
        .arg(format!("{{obs:{shard}}}"))
        .arg("ichnos")
        .query_async(conn)
        .await
        .unwrap();
    match reply {
        redis::Value::Array(parts) => match parts.first() {
            Some(redis::Value::Int(n)) => *n,
            _ => 0,
        },
        _ => 0,
    }
}

fn test_observation() -> Observation {
    Observation {
        key: StationKey::Wifi(unique_mac()),
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
async fn a_batch_taken_by_a_dead_worker_is_redelivered_to_the_next_one() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let shard = 41;
    let mut conn = private_shard(&url, shard).await;
    let wait = Duration::from_millis(300);
    let dead =
        RedisQueue::connect(&url, 48, 1_000, "dead".into(), wait, &QueueCfg::default(), 1_000)
            .await
            .unwrap();
    let next =
        RedisQueue::connect(&url, 48, 1_000, "next".into(), wait, &QueueCfg::default(), 1_000)
            .await
            .unwrap();

    let observation = test_observation();
    dead.publish(&[(shard, observation.clone())]).await.unwrap();
    let taken = dead.read(shard, 100).await.unwrap();
    assert_eq!(taken.len(), 1, "the first worker takes it, then dies");

    assert!(next.read(shard, 100).await.unwrap().is_empty(), "too soon: its owner may be alive");

    tokio::time::sleep(wait + Duration::from_millis(100)).await;
    let redelivered = next.read(shard, 100).await.unwrap();
    assert_eq!(redelivered.len(), 1, "the stuck batch comes back");
    assert_eq!(redelivered[0].id, taken[0].id);
    assert_eq!(redelivered[0].observation, observation);

    next.ack(shard, &[redelivered[0].id.clone()]).await.unwrap();
    assert_eq!(pending_in_group(&mut conn, shard).await, 0, "and once written, it is done");
}

#[tokio::test]
async fn an_undecodable_entry_is_acknowledged_rather_than_redelivered_for_ever() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let shard = 42;
    let mut conn = private_shard(&url, shard).await;
    let queue = RedisQueue::connect(
        &url,
        48,
        1_000,
        "reader".into(),
        Duration::ZERO,
        &QueueCfg::default(),
        1_000,
    )
    .await
    .unwrap();

    let _: String = redis::cmd("XADD")
        .arg(format!("{{obs:{shard}}}"))
        .arg("*")
        .arg("o")
        .arg("not json")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(queue.read(shard, 100).await.unwrap().is_empty());
    assert_eq!(pending_in_group(&mut conn, shard).await, 0, "not left pending");
    assert!(queue.read(shard, 100).await.unwrap().is_empty(), "and it does not come back");
}

#[tokio::test]
async fn redis_leases_give_one_shard_to_exactly_one_worker() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let leases = RedisLeases::connect(&url, 1_000).await.expect("leases should connect");

    // A shard number far outside the normal range, so a running worker is not
    // fighting this test for it.
    let shard = 9_000 + (unique_mac().0 % 500) as u32;
    let ttl = Duration::from_secs(10);
    let (a, b) = ("worker-a-test".to_string(), "worker-b-test".to_string());

    leases.release(shard, &a).await.ok();
    assert!(leases.acquire(shard, &a, ttl).await.unwrap());
    assert!(!leases.acquire(shard, &b, ttl).await.unwrap(), "two owners would break everything");
    assert_eq!(leases.holder(shard).await.unwrap().as_deref(), Some(a.as_str()));

    assert!(leases.renew(shard, &a, ttl).await.unwrap());
    assert!(!leases.renew(shard, &b, ttl).await.unwrap(), "only the holder may renew");

    leases.release(shard, &b).await.unwrap();
    assert_eq!(
        leases.holder(shard).await.unwrap().as_deref(),
        Some(a.as_str()),
        "and only the holder may release"
    );

    leases.release(shard, &a).await.unwrap();
    assert_eq!(leases.holder(shard).await.unwrap(), None);
}

#[tokio::test]
async fn workers_can_count_one_another_through_redis() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let leases = RedisLeases::connect(&url, 1_000).await.unwrap();
    let ttl = Duration::from_secs(30);

    let me = format!("counting-test-{}", unique_mac().0);
    let before = leases.live_workers(ttl).await.unwrap();
    leases.heartbeat(&me, ttl).await.unwrap();
    let after = leases.live_workers(ttl).await.unwrap();

    assert!(
        !before.contains(&me) && after.contains(&me),
        "announcing should make this worker visible"
    );
    let mut sorted = after.clone();
    sorted.sort();
    assert_eq!(after, sorted, "listed in order");
}

#[tokio::test]
async fn the_cached_store_reaches_the_real_database_once_and_then_stops() {
    let (Some(_), Some(cache_url)) = (cql_address(), redis_cache_url()) else {
        eprintln!("skipped: needs both ICHNOS_TEST_CQL and ICHNOS_TEST_REDIS");
        return;
    };
    let Some(store) = cql_store().await else { return };
    let cache = RedisCache::connect(&cache_url, &CacheCfg::default(), 1_000).await.unwrap();
    let cached = ichnos::store::cache::CachedStore::new(store, cache, CacheCfg::default());

    let key = StationKey::Wifi(unique_mac());
    let mut station = Station::new(key, chrono::Utc::now());
    station.lat = Some(35.7219);
    station.lon = Some(51.3347);

    cached.put_stations(&[station]).await.unwrap();
    for _ in 0..5 {
        assert_eq!(cached.get_stations(&[key]).await.unwrap().len(), 1);
    }
    let (hits, _, _, errors) = cached.stats().snapshot();
    assert!(hits >= 4, "repeat reads should be served from cache, got {hits} hits");
    assert_eq!(errors, 0);

    cached.delete_station(&key).await.unwrap();
}

#[tokio::test]
async fn finished_work_leaves_the_stream() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let shard = 43;
    let mut conn = private_shard(&url, shard).await;
    let queue = RedisQueue::connect(
        &url,
        48,
        1_000,
        "w".into(),
        Duration::from_secs(30),
        &QueueCfg::default(),
        1_000,
    )
    .await
    .unwrap();

    let batch: Vec<(u32, Observation)> = (0..3).map(|_| (shard, test_observation())).collect();
    queue.publish(&batch).await.unwrap();
    assert_eq!(queue.depth(shard).await.unwrap(), 3);
    let taken = queue.read(shard, 100).await.unwrap();
    assert_eq!(queue.depth(shard).await.unwrap(), 3, "taken is not done");
    let ids: Vec<String> = taken.iter().map(|e| e.id.clone()).collect();
    queue.ack(shard, &ids).await.unwrap();

    assert_eq!(stream_len(&mut conn, shard).await, 0, "acknowledged entries are deleted, not kept");
    assert_eq!(queue.depth(shard).await.unwrap(), 0);
    let acked: u64 = redis::cmd("GET")
        .arg(format!("{{obs:{shard}}}:acked"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(acked, 3);
}

#[tokio::test]
async fn finished_work_is_never_counted_as_lost() {
    // The defect this pins: without deleting finished entries every stream
    // sat at its cap, and the cap trimmed work that was already done.
    //
    // Trims are counted across every shard, so the two tests that count them
    // need a server each: a cluster has only database 0. This one takes the
    // cache's, where no other test keeps a stream.
    let Some(url) = redis_cache_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS is not set");
        return;
    };
    let shard = 44;
    let mut conn = private_shard(&url, shard).await;
    let queue = RedisQueue::connect(
        &url,
        48,
        10,
        "w".into(),
        Duration::from_secs(30),
        &QueueCfg::default(),
        1_000,
    )
    .await
    .unwrap();
    for _ in 0..50 {
        let batch: Vec<(u32, Observation)> = (0..10).map(|_| (shard, test_observation())).collect();
        queue.publish(&batch).await.unwrap();
        let taken = queue.read(shard, 100).await.unwrap();
        let ids: Vec<String> = taken.iter().map(|e| e.id.clone()).collect();
        queue.ack(shard, &ids).await.unwrap();
    }
    assert_eq!(stream_len(&mut conn, shard).await, 0);
    assert_eq!(queue.trimmed().await.unwrap(), 0, "500 entries through a cap of 10, none lost");
}

#[tokio::test]
async fn entries_dropped_unread_are_counted_exactly() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    // The other test that counts trims runs on the cache's server.
    let shard = 45;
    let mut conn = private_shard(&url, shard).await;
    let queue = RedisQueue::connect(
        &url,
        48,
        10,
        "w".into(),
        Duration::from_secs(30),
        &QueueCfg::default(),
        1_000,
    )
    .await
    .unwrap();
    // MAXLEN ~ trims whole nodes, so the exact count is whatever left the
    // stream: everything added less what is still there.
    let batch: Vec<(u32, Observation)> = (0..1_000).map(|_| (shard, test_observation())).collect();
    queue.publish(&batch).await.unwrap();
    let left = stream_len(&mut conn, shard).await;
    assert!(left < 1_000, "the cap trimmed nothing");
    assert_eq!(queue.trimmed().await.unwrap(), 1_000 - left);

    // A handle opened now does not report that loss again.
    let later = RedisQueue::connect(
        &url,
        48,
        10,
        "w".into(),
        Duration::from_secs(30),
        &QueueCfg::default(),
        1_000,
    )
    .await
    .unwrap();
    assert_eq!(later.trimmed().await.unwrap(), 0);
}

#[tokio::test]
async fn a_queue_server_that_restarts_empty_is_read_again() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let shard = 46;
    let mut conn = private_shard(&url, shard).await;
    let queue = RedisQueue::connect(
        &url,
        48,
        1_000,
        "w".into(),
        Duration::from_secs(30),
        &QueueCfg::default(),
        1_000,
    )
    .await
    .unwrap();

    // What a restart without persistence leaves: no stream, no group. A
    // publish makes the stream again, but not the group.
    let _: () =
        redis::cmd("DEL").arg(format!("{{obs:{shard}}}")).query_async(&mut conn).await.unwrap();
    let observation = test_observation();
    queue.publish(&[(shard, observation.clone())]).await.unwrap();

    let read = queue.read(shard, 100).await.unwrap();
    assert_eq!(read.len(), 1, "the group is made again, from the start of the stream");
    assert_eq!(read[0].observation, observation);
    queue.ack(shard, &[read[0].id.clone()]).await.unwrap();
    assert_eq!(pending_in_group(&mut conn, shard).await, 0);
}
