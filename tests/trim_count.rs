//! The queue's trim count on a cluster of several nodes.
//!
//! The count reads each stream with XINFO STREAM, then its acknowledgement
//! counter. Redis gives XINFO no key of its own, and a client that sent it to
//! any node would be answered MOVED on a cluster of several; resent after the
//! counter was read, it would count every chunk acknowledged in between as
//! trimmed. A one-node cluster cannot show that, so these tests mean most on
//! three primaries or more:
//!
//! ```bash
//! ICHNOS_TEST_REDIS_QUEUE=redis://127.0.0.1:17001/0 cargo test --test trim_count
//! ```
//!
//! They count trims across every shard, so they have a test binary of their
//! own: cargo runs one binary at a time, and nothing else trims meanwhile.

use ichnos::config::QueueCfg;
use ichnos::ingest::queue::Queue;
use ichnos::model::key::WifiKey;
use ichnos::model::observation::ReportSource;
use ichnos::model::{Observation, StationKey};
use ichnos::redis_backend::RedisQueue;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SHARDS: u32 = 4;

fn redis_queue_url() -> Option<String> {
    std::env::var("ICHNOS_TEST_REDIS_QUEUE").ok().filter(|s| !s.is_empty())
}

fn observation(n: u64) -> Observation {
    Observation {
        key: StationKey::Wifi(WifiKey(n & 0x0000_ffff_ffff_ffff)),
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

async fn queue(url: &str, consumer: &str) -> RedisQueue {
    RedisQueue::connect(
        url,
        SHARDS,
        1_000_000,
        consumer.into(),
        Duration::from_secs(60),
        &QueueCfg::default(),
        1_000,
    )
    .await
    .unwrap()
}

/// The MOVED replies every primary has sent so far.
async fn moved_on_every_node(url: &str) -> u64 {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let nodes: String = redis::cmd("CLUSTER").arg("NODES").query_async(&mut conn).await.unwrap();
    let mut total = 0;
    for line in nodes.lines().filter(|l| l.contains("master")) {
        let addr = line.split_whitespace().nth(1).unwrap().split('@').next().unwrap();
        let node = redis::Client::open(format!("redis://{addr}/0")).unwrap();
        let mut c = node.get_multiplexed_async_connection().await.unwrap();
        let info: String = redis::cmd("INFO").arg("errorstats").query_async(&mut c).await.unwrap();
        for l in info.lines() {
            if let Some(n) = l.trim().strip_prefix("errorstat_MOVED:count=") {
                total += n.parse::<u64>().unwrap();
            }
        }
    }
    total
}

#[tokio::test]
async fn acknowledgements_racing_the_trim_count_are_not_counted_as_lost() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let counter = queue(&url, "counter").await;
    let worker = queue(&url, "worker").await;
    let stop = Arc::new(AtomicBool::new(false));
    let busy = {
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                for shard in 0..SHARDS {
                    let batch: Vec<(u32, Observation)> = (0..50)
                        .map(|_| {
                            n += 1;
                            (shard, observation(n))
                        })
                        .collect();
                    worker.publish(&batch).await.unwrap();
                    let taken = worker.read(shard, 100).await.unwrap();
                    let ids: Vec<String> = taken.iter().map(|e| e.id.clone()).collect();
                    worker.ack(shard, &ids).await.unwrap();
                }
            }
        })
    };
    let mut worst = 0;
    for _ in 0..300 {
        worst = worst.max(counter.trimmed().await.unwrap());
    }
    stop.store(true, Ordering::Relaxed);
    busy.await.unwrap();
    assert_eq!(worst, 0, "nothing was trimmed, but {worst} entries were counted as lost");
}

#[tokio::test]
async fn queue_commands_go_straight_to_the_node_that_holds_the_stream() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
    let q = queue(&url, "router").await;
    let before = moved_on_every_node(&url).await;
    for shard in 0..SHARDS {
        q.publish(&[(shard, observation(u64::from(shard) + 1))]).await.unwrap();
        // The first read of a shard prunes its group: XINFO CONSUMERS.
        let taken = q.read(shard, 10).await.unwrap();
        let ids: Vec<String> = taken.iter().map(|e| e.id.clone()).collect();
        q.ack(shard, &ids).await.unwrap();
        q.depth(shard).await.unwrap();
    }
    q.trimmed().await.unwrap(); // XINFO STREAM on every shard
    queue(&url, "router-2").await; // XGROUP CREATE on every shard
    let after = moved_on_every_node(&url).await;
    assert_eq!(
        after,
        before,
        "{} commands went to a node that does not hold their key",
        after - before
    );
}
