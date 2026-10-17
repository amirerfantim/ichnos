//! A queue server that stops answering, against a real Redis.
//!
//! `CLIENT PAUSE` stops the server answering every client for a while, which
//! would stall the other backend tests, so this runs in a binary of its own.
//! Needs ICHNOS_TEST_REDIS_QUEUE; skipped without it.

use ichnos::config::QueueCfg;
use ichnos::ingest::queue::{Queue, QueueError};
use ichnos::model::key::WifiKey;
use ichnos::model::observation::ReportSource;
use ichnos::model::{Observation, StationKey};
use ichnos::redis_backend::RedisQueue;
use std::time::{Duration, Instant};

fn redis_queue_url() -> Option<String> {
    std::env::var("ICHNOS_TEST_REDIS_QUEUE").ok().filter(|s| !s.is_empty())
}

fn observation() -> Observation {
    Observation {
        key: StationKey::Wifi(WifiKey(0x0a0b_0c0d_0e0f)),
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
async fn a_queue_that_stops_answering_refuses_publishing_at_once_for_a_while() {
    let Some(url) = redis_queue_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS_QUEUE is not set");
        return;
    };
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
    .unwrap()
    .with_publish_pause(Duration::from_millis(300));
    let shard = 47;
    assert_eq!(queue.publish(&[(shard, observation())]).await.unwrap(), 1);

    let admin = redis::Client::open(url.as_str()).unwrap();
    let mut admin = admin.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1_000)
        .arg("ALL")
        .query_async(&mut admin)
        .await
        .unwrap();

    let started = Instant::now();
    let first = queue.publish(&[(shard, observation())]).await;
    assert!(matches!(first, Err(QueueError::Unavailable(_))), "{first:?}");
    assert!(
        started.elapsed() < Duration::from_millis(900),
        "gave up after {:?}",
        started.elapsed()
    );

    let started = Instant::now();
    let next = queue.publish(&[(shard, observation())]).await;
    assert!(matches!(next, Err(QueueError::Unavailable(_))), "{next:?}");
    assert!(started.elapsed() < Duration::from_millis(10), "paused: refused at once");

    // The server answers again after its pause, and publishing after its own.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(queue.publish(&[(shard, observation())]).await.unwrap(), 1);
}
