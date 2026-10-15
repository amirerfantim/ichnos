//! A quota counter server that stops answering, against a real Redis.
//!
//! `CLIENT PAUSE` stops the server answering every client for a while, which
//! would stall the other backend tests, so this runs in a binary of its own.
//! Needs ICHNOS_TEST_REDIS; skipped without it.

use ichnos::api::quota::QuotaCounter;
use ichnos::config::Cache;
use ichnos::redis_backend::RedisQuota;
use std::time::{Duration, Instant};

fn redis_url() -> Option<String> {
    std::env::var("ICHNOS_TEST_REDIS").ok().filter(|s| !s.is_empty())
}

#[tokio::test]
async fn a_counter_that_stops_answering_costs_a_caller_no_more_than_the_budget() {
    let Some(url) = redis_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS is not set");
        return;
    };
    let quota = RedisQuota::connect(&url, &Cache::default(), 48).await.unwrap();
    let key = format!("quota-outage-{}", std::process::id());
    assert!(quota.increment(&key).await.is_some());

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
    assert_eq!(quota.increment(&key).await, None, "no answer: the request is allowed uncounted");
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "gave up after {:?}",
        started.elapsed()
    );

    let started = Instant::now();
    assert_eq!(quota.increment(&key).await, None);
    assert!(started.elapsed() < Duration::from_millis(10), "paused: nothing is sent");

    // The server answers again after its pause, and counting after its own.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(quota.increment(&key).await.is_some());
}
