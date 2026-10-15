//! A cache server that stops answering, against a real Redis.
//!
//! `CLIENT PAUSE` stops the server answering every client for a while, which
//! would stall the other backend tests, so this runs in a binary of its own.
//! Needs ICHNOS_TEST_REDIS; skipped without it.

use ichnos::config::Cache;
use ichnos::redis_backend::RedisCache;
use ichnos::store::cache::CacheBackend;
use std::time::{Duration, Instant};

fn redis_url() -> Option<String> {
    std::env::var("ICHNOS_TEST_REDIS").ok().filter(|s| !s.is_empty())
}

#[tokio::test]
async fn a_cache_that_stops_answering_is_skipped_and_then_tried_again() {
    let Some(url) = redis_url() else {
        eprintln!("skipped: ICHNOS_TEST_REDIS is not set");
        return;
    };
    let pause = Duration::from_millis(300);
    let cfg = Cache { pause_ms: pause.as_millis() as u64, ..Cache::default() };
    let cache = RedisCache::connect(&url, &cfg).await.unwrap();
    let key = format!("cache-outage-{}", std::process::id());
    cache.set_many(&[(key.clone(), "1".into(), 60)]).await;
    assert_eq!(cache.get_many(std::slice::from_ref(&key)).await, vec![Some("1".to_string())]);

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
    assert_eq!(cache.get_many(std::slice::from_ref(&key)).await, vec![None], "no answer is a miss");
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "gave up after {:?}",
        started.elapsed()
    );
    assert_eq!(cache.errors(), 1);

    let started = Instant::now();
    assert_eq!(cache.get_many(std::slice::from_ref(&key)).await, vec![None]);
    cache.set_many(&[(key.clone(), "2".into(), 60)]).await;
    cache.delete_many(std::slice::from_ref(&key)).await;
    assert!(started.elapsed() < Duration::from_millis(10), "paused: nothing is sent");
    assert_eq!(cache.errors(), 1, "skipped commands are not failures");

    // The server answers again after its pause, and the cache after its own.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(cache.get_many(std::slice::from_ref(&key)).await, vec![Some("1".to_string())]);
    cache.delete_many(&[key]).await;
}
