//! Per-operation timings, for capacity planning.
//!
//! Not a load test: this measures the CPU cost of one call to each hot path, on
//! one core, with no network and no database. That is the number capacity
//! planning actually needs, because everything else scales from it.
//!
//! Run with `ICHNOS_PERF=1 cargo test --release --test perf -- --nocapture`.
//! Skipped otherwise, since timings are meaningless in a debug build and noisy
//! in CI.

use ichnos::api::schema::{CellTower, Position, SubmitItem, WifiAccessPoint};
use ichnos::config::Config;
use ichnos::ingest::fold;
use ichnos::ingest::validate::validate_item;
use ichnos::locate::{self, Network};
use ichnos::model::key::{StationKey, WifiKey};
use ichnos::model::observation::ReportSource;
use ichnos::model::{Observation, Station};
use std::time::Instant;

fn enabled() -> bool {
    std::env::var("ICHNOS_PERF").is_ok()
}

/// Time an operation, discarding a warm-up run so the first-call cost of
/// touching cold code does not land in the number.
fn time<F: FnMut()>(name: &str, iterations: usize, mut op: F) -> f64 {
    for _ in 0..(iterations / 10).max(1) {
        op();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        op();
    }
    let micros = start.elapsed().as_secs_f64() * 1e6 / iterations as f64;
    let per_core = 1e6 / micros;
    println!("  {name:<44} {micros:>9.2} us    {per_core:>10.0} /s per core");
    micros
}

/// A report shaped like the ones a vehicle fleet sends: a fix, a handful of
/// access points, a couple of towers.
fn collect_shaped_report() -> SubmitItem {
    SubmitItem {
        position: Some(Position {
            latitude: Some(35.7219),
            longitude: Some(51.3347),
            accuracy: Some(12.0),
            age: Some(500.0),
            speed: Some(8.0),
            source: None,
        }),
        timestamp: Some(1_700_000_000_000),
        wifi_access_points: (0..5)
            .map(|i| WifiAccessPoint {
                mac_address: Some(format!("aabbccdd{i:04x}")),
                signal_strength: Some(-60 - i * 5),
                frequency: Some(2_412),
                channel: Some(1),
                age: Some(400.0),
            })
            .collect(),
        cell_towers: (0..2)
            .map(|i| CellTower {
                radio_type: Some("lte".into()),
                mobile_country_code: Some(432),
                mobile_network_code: Some(11),
                location_area_code: Some(1_234),
                cell_id: Some(50_000 + i),
                primary_scrambling_code: Some(100),
                signal_strength: Some(-105),
                timing_advance: Some(3),
                age: Some(600.0),
            })
            .collect(),
    }
}

fn known_networks(count: usize) -> Vec<Network> {
    (0..count)
        .map(|i| Network {
            key: StationKey::Wifi(WifiKey(i as u64 + 1)),
            // Scattered over about 200 m, which is what a real cluster looks like.
            lat: 35.7219 + (i as f64) * 0.0004,
            lon: 51.3347 + ((i % 3) as f64) * 0.0004,
            radius: 50.0,
            age_ms: 1_500.0,
            signal: -60.0 - (i as f64) * 4.0,
            score: 1.0 + (i as f64) * 0.3,
            seen_today: true,
        })
        .collect()
}

fn observation(id: u64) -> Observation {
    Observation {
        key: StationKey::Wifi(WifiKey(id)),
        lat: 35.7219,
        lon: 51.3347,
        accuracy: Some(12.0),
        age_ms: Some(500.0),
        speed: Some(8.0),
        signal: Some(-70),
        source: ReportSource::Gnss,
        timestamp: 1_700_000_000_000,
    }
}

#[test]
fn measure_the_hot_paths() {
    if !enabled() {
        eprintln!("skipped: set ICHNOS_PERF=1 to measure");
        return;
    }
    let cfg = Config::for_tests();
    let now = chrono::Utc::now();

    println!("\n--- the write path, per report -------------------------------------");
    let item = collect_shaped_report();
    let body =
        serde_json::to_string(&ichnos::api::schema::SubmitRequest { items: vec![item.clone()] })
            .unwrap();
    println!("  (a report of 5 access points and 2 towers, {} bytes)", body.len());

    time("parse the request body", 200_000, || {
        let parsed: ichnos::api::schema::SubmitRequest =
            serde_json::from_str(std::hint::black_box(&body)).unwrap();
        std::hint::black_box(parsed);
    });

    time("validate and turn into observations", 200_000, || {
        std::hint::black_box(validate_item(std::hint::black_box(&item), &cfg.validation));
    });

    let observations = validate_item(&item, &cfg.validation).observations;
    time("serialise observations for the queue", 200_000, || {
        for o in &observations {
            std::hint::black_box(serde_json::to_string(o).unwrap());
        }
    });

    time("route each observation to a shard", 500_000, || {
        for o in &observations {
            std::hint::black_box(o.key.shard(32));
        }
    });

    println!("\n--- the worker, per station ---------------------------------------");
    let batch: Vec<Observation> = (0..8).map(|_| observation(1)).collect();
    let mut existing = Station::new(StationKey::Wifi(WifiKey(1)), now);
    existing.lat = Some(35.7219);
    existing.lon = Some(51.3347);
    existing.max_lat = Some(35.7220);
    existing.min_lat = Some(35.7218);
    existing.max_lon = Some(51.3348);
    existing.min_lon = Some(51.3346);
    existing.samples = 500;
    existing.weight = 800.0;
    existing.source = Some(ReportSource::Gnss);

    time("fold 8 sightings into one update", 100_000, || {
        std::hint::black_box(fold::fold_station(
            StationKey::Wifi(WifiKey(1)),
            std::hint::black_box(&batch),
            Some(&existing),
            &cfg,
            now,
        ));
    });

    println!("\n--- the read path, per locate --------------------------------------");
    for count in [3usize, 5, 10, 20] {
        let wifi = known_networks(count);
        time(&format!("locate from {count} known access points"), 20_000, || {
            std::hint::black_box(locate::search(std::hint::black_box(wifi.clone()), vec![], &cfg));
        });
    }

    println!("\n--- the pieces of a locate, at 5 networks --------------------------");
    let wifi = known_networks(5);
    time("cluster them", 100_000, || {
        std::hint::black_box(locate::cluster::cluster_by_distance(
            std::hint::black_box(wifi.clone()),
            500.0,
            2,
        ));
    });
    let cluster = locate::cluster::cluster_by_distance(wifi.clone(), 500.0, 2);
    let group = cluster.first().cloned().unwrap_or_else(|| wifi.clone());
    time("fit the position", 20_000, || {
        std::hint::black_box(locate::fit::fit_position(std::hint::black_box(&group)));
    });
    time("one distance calculation", 2_000_000, || {
        std::hint::black_box(ichnos::geo::distance(35.7219, 51.3347, 35.7250, 51.3390));
    });
    println!();
}
