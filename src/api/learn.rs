//! Learning from locate answers, as upstream does.
//!
//! When a key asks for it, an answered locate is fed back as a report: the
//! networks the request named, at the position the answer gave, marked as a
//! guess (`source: query`). The worker weighs guesses below fixes: a guess
//! creates a station nobody has measured, only confirms one that a fix put
//! where the guess agrees, and gives way to the first fix that arrives.
//!
//! Each step is upstream's, so a key learns exactly what it learned there:
//! the sampling, the rule that skips answers with nothing new in them, and
//! the shape of the report, which then passes the same validation as any
//! submitted one.

use std::collections::HashSet;

use crate::api::schema::{CellTower, LocateRequest, Position as ReportPosition, SubmitItem};
use crate::config::Config;
use crate::locate::Position;
use crate::model::key::{Radio, StationKey, WifiKey};

/// Whether to feed this answer back: `key_percent` of a key's answers, scaled
/// by `global_percent` for the whole service. `draw` is uniform in [0, 1).
pub fn sampled(key_percent: u8, global_percent: f64, draw: f64) -> bool {
    let rate = f64::from(key_percent) / 100.0 * (global_percent / 100.0);
    rate > 0.0 && rate >= draw
}

/// The report an answer teaches, or nothing when it would teach nothing new.
///
/// Upstream skips an answer only when every network the request named was
/// used for it and had already been seen today. A request naming any cell
/// is never skipped: upstream compares the request's cell areas with the
/// areas the answer used, and an answer from cells or WiFi uses none.
pub fn report(request: &LocateRequest, answer: &Position, cfg: &Config) -> Option<SubmitItem> {
    let mut wifi: HashSet<WifiKey> = request
        .wifi_access_points
        .iter()
        .filter_map(|ap| {
            let mac = ap.mac_address.as_deref()?.to_ascii_lowercase();
            if cfg.validation.invalid_macs.iter().any(|m| m.eq_ignore_ascii_case(&mac)) {
                return None;
            }
            WifiKey::parse(&mac).ok()
        })
        .collect();
    // Too few access points are no WiFi query at all upstream, and so
    // nothing to learn from either.
    if wifi.len() < cfg.locate.wifi.min_in_cluster {
        wifi.clear();
    }

    if !request.cell_towers.iter().any(|c| has_cell_area(c, cfg)) {
        let fresh: HashSet<WifiKey> = answer
            .used
            .iter()
            .filter(|u| u.seen_today)
            .filter_map(|u| match u.key {
                StationKey::Wifi(key) => Some(key),
                StationKey::Cell(_) => None,
            })
            .collect();
        if fresh == wifi {
            return None;
        }
    }

    Some(SubmitItem {
        position: Some(ReportPosition {
            latitude: Some(round7(answer.lat)),
            longitude: Some(round7(answer.lon)),
            accuracy: Some(round7(answer.accuracy)),
            age: None,
            speed: None,
            source: Some("query".into()),
        }),
        timestamp: None,
        cell_towers: request.cell_towers.clone(),
        wifi_access_points: if wifi.is_empty() {
            Vec::new()
        } else {
            request.wifi_access_points.clone()
        },
    })
}

/// Whether a cell names an area upstream would look up: a known radio, a
/// country and network in range, and an area code.
fn has_cell_area(cell: &CellTower, cfg: &Config) -> bool {
    let v = &cfg.validation;
    let in_range = |value: Option<i64>, lo: i64, hi: i64| value.is_some_and(|x| x >= lo && x <= hi);
    cell.radio_type.as_deref().and_then(Radio::parse).is_some()
        && in_range(cell.mobile_country_code, i64::from(v.mcc.0), i64::from(v.mcc.1))
        && cell.mobile_country_code.is_some_and(|mcc| {
            v.mcc_allowlist.is_empty()
                || i32::try_from(mcc).is_ok_and(|m| v.mcc_allowlist.contains(&m))
        })
        && in_range(cell.mobile_network_code, i64::from(v.mnc.0), i64::from(v.mnc.1))
        && in_range(cell.location_area_code, v.lac.0, v.lac.1)
}

/// Seven decimal places, as upstream rounds every answer it returns.
fn round7(value: f64) -> f64 {
    format!("{value:.7}").parse().unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::WifiAccessPoint;
    use crate::locate::{PositionSource, UsedNetwork};
    use crate::model::key::CellKey;

    fn ap(mac: &str) -> WifiAccessPoint {
        WifiAccessPoint {
            mac_address: Some(mac.into()),
            signal_strength: Some(-70),
            ..Default::default()
        }
    }

    fn lte() -> CellTower {
        CellTower {
            radio_type: Some("lte".into()),
            mobile_country_code: Some(432),
            mobile_network_code: Some(11),
            location_area_code: Some(1_234),
            cell_id: Some(56_789),
            ..Default::default()
        }
    }

    fn used(macs: &[&str], seen_today: bool) -> Vec<UsedNetwork> {
        macs.iter()
            .map(|m| UsedNetwork {
                key: StationKey::Wifi(WifiKey::parse(m).unwrap()),
                score: 1.0,
                seen_today,
            })
            .collect()
    }

    fn answer(used: Vec<UsedNetwork>) -> Position {
        Position {
            lat: 35.721_912_345_678,
            lon: 51.334_798_765_432,
            accuracy: 62.123_456_789,
            score: 1.0,
            source: PositionSource::Wifi,
            used,
        }
    }

    fn request(wifi: &[&str], cells: Vec<CellTower>) -> LocateRequest {
        LocateRequest {
            wifi_access_points: wifi.iter().map(|m| ap(m)).collect(),
            cell_towers: cells,
        }
    }

    #[test]
    fn sampling_follows_the_key_and_the_service() {
        assert!(sampled(100, 100.0, 0.999_999));
        assert!(!sampled(0, 100.0, 0.0), "zero never learns, even on a zero draw");
        assert!(!sampled(100, 0.0, 0.0), "the service switch overrides every key");
        assert!(sampled(50, 100.0, 0.49));
        assert!(!sampled(50, 100.0, 0.51));
        assert!(sampled(50, 50.0, 0.25), "the two percentages multiply");
        assert!(!sampled(50, 50.0, 0.26));
    }

    #[test]
    fn an_answer_becomes_a_guess_at_the_answered_position() {
        let req = request(&["aabbccddeeff", "112233445566"], vec![lte()]);
        let item = report(&req, &answer(vec![]), &Config::for_tests()).unwrap();
        let position = item.position.unwrap();
        assert_eq!(position.latitude, Some(35.721_912_3));
        assert_eq!(position.longitude, Some(51.334_798_8));
        assert_eq!(position.accuracy, Some(62.123_456_8));
        assert_eq!(position.source.as_deref(), Some("query"));
        assert_eq!(position.age, None);
        assert_eq!(item.timestamp, None);
        assert_eq!(item.wifi_access_points.len(), 2);
        assert_eq!(item.cell_towers.len(), 1);
    }

    #[test]
    fn nothing_new_is_learned_from_wifi_already_seen_today() {
        let req = request(&["aabbccddeeff", "112233445566"], vec![]);
        let all_fresh = answer(used(&["aabbccddeeff", "112233445566"], true));
        assert!(report(&req, &all_fresh, &Config::for_tests()).is_none());
    }

    #[test]
    fn a_stale_or_unused_access_point_is_worth_learning() {
        let req = request(&["aabbccddeeff", "112233445566", "665544332211"], vec![]);
        let cfg = Config::for_tests();
        assert!(
            report(&req, &answer(used(&["aabbccddeeff", "112233445566"], true)), &cfg).is_some()
        );
        assert!(report(
            &req,
            &answer(used(&["aabbccddeeff", "112233445566", "665544332211"], false)),
            &cfg
        )
        .is_some());
    }

    #[test]
    fn a_request_with_a_cell_is_always_worth_learning() {
        let req = request(&["aabbccddeeff", "112233445566"], vec![lte()]);
        let all_fresh = answer(used(&["aabbccddeeff", "112233445566"], true));
        assert!(report(&req, &all_fresh, &Config::for_tests()).is_some());
    }

    #[test]
    fn a_cell_without_an_area_counts_for_nothing() {
        let mut no_area = lte();
        no_area.location_area_code = None;
        let mut bad_radio = lte();
        bad_radio.radio_type = Some("5g".into());
        let mut bad_network = lte();
        bad_network.mobile_network_code = Some(1_000);
        let mut bad_country = lte();
        bad_country.mobile_country_code = Some(0);
        let req = request(
            &["aabbccddeeff", "112233445566"],
            vec![no_area, bad_radio, bad_network, bad_country],
        );
        let all_fresh = answer(used(&["aabbccddeeff", "112233445566"], true));
        assert!(report(&req, &all_fresh, &Config::for_tests()).is_none());
    }

    #[test]
    fn a_foreign_cell_counts_for_nothing_under_an_allowlist() {
        let mut cfg = Config::for_tests();
        cfg.validation.mcc_allowlist = vec![432];
        let mut foreign = lte();
        foreign.mobile_country_code = Some(262);
        let req = request(&["aabbccddeeff", "112233445566"], vec![foreign]);
        let all_fresh = answer(used(&["aabbccddeeff", "112233445566"], true));
        assert!(report(&req, &all_fresh, &cfg).is_none());
        let req = request(&["aabbccddeeff", "112233445566"], vec![lte()]);
        assert!(report(&req, &all_fresh, &cfg).is_some());
    }

    #[test]
    fn a_single_access_point_is_not_a_wifi_query() {
        // Upstream drops the WiFi of a request naming fewer than two, so it
        // neither teaches them nor counts them against the skip rule.
        let req =
            request(&["aabbccddeeff", "AABBCCDDEEFF", "000000000000", "nonsense"], vec![lte()]);
        let item = report(&req, &answer(vec![]), &Config::for_tests()).unwrap();
        assert!(item.wifi_access_points.is_empty());
        assert_eq!(item.cell_towers.len(), 1);

        let wifi_only = request(&["aabbccddeeff"], vec![]);
        assert!(report(&wifi_only, &answer(vec![]), &Config::for_tests()).is_none());
    }

    #[test]
    fn a_used_cell_does_not_count_as_wifi() {
        let req = request(&["aabbccddeeff", "112233445566"], vec![]);
        let mut networks = used(&["aabbccddeeff", "112233445566"], true);
        networks.push(UsedNetwork {
            key: StationKey::Cell(CellKey { radio: Radio::Lte, mcc: 432, mnc: 11, lac: 1, cid: 2 }),
            score: 1.0,
            seen_today: true,
        });
        assert!(report(&req, &answer(networks), &Config::for_tests()).is_none());
    }
}
