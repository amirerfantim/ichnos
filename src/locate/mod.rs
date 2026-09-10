//! Answering "where is this device" from the stations it can see.
//!
//! The shape of the work: group the known stations into sets that agree with
//! each other, turn each set into a candidate position, then pick the candidate
//! best supported by the evidence. WiFi and cell candidates compete on equal
//! terms; neither wins automatically.

pub mod cluster;
pub mod fit;
pub mod network;
pub mod score;

pub use network::Network;

use crate::config::{Config, LocateNetwork};
use crate::geo::distance;
use crate::model::StationKey;

/// Which kind of station produced a candidate. Reported for debugging, not used
/// to break ties: a cell answer that fits the evidence better than a WiFi one
/// should win.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSource {
    Wifi,
    Cell,
}

/// A station that contributed to an answer.
#[derive(Debug, Clone, PartialEq)]
pub struct UsedNetwork {
    pub key: StationKey,
    pub score: f64,
    pub seen_today: bool,
}

/// One candidate answer.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub lat: f64,
    pub lon: f64,
    /// Radius in metres inside which the device probably sits.
    pub accuracy: f64,
    /// Combined trust of every station behind this candidate.
    pub score: f64,
    pub source: PositionSource,
    pub used: Vec<UsedNetwork>,
}

/// Turn one cluster of access points into a candidate position.
///
/// The cluster is trimmed to the configured cap before fitting. Upstream sorts
/// ascending and keeps the head, which keeps the *least* trustworthy members
/// and contradicts its own comment ("pick the best sample of networks"); this
/// keeps the best, which is what the comment intends and what produces a better
/// answer. It only differs when a device sees more stations than the cap.
///
/// The reported score covers the whole cluster, not only the members that were
/// fitted, because the discarded members are still evidence that this cluster
/// is the real one.
pub fn aggregate_wifi_cluster(mut cluster: Vec<Network>, cfg: &LocateNetwork) -> Position {
    let total_score: f64 = cluster.iter().map(|n| n.score).sum();

    cluster.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    cluster.truncate(cfg.max_in_cluster);

    let (lat, lon) = fit::fit_position(&cluster);
    let accuracy =
        fit::accuracy_from_spread(lat, lon, &cluster, cfg.min_accuracy_m, cfg.max_accuracy_m);

    Position {
        lat,
        lon,
        accuracy,
        score: total_score,
        source: PositionSource::Wifi,
        used: cluster
            .iter()
            .map(|n| UsedNetwork { key: n.key, score: n.score, seen_today: n.seen_today })
            .collect(),
    }
}

/// Turn one cluster of cells into a candidate position.
///
/// Cells are not fitted. A single cell simply reports its own recorded position
/// and radius, and several cells in one area are averaged by weight. Fitting
/// would suggest a precision that cell coverage does not have.
pub fn aggregate_cell_cluster(cluster: Vec<Network>, cfg: &LocateNetwork) -> Position {
    let total_score: f64 = cluster.iter().map(|n| n.score).sum();

    let (lat, lon, accuracy) = if cluster.len() == 1 {
        let only = &cluster[0];
        (only.lat, only.lon, only.radius.clamp(cfg.min_accuracy_m, cfg.max_accuracy_m))
    } else {
        let (lat, lon) = fit::weighted_mean(&cluster);
        let accuracy =
            fit::accuracy_from_spread(lat, lon, &cluster, cfg.min_accuracy_m, cfg.max_accuracy_m);
        (lat, lon, accuracy)
    };

    Position {
        lat,
        lon,
        accuracy,
        score: total_score,
        source: PositionSource::Cell,
        used: cluster
            .iter()
            .map(|n| UsedNetwork { key: n.key, score: n.score, seen_today: n.seen_today })
            .collect(),
    }
}

/// Build every candidate position the evidence supports.
pub fn candidates(wifi: Vec<Network>, cells: Vec<Network>, cfg: &Config) -> Vec<Position> {
    let mut out = Vec::new();

    for group in cluster::cluster_by_distance(
        wifi,
        cfg.locate.wifi.max_cluster_meters,
        cfg.locate.wifi.min_in_cluster,
    ) {
        out.push(aggregate_wifi_cluster(group, &cfg.locate.wifi));
    }

    for group in cluster::cluster_cells_by_area(cells) {
        if group.len() >= cfg.locate.cell.min_in_cluster {
            out.push(aggregate_cell_cluster(group, &cfg.locate.cell));
        }
    }

    out
}

/// The best-supported group of candidates that agree with one another.
///
/// Candidates within reach of each other are evidence for the same place, so
/// they are gathered and their scores added. The group with the most combined
/// support wins. This is what stops one lonely high-accuracy candidate from
/// beating three that agree.
pub fn best_cluster(results: &[Position]) -> Vec<&Position> {
    if results.len() <= 1 {
        return results.iter().collect();
    }

    let mut sorted: Vec<&Position> = results.iter().collect();
    sorted.sort_by(|a, b| a.accuracy.partial_cmp(&b.accuracy).unwrap_or(std::cmp::Ordering::Equal));

    let mut groups: Vec<Vec<&Position>> = Vec::with_capacity(sorted.len());
    for (i, first) in sorted.iter().enumerate() {
        let mut group = vec![*first];
        // A 50% buffer around each radius, so candidates that nearly overlap
        // still count as agreeing.
        let radius1 = first.accuracy * 1.5;
        for second in sorted.iter().skip(i + 1) {
            let radius2 = second.accuracy * 1.5;
            let apart = distance(first.lat, first.lon, second.lat, second.lon);
            if apart <= radius1.max(radius2) {
                group.push(*second);
            }
        }
        groups.push(group);
    }

    groups
        .into_iter()
        .max_by(|a, b| {
            let key = |g: &Vec<&Position>| {
                let total: f64 = g.iter().map(|p| p.score).sum();
                let best = g.iter().map(|p| p.score).fold(f64::MIN, f64::max);
                (total, best)
            };
            let (ta, ba) = key(a);
            let (tb, bb) = key(b);
            ta.partial_cmp(&tb)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(ba.partial_cmp(&bb).unwrap_or(std::cmp::Ordering::Equal))
        })
        .unwrap_or_default()
}

/// The single best answer, or nothing if the evidence supports none.
///
/// Within the winning group, the tightest accuracy wins, and a tie is broken by
/// the higher score.
pub fn best(results: &[Position]) -> Option<Position> {
    let group = best_cluster(results);
    match group.len() {
        0 => None,
        1 => Some(group[0].clone()),
        _ => group
            .into_iter()
            .min_by(|a, b| {
                a.accuracy
                    .partial_cmp(&b.accuracy)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal))
            })
            .cloned(),
    }
}

/// Locate a device from the stations it reported seeing.
pub fn search(wifi: Vec<Network>, cells: Vec<Network>, cfg: &Config) -> Option<Position> {
    best(&candidates(wifi, cells, cfg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::{CellKey, Radio, WifiKey};

    fn wifi_net(id: u64, lat: f64, lon: f64, score: f64) -> Network {
        Network {
            key: StationKey::Wifi(WifiKey(id)),
            lat,
            lon,
            radius: 50.0,
            age_ms: 1_000.0,
            signal: -70.0,
            score,
            seen_today: true,
        }
    }

    fn cell_net(lac: i64, cid: i64, lat: f64, lon: f64, score: f64, radius: f64) -> Network {
        Network {
            key: StationKey::Cell(CellKey { radio: Radio::Lte, mcc: 432, mnc: 11, lac, cid }),
            lat,
            lon,
            radius,
            age_ms: 1_000.0,
            signal: -105.0,
            score,
            seen_today: true,
        }
    }

    fn position(lat: f64, lon: f64, accuracy: f64, score: f64) -> Position {
        Position { lat, lon, accuracy, score, source: PositionSource::Wifi, used: vec![] }
    }

    #[test]
    fn a_wifi_cluster_answers_from_inside_itself() {
        let cfg = Config::for_tests();
        let cluster = vec![
            wifi_net(1, 35.7200, 51.3300, 1.0),
            wifi_net(2, 35.7210, 51.3310, 1.0),
            wifi_net(3, 35.7205, 51.3320, 1.0),
        ];
        let p = aggregate_wifi_cluster(cluster, &cfg.locate.wifi);
        assert!((35.719..=35.722).contains(&p.lat));
        assert_eq!(p.source, PositionSource::Wifi);
        assert_eq!(p.score, 3.0, "the score is the whole cluster's");
        assert_eq!(p.used.len(), 3);
    }

    #[test]
    fn a_wifi_cluster_is_trimmed_to_the_best_scoring_members() {
        let mut cfg = Config::for_tests();
        cfg.locate.wifi.max_in_cluster = 2;
        let cluster = vec![
            wifi_net(1, 35.7200, 51.3300, 1.0),
            wifi_net(2, 35.7201, 51.3301, 9.0),
            wifi_net(3, 35.7202, 51.3302, 5.0),
        ];
        let p = aggregate_wifi_cluster(cluster, &cfg.locate.wifi);
        assert_eq!(p.used.len(), 2);
        let kept: Vec<f64> = p.used.iter().map(|u| u.score).collect();
        assert!(kept.contains(&9.0) && kept.contains(&5.0), "kept {kept:?}, expected the best two");
        assert_eq!(p.score, 15.0, "but the score still counts the discarded member");
    }

    #[test]
    fn accuracy_is_bounded_by_the_configured_range() {
        let cfg = Config::for_tests();
        // Two stations at the same point: no spread at all.
        let cluster = vec![wifi_net(1, 35.72, 51.33, 1.0), wifi_net(2, 35.72, 51.33, 1.0)];
        let p = aggregate_wifi_cluster(cluster, &cfg.locate.wifi);
        assert_eq!(p.accuracy, 10.0, "never tighter than the floor");
    }

    #[test]
    fn a_single_cell_reports_its_own_position_and_radius() {
        let cfg = Config::for_tests();
        let p = aggregate_cell_cluster(
            vec![cell_net(1, 1, 35.70, 51.30, 2.0, 3_000.0)],
            &cfg.locate.cell,
        );
        assert_eq!((p.lat, p.lon), (35.70, 51.30));
        assert_eq!(p.accuracy, 3_000.0);
        assert_eq!(p.source, PositionSource::Cell);
    }

    #[test]
    fn a_single_cell_radius_is_clamped_to_the_cell_range() {
        let cfg = Config::for_tests();
        let tiny =
            aggregate_cell_cluster(vec![cell_net(1, 1, 35.7, 51.3, 1.0, 5.0)], &cfg.locate.cell);
        assert_eq!(tiny.accuracy, 1_000.0, "a cell is never as precise as 5 m");
        let huge = aggregate_cell_cluster(
            vec![cell_net(1, 1, 35.7, 51.3, 1.0, 9_000_000.0)],
            &cfg.locate.cell,
        );
        assert_eq!(huge.accuracy, 50_000.0);
    }

    #[test]
    fn several_cells_in_an_area_are_averaged() {
        let cfg = Config::for_tests();
        let p = aggregate_cell_cluster(
            vec![
                cell_net(1, 1, 35.70, 51.30, 1.0, 2_000.0),
                cell_net(1, 2, 35.72, 51.30, 1.0, 2_000.0),
            ],
            &cfg.locate.cell,
        );
        assert!((35.709..=35.711).contains(&p.lat), "expected the midpoint, got {}", p.lat);
        assert_eq!(p.score, 2.0);
    }

    #[test]
    fn no_stations_means_no_answer() {
        let cfg = Config::for_tests();
        assert!(search(vec![], vec![], &cfg).is_none());
    }

    #[test]
    fn one_known_access_point_is_not_enough_to_answer() {
        let cfg = Config::for_tests();
        // The two-station minimum is what stops a single access point being
        // located by anyone who cares to ask.
        assert!(search(vec![wifi_net(1, 35.72, 51.33, 5.0)], vec![], &cfg).is_none());
    }

    #[test]
    fn one_access_point_is_enough_when_the_minimum_is_lowered() {
        let mut cfg = Config::for_tests();
        cfg.locate.wifi.min_in_cluster = 1;
        assert!(search(vec![wifi_net(1, 35.72, 51.33, 5.0)], vec![], &cfg).is_some());
    }

    #[test]
    fn a_single_cell_alone_does_answer() {
        let cfg = Config::for_tests();
        let answer = search(vec![], vec![cell_net(1, 1, 35.70, 51.30, 1.0, 4_000.0)], &cfg);
        let answer = answer.expect("one cell is a usable answer, just a vague one");
        assert_eq!(answer.source, PositionSource::Cell);
        assert_eq!(answer.accuracy, 4_000.0);
    }

    #[test]
    fn a_tight_wifi_answer_beats_a_vague_cell_one_in_the_same_place() {
        let cfg = Config::for_tests();
        let answer = search(
            vec![wifi_net(1, 35.7200, 51.3300, 3.0), wifi_net(2, 35.7202, 51.3302, 3.0)],
            vec![cell_net(1, 1, 35.7210, 51.3310, 1.0, 5_000.0)],
            &cfg,
        )
        .unwrap();
        assert_eq!(answer.source, PositionSource::Wifi);
        assert!(answer.accuracy < 500.0);
    }

    #[test]
    fn the_candidate_with_the_most_agreement_wins_not_the_tightest_one() {
        // One very precise candidate on its own, against two that agree with
        // each other. Agreement should carry the day.
        let lonely = position(35.9000, 51.9000, 10.0, 4.0);
        let pair_a = position(35.7200, 51.3300, 300.0, 3.0);
        let pair_b = position(35.7201, 51.3301, 300.0, 3.0);
        let chosen = best(&[lonely, pair_a, pair_b]).unwrap();
        assert!(
            (35.719..=35.721).contains(&chosen.lat),
            "expected the agreeing pair, got {}",
            chosen.lat
        );
    }

    #[test]
    fn within_the_winning_group_the_tightest_answer_is_returned() {
        let loose = position(35.7200, 51.3300, 400.0, 5.0);
        let tight = position(35.7201, 51.3301, 50.0, 5.0);
        let chosen = best(&[loose, tight]).unwrap();
        assert_eq!(chosen.accuracy, 50.0);
    }

    #[test]
    fn a_tie_on_accuracy_is_broken_by_the_higher_score() {
        let weak = position(35.7200, 51.3300, 100.0, 1.0);
        let strong = position(35.7201, 51.3301, 100.0, 9.0);
        assert_eq!(best(&[weak, strong]).unwrap().score, 9.0);
    }

    #[test]
    fn a_lone_candidate_is_returned_unchanged() {
        let only = position(35.7200, 51.3300, 123.0, 1.0);
        assert_eq!(best(std::slice::from_ref(&only)).unwrap(), only);
    }

    #[test]
    fn best_of_nothing_is_nothing() {
        assert!(best(&[]).is_none());
        assert!(best_cluster(&[]).is_empty());
    }

    #[test]
    fn two_wifi_clusters_far_apart_both_become_candidates() {
        let cfg = Config::for_tests();
        let wifi = vec![
            wifi_net(1, 35.7200, 51.3300, 1.0),
            wifi_net(2, 35.7202, 51.3302, 1.0),
            wifi_net(3, 35.9000, 51.9000, 9.0),
            wifi_net(4, 35.9002, 51.9002, 9.0),
        ];
        let found = candidates(wifi, vec![], &cfg);
        assert_eq!(found.len(), 2);
        // The better-supported pair should win the run-off.
        let chosen = best(&found).unwrap();
        assert!(chosen.lat > 35.85, "expected the higher-scoring cluster");
    }

    #[test]
    fn cells_in_different_areas_produce_separate_candidates() {
        let cfg = Config::for_tests();
        let cells = vec![
            cell_net(100, 1, 35.70, 51.30, 1.0, 3_000.0),
            cell_net(200, 2, 35.90, 51.90, 5.0, 3_000.0),
        ];
        assert_eq!(candidates(vec![], cells, &cfg).len(), 2);
    }

    #[test]
    fn an_answer_never_contains_a_number_a_client_cannot_use() {
        let cfg = Config::for_tests();
        let answer = search(
            vec![wifi_net(1, 35.7200, 51.3300, 1.0), wifi_net(2, 35.7202, 51.3302, 1.0)],
            vec![cell_net(1, 1, 35.7210, 51.3310, 1.0, 5_000.0)],
            &cfg,
        )
        .unwrap();
        assert!(answer.lat.is_finite() && answer.lon.is_finite());
        assert!(answer.accuracy.is_finite() && answer.accuracy > 0.0);
        assert!((-90.0..=90.0).contains(&answer.lat));
        assert!((-180.0..=180.0).contains(&answer.lon));
    }
}
