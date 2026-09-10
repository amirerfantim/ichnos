//! Folding a batch of observations into one station update.
//!
//! This is where the write path earns its keep. A worker drains a window of
//! observations, groups them by station, and turns each group into a single
//! update. Twenty sightings of one access point become one row written once,
//! not twenty read-modify-write cycles.
//!
//! It is also where the service decides whether to believe what it is told. A
//! station that suddenly reports from somewhere else has either moved or is
//! being misreported, and either way its recorded position is no longer
//! trustworthy. The state machine below is Ichnaea's, ported whole.

use crate::config::Config;
use crate::geo::{circle_radius, distance};
use crate::model::observation::ReportSource;
use crate::model::{Observation, Station, StationKey, StationKind};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use std::collections::BTreeMap;

/// Ceilings that stop a long-lived station overflowing its columns.
const MAX_SAMPLES: i64 = 4_294_967_295;
const MAX_WEIGHT: f64 = 1_000_000_000.0;

/// What the state machine decided to do with a station.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Never seen before, and the report is coherent.
    New,
    /// Never seen before, and the report is not coherent. Recorded with no
    /// position so the incoherence is remembered rather than relearned.
    NewBlock,
    /// Blend the new evidence into what is already known.
    Change,
    /// Throw the old position away and start from this report.
    Replace,
    /// The report agrees; note that the station was seen and change nothing
    /// else.
    Confirm,
    /// The report disagrees. Wipe the position and start the clock again.
    Block,
    /// Nothing worth writing.
    Nothing,
}

/// A station update ready to be written.
#[derive(Debug, Clone)]
pub struct StationUpdate {
    pub action: Action,
    pub station: Station,
}

/// The aggregate of one batch of observations for one station.
#[derive(Debug, Clone)]
struct ObsData {
    lat: f64,
    lon: f64,
    max_lat: f64,
    min_lat: f64,
    max_lon: f64,
    min_lon: f64,
    samples: i64,
    weight: f64,
}

/// Group observations by station.
///
/// The grouping is the whole saving: it is what turns repeated sightings into
/// one write.
pub fn group(observations: Vec<Observation>) -> BTreeMap<StationKey, Vec<Observation>> {
    let mut grouped: BTreeMap<StationKey, Vec<Observation>> = BTreeMap::new();
    for observation in observations {
        grouped.entry(observation.key).or_default().push(observation);
    }
    grouped
}

/// Fold one station's observations against what is already known about it.
///
/// `existing` is `None` for a station never seen before.
pub fn fold_station(
    key: StationKey,
    observations: &[Observation],
    existing: Option<&Station>,
    cfg: &Config,
    now: DateTime<Utc>,
) -> StationUpdate {
    let today = now.date_naive();
    let max_dist = match key.kind() {
        StationKind::Wifi => cfg.station.wifi_max_radius_m,
        StationKind::Cell => cfg.station.cell_max_radius_m,
    };

    let obs_data = aggregate(observations, cfg, max_dist);
    let source = best_source(observations);

    let action = decide(existing, obs_data.as_ref(), observations, max_dist, today, cfg);

    let station = match action {
        Action::Nothing => {
            return StationUpdate {
                action,
                station: existing.cloned().unwrap_or_else(|| Station::new(key, now)),
            }
        }
        Action::New => {
            let data = obs_data.expect("New requires coherent observations");
            let mut station = Station::new(key, now);
            apply_position(&mut station, &data, cfg);
            station.samples = bound_samples(data.samples);
            station.weight = bound_weight(data.weight);
            station.source = Some(source);
            station.last_seen = Some(today);
            station.modified = now;
            station
        }
        Action::NewBlock => {
            let mut station = Station::new(key, now);
            station.block_first = Some(today);
            station.block_last = Some(today);
            station.block_count = 1;
            station.modified = now;
            station
        }
        Action::Block => {
            let mut station = existing.cloned().unwrap_or_else(|| Station::new(key, now));
            // The position is wiped rather than kept and ignored: keeping it
            // would let the station be relearned from evidence already judged
            // untrustworthy.
            station.lat = None;
            station.lon = None;
            station.radius = None;
            station.max_lat = None;
            station.min_lat = None;
            station.max_lon = None;
            station.min_lon = None;
            station.h3 = None;
            station.samples = 0;
            station.weight = 0.0;
            station.source = None;
            station.block_first = station.block_first.or(Some(today));
            station.block_last = Some(today);
            station.block_count += 1;
            station.modified = now;
            station
        }
        Action::Replace => {
            let data = obs_data.expect("Replace requires coherent observations");
            let mut station = existing.cloned().unwrap_or_else(|| Station::new(key, now));
            apply_position(&mut station, &data, cfg);
            station.samples = bound_samples(data.samples);
            station.weight = bound_weight(data.weight);
            station.source = Some(source);
            station.last_seen = Some(today);
            station.modified = now;
            station
        }
        Action::Change => {
            let data = obs_data.expect("Change requires coherent observations");
            let mut station = existing.cloned().unwrap_or_else(|| Station::new(key, now));
            blend(&mut station, &data, cfg);
            station.source = Some(source.min_quality(station.source));
            station.last_seen = Some(today);
            station.modified = now;
            station
        }
        Action::Confirm => {
            let mut station = existing.cloned().expect("Confirm requires an existing station");
            station.last_seen = Some(today);
            station
        }
    };

    StationUpdate { action, station }
}

/// Pick the action for one station.
///
/// The table is Ichnaea's. Two axes: whether the batch is coherent with itself,
/// and whether it agrees with what is already recorded.
fn decide(
    existing: Option<&Station>,
    obs_data: Option<&ObsData>,
    observations: &[Observation],
    max_dist: f64,
    today: NaiveDate,
    cfg: &Config,
) -> Action {
    let consistent = obs_data.is_some();
    let source = best_source(observations);
    let from_query = source == ReportSource::Query;

    let Some(station) = existing else {
        // Never seen before.
        return if consistent { Action::New } else { Action::NewBlock };
    };

    if !station.has_position() {
        return if consistent { Action::Change } else { Action::Nothing };
    }

    let agrees = observations_agree(station, observations, max_dist);

    if agrees {
        let station_from_query = station.source == Some(ReportSource::Query);
        return match (station_from_query, from_query, consistent) {
            // The recorded position was itself only a guess, and a real fix has
            // arrived. A measurement beats an inference.
            (true, false, true) => Action::Replace,
            // Agreeing evidence of the same quality: blend it in.
            (_, _, true) if station_from_query == from_query => Action::Change,
            // A guess agreeing with a measured position adds nothing to the
            // position, but does confirm the station is still there.
            (false, true, true) => Action::Confirm,
            (_, _, true) => Action::Change,
            // Agreement with an incoherent batch is not agreement.
            (_, _, false) => Action::Block,
        };
    }

    // The report disagrees with the recorded position.
    if station.is_old_position(today, cfg.station.old_position_days) && consistent {
        // Old enough that the world probably changed rather than the data
        // being wrong.
        Action::Replace
    } else {
        Action::Block
    }
}

/// Does every observation sit close enough to the recorded position?
fn observations_agree(station: &Station, observations: &[Observation], max_dist: f64) -> bool {
    let (Some(lat), Some(lon)) = (station.lat, station.lon) else {
        return false;
    };
    observations.iter().all(|o| distance(o.lat, o.lon, lat, lon) <= max_dist)
}

/// Aggregate a batch into one weighted point, or `None` if the batch disagrees
/// with itself.
///
/// An incoherent batch means the same station was reported from two places at
/// once, which says the station moved, the report is wrong, or the identifier
/// is being reused. None of those should be averaged into a position.
fn aggregate(observations: &[Observation], cfg: &Config, max_dist: f64) -> Option<ObsData> {
    if observations.is_empty() {
        return None;
    }

    let mut max_lat = f64::MIN;
    let mut min_lat = f64::MAX;
    let mut max_lon = f64::MIN;
    let mut min_lon = f64::MAX;
    for o in observations {
        max_lat = max_lat.max(o.lat);
        min_lat = min_lat.min(o.lat);
        max_lon = max_lon.max(o.lon);
        min_lon = min_lon.min(o.lon);
    }

    if distance(min_lat, min_lon, max_lat, max_lon) > max_dist {
        return None;
    }

    let mut sum_weight = 0.0;
    let mut sum_lat = 0.0;
    let mut sum_lon = 0.0;
    for o in observations {
        let w = o.weight(&cfg.validation);
        sum_weight += w;
        sum_lat += o.lat * w;
        sum_lon += o.lon * w;
    }
    if sum_weight <= 0.0 || !sum_weight.is_finite() {
        return None;
    }

    Some(ObsData {
        lat: sum_lat / sum_weight,
        lon: sum_lon / sum_weight,
        max_lat,
        min_lat,
        max_lon,
        min_lon,
        samples: observations.len() as i64,
        weight: sum_weight,
    })
}

/// Write a fresh position onto a station.
fn apply_position(station: &mut Station, data: &ObsData, cfg: &Config) {
    station.lat = Some(data.lat);
    station.lon = Some(data.lon);
    station.max_lat = Some(data.max_lat);
    station.min_lat = Some(data.min_lat);
    station.max_lon = Some(data.max_lon);
    station.min_lon = Some(data.min_lon);
    station.radius = Some(circle_radius(
        data.lat,
        data.lon,
        data.max_lat,
        data.max_lon,
        data.min_lat,
        data.min_lon,
    ));
    station.h3 = h3_cell(data.lat, data.lon, cfg.spatial.h3_resolution);
}

/// Blend new evidence into an existing position.
///
/// The existing position's weight is capped, which is what keeps a station that
/// has been observed thousands of times from becoming immovable. Without the
/// cap, a station that genuinely moved could never be relearned; with it, the
/// old position still dominates in the short term but yields to sustained new
/// evidence.
fn blend(station: &mut Station, data: &ObsData, cfg: &Config) {
    let old_weight =
        if station.has_position() { station.weight.min(cfg.station.max_old_weight) } else { 0.0 };
    let total = data.weight + old_weight;

    let lat = (data.lat * data.weight + station.lat.unwrap_or(0.0) * old_weight) / total;
    let lon = (data.lon * data.weight + station.lon.unwrap_or(0.0) * old_weight) / total;

    // The bounding box only ever grows, so the radius reflects everything the
    // station has ever been seen from.
    let max_lat = data.max_lat.max(station.max_lat.unwrap_or(f64::MIN));
    let min_lat = data.min_lat.min(station.min_lat.unwrap_or(f64::MAX));
    let max_lon = data.max_lon.max(station.max_lon.unwrap_or(f64::MIN));
    let min_lon = data.min_lon.min(station.min_lon.unwrap_or(f64::MAX));

    station.lat = Some(lat);
    station.lon = Some(lon);
    station.max_lat = Some(max_lat);
    station.min_lat = Some(min_lat);
    station.max_lon = Some(max_lon);
    station.min_lon = Some(min_lon);
    station.radius = Some(circle_radius(lat, lon, max_lat, max_lon, min_lat, min_lon));
    station.h3 = h3_cell(lat, lon, cfg.spatial.h3_resolution);
    station.samples = bound_samples(i64::from(station.samples) + data.samples);
    station.weight = bound_weight(station.weight + data.weight);
}

/// The spatial index cell a position falls in.
fn h3_cell(lat: f64, lon: f64, resolution: u8) -> Option<i64> {
    let coord = h3o::LatLng::new(lat, lon).ok()?;
    let resolution = h3o::Resolution::try_from(resolution).ok()?;
    Some(u64::from(coord.to_cell(resolution)) as i64)
}

/// The best-quality source in a batch. Lower is better.
fn best_source(observations: &[Observation]) -> ReportSource {
    observations.iter().map(|o| o.source).min_by_key(|s| s.as_i8()).unwrap_or(ReportSource::Gnss)
}

impl ReportSource {
    /// Keep the better of two sources, treating an absent one as unknown.
    fn min_quality(self, other: Option<ReportSource>) -> ReportSource {
        match other {
            Some(o) if o.as_i8() < self.as_i8() => o,
            _ => self,
        }
    }
}

fn bound_samples(samples: i64) -> i32 {
    samples.clamp(0, MAX_SAMPLES).min(i64::from(i32::MAX)) as i32
}

fn bound_weight(weight: f64) -> f64 {
    weight.clamp(0.0, MAX_WEIGHT)
}

/// How much folding saved: observations in, stations out.
pub fn fold_ratio(observations: usize, stations: usize) -> f64 {
    if stations == 0 {
        return 0.0;
    }
    observations as f64 / stations as f64
}

/// Convenience for tests and the replay harness: how long ago a station was
/// last written.
pub fn age_days(station: &Station, now: DateTime<Utc>) -> i64 {
    (now - station.modified).num_days().max(0)
}

/// A station that has not been seen for longer than the row lifetime would have
/// expired on its own. Used by the replay harness, which has no expiry.
pub fn is_expired(station: &Station, now: DateTime<Utc>, ttl_days: i64) -> bool {
    station.modified < now - Duration::days(ttl_days)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::WifiKey;

    fn now() -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(2026, 6, 1).unwrap().and_hms_opt(12, 0, 0).unwrap().and_utc()
    }

    fn key() -> StationKey {
        StationKey::Wifi(WifiKey(1))
    }

    fn obs_at(lat: f64, lon: f64) -> Observation {
        Observation {
            key: key(),
            lat,
            lon,
            accuracy: Some(10.0),
            age_ms: Some(0.0),
            speed: Some(0.0),
            signal: Some(-70),
            source: ReportSource::Gnss,
            timestamp: 1_700_000_000_000,
        }
    }

    fn existing_at(lat: f64, lon: f64, weight: f64) -> Station {
        let cfg = Config::for_tests();
        let mut s = Station::new(key(), now());
        let data = aggregate(&[obs_at(lat, lon)], &cfg, 5_000.0).unwrap();
        apply_position(&mut s, &data, &cfg);
        s.samples = 10;
        s.weight = weight;
        s.source = Some(ReportSource::Gnss);
        s.last_seen = Some(now().date_naive() - Duration::days(1));
        s
    }

    #[test]
    fn observations_group_by_station() {
        let a = obs_at(35.72, 51.33);
        let mut b = obs_at(35.73, 51.34);
        b.key = StationKey::Wifi(WifiKey(2));
        let grouped = group(vec![a.clone(), b, a]);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[&key()].len(), 2);
    }

    #[test]
    fn twenty_sightings_of_one_station_become_one_update() {
        // This is the saving the whole write path is built around.
        let cfg = Config::for_tests();
        let observations: Vec<Observation> = (0..20).map(|_| obs_at(35.7219, 51.3347)).collect();
        let grouped = group(observations.clone());
        assert_eq!(grouped.len(), 1);
        assert_eq!(fold_ratio(observations.len(), grouped.len()), 20.0);

        let update = fold_station(key(), &observations, None, &cfg, now());
        assert_eq!(update.action, Action::New);
        assert_eq!(update.station.samples, 20, "but all twenty still count as evidence");
    }

    #[test]
    fn an_unknown_station_with_a_coherent_report_is_learned() {
        let cfg = Config::for_tests();
        let update = fold_station(key(), &[obs_at(35.7219, 51.3347)], None, &cfg, now());
        assert_eq!(update.action, Action::New);
        assert!(update.station.has_position());
        assert_eq!(update.station.last_seen, Some(now().date_naive()));
        assert!(update.station.h3.is_some(), "and it lands in the spatial index");
    }

    #[test]
    fn an_unknown_station_with_an_incoherent_report_is_recorded_without_a_position() {
        // Two sightings 50 km apart cannot both be right. Remembering the
        // incoherence is better than averaging it into a position.
        let cfg = Config::for_tests();
        let observations = vec![obs_at(35.7219, 51.3347), obs_at(36.2000, 51.3347)];
        let update = fold_station(key(), &observations, None, &cfg, now());
        assert_eq!(update.action, Action::NewBlock);
        assert!(!update.station.has_position());
        assert_eq!(update.station.block_count, 1);
    }

    #[test]
    fn agreeing_evidence_is_blended_in() {
        let cfg = Config::for_tests();
        let existing = existing_at(35.7219, 51.3347, 100.0);
        let update = fold_station(key(), &[obs_at(35.7220, 51.3348)], Some(&existing), &cfg, now());
        assert_eq!(update.action, Action::Change);
        assert!(update.station.samples > existing.samples);
        assert!(update.station.weight > existing.weight);
    }

    #[test]
    fn a_well_established_position_barely_moves_on_one_new_sighting() {
        let cfg = Config::for_tests();
        let existing = existing_at(35.7219, 51.3347, 5_000.0);
        let update = fold_station(key(), &[obs_at(35.7250, 51.3347)], Some(&existing), &cfg, now());
        let moved = distance(
            existing.lat.unwrap(),
            existing.lon.unwrap(),
            update.station.lat.unwrap(),
            update.station.lon.unwrap(),
        );
        assert!(moved < 1.0, "moved {moved:.3} m; heavy stations should be steady");
    }

    #[test]
    fn a_lightly_evidenced_position_moves_readily() {
        let cfg = Config::for_tests();
        let existing = existing_at(35.7219, 51.3347, 0.5);
        let update = fold_station(key(), &[obs_at(35.7250, 51.3347)], Some(&existing), &cfg, now());
        let moved = distance(
            existing.lat.unwrap(),
            existing.lon.unwrap(),
            update.station.lat.unwrap(),
            update.station.lon.unwrap(),
        );
        assert!(moved > 10.0, "moved only {moved:.3} m; weak stations should yield");
    }

    #[test]
    fn the_old_weight_cap_keeps_a_moved_station_relearnable() {
        // Without the cap, a station observed a million times could never be
        // corrected. With it, the old position dominates but does not win
        // forever.
        let cfg = Config::for_tests();
        let mut heavy = existing_at(35.7219, 51.3347, 1_000_000.0);
        heavy.weight = 1_000_000.0;
        let update = fold_station(key(), &[obs_at(35.7250, 51.3347)], Some(&heavy), &cfg, now());
        let moved = distance(
            heavy.lat.unwrap(),
            heavy.lon.unwrap(),
            update.station.lat.unwrap(),
            update.station.lon.unwrap(),
        );
        assert!(moved > 0.0, "the cap must leave some room to move");
    }

    #[test]
    fn a_station_reported_from_far_away_is_blocked() {
        let cfg = Config::for_tests();
        let existing = existing_at(35.7219, 51.3347, 100.0);
        let update = fold_station(key(), &[obs_at(36.5000, 52.0000)], Some(&existing), &cfg, now());

        assert_eq!(update.action, Action::Block);
        assert!(!update.station.has_position(), "the position is wiped, not merely ignored");
        assert_eq!(update.station.block_count, 1);
        assert_eq!(update.station.block_last, Some(now().date_naive()));
        assert_eq!(update.station.samples, 0);
    }

    #[test]
    fn blocking_again_raises_the_count_and_keeps_the_first_date() {
        let cfg = Config::for_tests();
        let mut existing = existing_at(35.7219, 51.3347, 100.0);
        existing.block_count = 1;
        existing.block_first = Some(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());

        let update = fold_station(key(), &[obs_at(36.5000, 52.0000)], Some(&existing), &cfg, now());
        assert_eq!(update.station.block_count, 2);
        assert_eq!(update.station.block_first, Some(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()));
        assert_eq!(update.station.block_last, Some(now().date_naive()));
    }

    #[test]
    fn a_stale_position_is_replaced_rather_than_blocked() {
        // Past a year, a disagreement is more likely to mean the world changed
        // than that the report is wrong.
        let cfg = Config::for_tests();
        let mut existing = existing_at(35.7219, 51.3347, 100.0);
        existing.modified = now() - Duration::days(400);

        let update = fold_station(key(), &[obs_at(36.5000, 52.0000)], Some(&existing), &cfg, now());
        assert_eq!(update.action, Action::Replace);
        assert!(update.station.has_position());
        assert_eq!(update.station.block_count, 0, "and it is not held against the station");
    }

    #[test]
    fn a_measured_fix_replaces_a_guessed_one() {
        let cfg = Config::for_tests();
        let mut guessed = existing_at(35.7219, 51.3347, 100.0);
        guessed.source = Some(ReportSource::Query);

        let update = fold_station(key(), &[obs_at(35.7220, 51.3348)], Some(&guessed), &cfg, now());
        assert_eq!(update.action, Action::Replace);
        assert_eq!(update.station.source, Some(ReportSource::Gnss));
    }

    #[test]
    fn a_guess_agreeing_with_a_measured_position_only_confirms_it() {
        let cfg = Config::for_tests();
        let existing = existing_at(35.7219, 51.3347, 100.0);
        let mut guess = obs_at(35.7220, 51.3348);
        guess.source = ReportSource::Query;

        let update = fold_station(key(), &[guess], Some(&existing), &cfg, now());
        assert_eq!(update.action, Action::Confirm);
        assert_eq!(update.station.lat, existing.lat, "the position is untouched");
        assert_eq!(update.station.last_seen, Some(now().date_naive()));
    }

    #[test]
    fn a_station_recorded_without_a_position_can_gain_one() {
        let cfg = Config::for_tests();
        let mut blocked = Station::new(key(), now());
        blocked.block_count = 1;
        blocked.block_last = Some(now().date_naive() - Duration::days(30));

        let update = fold_station(key(), &[obs_at(35.7219, 51.3347)], Some(&blocked), &cfg, now());
        assert_eq!(update.action, Action::Change);
        assert!(update.station.has_position());
    }

    #[test]
    fn an_incoherent_report_about_a_positionless_station_changes_nothing() {
        let cfg = Config::for_tests();
        let blocked = Station::new(key(), now());
        let observations = vec![obs_at(35.7219, 51.3347), obs_at(36.2000, 51.3347)];
        let update = fold_station(key(), &observations, Some(&blocked), &cfg, now());
        assert_eq!(update.action, Action::Nothing);
    }

    #[test]
    fn cells_tolerate_a_far_wider_spread_than_access_points() {
        let cfg = Config::for_tests();
        // 50 km apart: incoherent for WiFi, ordinary for a cell.
        let far = vec![obs_at(35.7219, 51.3347), obs_at(36.2000, 51.3347)];
        assert!(aggregate(&far, &cfg, cfg.station.wifi_max_radius_m).is_none());
        assert!(aggregate(&far, &cfg, cfg.station.cell_max_radius_m).is_some());
    }

    #[test]
    fn the_bounding_box_only_ever_grows() {
        let cfg = Config::for_tests();
        let existing = existing_at(35.7219, 51.3347, 100.0);
        let update = fold_station(key(), &[obs_at(35.7300, 51.3400)], Some(&existing), &cfg, now());

        assert!(update.station.max_lat.unwrap() >= existing.max_lat.unwrap());
        assert!(update.station.min_lat.unwrap() <= existing.min_lat.unwrap());
        assert!(update.station.radius.unwrap() > 0, "and the radius follows it");
    }

    #[test]
    fn an_empty_batch_produces_nothing() {
        let cfg = Config::for_tests();
        let update = fold_station(key(), &[], None, &cfg, now());
        assert_eq!(update.action, Action::NewBlock);
    }

    #[test]
    fn counters_are_capped_so_a_long_lived_station_cannot_overflow() {
        assert_eq!(bound_samples(i64::MAX), i32::MAX);
        assert_eq!(bound_samples(-5), 0);
        assert_eq!(bound_weight(f64::MAX), MAX_WEIGHT);
        assert_eq!(bound_weight(-1.0), 0.0);
    }

    #[test]
    fn the_spatial_cell_follows_the_position() {
        let a = h3_cell(35.7219, 51.3347, 9).unwrap();
        let b = h3_cell(35.7219, 51.3347, 9).unwrap();
        let far = h3_cell(35.9000, 51.9000, 9).unwrap();
        assert_eq!(a, b, "the same point always lands in the same cell");
        assert_ne!(a, far);
        assert_eq!(h3_cell(f64::NAN, 0.0, 9), None, "a bad position has no cell");
    }
}
