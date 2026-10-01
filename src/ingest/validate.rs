//! Turning a submitted report into observations worth keeping.
//!
//! The rules are Ichnaea's, field for field, because the answers must be
//! Ichnaea's: a client that switches sees the same stations learned from the
//! same reports.
//!
//! Three levels, and the difference is deliberate:
//!
//! - A report without a usable **position** is dropped whole. Every station
//!   in it would be learned at the wrong place.
//! - A **station** without a usable identity (address, radio, country,
//!   network, area, cell) drops only itself. The others are still evidence.
//! - Any other **field** that is out of range is forgotten, and the station is
//!   kept: a signal of +5 dBm says nothing about the signal, but the fix it
//!   came with is still a fix.
//!
//! Scan ages are measured from the position fix, not from the request: a
//! scan taken 11 s before the request, with a fix taken 8 s before it, is
//! 3 s from the fix. That is the age that decides how much the scan counts.

use std::collections::HashMap;

use crate::api::schema::{CellTower, SubmitItem, WifiAccessPoint};
use crate::config::Validation;
use crate::model::key::{CellKey, Radio, StationKey, WifiKey};
use crate::model::observation::{Observation, ReportSource};
use chrono::Utc;

/// Why a report or a station was not kept. Counted, so the shape of the
/// incoming data is visible without reading logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    NoPosition,
    PositionOutOfRange,
    /// The position was too poor to learn anything from, which the weights
    /// express as a weight of zero.
    Worthless,
    BadMac,
    PlaceholderMac,
    UnknownRadio,
    MccNotAllowed,
    CellFieldOutOfRange,
    /// The same station twice in one report: the weaker sighting is dropped.
    Duplicate,
}

impl DropReason {
    /// A stable metric label.
    ///
    /// Formatting the enum instead would allocate a string for every dropped
    /// station, on the busiest path in the service.
    pub fn as_str(self) -> &'static str {
        match self {
            DropReason::NoPosition => "no_position",
            DropReason::PositionOutOfRange => "position_out_of_range",
            DropReason::Worthless => "worthless",
            DropReason::BadMac => "bad_mac",
            DropReason::PlaceholderMac => "placeholder_mac",
            DropReason::UnknownRadio => "unknown_radio",
            DropReason::MccNotAllowed => "mcc_not_allowed",
            DropReason::CellFieldOutOfRange => "cell_field_out_of_range",
            DropReason::Duplicate => "duplicate",
        }
    }
}

/// What came out of one report.
#[derive(Debug, Default, Clone)]
pub struct Validated {
    pub observations: Vec<Observation>,
    /// One entry per dropped station, or one per report when the position
    /// failed.
    pub dropped: Vec<DropReason>,
}

impl Validated {
    fn drop_all(reason: DropReason) -> Self {
        Self { observations: vec![], dropped: vec![reason] }
    }

    pub fn is_empty(&self) -> bool {
        self.observations.is_empty()
    }
}

/// A station seen in a report, with what decides between two sightings of
/// it. Timing advance is only ever used for that choice.
struct Sighting {
    observation: Observation,
    timing_advance: Option<i64>,
}

impl Sighting {
    /// Whether this sighting beats a later one of the same station.
    ///
    /// Upstream's rule, quirks included: the comparisons run in order (timing
    /// advance, then signal, then age), each only when both sightings have the
    /// value, and the first one this sighting wins decides. A comparison it
    /// loses does not stop the next one from being tried.
    fn better(&self, other: &Sighting) -> bool {
        let (a, b) = (&self.observation, &other.observation);
        let wins = [
            both(self.timing_advance, other.timing_advance).is_some_and(|(x, y)| x < y),
            both(a.signal, b.signal).is_some_and(|(x, y)| x > y),
            both(a.age_ms, b.age_ms).is_some_and(|(x, y)| x < y),
        ];
        wins.into_iter().any(|w| w)
    }
}

fn both<T>(a: Option<T>, b: Option<T>) -> Option<(T, T)> {
    a.zip(b)
}

/// Validate one report and turn it into observations.
pub fn validate_item(item: &SubmitItem, cfg: &Validation) -> Validated {
    let Some(position) = item.position.as_ref() else {
        return Validated::drop_all(DropReason::NoPosition);
    };
    let (lat, lon) = match (position.latitude, position.longitude) {
        (Some(lat), Some(lon)) => (lat, lon),
        _ => return Validated::drop_all(DropReason::NoPosition),
    };
    if let Err(reason) = check_position(lat, lon, cfg) {
        return Validated::drop_all(reason);
    }

    let accuracy = within(position.accuracy, cfg.min_accuracy_m, cfg.max_accuracy_m);
    let speed = within(position.speed, cfg.min_speed_mps, cfg.max_speed_mps);
    let source = parse_source(position.source.as_deref());
    // How long before the request the fix was taken. Zero or absent means
    // the times in the report are already the fix's.
    let fix_age = position.age.filter(|age| age.is_finite() && *age != 0.0);
    let timestamp =
        fix_timestamp(item.timestamp, fix_age).unwrap_or_else(|| Utc::now().timestamp_millis());

    let observation = |key, own_age, signal| Observation {
        key,
        lat,
        lon,
        accuracy,
        age_ms: scan_age(own_age, fix_age, cfg),
        speed,
        signal,
        source,
        timestamp,
    };

    let mut out = Validated::default();
    let mut sightings: Vec<Sighting> = Vec::new();
    let mut index: HashMap<StationKey, usize> = HashMap::new();
    let mut keep =
        |sighting: Sighting, out: &mut Validated| match index.get(&sighting.observation.key) {
            Some(&i) => {
                out.dropped.push(DropReason::Duplicate);
                if !sightings[i].better(&sighting) {
                    sightings[i] = sighting;
                }
            }
            None => {
                index.insert(sighting.observation.key, sightings.len());
                sightings.push(sighting);
            }
        };

    for wifi in &item.wifi_access_points {
        match validate_wifi(wifi, cfg) {
            Ok((key, signal)) => keep(
                Sighting {
                    observation: observation(StationKey::Wifi(key), wifi.age, signal),
                    timing_advance: None,
                },
                &mut out,
            ),
            Err(reason) => out.dropped.push(reason),
        }
    }
    for cell in &item.cell_towers {
        match validate_cell(cell, cfg) {
            Ok((key, signal, timing_advance)) => keep(
                Sighting {
                    observation: observation(StationKey::Cell(key), cell.age, signal),
                    timing_advance,
                },
                &mut out,
            ),
            Err(reason) => out.dropped.push(reason),
        }
    }

    // The weight is judged on the sighting that won, as upstream judges it:
    // a weightless best sighting loses the station even when a weaker one
    // carried weight.
    for sighting in sightings {
        keep_if_useful(sighting.observation, cfg, &mut out);
    }
    out
}

/// The report's time moved back to the fix. Outside 2001-2286 it is not a
/// time at all, and the caller uses the clock instead.
fn fix_timestamp(timestamp: Option<i64>, fix_age: Option<f64>) -> Option<i64> {
    const EARLIEST: i64 = 1_000_000_000_000;
    const LATEST: i64 = 10_000_000_000_000;
    let at = timestamp.filter(|t| *t != 0)?;
    let at = match fix_age {
        Some(age) => (at as f64 - age) as i64,
        None => at,
    };
    (EARLIEST..=LATEST).contains(&at).then_some(at)
}

/// A scan's age measured from the fix, in whole milliseconds, or nothing
/// when it falls outside the plausible range. A scan with no age of its own
/// was taken with the request, so it is as old as the fix.
fn scan_age(own: Option<f64>, fix_age: Option<f64>, cfg: &Validation) -> Option<f64> {
    let age = match fix_age {
        Some(fix) => Some(own.unwrap_or(0.0) - fix),
        None => own,
    }?;
    let age = age.trunc();
    (age.is_finite() && age >= cfg.min_age_ms as f64 && age <= cfg.max_age_ms as f64).then_some(age)
}

/// The value when it is a finite number inside [lo, hi], otherwise nothing.
fn within(value: Option<f64>, lo: f64, hi: f64) -> Option<f64> {
    value.filter(|v| v.is_finite() && *v >= lo && *v <= hi)
}

/// A signal strength in [lo, hi], otherwise nothing. Zero is how clients
/// spell "not measured", and falls outside every range anyway.
fn signal_within(value: Option<i64>, lo: i64, hi: i64) -> Option<i32> {
    value.filter(|s| *s != 0 && *s >= lo && *s <= hi).map(|s| s as i32)
}

/// Keep an observation only if it carries weight.
///
/// A weight of zero is how the weighting expresses "too inaccurate, too stale,
/// or too fast to be evidence". Such an observation must be dropped rather than
/// stored, because a batch of zeros has nothing to average.
fn keep_if_useful(observation: Observation, cfg: &Validation, out: &mut Validated) {
    if observation.base_weight(cfg) > 0.0 {
        out.observations.push(observation);
    } else {
        out.dropped.push(DropReason::Worthless);
    }
}

fn check_position(lat: f64, lon: f64, cfg: &Validation) -> Result<(), DropReason> {
    if !lat.is_finite() || !lon.is_finite() {
        return Err(DropReason::PositionOutOfRange);
    }
    if lat < cfg.lat_bounds.0 || lat > cfg.lat_bounds.1 {
        return Err(DropReason::PositionOutOfRange);
    }
    if lon < cfg.lon_bounds.0 || lon > cfg.lon_bounds.1 {
        return Err(DropReason::PositionOutOfRange);
    }
    Ok(())
}

/// Absent means satellite navigation, which is what a client that says nothing
/// almost always means.
fn parse_source(source: Option<&str>) -> ReportSource {
    match source.map(str::to_ascii_lowercase).as_deref() {
        Some("fixed") => ReportSource::Fixed,
        Some("fused") => ReportSource::Fused,
        Some("query") => ReportSource::Query,
        _ => ReportSource::Gnss,
    }
}

/// An access point's identity and signal. Its frequency only has to be
/// plausible to be useful, and nothing here uses it, so a wrong one costs
/// nothing.
fn validate_wifi(
    wifi: &WifiAccessPoint,
    cfg: &Validation,
) -> Result<(WifiKey, Option<i32>), DropReason> {
    let raw = wifi.mac_address.as_deref().ok_or(DropReason::BadMac)?;
    let normalised = raw.to_ascii_lowercase();
    if cfg.invalid_macs.iter().any(|m| m.eq_ignore_ascii_case(&normalised)) {
        return Err(DropReason::PlaceholderMac);
    }
    let key = WifiKey::parse(&normalised).map_err(|_| DropReason::BadMac)?;
    let signal = signal_within(
        wifi.signal_strength,
        i64::from(cfg.wifi_signal_dbm.0),
        i64::from(cfg.wifi_signal_dbm.1),
    );
    Ok((key, signal))
}

/// A cell's identity, signal and timing advance.
fn validate_cell(
    cell: &CellTower,
    cfg: &Validation,
) -> Result<(CellKey, Option<i32>, Option<i64>), DropReason> {
    let mut radio =
        cell.radio_type.as_deref().and_then(Radio::parse).ok_or(DropReason::UnknownRadio)?;
    // GSM identifiers are 16 bits, so a larger one is a WCDMA cell the client
    // labelled GSM.
    if radio == Radio::Gsm && cell.cell_id.is_some_and(|cid| cid > cfg.cid_gsm_max) {
        radio = Radio::Wcdma;
    }

    let mcc = i32::try_from(cell.mobile_country_code.ok_or(DropReason::CellFieldOutOfRange)?)
        .map_err(|_| DropReason::CellFieldOutOfRange)?;
    if mcc < cfg.mcc.0 || mcc > cfg.mcc.1 {
        return Err(DropReason::CellFieldOutOfRange);
    }
    if !cfg.mcc_allowlist.is_empty() && !cfg.mcc_allowlist.contains(&mcc) {
        return Err(DropReason::MccNotAllowed);
    }

    let mnc = i32::try_from(cell.mobile_network_code.ok_or(DropReason::CellFieldOutOfRange)?)
        .map_err(|_| DropReason::CellFieldOutOfRange)?;
    if mnc < cfg.mnc.0 || mnc > cfg.mnc.1 {
        return Err(DropReason::CellFieldOutOfRange);
    }

    let lac = cell.location_area_code.ok_or(DropReason::CellFieldOutOfRange)?;
    if lac < cfg.lac.0 || lac > cfg.lac.1 {
        return Err(DropReason::CellFieldOutOfRange);
    }

    let cid = cell.cell_id.ok_or(DropReason::CellFieldOutOfRange)?;
    if cid < cfg.cid.0 || cid > cfg.cid.1 {
        return Err(DropReason::CellFieldOutOfRange);
    }

    // WCDMA has no timing advance; anything a client sends there is noise.
    let timing_advance = if radio == Radio::Wcdma {
        None
    } else {
        cell.timing_advance
            .filter(|ta| *ta >= i64::from(cfg.cell_ta.0) && *ta <= i64::from(cfg.cell_ta.1))
    };

    let (min_signal, max_signal) = match radio {
        Radio::Gsm => (-113, -51),
        Radio::Wcdma => (-121, -25),
        Radio::Lte => (-140, -43),
        Radio::Cdma => (-140, -25),
    };
    let signal = signal_within(cell.signal_strength, min_signal, max_signal);

    Ok((CellKey { radio, mcc, mnc, lac, cid }, signal, timing_advance))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::Position as ApiPosition;

    fn good_position() -> ApiPosition {
        ApiPosition {
            latitude: Some(35.7219),
            longitude: Some(51.3347),
            accuracy: Some(12.0),
            age: Some(0.0),
            speed: Some(5.0),
            source: None,
        }
    }

    fn good_wifi(mac: &str) -> WifiAccessPoint {
        WifiAccessPoint {
            mac_address: Some(mac.into()),
            signal_strength: Some(-70),
            frequency: Some(2_412),
            channel: Some(1),
            age: Some(500.0),
        }
    }

    fn good_cell() -> CellTower {
        CellTower {
            radio_type: Some("lte".into()),
            mobile_country_code: Some(432),
            mobile_network_code: Some(11),
            location_area_code: Some(1_234),
            cell_id: Some(56_789),
            primary_scrambling_code: Some(100),
            signal_strength: Some(-105),
            timing_advance: Some(3),
            age: Some(400.0),
        }
    }

    fn item() -> SubmitItem {
        SubmitItem {
            position: Some(good_position()),
            timestamp: Some(1_700_000_000_000),
            cell_towers: vec![good_cell()],
            wifi_access_points: vec![good_wifi("aabbccddeeff")],
        }
    }

    #[test]
    fn every_drop_reason_has_its_own_label() {
        // Two reasons sharing a label would merge two different problems into
        // one line on a dashboard, which is worse than not counting either.
        let all = [
            DropReason::NoPosition,
            DropReason::PositionOutOfRange,
            DropReason::Worthless,
            DropReason::BadMac,
            DropReason::PlaceholderMac,
            DropReason::UnknownRadio,
            DropReason::MccNotAllowed,
            DropReason::CellFieldOutOfRange,
            DropReason::Duplicate,
        ];
        let mut seen = std::collections::HashSet::new();
        for reason in all {
            let label = reason.as_str();
            assert!(!label.is_empty(), "{reason:?} has no label");
            assert!(seen.insert(label), "{label} is used by more than one reason");
        }
    }

    #[test]
    fn a_good_report_produces_one_observation_per_station() {
        let out = validate_item(&item(), &Validation::default());
        assert_eq!(out.observations.len(), 2);
        assert!(out.dropped.is_empty());
    }

    #[test]
    fn a_report_with_no_position_is_dropped_whole() {
        let mut bad = item();
        bad.position = None;
        let out = validate_item(&bad, &Validation::default());
        assert!(out.is_empty());
        assert_eq!(out.dropped, vec![DropReason::NoPosition]);
    }

    #[test]
    fn a_bad_position_drops_every_station_in_the_report() {
        // Learning even one station from a wrong position is worse than
        // learning nothing.
        for (label, position) in [
            ("latitude off the map", ApiPosition { latitude: Some(91.0), ..good_position() }),
            ("longitude off the map", ApiPosition { longitude: Some(200.0), ..good_position() }),
            ("not a number", ApiPosition { latitude: Some(f64::NAN), ..good_position() }),
        ] {
            let mut bad = item();
            bad.position = Some(position);
            let out = validate_item(&bad, &Validation::default());
            assert!(out.is_empty(), "{label} should have dropped the whole report");
        }
    }

    #[test]
    fn a_bad_station_drops_only_itself() {
        let mut mixed = item();
        mixed.wifi_access_points = vec![good_wifi("aabbccddeeff"), good_wifi("not-a-mac!")];
        let out = validate_item(&mixed, &Validation::default());
        assert_eq!(out.observations.len(), 2, "one good wifi and the cell survive");
        assert_eq!(out.dropped, vec![DropReason::BadMac]);
    }

    #[test]
    fn placeholder_addresses_are_never_stored() {
        for mac in ["000000000000", "ffffffffffff", "01005e901000", "FFFFFFFFFFFF"] {
            let mut bad = item();
            bad.cell_towers.clear();
            bad.wifi_access_points = vec![good_wifi(mac)];
            let out = validate_item(&bad, &Validation::default());
            assert!(out.is_empty(), "{mac} should have been rejected");
            assert_eq!(out.dropped, vec![DropReason::PlaceholderMac]);
        }
    }

    #[test]
    fn addresses_are_matched_regardless_of_case() {
        let mut upper = item();
        upper.cell_towers.clear();
        upper.wifi_access_points = vec![good_wifi("AABBCCDDEEFF")];
        let out = validate_item(&upper, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        assert_eq!(
            out.observations[0].key,
            StationKey::Wifi(WifiKey::parse("aabbccddeeff").unwrap())
        );
    }

    #[test]
    fn a_signal_of_zero_means_not_measured_rather_than_extraordinary() {
        let mut zeroed = item();
        zeroed.cell_towers.clear();
        zeroed.wifi_access_points =
            vec![WifiAccessPoint { signal_strength: Some(0), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&zeroed, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        assert_eq!(out.observations[0].signal, None);
    }

    #[test]
    fn an_implausible_position_accuracy_or_speed_is_forgotten_not_fatal() {
        // The fix is still a fix; only the claim about it is nonsense.
        for position in [
            ApiPosition { accuracy: Some(2_000_000.0), ..good_position() },
            ApiPosition { speed: Some(400.0), ..good_position() },
            ApiPosition { accuracy: Some(-1.0), speed: Some(-1.0), ..good_position() },
        ] {
            let mut report = item();
            report.position = Some(position.clone());
            let out = validate_item(&report, &Validation::default());
            assert_eq!(out.observations.len(), 2, "{position:?}");
            assert!(out.dropped.is_empty());
            let o = &out.observations[0];
            if position.accuracy != Some(12.0) {
                assert_eq!(o.accuracy, None);
            }
            if position.speed != Some(5.0) {
                assert_eq!(o.speed, None);
            }
        }
    }

    #[test]
    fn an_implausible_wifi_signal_is_forgotten_and_the_station_kept() {
        for signal in [50, -101, -5] {
            let mut report = item();
            report.cell_towers.clear();
            report.wifi_access_points = vec![WifiAccessPoint {
                signal_strength: Some(signal),
                ..good_wifi("aabbccddeeff")
            }];
            let out = validate_item(&report, &Validation::default());
            assert_eq!(out.observations.len(), 1, "{signal}");
            assert_eq!(out.observations[0].signal, None);
        }
    }

    #[test]
    fn a_frequency_outside_the_wifi_bands_costs_nothing() {
        let mut report = item();
        report.cell_towers.clear();
        report.wifi_access_points =
            vec![WifiAccessPoint { frequency: Some(900), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        assert!(out.dropped.is_empty());
    }

    #[test]
    fn a_cell_signal_outside_its_radio_range_is_forgotten() {
        // -24 is plausible on LTE's scale but not on WCDMA's.
        let mut report = item();
        report.wifi_access_points.clear();
        report.cell_towers = vec![CellTower {
            radio_type: Some("wcdma".into()),
            signal_strength: Some(-24),
            ..good_cell()
        }];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        assert_eq!(out.observations[0].signal, None);
    }

    #[test]
    fn a_bad_timing_advance_or_scrambling_code_is_forgotten() {
        for cell in [
            CellTower { timing_advance: Some(64), ..good_cell() },
            CellTower { primary_scrambling_code: Some(510), ..good_cell() },
            CellTower { primary_scrambling_code: Some(i64::from(i32::MAX)), ..good_cell() },
        ] {
            let mut report = item();
            report.wifi_access_points.clear();
            report.cell_towers = vec![cell];
            let out = validate_item(&report, &Validation::default());
            assert_eq!(out.observations.len(), 1);
            assert!(out.dropped.is_empty());
        }
    }

    #[test]
    fn an_unknown_radio_drops_the_cell() {
        let mut bad = item();
        bad.wifi_access_points.clear();
        bad.cell_towers =
            vec![CellTower { radio_type: Some("smoke signals".into()), ..good_cell() }];
        assert_eq!(
            validate_item(&bad, &Validation::default()).dropped,
            vec![DropReason::UnknownRadio]
        );
    }

    #[test]
    fn a_gsm_cell_id_beyond_sixteen_bits_is_a_wcdma_cell() {
        // A GSM identifier is 16 bits, so a larger one belongs to WCDMA.
        let mut report = item();
        report.wifi_access_points.clear();
        report.cell_towers = vec![CellTower {
            radio_type: Some("gsm".into()),
            cell_id: Some(70_000),
            signal_strength: Some(-90),
            ..good_cell()
        }];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        let StationKey::Cell(key) = out.observations[0].key else { panic!("not a cell") };
        assert_eq!(key.radio, Radio::Wcdma);
        assert_eq!(out.observations[0].signal, Some(-90));
    }

    #[test]
    fn a_cell_without_a_usable_identity_drops_itself() {
        for cell in [
            CellTower { location_area_code: Some(65_535), ..good_cell() },
            CellTower { cell_id: Some(1 << 28), ..good_cell() },
            CellTower { cell_id: None, ..good_cell() },
            CellTower { mobile_network_code: Some(1_000), ..good_cell() },
        ] {
            let mut report = item();
            report.wifi_access_points.clear();
            report.cell_towers = vec![cell];
            assert_eq!(
                validate_item(&report, &Validation::default()).dropped,
                vec![DropReason::CellFieldOutOfRange]
            );
        }
    }

    #[test]
    fn the_same_cell_id_is_fine_on_lte() {
        let mut ok = item();
        ok.wifi_access_points.clear();
        ok.cell_towers = vec![CellTower { cell_id: Some(70_000), ..good_cell() }];
        assert_eq!(validate_item(&ok, &Validation::default()).observations.len(), 1);
    }

    #[test]
    fn an_allowlist_keeps_foreign_networks_out() {
        let cfg = Validation { mcc_allowlist: vec![432], ..Validation::default() };
        let mut foreign = item();
        foreign.wifi_access_points.clear();
        foreign.cell_towers = vec![CellTower { mobile_country_code: Some(262), ..good_cell() }];
        assert_eq!(validate_item(&foreign, &cfg).dropped, vec![DropReason::MccNotAllowed]);
        // And the allowed country still passes.
        assert_eq!(validate_item(&item(), &cfg).observations.len(), 2);
    }

    #[test]
    fn an_empty_allowlist_accepts_every_country() {
        let mut foreign = item();
        foreign.wifi_access_points.clear();
        foreign.cell_towers = vec![CellTower { mobile_country_code: Some(262), ..good_cell() }];
        assert_eq!(validate_item(&foreign, &Validation::default()).observations.len(), 1);
    }

    #[test]
    fn an_observation_with_no_weight_is_dropped_rather_than_stored() {
        // A stale scan carries no weight, and a batch of weightless
        // observations has nothing to average.
        let mut stale = item();
        stale.cell_towers.clear();
        stale.wifi_access_points =
            vec![WifiAccessPoint { age: Some(60_000.0), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&stale, &Validation::default());
        assert!(out.is_empty());
        assert_eq!(out.dropped, vec![DropReason::Worthless]);
    }

    #[test]
    fn a_scan_is_aged_from_the_fix_not_from_the_request() {
        // Scanned 11 s before the request, fixed 8 s before it: 3 s apart.
        let mut report = item();
        report.cell_towers.clear();
        report.position = Some(ApiPosition { age: Some(8_000.0), ..good_position() });
        report.wifi_access_points =
            vec![WifiAccessPoint { age: Some(11_000.0), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations[0].age_ms, Some(3_000.0));
        assert_eq!(out.observations[0].timestamp, 1_700_000_000_000 - 8_000);
    }

    #[test]
    fn a_scan_with_no_age_of_its_own_is_as_old_as_the_fix() {
        let mut report = item();
        report.cell_towers.clear();
        report.position = Some(ApiPosition { age: Some(4_000.0), ..good_position() });
        report.wifi_access_points =
            vec![WifiAccessPoint { age: None, ..good_wifi("aabbccddeeff") }];
        assert_eq!(
            validate_item(&report, &Validation::default()).observations[0].age_ms,
            Some(-4_000.0)
        );
    }

    #[test]
    fn without_a_fix_age_a_scan_keeps_its_own() {
        for fix_age in [None, Some(0.0)] {
            let mut report = item();
            report.cell_towers.clear();
            report.position = Some(ApiPosition { age: fix_age, ..good_position() });
            report.wifi_access_points =
                vec![WifiAccessPoint { age: Some(1_500.7), ..good_wifi("aabbccddeeff") }];
            let out = validate_item(&report, &Validation::default());
            assert_eq!(out.observations[0].age_ms, Some(1_500.0), "whole milliseconds");
            assert_eq!(out.observations[0].timestamp, 1_700_000_000_000);
        }
    }

    #[test]
    fn a_scan_an_age_beyond_an_hour_from_the_fix_has_no_age() {
        let mut report = item();
        report.cell_towers.clear();
        report.wifi_access_points =
            vec![WifiAccessPoint { age: Some(4_000_000.0), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations[0].age_ms, None, "forgotten, so weighed as a fresh scan");
    }

    #[test]
    fn a_report_time_that_is_not_a_time_falls_back_to_the_clock() {
        let before = Utc::now().timestamp_millis();
        for timestamp in [None, Some(0), Some(5), Some(20_000_000_000_000)] {
            let mut report = item();
            report.timestamp = timestamp;
            let out = validate_item(&report, &Validation::default());
            assert!(out.observations[0].timestamp >= before, "{timestamp:?}");
        }
    }

    #[test]
    fn the_same_station_twice_keeps_the_stronger_sighting() {
        let mut report = item();
        report.cell_towers.clear();
        report.wifi_access_points = vec![
            WifiAccessPoint { signal_strength: Some(-80), ..good_wifi("aabbccddeeff") },
            WifiAccessPoint { signal_strength: Some(-60), ..good_wifi("AABBCCDDEEFF") },
        ];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        assert_eq!(out.observations[0].signal, Some(-60));
        assert_eq!(out.dropped, vec![DropReason::Duplicate]);
    }

    #[test]
    fn between_equal_signals_the_fresher_sighting_wins() {
        let mut report = item();
        report.cell_towers.clear();
        report.wifi_access_points = vec![
            WifiAccessPoint { age: Some(2_000.0), ..good_wifi("aabbccddeeff") },
            WifiAccessPoint { age: Some(900.0), ..good_wifi("aabbccddeeff") },
        ];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations[0].age_ms, Some(900.0));
    }

    #[test]
    fn the_first_comparison_a_sighting_wins_decides_even_if_it_loses_an_earlier_one() {
        // Upstream's quirk, kept on purpose: the earlier sighting has the
        // weaker signal but the fresher scan, and the fresher scan is enough.
        let mut report = item();
        report.cell_towers.clear();
        report.wifi_access_points = vec![
            WifiAccessPoint {
                signal_strength: Some(-80),
                age: Some(500.0),
                ..good_wifi("aabbccddeeff")
            },
            WifiAccessPoint {
                signal_strength: Some(-60),
                age: Some(900.0),
                ..good_wifi("aabbccddeeff")
            },
        ];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations[0].signal, Some(-80));
    }

    #[test]
    fn cells_compare_timing_advance_first() {
        let mut report = item();
        report.wifi_access_points.clear();
        report.cell_towers = vec![
            CellTower { timing_advance: Some(9), signal_strength: Some(-100), ..good_cell() },
            CellTower { timing_advance: Some(2), signal_strength: Some(-100), ..good_cell() },
        ];
        let out = validate_item(&report, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        assert_eq!(out.dropped, vec![DropReason::Duplicate]);
    }

    #[test]
    fn a_weightless_best_sighting_loses_the_station() {
        // Weight is judged after the choice, as upstream judges it.
        let mut report = item();
        report.cell_towers.clear();
        report.wifi_access_points = vec![
            WifiAccessPoint {
                signal_strength: Some(-60),
                age: Some(60_000.0),
                ..good_wifi("aabbccddeeff")
            },
            WifiAccessPoint {
                signal_strength: Some(-80),
                age: Some(1_000.0),
                ..good_wifi("aabbccddeeff")
            },
        ];
        let out = validate_item(&report, &Validation::default());
        assert!(out.is_empty());
        assert_eq!(out.dropped, vec![DropReason::Duplicate, DropReason::Worthless]);
    }

    #[test]
    fn the_report_source_is_read_when_given_and_assumed_otherwise() {
        let mut typed = item();
        typed.position = Some(ApiPosition { source: Some("query".into()), ..good_position() });
        assert_eq!(
            validate_item(&typed, &Validation::default()).observations[0].source,
            ReportSource::Query
        );

        assert_eq!(
            validate_item(&item(), &Validation::default()).observations[0].source,
            ReportSource::Gnss,
            "silence means satellite navigation"
        );
    }

    #[test]
    fn a_report_with_no_stations_yields_nothing_and_complains_about_nothing() {
        let bare = SubmitItem { position: Some(good_position()), ..Default::default() };
        let out = validate_item(&bare, &Validation::default());
        assert!(out.is_empty());
        assert!(out.dropped.is_empty());
    }
}
