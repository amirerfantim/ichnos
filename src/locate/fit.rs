//! Turning a cluster of known stations into one position and an accuracy.

use crate::geo::distance;
use crate::locate::network::Network;

/// Weighted mean of the cluster's positions.
///
/// Used directly for cells, and as the starting point for the WiFi fit. It is
/// a reasonable answer on its own; the fit refines it.
pub fn weighted_mean(networks: &[Network]) -> (f64, f64) {
    let mut sum_w = 0.0;
    let mut sum_lat = 0.0;
    let mut sum_lon = 0.0;
    for net in networks {
        let w = net.weight();
        sum_w += w;
        sum_lat += net.lat * w;
        sum_lon += net.lon * w;
    }
    if sum_w <= 0.0 || !sum_w.is_finite() {
        // Every weight vanished, so fall back to an unweighted mean rather than
        // dividing by zero and answering with NaN.
        let n = networks.len().max(1) as f64;
        return (
            networks.iter().map(|x| x.lat).sum::<f64>() / n,
            networks.iter().map(|x| x.lon).sum::<f64>() / n,
        );
    }
    (sum_lat / sum_w, sum_lon / sum_w)
}

/// Fit a position by minimising the weighted distances to every station.
///
/// This is the same idea as upstream's least-squares call: find the point that
/// sits best among the stations, letting fresh, strongly heard ones pull
/// hardest. Levenberg-Marquardt over two parameters, starting from the weighted
/// mean, with a numerical Jacobian because the distance function is Vincenty
/// and has no tidy derivative.
///
/// If the iteration fails to improve on the starting point, the starting point
/// is returned. An answer that is merely good beats no answer.
pub fn fit_position(networks: &[Network]) -> (f64, f64) {
    let (mut lat, mut lon) = weighted_mean(networks);
    if networks.len() < 2 {
        return (lat, lon);
    }

    // About a centimetre, small enough to differentiate over and large enough
    // to stay clear of floating-point noise.
    const STEP: f64 = 1e-7;
    const MAX_ITERATIONS: usize = 50;

    let cost = |lat: f64, lon: f64| -> f64 {
        networks.iter().map(|n| residual(n, lat, lon).powi(2)).sum::<f64>()
    };

    let mut lambda = 1e-3;
    let mut current = cost(lat, lon);

    for _ in 0..MAX_ITERATIONS {
        // Numerical Jacobian: one column per parameter.
        let mut jtj = [[0.0f64; 2]; 2];
        let mut jtr = [0.0f64; 2];
        for net in networks {
            let r = residual(net, lat, lon);
            let d_lat = (residual(net, lat + STEP, lon) - r) / STEP;
            let d_lon = (residual(net, lat, lon + STEP) - r) / STEP;
            jtj[0][0] += d_lat * d_lat;
            jtj[0][1] += d_lat * d_lon;
            jtj[1][0] += d_lat * d_lon;
            jtj[1][1] += d_lon * d_lon;
            jtr[0] += d_lat * r;
            jtr[1] += d_lon * r;
        }

        // Damped normal equations, solved directly because there are only two
        // unknowns.
        let a = jtj[0][0] * (1.0 + lambda);
        let b = jtj[0][1];
        let c = jtj[1][0];
        let d = jtj[1][1] * (1.0 + lambda);
        let det = a * d - b * c;
        if det.abs() < f64::EPSILON {
            break;
        }
        let delta_lat = -(d * jtr[0] - b * jtr[1]) / det;
        let delta_lon = -(a * jtr[1] - c * jtr[0]) / det;
        if !delta_lat.is_finite() || !delta_lon.is_finite() {
            break;
        }

        let (try_lat, try_lon) = (lat + delta_lat, lon + delta_lon);
        let trial = cost(try_lat, try_lon);
        if trial < current {
            let improvement = current - trial;
            lat = try_lat;
            lon = try_lon;
            current = trial;
            lambda = (lambda / 10.0).max(1e-12);
            if improvement < 1e-12 {
                break; // converged
            }
        } else {
            lambda *= 10.0;
            if lambda > 1e12 {
                break; // no step helps
            }
        }
    }

    (lat, lon)
}

/// One station's contribution to the objective: its distance from the candidate
/// point, scaled by how much the station should matter.
fn residual(net: &Network, lat: f64, lon: f64) -> f64 {
    distance(net.lat, net.lon, lat, lon) * net.residual_weight()
}

/// How far off the answer might be, in metres.
///
/// The 95th percentile of the distances from the fitted point to the stations
/// it was fitted from. This measures how much the evidence disagrees with
/// itself, which is honest: it is not a claim about satellite quality, and it
/// does not pretend to precision the cluster does not support.
pub fn accuracy_from_spread(
    lat: f64,
    lon: f64,
    networks: &[Network],
    min_accuracy: f64,
    max_accuracy: f64,
) -> f64 {
    let mut distances: Vec<f64> =
        networks.iter().map(|n| distance(lat, lon, n.lat, n.lon)).collect();
    let spread = percentile(&mut distances, 95.0);
    spread.max(min_accuracy).min(max_accuracy)
}

/// Linear-interpolated percentile, matching numpy's default.
///
/// Sorts in place. Returns 0.0 for an empty slice, which callers treat as
/// "no spread" and then clamp to the configured floor.
pub fn percentile(values: &mut [f64], q: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if values.len() == 1 {
        return values[0];
    }
    let index = (q / 100.0) * (values.len() - 1) as f64;
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    if lower == upper {
        return values[lower];
    }
    let frac = index - lower as f64;
    values[lower] + (values[upper] - values[lower]) * frac
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::{StationKey, WifiKey};
    use approx::assert_relative_eq;

    fn net(id: u64, lat: f64, lon: f64, score: f64, signal: f64) -> Network {
        Network {
            key: StationKey::Wifi(WifiKey(id)),
            lat,
            lon,
            radius: 50.0,
            age_ms: 1_000.0,
            signal,
            score,
            seen_today: true,
        }
    }

    #[test]
    fn percentile_matches_numpy_linear_interpolation() {
        assert_relative_eq!(percentile(&mut [1.0, 2.0, 3.0, 4.0], 95.0), 3.85, epsilon = 1e-12);
        assert_relative_eq!(percentile(&mut [1.0, 2.0, 3.0, 4.0], 50.0), 2.5, epsilon = 1e-12);
        assert_relative_eq!(percentile(&mut [1.0, 2.0, 3.0, 4.0], 0.0), 1.0, epsilon = 1e-12);
        assert_relative_eq!(percentile(&mut [1.0, 2.0, 3.0, 4.0], 100.0), 4.0, epsilon = 1e-12);
        assert_relative_eq!(percentile(&mut [10.0], 95.0), 10.0, epsilon = 1e-12);
        assert_relative_eq!(percentile(&mut [], 95.0), 0.0, epsilon = 1e-12);
    }

    #[test]
    fn percentile_sorts_before_measuring() {
        assert_relative_eq!(percentile(&mut [4.0, 1.0, 3.0, 2.0], 50.0), 2.5, epsilon = 1e-12);
    }

    #[test]
    fn the_weighted_mean_lands_between_the_stations() {
        let nets = vec![net(1, 35.70, 51.30, 1.0, -70.0), net(2, 35.72, 51.32, 1.0, -70.0)];
        let (lat, lon) = weighted_mean(&nets);
        assert_relative_eq!(lat, 35.71, epsilon = 1e-9);
        assert_relative_eq!(lon, 51.31, epsilon = 1e-9);
    }

    #[test]
    fn the_weighted_mean_leans_towards_the_heavier_station() {
        let nets = vec![net(1, 35.70, 51.30, 10.0, -70.0), net(2, 35.72, 51.30, 1.0, -70.0)];
        let (lat, _) = weighted_mean(&nets);
        assert!(lat < 35.71, "expected a pull towards the first station, got {lat}");
    }

    #[test]
    fn a_stronger_signal_pulls_the_mean_towards_it() {
        let strong = vec![net(1, 35.70, 51.30, 1.0, -40.0), net(2, 35.72, 51.30, 1.0, -90.0)];
        let (lat, _) = weighted_mean(&strong);
        assert!(lat < 35.705, "the strongly heard station should dominate, got {lat}");
    }

    #[test]
    fn zero_weights_fall_back_to_an_unweighted_mean_rather_than_nan() {
        let mut nets = vec![net(1, 35.70, 51.30, 0.0, -70.0), net(2, 35.72, 51.30, 0.0, -70.0)];
        for n in &mut nets {
            n.score = 0.0;
        }
        let (lat, lon) = weighted_mean(&nets);
        assert!(lat.is_finite() && lon.is_finite());
        assert_relative_eq!(lat, 35.71, epsilon = 1e-9);
    }

    #[test]
    fn the_fit_sits_inside_the_cluster() {
        let nets = vec![
            net(1, 35.7200, 51.3300, 1.0, -70.0),
            net(2, 35.7220, 51.3320, 1.0, -70.0),
            net(3, 35.7210, 51.3340, 1.0, -70.0),
        ];
        let (lat, lon) = fit_position(&nets);
        assert!((35.7195..=35.7225).contains(&lat), "lat {lat} escaped the cluster");
        assert!((51.3295..=51.3345).contains(&lon), "lon {lon} escaped the cluster");
    }

    #[test]
    fn the_fit_never_makes_the_objective_worse_than_the_starting_point() {
        let nets = vec![
            net(1, 35.7200, 51.3300, 3.0, -60.0),
            net(2, 35.7260, 51.3380, 1.0, -85.0),
            net(3, 35.7180, 51.3410, 2.0, -75.0),
            net(4, 35.7230, 51.3290, 5.0, -55.0),
        ];
        let cost = |lat: f64, lon: f64| -> f64 {
            nets.iter().map(|n| residual(n, lat, lon).powi(2)).sum::<f64>()
        };
        let (start_lat, start_lon) = weighted_mean(&nets);
        let (lat, lon) = fit_position(&nets);
        assert!(
            cost(lat, lon) <= cost(start_lat, start_lon) + 1e-12,
            "the fit should not be worse than where it started"
        );
    }

    #[test]
    fn a_single_station_fits_to_itself() {
        let nets = vec![net(1, 35.7219, 51.3347, 1.0, -70.0)];
        let (lat, lon) = fit_position(&nets);
        assert_relative_eq!(lat, 35.7219, epsilon = 1e-9);
        assert_relative_eq!(lon, 51.3347, epsilon = 1e-9);
    }

    #[test]
    fn identical_stations_fit_to_the_shared_point() {
        let nets = vec![net(1, 35.7219, 51.3347, 1.0, -70.0), net(2, 35.7219, 51.3347, 1.0, -70.0)];
        let (lat, lon) = fit_position(&nets);
        assert_relative_eq!(lat, 35.7219, epsilon = 1e-7);
        assert_relative_eq!(lon, 51.3347, epsilon = 1e-7);
    }

    #[test]
    fn the_fit_always_returns_a_usable_number() {
        // Degenerate inputs must not produce NaN: the API would serve it.
        let nets = vec![
            net(1, 0.0, 0.0, 0.0, -70.0),
            net(2, 0.0, 0.0, 0.0, -70.0),
            net(3, 0.0, 0.0, 0.0, -70.0),
        ];
        let (lat, lon) = fit_position(&nets);
        assert!(lat.is_finite() && lon.is_finite(), "got ({lat}, {lon})");
    }

    #[test]
    fn accuracy_grows_with_disagreement() {
        let tight =
            vec![net(1, 35.7200, 51.3300, 1.0, -70.0), net(2, 35.7202, 51.3302, 1.0, -70.0)];
        let loose =
            vec![net(1, 35.7200, 51.3300, 1.0, -70.0), net(2, 35.7400, 51.3500, 1.0, -70.0)];
        let a = accuracy_from_spread(35.7201, 51.3301, &tight, 10.0, 500.0);
        let b = accuracy_from_spread(35.7300, 51.3400, &loose, 10.0, 500.0);
        assert!(b > a, "a scattered cluster should report worse accuracy");
    }

    #[test]
    fn accuracy_is_clamped_at_both_ends() {
        let nets = vec![net(1, 35.7200, 51.3300, 1.0, -70.0), net(2, 35.7200, 51.3300, 1.0, -70.0)];
        // No spread at all still reports the floor, never zero.
        assert_relative_eq!(
            accuracy_from_spread(35.7200, 51.3300, &nets, 10.0, 500.0),
            10.0,
            epsilon = 1e-9
        );

        let far = vec![net(1, 35.0, 51.0, 1.0, -70.0), net(2, 36.0, 52.0, 1.0, -70.0)];
        assert_relative_eq!(
            accuracy_from_spread(35.5, 51.5, &far, 10.0, 500.0),
            500.0,
            epsilon = 1e-9
        );
    }

    // ---- reference values from the real scipy ----------------------------
    //
    // Produced by running scipy.optimize.leastsq over Ichnaea's own objective
    // function, on the cluster shapes below. Regenerate with the script in
    // the repository's docs if the objective ever changes.

    fn n2(lat: f64, lon: f64, score: f64, signal: f64, age_ms: f64) -> Network {
        Network {
            key: StationKey::Wifi(WifiKey(1)),
            lat,
            lon,
            radius: 50.0,
            age_ms,
            signal,
            score,
            seen_today: true,
        }
    }

    struct FitCase {
        name: &'static str,
        nets: Vec<Network>,
        want_initial: (f64, f64),
        want_fit: (f64, f64),
        want_acc95: f64,
    }

    fn fit_cases() -> Vec<FitCase> {
        vec![
            FitCase {
                name: "tight_triangle",
                nets: vec![
                    n2(35.7200, 51.3300, 1.0, -70.0, 1000.0),
                    n2(35.7220, 51.3320, 1.0, -70.0, 1000.0),
                    n2(35.7210, 51.3340, 1.0, -70.0, 1000.0),
                ],
                want_initial: (35.7210000, 51.3320000),
                want_fit: (35.7210000, 51.3320000),
                want_acc95: 209.1375,
            },
            FitCase {
                name: "weighted_pair",
                nets: vec![
                    n2(35.7000, 51.3000, 10.0, -70.0, 1000.0),
                    n2(35.7200, 51.3000, 1.0, -70.0, 1000.0),
                ],
                want_initial: (35.7018182, 51.3000000),
                want_fit: (35.7018182, 51.3000000),
                want_acc95: 1926.5588,
            },
            FitCase {
                name: "signal_spread",
                nets: vec![
                    n2(35.7200, 51.3300, 3.0, -60.0, 1000.0),
                    n2(35.7260, 51.3380, 1.0, -85.0, 1000.0),
                    n2(35.7180, 51.3410, 2.0, -75.0, 1000.0),
                    n2(35.7230, 51.3290, 5.0, -55.0, 1000.0),
                ],
                want_initial: (35.7217039, 51.3311293),
                want_fit: (35.7216003, 51.3316509),
                want_acc95: 908.2510,
            },
            FitCase {
                name: "colinear_five",
                nets: (0..5)
                    .map(|i| {
                        n2(
                            35.7200 + f64::from(i) * 0.0008,
                            51.3300,
                            1.0 + f64::from(i),
                            -60.0 - 5.0 * f64::from(i),
                            1000.0,
                        )
                    })
                    .collect(),
                want_initial: (35.7219487, 51.3300000),
                want_fit: (35.7219487, 51.3300000),
                want_acc95: 200.7386,
            },
            FitCase {
                name: "identical_two",
                nets: vec![
                    n2(35.7219, 51.3347, 1.0, -70.0, 1000.0),
                    n2(35.7219, 51.3347, 1.0, -70.0, 1000.0),
                ],
                want_initial: (35.7219000, 51.3347000),
                want_fit: (35.7219000, 51.3347000),
                want_acc95: 0.0,
            },
            FitCase {
                name: "aged_mix",
                nets: vec![
                    n2(35.7200, 51.3300, 2.0, -65.0, 1000.0),
                    n2(35.7250, 51.3350, 2.0, -65.0, 16000.0),
                    n2(35.7210, 51.3390, 4.0, -80.0, 4000.0),
                ],
                want_initial: (35.7211811, 51.3344467),
                want_fit: (35.7206271, 51.3319264),
                want_acc95: 633.1432,
            },
        ]
    }

    #[test]
    fn weighted_mean_matches_scipy_reference_starting_points() {
        for case in fit_cases() {
            let (lat, lon) = weighted_mean(&case.nets);
            assert_relative_eq!(lat, case.want_initial.0, epsilon = 1e-7);
            assert_relative_eq!(lon, case.want_initial.1, epsilon = 1e-7);
        }
    }

    #[test]
    fn fit_matches_scipy_on_every_realistic_cluster() {
        // Agreement is to centimetres on every shape the clusterer can
        // actually produce. `weighted_pair` is excluded because its two
        // stations are 2.2 km apart: no cluster at a 500 m threshold could
        // ever contain them, so the case exists only to exercise the optimiser
        // under a scale mismatch. It is covered by the test below instead.
        for case in fit_cases() {
            if case.name == "weighted_pair" {
                continue;
            }
            let (lat, lon) = fit_position(&case.nets);
            let off = crate::geo::distance(lat, lon, case.want_fit.0, case.want_fit.1);
            assert!(
                off < 0.1,
                "{}: fit ({lat:.7}, {lon:.7}) is {off:.3} m from scipy's ({:.7}, {:.7})",
                case.name,
                case.want_fit.0,
                case.want_fit.1
            );
        }
    }

    #[test]
    fn the_fit_is_never_worse_than_scipys_even_where_it_differs() {
        // The objective is the thing being minimised, so a lower value is a
        // better answer regardless of which implementation produced it.
        //
        // On `weighted_pair` scipy's MINPACK stops after eight evaluations at a
        // point whose cost is 67% above the minimum, because the parameters are
        // degrees near 35 and 51 while the residuals are near 0.3, and its
        // relative-step test fires on that mismatch. This implementation runs
        // to convergence and lands within 0.05% of the true minimum.
        for case in fit_cases() {
            let cost = |la: f64, lo: f64| -> f64 {
                case.nets.iter().map(|n| residual(n, la, lo).powi(2)).sum::<f64>()
            };
            let (lat, lon) = fit_position(&case.nets);
            let ours = cost(lat, lon);
            let theirs = cost(case.want_fit.0, case.want_fit.1);
            assert!(
                ours <= theirs + 1e-9,
                "{}: our cost {ours:.10} is worse than scipy's {theirs:.10}",
                case.name
            );
        }
    }

    #[test]
    fn accuracy_matches_scipy_reference_percentiles() {
        for case in fit_cases() {
            if case.name == "weighted_pair" {
                continue; // fits to a different point, so a different spread
            }
            let (lat, lon) = fit_position(&case.nets);
            // Unclamped, so the comparison is against the raw percentile.
            let got = accuracy_from_spread(lat, lon, &case.nets, 0.0, f64::MAX);
            assert_relative_eq!(got, case.want_acc95, max_relative = 1e-3);
        }
    }
}
