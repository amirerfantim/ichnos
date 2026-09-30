//! Geodesic maths, ported line for line from Ichnaea's `geocalclib/geocalc.pyx`.
//!
//! Fidelity matters more than elegance here: every clustering decision and
//! every station radius is a distance, so a different distance function would
//! give different answers from the algorithm Ichnos replaces. `distance` is
//! therefore Vincenty with a Haversine fallback, exactly as upstream, and not
//! the more obvious plain Haversine.

/// Earth radius in km, used by the Haversine formula.
const EARTH_RADIUS: f64 = 6371.009;

/// WGS-84 ellipsoid constants, used by Vincenty.
const EARTH_MAJOR_RADIUS: f64 = 6378.137;
const EARTH_MINOR_RADIUS: f64 = 6_356.752_314_245;
const EARTH_FLATTENING: f64 = 0.003_352_810_664_747_480_5; // 1.0 / 298.257223563

const VINCENTY_CUTOFF: f64 = 0.000_000_000_01; // 10e-12
const VINCENTY_ITERATIONS: i32 = 25;

#[inline]
fn deg2rad(degrees: f64) -> f64 {
    degrees * std::f64::consts::PI / 180.0
}

/// Distance in metres between two points.
///
/// Vincenty first; Haversine when Vincenty fails to converge. Upstream signals
/// non-convergence with an exception, which is a `None` here.
pub fn distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    vincenty_distance(lat1, lon1, lat2, lon2)
        .unwrap_or_else(|| haversine_distance(lat1, lon1, lat2, lon2))
}

/// Great-circle distance in metres on a sphere.
pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let d_lat = deg2rad(lat2 - lat1) / 2.0;
    let d_lon = deg2rad(lon2 - lon1) / 2.0;

    let lat1 = deg2rad(lat1);
    let lat2 = deg2rad(lat2);

    let a = d_lat.sin().powi(2) + lat1.cos() * lat2.cos() * d_lon.sin().powi(2);
    let c = a.sqrt().min(1.0).asin();
    1000.0 * 2.0 * EARTH_RADIUS * c
}

/// Distance in metres on the WGS-84 ellipsoid.
///
/// `None` means the iteration did not converge, which is upstream's
/// `ValueError`. Nearly antipodal points are the usual cause.
pub fn vincenty_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> Option<f64> {
    let lat1 = deg2rad(lat1);
    let lon1 = deg2rad(lon1);
    let lat2 = deg2rad(lat2);
    let lon2 = deg2rad(lon2);

    let delta_lon = lon2 - lon1;

    let reduced_lat1 = ((1.0 - EARTH_FLATTENING) * lat1.tan()).atan();
    let reduced_lat2 = ((1.0 - EARTH_FLATTENING) * lat2.tan()).atan();

    let sin_reduced1 = reduced_lat1.sin();
    let cos_reduced1 = reduced_lat1.cos();
    let sin_reduced2 = reduced_lat2.sin();
    let cos_reduced2 = reduced_lat2.cos();

    let mut lambda_lon = delta_lon;
    let mut lambda_prime = 2.0 * std::f64::consts::PI;

    // Carried out of the loop because the closing formula needs the last values.
    let mut sin_sigma = 0.0f64;
    let mut cos_sigma = 0.0f64;
    let mut sigma = 0.0f64;
    let mut cos_sq_alpha = 0.0f64;
    let mut cos2_sigma_m = 0.0f64;

    let mut i: i32 = 0;
    while (lambda_lon - lambda_prime).abs() > VINCENTY_CUTOFF && i <= VINCENTY_ITERATIONS {
        i += 1;

        let sin_lambda_lon = lambda_lon.sin();
        let cos_lambda_lon = lambda_lon.cos();

        sin_sigma = ((cos_reduced2 * sin_lambda_lon).powi(2)
            + (cos_reduced1 * sin_reduced2 - sin_reduced1 * cos_reduced2 * cos_lambda_lon).powi(2))
        .sqrt();

        if sin_sigma == 0.0 {
            return Some(0.0); // coincident points
        }

        cos_sigma = sin_reduced1 * sin_reduced2 + cos_reduced1 * cos_reduced2 * cos_lambda_lon;
        sigma = sin_sigma.atan2(cos_sigma);

        let sin_alpha = cos_reduced1 * cos_reduced2 * sin_lambda_lon / sin_sigma;
        cos_sq_alpha = 1.0 - sin_alpha.powi(2);

        cos2_sigma_m = if cos_sq_alpha != 0.0 {
            cos_sigma - (2.0 * sin_reduced1 * sin_reduced2 / cos_sq_alpha)
        } else {
            0.0 // equatorial line
        };

        let c = EARTH_FLATTENING / 16.0
            * cos_sq_alpha
            * (4.0 + EARTH_FLATTENING * (4.0 - 3.0 * cos_sq_alpha));

        lambda_prime = lambda_lon;
        lambda_lon = delta_lon
            + (1.0 - c)
                * EARTH_FLATTENING
                * sin_alpha
                * (sigma
                    + c * sin_sigma
                        * (cos2_sigma_m + c * cos_sigma * (-1.0 + 2.0 * cos2_sigma_m.powi(2))));
    }

    if i > VINCENTY_ITERATIONS {
        return None; // failed to converge
    }

    let u_sq = cos_sq_alpha * (EARTH_MAJOR_RADIUS.powi(2) - EARTH_MINOR_RADIUS.powi(2))
        / EARTH_MINOR_RADIUS.powi(2);

    let a = 1.0 + u_sq / 16384.0 * (4096.0 + u_sq * (-768.0 + u_sq * (320.0 - 175.0 * u_sq)));
    let b = u_sq / 1024.0 * (256.0 + u_sq * (-128.0 + u_sq * (74.0 - 47.0 * u_sq)));

    let delta_sigma = b
        * sin_sigma
        * (cos2_sigma_m
            + b / 4.0
                * (cos_sigma * (-1.0 + 2.0 * cos2_sigma_m.powi(2))
                    - b / 6.0
                        * cos2_sigma_m
                        * (-3.0 + 4.0 * sin_sigma.powi(2))
                        * (-3.0 + 4.0 * cos2_sigma_m.powi(2))));

    Some(1000.0 * EARTH_MINOR_RADIUS * a * (sigma - delta_sigma))
}

/// Greatest distance in metres from a point to any point in `points`.
pub fn max_distance(lat: f64, lon: f64, points: &[(f64, f64)]) -> f64 {
    let mut result = 0.0f64;
    for &(p_lat, p_lon) in points {
        let d = distance(lat, lon, p_lat, p_lon);
        if d > result {
            result = d;
        }
    }
    result
}

/// Radius in metres of a circle at (lat, lon) covering the whole bounding box.
///
/// Rounding is half-to-even to match Python's `round`, so a value landing
/// exactly on .5 agrees with upstream rather than differing by a metre.
pub fn circle_radius(
    lat: f64,
    lon: f64,
    max_lat: f64,
    max_lon: f64,
    min_lat: f64,
    min_lon: f64,
) -> i32 {
    let points = [(min_lat, min_lon), (min_lat, max_lon), (max_lat, min_lon), (max_lat, max_lon)];
    round_half_to_even(max_distance(lat, lon, &points)) as i32
}

/// Python's `round()`: ties go to the even neighbour, not away from zero.
pub fn round_half_to_even(value: f64) -> f64 {
    let rounded = value.round();
    if (value - value.trunc()).abs() == 0.5 && rounded % 2.0 != 0.0 {
        rounded - value.signum()
    } else {
        rounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    /// Golden values produced by transcribing `geocalc.pyx` into Python and
    /// running it. If a change here breaks these, the port has drifted from
    /// upstream and every position the service computes will drift with it.
    const GOLDEN: &[(f64, f64, f64, f64, f64, f64)] = &[
        // lat1, lon1, lat2, lon2, distance (vincenty), haversine
        (35.7219, 51.3347, 35.7000, 51.3900, 5_562.930_115_973_376, 5_555.086_558_405_209),
        (35.7219, 51.3347, 35.7300, 51.3400, 1_018.648_575_262_783_4, 1_019.863_969_984_350_7),
        (35.7219, 51.3347, 35.7219, 51.3347, 0.0, 0.0),
        (35.6892, 51.3890, 32.6539, 51.6660, 337_659.106_530_146_9, 338_470.595_001_159_4),
        (0.0, 0.0, 0.0, 1.0, 111_319.490_779_203_23, 111_195.083_724_191_41),
        (51.5074, -0.1278, 40.7128, -74.0060, 5_585_233.578_943_139, 5_570_230.048_518_308),
        (-33.8688, 151.2093, 35.6762, 139.6503, 7_792_174.827_117_02, 7_825_829.671_667_241),
    ];

    #[test]
    fn distance_matches_upstream_golden_values() {
        for &(lat1, lon1, lat2, lon2, want, _) in GOLDEN {
            let got = distance(lat1, lon1, lat2, lon2);
            assert_relative_eq!(got, want, max_relative = 1e-12);
        }
    }

    #[test]
    fn haversine_matches_upstream_golden_values() {
        for &(lat1, lon1, lat2, lon2, _, want) in GOLDEN {
            let got = haversine_distance(lat1, lon1, lat2, lon2);
            assert_relative_eq!(got, want, max_relative = 1e-12);
        }
    }

    #[test]
    fn vincenty_converges_for_every_golden_case() {
        for &(lat1, lon1, lat2, lon2, _, _) in GOLDEN {
            assert!(
                vincenty_distance(lat1, lon1, lat2, lon2).is_some(),
                "expected convergence for ({lat1}, {lon1}) -> ({lat2}, {lon2})"
            );
        }
    }

    #[test]
    fn coincident_points_are_zero() {
        assert_eq!(distance(35.7219, 51.3347, 35.7219, 51.3347), 0.0);
    }

    #[test]
    fn near_antipodal_points_fall_back_to_haversine() {
        // Vincenty is known not to converge here, which is the whole reason the
        // fallback exists. Upstream raises; we return None and fall through.
        assert!(vincenty_distance(0.0, 0.0, 0.5, 179.7).is_none());
        let d = distance(0.0, 0.0, 0.5, 179.7);
        let h = haversine_distance(0.0, 0.0, 0.5, 179.7);
        assert_relative_eq!(d, h, max_relative = 1e-12);
    }

    #[test]
    fn distance_is_symmetric() {
        for &(lat1, lon1, lat2, lon2, _, _) in GOLDEN {
            let a = distance(lat1, lon1, lat2, lon2);
            let b = distance(lat2, lon2, lat1, lon1);
            assert_relative_eq!(a, b, max_relative = 1e-12);
        }
    }

    #[test]
    fn circle_radius_matches_upstream_golden_values() {
        assert_eq!(circle_radius(35.7219, 51.3347, 35.7264, 51.3402, 35.7174, 51.3292), 705);
        assert_eq!(circle_radius(35.7219, 51.3347, 35.7219, 51.3347, 35.7219, 51.3347), 0);
    }

    #[test]
    fn max_distance_picks_the_furthest_point() {
        let points = [(35.7219, 51.3347), (35.7000, 51.3900), (35.7300, 51.3400)];
        let got = max_distance(35.7219, 51.3347, &points);
        assert_relative_eq!(got, 5_562.930_115_973_376, max_relative = 1e-12);
    }

    #[test]
    fn max_distance_of_no_points_is_zero() {
        assert_eq!(max_distance(35.7219, 51.3347, &[]), 0.0);
    }

    #[test]
    fn python_style_rounding_breaks_ties_to_even() {
        assert_eq!(round_half_to_even(0.5), 0.0);
        assert_eq!(round_half_to_even(1.5), 2.0);
        assert_eq!(round_half_to_even(2.5), 2.0);
        assert_eq!(round_half_to_even(3.5), 4.0);
        assert_eq!(round_half_to_even(-0.5), 0.0);
        assert_eq!(round_half_to_even(-1.5), -2.0);
        assert_eq!(round_half_to_even(-2.5), -2.0);
        assert_eq!(round_half_to_even(2.4), 2.0);
        assert_eq!(round_half_to_even(2.6), 3.0);
        assert_eq!(round_half_to_even(0.0), 0.0);
    }
}
