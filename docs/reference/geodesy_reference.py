# Direct transcription of ichnaea/geocalclib/geocalc.pyx, used only to produce
# golden values for the Rust port.
from math import asin, atan, atan2, cos, pi, sin, sqrt, tan
import json

EARTH_RADIUS = 6371.009
EARTH_MAJOR_RADIUS = 6378.137
EARTH_MINOR_RADIUS = 6356.752314245
EARTH_FLATTENING = 0.0033528106647474805
VINCENTY_CUTOFF = 0.00000000001
VINCENTY_ITERATIONS = 25

def deg2rad(d): return d * pi / 180.0

def haversine_distance(lat1, lon1, lat2, lon2):
    dLat = deg2rad(lat2 - lat1) / 2.0
    dLon = deg2rad(lon2 - lon1) / 2.0
    lat1 = deg2rad(lat1); lat2 = deg2rad(lat2)
    a = sin(dLat) ** 2 + cos(lat1) * cos(lat2) * sin(dLon) ** 2
    c = asin(min(1, sqrt(a)))
    return 1000.0 * 2.0 * EARTH_RADIUS * c

def vincenty_distance(lat1, lon1, lat2, lon2):
    lat1 = deg2rad(lat1); lon1 = deg2rad(lon1)
    lat2 = deg2rad(lat2); lon2 = deg2rad(lon2)
    delta_lon = lon2 - lon1
    reduced_lat1 = atan((1.0 - EARTH_FLATTENING) * tan(lat1))
    reduced_lat2 = atan((1.0 - EARTH_FLATTENING) * tan(lat2))
    sin_reduced1 = sin(reduced_lat1); cos_reduced1 = cos(reduced_lat1)
    sin_reduced2 = sin(reduced_lat2); cos_reduced2 = cos(reduced_lat2)
    lambda_lon = delta_lon
    lambda_prime = 2.0 * pi
    i = 0
    sin_sigma = cos_sigma = sigma = cos_sq_alpha = cos2_sigma_m = 0.0
    while (abs(lambda_lon - lambda_prime) > VINCENTY_CUTOFF and i <= VINCENTY_ITERATIONS):
        i += 1
        sin_lambda_lon = sin(lambda_lon); cos_lambda_lon = cos(lambda_lon)
        sin_sigma = sqrt((cos_reduced2 * sin_lambda_lon) ** 2 +
                         (cos_reduced1 * sin_reduced2 -
                          sin_reduced1 * cos_reduced2 * cos_lambda_lon) ** 2)
        if sin_sigma == 0:
            return 0.0
        cos_sigma = sin_reduced1 * sin_reduced2 + cos_reduced1 * cos_reduced2 * cos_lambda_lon
        sigma = atan2(sin_sigma, cos_sigma)
        sin_alpha = cos_reduced1 * cos_reduced2 * sin_lambda_lon / sin_sigma
        cos_sq_alpha = 1.0 - sin_alpha ** 2
        cos2_sigma_m = (cos_sigma - (2.0 * sin_reduced1 * sin_reduced2 / cos_sq_alpha)) if cos_sq_alpha != 0.0 else 0.0
        C = EARTH_FLATTENING / 16.0 * cos_sq_alpha * (4.0 + EARTH_FLATTENING * (4.0 - 3.0 * cos_sq_alpha))
        lambda_prime = lambda_lon
        lambda_lon = (delta_lon + (1.0 - C) * EARTH_FLATTENING * sin_alpha *
                      (sigma + C * sin_sigma * (cos2_sigma_m + C * cos_sigma * (-1.0 + 2.0 * cos2_sigma_m ** 2))))
    if i > VINCENTY_ITERATIONS:
        raise ValueError("Vincenty formula failed to converge!")
    u_sq = cos_sq_alpha * (EARTH_MAJOR_RADIUS ** 2 - EARTH_MINOR_RADIUS ** 2) / EARTH_MINOR_RADIUS ** 2
    A = 1.0 + u_sq / 16384.0 * (4096.0 + u_sq * (-768.0 + u_sq * (320.0 - 175.0 * u_sq)))
    B = u_sq / 1024.0 * (256.0 + u_sq * (-128.0 + u_sq * (74.0 - 47.0 * u_sq)))
    delta_sigma = (B * sin_sigma * (cos2_sigma_m + B / 4.0 *
                   (cos_sigma * (-1.0 + 2.0 * cos2_sigma_m ** 2) -
                    B / 6.0 * cos2_sigma_m * (-3.0 + 4.0 * sin_sigma ** 2) * (-3.0 + 4.0 * cos2_sigma_m ** 2))))
    return 1000.0 * EARTH_MINOR_RADIUS * A * (sigma - delta_sigma)

def distance(lat1, lon1, lat2, lon2):
    try:
        return vincenty_distance(lat1, lon1, lat2, lon2)
    except ValueError:
        return haversine_distance(lat1, lon1, lat2, lon2)

def max_distance(lat, lon, points):
    return max(distance(lat, lon, p[0], p[1]) for p in points)

def circle_radius(lat, lon, max_lat, max_lon, min_lat, min_lon):
    pts = [(min_lat, min_lon), (min_lat, max_lon), (max_lat, min_lon), (max_lat, max_lon)]
    return round(max_distance(lat, lon, pts))

