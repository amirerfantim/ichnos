"""Reference values from the real scipy, mirroring Ichnaea's aggregate_mac_position
and cluster_networks. Output is pasted into the Rust tests as golden data."""
import json, itertools, math
import numpy
from scipy.optimize import leastsq
from scipy.cluster import hierarchy
from geodesy_reference import distance  # the exact geocalc transcription

def fit(nets):
    """nets: list of dicts with lat, lon, age, signal, score."""
    def func(point, points):
        return numpy.array([
            distance(p["lat"], p["lon"], point[0], point[1])
            * min(math.sqrt(2000.0 / p["age"]), 1.0)
            / math.pow(p["signal"], 2)
            for p in points])
    points = numpy.array([(n["lat"], n["lon"]) for n in nets], dtype=numpy.double)
    weights = numpy.array([
        n["score"] * min(math.sqrt(2000.0 / n["age"]), 1.0) / math.pow(n["signal"], 2)
        for n in nets], dtype=numpy.double)
    initial = numpy.average(points, axis=0, weights=weights)
    (lat, lon), cov_x, info, mesg, ier = leastsq(func, initial, args=nets, full_output=True)
    if ier not in (1, 2, 3, 4):
        lat, lon = initial
    distances = numpy.array([distance(lat, lon, n["lat"], n["lon"]) for n in nets])
    acc95 = float(numpy.percentile(distances, 95))
    return {"initial": [float(initial[0]), float(initial[1])],
            "fit": [float(lat), float(lon)], "acc95": acc95, "ier": int(ier)}

def clusters(pts, max_distance):
    n = len(pts)
    if n < 2:
        return [[0]] if n == 1 else []
    if n == 2:
        d = distance(pts[0][0], pts[0][1], pts[1][0], pts[1][1])
        return [[0, 1]] if d <= max_distance else []
    dm = numpy.zeros(n * (n - 1) // 2, dtype=numpy.double)
    for i, (a, b) in enumerate(itertools.combinations(pts, 2)):
        dm[i] = distance(a[0], a[1], b[0], b[1])
    link = hierarchy.linkage(dm, method="complete")
    assign = hierarchy.fcluster(link, max_distance, criterion="distance", depth=2)
    groups = {}
    for idx, c in enumerate(assign):
        groups.setdefault(int(c), []).append(idx)
    return sorted([sorted(v) for v in groups.values()])

def net(lat, lon, score=1.0, signal=-70.0, age=1000.0):
    return {"lat": lat, "lon": lon, "score": score, "signal": signal, "age": age}

cases = {
  "tight_triangle": [net(35.7200, 51.3300), net(35.7220, 51.3320), net(35.7210, 51.3340)],
  "weighted_pair":  [net(35.7000, 51.3000, score=10.0), net(35.7200, 51.3000, score=1.0)],
  "signal_spread":  [net(35.7200, 51.3300, 3.0, -60.0), net(35.7260, 51.3380, 1.0, -85.0),
                     net(35.7180, 51.3410, 2.0, -75.0), net(35.7230, 51.3290, 5.0, -55.0)],
  "colinear_five":  [net(35.7200 + i * 0.0008, 51.3300, 1.0 + i, -60.0 - 5 * i) for i in range(5)],
  "identical_two":  [net(35.7219, 51.3347), net(35.7219, 51.3347)],
  "aged_mix":       [net(35.7200, 51.3300, 2.0, -65.0, 1000.0), net(35.7250, 51.3350, 2.0, -65.0, 16000.0),
                     net(35.7210, 51.3390, 4.0, -80.0, 4000.0)],
}
out = {"fits": {k: fit(v) for k, v in cases.items()}}

cluster_cases = {
  "one_group":  ([(35.7219,51.3347),(35.7222,51.3350),(35.7216,51.3344)], 500.0),
  "split_off":  ([(35.7219,51.3347),(35.7222,51.3350),(35.7700,51.3900)], 500.0),
  "two_groups": ([(35.7219,51.3347),(35.7222,51.3350),(35.7700,51.3900),(35.7703,51.3903)], 500.0),
  "chain":      ([(35.7000,51.3000),(35.7035,51.3000),(35.7070,51.3000),(35.7105,51.3000)], 500.0),
  "grid_nine":  ([(35.7200 + r*0.002, 51.3300 + c*0.002) for r in range(3) for c in range(3)], 300.0),
}
out["clusters"] = {k: {"max_distance": d, "groups": clusters(p, d)} for k, (p, d) in cluster_cases.items()}
print(json.dumps(out, indent=1))
