# Changelog

## 0.1.0

First release. Locates devices from WiFi access points and cell towers, and
learns station positions from devices that already know their own.

### The algorithm

Ported from Mozilla Ichnaea and deliberately unchanged, so answers can be
compared against the original directly:

- Vincenty distance on the WGS-84 ellipsoid, with a Haversine fallback where it
  does not converge. Agrees with the original to 1e-12.
- Observation weighting by accuracy, age, speed and signal strength.
- Station scoring by record age, how long the station has been observed, and how
  many samples it has.
- Complete-linkage clustering. Checked against `scipy.cluster.hierarchy`, and
  reproduces its groupings exactly rather than merely its cluster counts.
- Weighted least-squares position fitting, then accuracy as the 95th percentile
  of the distances to the stations behind the answer.
- Blocking of stations that report from somewhere else, with the same two
  recovery rules.

Two deliberate divergences, both recorded at the point in the code where they
happen:

1. When a cluster exceeds the size cap, the best-scoring members are kept.
   Upstream sorts ascending and keeps the worst, contradicting its own comment.
2. The fit runs to convergence. MINPACK's relative-step test stops early on
   clusters with a large scale mismatch, in one measured case 67% above the
   minimum. A test asserts our objective value is never the worse of the two,
   and on every cluster shape the clusterer can produce the two agree to within
   seven centimetres.

### Around the algorithm

- Observations are folded per station and written in batches, so repeated
  sightings of one station cost one write rather than one each.
- One worker owns a shard at a time, which removes row locks from the write
  path entirely.
- Unchanged stations are not rewritten, beyond one write a day to keep the row
  alive.
- Workers claim shards themselves; there is no scheduler to be a single point of
  failure.
- The queue is capped. At the cap it drops its oldest entries and counts them,
  rather than growing until memory runs out.
- Reads are cached, including the misses, which is what stops unknown stations
  costing a database read on every request.
- Rows expire on their own, so there is no cleanup job to fall behind.
- `ichnos replay` reprocesses recorded traffic offline and deterministically,
  which makes a change to the maths measurable before it is deployed.

### Interface

- `POST /v2/geosubmit` and `POST /v1/geolocate`, wire-compatible with Ichnaea.
- `?explain=1` returns the working behind an answer, and costs nothing when not
  asked for.
- `GET /debug/station/{kind}/{key}` behind an admin key.
- Prometheus metrics, liveness and readiness.
- API keys in the database, with enforced daily allowances.
- Every threshold is configuration; nothing that shapes behaviour is a constant
  in the code.
