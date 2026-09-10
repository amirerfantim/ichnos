# Ichnos

Ichnos works out where a device is from the WiFi access points and cell towers
it can see, and learns where those stations are from devices that already know
their own position.

It is a ground-up reimplementation of [Mozilla Ichnaea][ichnaea]'s positioning
algorithm in Rust, wire-compatible with its API. The maths is deliberately
unchanged, so answers can be compared against the original directly. What
differs is everything around the maths, which is built for a very different
traffic shape: many devices reporting the same stations, over and over, all day.

[ichnaea]: https://github.com/mozilla/ichnaea

```
POST /v2/geosubmit   "I am here, and these are the stations I can see"
POST /v1/geolocate   "These are the stations I can see. Where am I?"
```

---

## Why it exists

Ichnaea was designed for volunteers walking around with phones. Its write path
reads a station, blends in one observation, and writes it back, under a row
lock, once per sighting.

That shape does not survive a fleet of vehicles reporting every twenty seconds.
The same access point is seen thousands of times an hour, and each sighting
costs a read, a lock and a write for a change measured in centimetres.

Ichnos keeps the algorithm and rebuilds the machine around it:

| | Ichnaea | Ichnos |
|---|---|---|
| Write path | one read-modify-write per sighting, under a row lock | observations are folded per station, then written in one batch |
| Concurrency | `SELECT ... FOR UPDATE` | one worker owns a shard, so no locks at all |
| Unchanged stations | rewritten anyway | not written at all |
| Scheduling | a scheduler hands out work on a timer | workers claim shards themselves |
| Queue | unbounded | capped, drops oldest, counts what it dropped |
| Reads | straight to the database | cached, including the misses |
| Old data | never deleted | expires on its own |
| Testing a change | deploy and wait weeks | replay recorded traffic in seconds |

---

## Quick start

Everything in memory, no dependencies, one command:

```bash
cargo run -- serve --in-memory --with-worker
```

Then teach it two access points and ask where they are:

```bash
curl -X POST 'localhost:8080/v2/geosubmit?key=dev-key' \
  -H 'Content-Type: application/json' -d '{
    "items": [{
      "position": {"latitude": 52.3676, "longitude": 4.9041, "accuracy": 10},
      "wifiAccessPoints": [
        {"macAddress": "aabbccddee01", "signalStrength": -65, "frequency": 2412},
        {"macAddress": "aabbccddee02", "signalStrength": -70, "frequency": 2437}]}]}'

# wait for the fold window, then:
curl -X POST 'localhost:8080/v1/geolocate?key=dev-key' \
  -H 'Content-Type: application/json' -d '{
    "wifiAccessPoints": [
      {"macAddress": "aabbccddee01", "signalStrength": -65},
      {"macAddress": "aabbccddee02", "signalStrength": -70}]}'

# {"location":{"lat":52.3676,"lng":4.9041},"accuracy":10.0}
```

With real storage:

```bash
docker compose up -d                      # ScyllaDB and two Redis instances
cqlsh -f schema/001_init.cql              # once
cargo run -- keys add my-key --locate --submit
cargo run -- serve                        # the API
cargo run -- worker                       # the writer, in another terminal
```

---

## The API

### `POST /v2/geosubmit`

A device reporting where it is and what it can see. Always answers `200`, even
when every report in the batch was rejected: clients fire this and forget it, so
failing them over data quality would lose the reports that were fine and gain
nothing. What was rejected, and why, appears in the metrics.

### `POST /v1/geolocate`

A device asking where it is. Answers `200` with `{"location": {"lat", "lng"},
"accuracy"}`, or `404` with the same error body Ichnaea returns.

Add `?explain=1` and the answer also carries the working: which stations were
known, how they clustered, which cluster won and why. It costs nothing unless
asked for.

If the database is unreachable, the answer comes from cache alone and carries
`X-Ichnos-Degraded: 1`, so a caller can tell "we do not know" from "we could not
look".

### `GET /debug/station/{wifi|cell}/{key}`

Everything known about one station: position, radius, sample count, weight,
block history, score. Needs a key with the admin permission. It exists because
the alternative is opening a database shell every time somebody asks why an
answer was wrong.

### `GET /health`, `GET /ready`, `GET /metrics`

Liveness, readiness, and Prometheus.

---

## How it works

### Writing

```
report → validate → one message per station → hash(key) → shard stream
                                                              ↓
     ScyllaDB ← batched write ← elide ← fold ← worker that owns that shard
```

Every sighting of a station is routed to the same shard by hashing its key, and
exactly one worker owns a shard at a time. That single fact is what removes the
locks: no two workers can ever touch the same row.

A worker drains a window of observations, groups them by station, and turns each
group into one update. Twenty sightings of an access point become one row
written once.

**Folding** blends new evidence into the stored position, weighted by how
accurate, how fresh and how strongly heard each sighting was. The stored
position's weight is capped, which keeps a well-observed station steady without
making it impossible to correct if it genuinely moves.

**Blocking** handles stations that report from somewhere else entirely. Their
position is wiped rather than ignored, because keeping it would let the station
be relearned from evidence already judged untrustworthy. It recovers on a
schedule that lengthens with each repeat offence.

**Elision** is the last step: if the position moved less than a threshold and
the station has already been written today, nothing is written at all.

### Reading

```
request → cache (hits and misses) → database, only for what missed
                    ↓
        cluster → weight → fit → accuracy → answer
```

Access points are clustered by complete linkage, so every member of a cluster
agrees with every other member, not merely with its nearest neighbour. A cluster
needs at least two known stations before it may answer, which is what stops
anyone locating a single access point by pairing it with a made-up one.

The position is then fitted by weighted least squares, and the accuracy is the
95th percentile of the distances from that point to the stations behind it: a
measure of how much the evidence disagrees with itself, not a claim about
satellite quality.

Cells are averaged rather than fitted, and a lone cell reports its own recorded
radius. Fitting cell positions would imply a precision that cell coverage does
not have.

### Caching the misses

Most access points a device reports have never been seen before. Without a
negative cache entry, each of those costs a database read on every single
request, forever. Remembering "we do not know this one" for a minute removes
most of that traffic, and learning a station clears its negative entry at once.

---

## Configuration

Every threshold the service uses is configuration. Nothing that shapes behaviour
is a constant in the code, and each default records which Ichnaea constant it
came from, so any deviation is deliberate and traceable.

```bash
ichnos --config config.toml serve
ICHNOS__INGEST__SHARDS=64 ichnos serve     # environment overrides the file
ichnos show-config                          # what the settings actually resolved to
```

Some of the ones worth knowing:

| Key | Default | What it does |
|---|---|---|
| `locate.wifi.min_in_cluster` | `2` | Known stations needed before an answer is given |
| `locate.wifi.max_cluster_meters` | `500` | How far apart stations may be and still agree |
| `ingest.shards` | `32` | Queue shards; one worker owns whole shards |
| `ingest.fold_window_secs` | `45` | How long a worker collects before writing |
| `ingest.stream_maxlen` | `200000` | Cap per shard, beyond which the oldest are dropped |
| `elision.min_move_m` | `1.0` | Below this movement, an unchanged station is not rewritten |
| `block.temporary_days` | `7` | How long a station stays blocked after moving |
| `block.budget_days` | `10` | One forgiven block per this many days of known age |
| `cache.miss_ttl_secs` | `60` | How long an unknown station stays cached as unknown |
| `cql.ttl_days` | `180` | Row lifetime, refreshed by every write |

Run `ichnos show-config` for the full list with defaults.

> **On the block schedule.** Ichnaea's widely observed "45 day" recovery is not a
> constant anywhere. It falls out of `block_count >= round(age_in_days / 30)`:
> a station blocked once stays blocked until that rounds up to two, which lands
> at 45 days. To make blocks shorter, change `block.budget_days`, not
> `block.temporary_days`.

---

## Operations

### Modes

```bash
ichnos serve                  # answer requests
ichnos worker                 # drain the queue and write
ichnos replay traffic.ndjson  # run recorded traffic through the pipeline
ichnos import stations.ndjson # load a station table
ichnos keys add KEY --locate --submit
ichnos show-config
```

`serve` and `worker` are separate processes so the read path and the write path
can be scaled apart, and so a busy worker cannot slow down an answer. For
development, `serve --with-worker` runs both.

### Metrics worth watching

- **`ichnos_stream_depth`** — the early warning. A worker falling behind shows
  up as a growing queue long before it shows up as a bad answer.
- **`ichnos_fold_ratio`** — observations per station. If it falls towards one,
  folding has stopped saving anything.
- **`ichnos_stream_trimmed_total`** — queue entries dropped at the cap. Some is
  fine; a rising rate means the writers cannot keep up.
- **`ichnos_degraded`** — the read path is answering from cache alone.
- **`ichnos_cache_total{result}`** — a falling hit rate means more database
  traffic, and it is the cheapest thing to fix.

### When things break

| What fails | What happens |
|---|---|
| Cache Redis | Reads get slower. Nothing fails. |
| Queue Redis | Learning pauses; answering is unaffected. Dropped observations are counted. |
| One database node | Nothing visible, at a replication factor above one. |
| The whole database | Answers come from cache with `X-Ichnos-Degraded`. Workers back off and retry. |
| A worker dies | Its lease expires, another takes the shard, and its unacknowledged batch is redelivered. |
| A shard fills up | Oldest entries drop and are counted. A device re-reports the same stations within seconds, so the loss is history, not coverage. |
| A bad deploy | Roll back, then rebuild by replaying recorded traffic. |

---

## Replay

Replay runs validation, folding, the state machine and elision exactly as
production does, against an in-memory store. A week of traffic takes seconds and
the result is deterministic, which is what makes a change to the maths
measurable *before* it is deployed rather than weeks after.

```bash
ichnos replay traffic.ndjson --output stations.ndjson
# reports 4820 (0 rejected) -> 31204 observations -> 6910 station updates
# written 6910, skipped 0, blocked 12
# fold ratio 4.52x, elision 0%, stations held 6910
```

Run it twice with different settings and compare the two station tables.

---

## Development

```bash
cargo test              # unit and integration tests
cargo clippy --all-targets
cargo run -- serve --in-memory --with-worker
```

The maths is checked against the original rather than against a description of
it. `docs/reference/` holds the generators: one transcribes Ichnaea's geodesy
into Python, the other runs the real `scipy` over Ichnaea's own objective
function and clustering. The golden values in the tests come from those, and can
be regenerated.

Two deliberate differences from upstream are recorded in the code where they
happen:

1. **Cluster trimming.** When a cluster exceeds the size cap, Ichnaea sorts
   ascending and keeps the head, which keeps the *least* trustworthy members and
   contradicts its own comment. Ichnos keeps the best.
2. **Fit convergence.** On clusters with a large scale mismatch, MINPACK's
   relative-step test stops early, in one measured case 67% above the minimum.
   Ichnos runs to convergence. A test asserts our objective value is never the
   worse of the two, and on every cluster shape the clusterer can actually
   produce the two agree to within seven centimetres.

### Layout

```
src/
  geo.rs          distance on the ellipsoid, ported exactly
  config.rs       every threshold the service has
  model/          stations, observations, keys, weights
  locate/         scoring, clustering, fitting, choosing
  ingest/         validation, queue, leases, folding, elision
  store/          the trait, CQL, in-memory, cache
  api/            HTTP surface and authentication
  worker.rs       claim shards, drain, fold, write
  replay.rs       offline reprocessing
```

---

## Licence

Apache-2.0. See [LICENSE](LICENSE).

The positioning algorithm is derived from [Mozilla Ichnaea][ichnaea], which is
licensed under the Mozilla Public License 2.0. No Ichnaea source is included
here; the algorithm was reimplemented and checked against it.
