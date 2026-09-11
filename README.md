# Ichnos

Ichnos works out where a device is from the WiFi access points and cell towers
it can see, and learns where those stations are from devices that already know
their own position.

It is a ground-up reimplementation of [Mozilla Ichnaea][ichnaea]'s positioning
algorithm in Rust. The maths is deliberately unchanged, so answers can be
compared against the original directly. What differs is everything around the
maths, which is built for a very different traffic shape: many devices reporting
the same stations, over and over, all day.

The request and response bodies are Ichnaea's, so a client only has to change
which URL it posts to. The routes themselves are not: upstream's `/v2/geosubmit`
and `/v1/geolocate` carry a version mismatch inherited from its own history, and
"geosubmit" is not a name anyone would invent. Ichnos posts observations to a
collection and asks a question of an endpoint that computes.

[ichnaea]: https://github.com/mozilla/ichnaea

```
POST /v1/observations   "I am here, and these are the stations I can see"
POST /v1/locate         "These are the stations I can see. Where am I?"
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
| Scheduling | a scheduler hands out work on a timer | workers claim shards themselves and rebalance |
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
curl -X POST 'localhost:8080/v1/observations?key=dev-key' \
  -H 'Content-Type: application/json' -d '{
    "items": [{
      "position": {"latitude": 52.3676, "longitude": 4.9041, "accuracy": 10},
      "wifiAccessPoints": [
        {"macAddress": "aabbccddee01", "signalStrength": -65, "frequency": 2412},
        {"macAddress": "aabbccddee02", "signalStrength": -70, "frequency": 2437}]}]}'

# wait for the fold window, then:
curl -X POST 'localhost:8080/v1/locate?key=dev-key' \
  -H 'Content-Type: application/json' -d '{
    "wifiAccessPoints": [
      {"macAddress": "aabbccddee01", "signalStrength": -65},
      {"macAddress": "aabbccddee02", "signalStrength": -70}]}'

# {"location":{"lat":52.367599999999996,"lng":4.904099999999999},"accuracy":10.0}
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

### `POST /v1/observations`

A device reporting where it is and what it can see. Always answers `200`, even
when every report in the batch was rejected: clients fire this and forget it, so
failing them over data quality would lose the reports that were fine and gain
nothing. What was rejected, and why, appears in the metrics.

### `POST /v1/locate`

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

Workers announce themselves in a registry that forgets anyone who goes quiet, so
each one can see how many siblings it has and work out an equal share. A worker
holding more than its share gives the excess back before claiming, which is what
lets a newly started worker pick work up straight away rather than waiting for an
older one to die. When a worker disappears, its lease lapses, its unacknowledged
batch is redelivered, and the survivors take over on their next tick.

Scale the deployment from one worker to six and the shards redistribute
themselves. There is nothing to configure and no process whose death stops the
pipeline.

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

### Metrics

Everything is published at `GET /metrics` in Prometheus text format. The metrics
divide into three kinds, and the difference is what makes a dashboard useful:
**health** says wake someone, **behaviour** says why the health numbers moved,
and **quality** says whether the answers are getting better or worse.

#### Watch these four

| Metric | Why |
|---|---|
| `ichnos_stream_fill_ratio` | The fullest shard against its cap. Rises before anything breaks. Depth alone means nothing without the cap it is measured against. |
| `ichnos_stream_trimmed_total` | Data loss. Should be flat at zero. |
| `ichnos_locate_coverage` | Share of asked-about networks the service knew. Falls long before any error appears. |
| `ichnos_degraded` | The read path is answering from cache alone. |

#### Health

`ichnos_http_requests_total{route,status}`, `ichnos_http_duration_seconds{route}`,
`ichnos_http_in_flight{route}`, `ichnos_auth_total{route,result}`,
`ichnos_quota_total{route,result}`, `ichnos_worker_errors_total{stage}`,
`ichnos_db_errors_total{op}`, `ichnos_queue_errors_total{op}`,
`ichnos_stream_depth{shard}`, `ichnos_stream_depth_total`,
`ichnos_queue_lag_seconds`, `ichnos_build_info{version}`,
`ichnos_start_time_seconds`.

`ichnos_queue_lag_seconds` is the one to reach for during an incident. Depth
says how much is waiting; lag says how long it has been waiting, which is what
decides whether a backlog is actually hurting.

#### Behaviour

| Metric | What it tells you |
|---|---|
| `ichnos_station_writes_total{type,action}` | The state machine's decisions: `new`, `change`, `confirm`, `replace`, `block`, `new_block`, `nothing`. New stations per day and blocks per day both come from here. |
| `ichnos_elision_total{decision}` | `write` against `skip`. The skip share is the write saving. |
| `ichnos_fold_ratio`, `ichnos_fold_ratio_hist` | Observations per station. Towards one means folding has stopped saving anything. |
| `ichnos_reports_dropped_total{reason}` | Why incoming data was rejected, one label per reason. |
| `ichnos_observation_source_total{source}` | `gnss`, `fused`, `fixed`, `query`. |
| `ichnos_cache_total{result}` | `hit`, `miss`, `negative`, `corrupt`. A falling hit rate is the cheapest thing to fix. |
| `ichnos_lease_events_total{event}` | `claimed`, `renewed`, `released`, `lost`. A rising `lost` means workers are taking shards from each other. |
| `ichnos_leases_held`, `ichnos_leases_target`, `ichnos_workers_live` | A lasting gap between held and target means a rebalance is stuck. |

#### Quality

These move slowly and no alert fires on them. They are the only numbers that
say whether the service is good at its job.

| Metric | What it tells you |
|---|---|
| `ichnos_locate_accuracy_meters{source}` | How precise the answers actually were. |
| `ichnos_locate_coverage` | Known networks over asked-about networks. |
| `ichnos_locate_no_answer_total{reason}` | `nothing_submitted`, `none_known`, `no_cluster`, `store_unavailable`. `none_known` is a coverage problem; `no_cluster` is not. |
| `ichnos_locate_disagreement_meters` | How far the runner-up candidate sat from the winner. One device should not see two sets of stations that disagree by kilometres; when it does, either the stations are wrong or the report is not honest. |
| `ichnos_locate_cluster_size{source}`, `ichnos_locate_score{source}` | How much evidence stood behind each answer. |
| `ichnos_locate_stale_networks_total` | Stations used in an answer that were last confirmed before today. |
| `ichnos_station_moved_meters` | How far stations move when they move. A settled station moves centimetres; a new tail at kilometres means something changed or something is lying. |
| `ichnos_observation_speed_mps` | Reporter speed. The lowest bucket is a stationary vehicle, which for a ride-hailing fleet is the same spot counted over and over. |
| `ichnos_observation_weight`, `ichnos_observation_accuracy_meters`, `ichnos_observation_age_ms` | The shape of the incoming data. |

The four per-observation histograms sit on the hottest path there is. Set
`metrics.observation_detail = false` to switch them off once the weights stop
changing.

#### Latency

`ichnos_db_operation_seconds{op}`, `ichnos_cache_operation_seconds{op}`,
`ichnos_queue_publish_seconds`, `ichnos_shard_drain_seconds`,
`ichnos_db_batch_seconds`, `ichnos_shard_batch_observations`.

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
