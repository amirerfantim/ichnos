# Changelog

## Unreleased

### Fixed

- **A worker's metrics were never scraped.** `ichnos worker` opened no port,
  so everything measured on the write path (leases held and lost, fold
  ratio, elision, write and lease errors, queue lag) existed only in the
  worker's memory. A worker now serves `/health` and `/metrics` on
  `server.bind`, and the Kubernetes manifest scrapes it.
- **Finished queue entries were never removed, so the queue metrics and the
  cap were both wrong.** Acknowledging does not delete an entry from a Redis
  stream, and nothing else did, so every stream filled to its cap and stayed
  there: `ichnos_stream_depth` and `ichnos_stream_fill_ratio` counted finished
  work and sat near full, the cap trimmed work that was already done, and 32
  shards could hold millions of dead entries. And `ichnos_stream_trimmed_total`
  was never incremented by the Redis queue at all. Acknowledging now also
  deletes, in one script, and counts the deletions; what the cap trims is
  what was added, less what is left, less what was deleted, which Redis 7
  reports exactly. Depth now means entries not yet done, in memory as in
  Redis. The queue Redis must be version 7 or later.
- **The WiFi position fit often stopped before the minimum.** It searched with
  fifty steps of Levenberg-Marquardt, and on a flat objective (two or three
  stations, or a line of them) it crawled: over 2,607 random clusters it ended
  more than a metre from the minimum 365 times, up to 209 m, and more often
  than Ichnaea's MINPACK (83 times). Where it stopped depended on the last bit
  of the distance function, so the same request gave answers up to 19 m apart
  on arm64 and on amd64. The objective is a weighted sum of squared
  distances, whose minimum is a weighted average, so it is now computed
  directly: within 6 mm of the exact minimum, never worse than MINPACK, and
  the same on every processor. `colinear_five` joins `weighted_pair` as a
  reference case where MINPACK stops short (55% above the minimum).
- Documentation implied answers would be compared with Ichnaea in production.
  Ichnos replaces Ichnaea; nothing at run time depends on it.

- **A batch that was taken and never acknowledged stayed pending for ever.**
  The documentation said a dead worker's batch is redelivered. It was not: the
  Redis queue only ever read entries never delivered to anyone
  (`XREADGROUP … >`), and nothing took over another consumer's pending
  entries. A worker that died mid-batch, lost its shard, or failed to write
  left those observations stuck in Redis, uncounted and never learned. A read
  now first takes entries that have waited longer than
  `ingest.redeliver_after_secs` (`XAUTOCLAIM`), then new ones. The in-memory
  queue behaved correctly only because tests put entries back by hand; it now
  redelivers on its own, the same way Redis does, and the manual step is gone.
- An entry that could not be decoded was skipped but never acknowledged. With
  redelivery it would have come back on every read; it is now acknowledged
  and counted as `ichnos_queue_errors_total{op="decode"}`.
- `tests/metrics.rs` could fail at random: tests running the depth poller in
  parallel overwrote each other's process-wide gauges. They now take turns.

- **Five declared metrics were never written by any code path.** The metrics
  module named `ichnos_stream_depth` and `ichnos_fold_ratio` as two of the
  three an operator watches, and nothing set either of them. Also dead:
  `ichnos_stream_trimmed_total`, which counts lost data,
  `ichnos_station_writes_total`, `ichnos_db_batch_seconds` and
  `ichnos_cache_total`. The whole worker and storage layer reported nothing.
  Nothing failed; the dashboard was simply empty, and would have stayed empty
  until someone needed it during an incident. `tests/metrics.rs` now drives the
  assembled service and checks every declared family is actually written.
- Dropping a station allocated a string for its metric label, on the hottest
  path in the service. Labels are now static.

### Added

- `ingest.redeliver_after_secs` (default 30, at least `lease.ttl_secs`) and
  `ichnos_queue_redelivered_total`. A steady rise in the counter means workers
  are dying or failing to write while the queue itself looks healthy.
- CI runs the Redis tests against a real Redis instead of skipping them.

- Queue depth is sampled by a background poller, in every process that can
  reach the queue rather than only in a worker: a deployment whose workers have
  all died is exactly when someone needs depth to still be reported.
- `ichnos_stream_fill_ratio`, depth against the cap, taken from the fullest
  shard. Depth alone means nothing without the cap it is measured against, and
  one stuck shard loses data while thirty-one others look idle.
- `ichnos_queue_lag_seconds`: how long the oldest observation in a batch
  waited. Depth says how much is waiting; lag says whether it is hurting.
- Answer-quality metrics, none of which existed: `ichnos_locate_accuracy_meters`,
  `ichnos_locate_coverage`, `ichnos_locate_no_answer_total{reason}`,
  `ichnos_locate_cluster_size`, `ichnos_locate_score`,
  `ichnos_locate_candidates`, `ichnos_locate_stale_networks_total`.
- `ichnos_locate_disagreement_meters`: how far the runner-up candidate sat from
  the winner. One device should not see two sets of stations that disagree by
  kilometres; when it does, either the stations are wrong or the report is not
  honest.
- `ichnos_station_moved_meters`: how far a station's position moved in one
  update. A settled station moves centimetres.
- Incoming-data shape: `ichnos_observation_weight`,
  `ichnos_observation_accuracy_meters`, `ichnos_observation_speed_mps`,
  `ichnos_observation_age_ms`, `ichnos_observation_source_total`. The speed
  histogram's lowest bucket is the stationary vehicle, which for a ride-hailing
  fleet means the same spot counted over and over.
- Per-operation latency and errors for storage, cache and queue:
  `ichnos_db_operation_seconds{op}`, `ichnos_db_errors_total{op}`,
  `ichnos_db_rows_total{op}`, `ichnos_cache_operation_seconds{op}`,
  `ichnos_queue_publish_seconds`, `ichnos_queue_errors_total{op}`.
- Worker ownership: `ichnos_workers_live`, `ichnos_leases_target`,
  `ichnos_lease_events_total{event}` where `lost` is the takeover signal, and
  `ichnos_worker_errors_total{stage}`.
- Request-level detail: `ichnos_http_in_flight{route}`,
  `ichnos_auth_total{route,result}`, `ichnos_quota_total{route,result}`,
  `ichnos_submit_items`, `ichnos_submit_stations`.
- `ichnos_build_info{version}` and `ichnos_start_time_seconds`.
- A `[metrics]` config section: `poll_enabled`, `poll_secs`, and
  `observation_detail` to switch off the four per-observation histograms once
  the weights stop changing.

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

- `POST /v1/observations` and `POST /v1/locate`. The bodies are Ichnaea's, so a
  client only changes which URL it posts to.
- `?explain=1` returns the working behind an answer, and costs nothing when not
  asked for.
- `GET /debug/station/{kind}/{key}` behind an admin key.
- Prometheus metrics, liveness and readiness.
- API keys in the database, with enforced daily allowances.
- Every threshold is configuration; nothing that shapes behaviour is a constant
  in the code.
