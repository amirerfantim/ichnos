# Changelog

## Unreleased

### Added

- **Redis is Redis Cluster.** Every Redis connection is a cluster
  connection, so the cache, the quota counters, the queue and the leases can
  live on a cluster of any size; a small deployment runs a cluster of one
  node, which holds every slot. The queue streams are named `{obs:N}` and
  their counters `{obs:N}:acked`: the braces keep a stream and its counter on
  one slot, which the acknowledgement script needs. A cluster refuses a
  command or a pipeline whose keys are in different slots, so the cache sends
  one `GET`, `SET` or `DEL` per key, all at once, instead of `MGET`, one
  pipeline or a multi-key `DEL`, and a publish sends one pipeline per shard.
  A Redis URL may name only database 0. A command a node did not answer makes
  the connection again, with a fresh map of the cluster, at most once per
  `redis.clusterReloadMs` (1 s): the library reads the map again only when a
  node redirects it, which a dead primary cannot. Upgrading drops what the
  old streams held.

- **Learning from locate answers, as upstream does.** A key with
  `store_sample_locate` above zero (`ichnos keys add --store-sample-locate`)
  feeds that share of its answered locates back as reports: the networks the
  request named, at the answered position, marked as a guess (`query`).
  Upstream's rules throughout: an answer is skipped when every network it
  named was used and already seen today (never when it named a cell); fewer
  than two access points are not learned; the report passes the same
  validation as a submitted one. `api.locateSamplePercent` scales every key,
  like upstream's `global_locate_sample_rate`. Keys default to zero, as
  upstream. Counted by `ichnos_locate_learned_total{outcome}`. An older
  keyspace needs `schema/002_store_sample_locate.cql`.

### Changed

- **One YAML config file.** Every setting lives in one file: `--config`,
  else `/etc/ichnos/config.yaml`, else `config/config.yaml`, else the
  defaults. Keys are camelCase (`ingest.foldWindowSecs`), as the Go sibling
  writes them. A file only needs the keys it changes, down to one field of a
  nested section; an unknown key stops the start. The `ICHNOS__SECTION__KEY`
  environment overrides, `ICHNOS_LOG`, `ICHNOS_LOG_FORMAT` and `POD_NAME` are
  gone: use `log.level`, `log.format` and `worker.identity` (empty uses the
  host name). `show-config` prints YAML and names the file it read.
  `config/example.toml` is now `config/config.yaml`, which lists every key.
- **The fixed limits are settings too**, with the old values as defaults:
  the cache and quota timeouts, budget and pause (`cache.dialTimeoutMs`,
  `readTimeoutMs`, `budgetMs`, `pauseMs`); the queue's timeouts, retries,
  publish pause, consumer pruning, reclaim calls and acknowledgement size
  (`queue.*`); how long a quota counter lives (`api.quotaKeepHours`); the
  import batch (`import.batch`); and the range of report times accepted
  (`validation.timestampMs`). The names and defaults are the Go sibling's.

### Fixed

- **A slow database could cost a worker its leases.** A drain pass renews the
  leases between shards when a renewal is due, skips a shard it lost, and
  ends at the first shard that finds the database or the queue unavailable.
- **Shards were shared unevenly.** With 32 shards and 10 workers, two workers
  sat idle. Each worker now takes `shards / workers`, and the first
  `shards % workers` by name take one more.
- **A cache call could outlast a 500 ms caller** while the client reconnected
  and retried. Every cache call now has a 150 ms limit in total.
- **Stopped workers stayed in each stream's consumer list for ever.** A read
  removes consumers idle for an hour that hold no entries, at most once
  every ten minutes per shard, in one atomic step.
- **A submit whose caller gave up lost its observations uncounted.** They are
  now counted in `ichnos_observations_total{outcome="dropped"}`.
- **`ichnos_queue_lag_seconds` measured the scan's age, not the wait in the
  queue.** It now counts from the stream entry's time; the scan's age has its
  own histogram, `ichnos_report_age_seconds`.
- The log line when the cache server is unreachable at start says what
  happens: an in-process cache with no expiry, until restart.
- **A daily-quota count could hold a request for seconds while the cache
  server was down**: the counter kept the client's default timeouts and had
  no pause. It now has the cache's limits: 150 ms in total per count, then
  counting is skipped for a second; the request is allowed, as before.

- **A blocked station was still learned from.** The read path hid blocked
  stations, but the write path never checked: every report of a blocked
  station was folded in, so a station blocked for moving recovered a position
  at once, and each further disagreeing report counted another block against
  it (821 stations blocked twice and 595 three or more times in a million real
  reports, where Ichnaea blocks each once). Now, as upstream, a blocked
  station's reports are ignored until the block ends, and counted as
  `ichnos_station_writes_total{action="ignored_blocked"}`.

- **A batch mixing fixes and guesses averaged them together.** Upstream
  judges a station on its measured fixes when a batch has any, and on guesses
  only when it has none; and it records any measured fix, fused or fixed
  included, as `gnss`. Both now hold.
- **Reports were validated by stricter rules than Ichnaea's, and scans were
  aged from the wrong moment.** Measured against Ichnaea's own code over a
  million real reports: 80% of observations got a different weight, and
  199,646 that Ichnaea keeps were dropped. Now, as upstream:
  - a scan's age is measured from the position fix (`scan age - fix age`),
    and the report's time is moved back to the fix. Ichnos used the scan's
    age from the request, so a scan taken 3 s from an 8 s old fix counted
    as 11 s old, and 165,695 scans looked too old to count at all;
  - a field out of range is forgotten and its station kept: WiFi signal and
    frequency, cell signal, timing advance and scrambling code, and the
    fix's accuracy and speed. Only a missing or impossible identity, or
    position, still drops data;
  - a GSM cell id above 16 bits is taken as a WCDMA cell;
  - a station named twice in one report keeps one sighting, chosen by
    upstream's rule (timing advance, then signal, then age), and weighed
    after the choice.

  The labels `accuracy_out_of_range`, `speed_out_of_range`,
  `age_out_of_range`, `frequency_out_of_range` and `signal_out_of_range` of
  `ichnos_reports_dropped_total` are gone; `duplicate` is new.
- **The same locate request could get a different answer each time.** The
  stations of a request were kept in a hash map, whose order is random per
  request, and summed in that order, so positions, accuracies and scores
  moved in their last bits between identical requests, and `explain.used`
  came out in a random order. They are now taken in the order the client
  named them, whatever order the store answers in.
- **`explain` could mark two clusters chosen.** A candidate was marked when
  it sat at the winner's position, so a WiFi cluster and a cell on the same
  point were both marked. Now only the winner is.
- **The API key cache could grow without end.** An entry was dropped only
  when its own key was asked for again, so a flood of made-up keys, each
  asked once, stayed in memory for ever. Past 10,000 entries, expired ones
  are now swept on insert, which bounds the cache by what arrives within
  `api.keyCacheSecs`.
- **`server.requestTimeoutMs` and `api.maxBodyBytes` did nothing.** No
  request was ever timed out, and bodies up to axum's 2 MiB default were read
  whatever the setting said. A client request (`/v1/observations`,
  `/v1/locate`) that runs past the timeout is now answered with a 503 and the
  usual error body, "Request timed out"; the operator routes are not bounded.
  A body over the limit is answered as any unreadable body is: 400, "Invalid
  request body".
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
  `ingest.redeliverAfterSecs` (`XAUTOCLAIM`), then new ones. The in-memory
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

- `ingest.redeliverAfterSecs` (default 30, at least `lease.ttlSecs`) and
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
