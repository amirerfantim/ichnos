-- Ichnos schema.
--
-- Every station is its own partition. Keys are effectively random, so tokens
-- spread evenly and there is no such thing as a hot partition here. Reads and
-- writes are both single-partition, which is the shape this database is fastest
-- at.
--
-- Replication is set for a single-node development cluster. For production,
-- replace it with NetworkTopologyStrategy and a replication factor of 3.

CREATE KEYSPACE IF NOT EXISTS ichnos
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- WiFi access points.
--
-- default_time_to_live is what bounds the dataset: every write refreshes it, so
-- a station still being seen lives on and a forgotten one expires by itself.
-- There is no cleanup job to fall behind.
CREATE TABLE IF NOT EXISTS ichnos.wifi (
  mac         bigint PRIMARY KEY,  -- the 12 hex characters packed into an int64
  lat         double,
  lon         double,
  radius      int,
  max_lat     double,              -- bounding box of every observation so far;
  min_lat     double,              -- the radius is derived from it and has to
  max_lon     double,              -- be recomputed as the box grows
  min_lon     double,
  h3          bigint,              -- spatial index cell, mirrored into station_by_h3
  samples     int,
  weight      double,
  source      tinyint,             -- gnss = 3, query = 9; drives the state machine
  created     timestamp,
  modified    timestamp,
  last_seen   date,
  block_first date,
  block_last  date,
  block_count smallint
) WITH default_time_to_live = 15552000
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND speculative_retry = '99PERCENTILE';

-- Cell towers. Same columns, a composite partition key.
CREATE TABLE IF NOT EXISTS ichnos.cell (
  radio       tinyint,
  mcc         int,
  mnc         int,
  lac         bigint,
  cid         bigint,
  lat         double,
  lon         double,
  radius      int,
  max_lat     double,
  min_lat     double,
  max_lon     double,
  min_lon     double,
  h3          bigint,
  samples     int,
  weight      double,
  source      tinyint,
  created     timestamp,
  modified    timestamp,
  last_seen   date,
  block_first date,
  block_last  date,
  block_count smallint,
  PRIMARY KEY ((radio, mcc, mnc, lac, cid))
) WITH default_time_to_live = 15552000
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND speculative_retry = '99PERCENTILE';

-- "What do we know near here", for the debug routes and coverage checks.
--
-- A denormalised mirror rather than a secondary index, because this database
-- cannot scan one efficiently. It is written only when a station's position
-- moves far enough to change its cell, which write elision makes rare.
CREATE TABLE IF NOT EXISTS ichnos.station_by_h3 (
  h3   bigint,
  kind tinyint,   -- wifi = 0, cell = 1
  key  bigint,    -- packed station key; for cells, a hash of the composite key
  PRIMARY KEY ((h3), kind, key)
) WITH default_time_to_live = 15552000;

-- Callers and what they may do. No time-to-live: a key should not quietly
-- expire and lock someone out.
CREATE TABLE IF NOT EXISTS ichnos.api_key (
  key          text PRIMARY KEY,
  name         text,
  enabled      boolean,
  allow_locate boolean,
  allow_submit boolean,
  allow_admin  boolean,
  daily_quota  bigint,
  store_sample_locate int,
  created      timestamp
);
