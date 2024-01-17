
-- Sensors rollups will store the summary for the sensors
-- entire lifespan
--DROP TABLE IF EXISTS sensors_rollup;
CREATE SEQUENCE IF NOT EXISTS sensors_rollup_sq START 10;
CREATE TABLE IF NOT EXISTS sensors_rollup (
    sensors_id int PRIMARY KEY REFERENCES sensors
  , datetime_first timestamptz -- first recorded measument datetime (@ingest)
  , datetime_last timestamptz -- last recorded measurement time (@ingest)
  , geom_latest geometry -- last recorded point (@ingest)
  , value_latest double precision -- last recorded measurement (@ingest)
  , value_count int NOT NULL -- total count of measurements (@ingest, @rollup)
  , value_avg double precision -- average of all measurements (@ingest, @rollup)
  , value_sd double precision -- sd of all measurements (@ingest, @rollup)
  , value_min double precision -- lowest measurement value (@ingest, @rollup)
  , value_max double precision -- highest value measured (@ingest, @rollup)
  --, value_p05 double precision -- 5th percentile (@rollup)
  --, value_p50 double precision -- median (@rollup)
  --, value_p95 double precision -- 95th percentile (@rollup)
  , added_on timestamptz NOT NULL DEFAULT now() -- first time measurements were added (@ingest)
  , modified_on timestamptz NOT NULL DEFAULT now() -- last time we measurements were added (@ingest)
  --, calculated_on timestamptz -- last time data was rolled up (@rollup)
);


-- Sensors latest will act as a cache for the most recent
-- sensor value, managed by the ingester
CREATE TABLE IF NOT EXISTS sensors_latest (
    sensors_id int PRIMARY KEY NOT NULL REFERENCES sensors
  , datetime timestamptz
  , value double precision NOT NULL
  , lat double precision -- so that nulls dont take up space
  , lon double precision
  , modified_on timestamptz DEFAULT now()
  , fetchlogs_id int -- for debugging issues, no reference constraint
);


CREATE TABLE IF NOT EXISTS rollups (
    groups_id int REFERENCES groups (groups_id),
    measurands_id int,
    sensors_id int,
    rollup text,
    st timestamptz,
    et timestamptz,
    first_datetime timestamptz,
    last_datetime timestamptz,
    value_count bigint,
    value_sum float,
    last_value float,
    minx float,
    miny float,
    maxx float,
    maxy float,
    last_point geography,
    PRIMARY KEY (groups_id, measurands_id, rollup, et)
);

CREATE INDEX rollups_measurands_id_idx ON rollups USING btree (measurands_id);
CREATE INDEX rollups_rollup_idx ON rollups USING btree (rollup);
CREATE INDEX rollups_sensors_id_idx ON rollups USING btree (sensors_id);
CREATE INDEX rollups_st_idx ON rollups USING btree (st);

-- The following tables, functions and views are to handle
-- tracking coverage for the system. If possibly we may also want to replace
-- the rollups table above (which uses groups) with the hourly_data table
-- below. Therefor the table below also includes some extended summary stats
SET search_path = public;
CREATE SCHEMA IF NOT EXISTS _measurements_internal;

CREATE TABLE IF NOT EXISTS hourly_data (
  sensors_id int NOT NULL --REFERENCES sensors ON DELETE CASCADE
, datetime timestamptz NOT NULL
, measurands_id int NOT NULL --REFERENCES measurands -- required for partition
, first_datetime timestamptz NOT NULL
, last_datetime timestamptz NOT NULL
, value_count int NOT NULL
, value_avg double precision
, value_sd double precision
, value_min double precision
, value_max double precision
, value_p02 double precision
, value_p25 double precision
, value_p50 double precision
, value_p75 double precision
, value_p98 double precision
, threshold_values jsonb
, updated_on timestamptz -- last time the sensor was updated
, calculated_on timestamptz-- last time the row rollup was calculated
, UNIQUE(sensors_id, measurands_id, datetime)
) PARTITION BY RANGE (datetime);

CREATE INDEX IF NOT EXISTS hourly_data_sensors_id_idx
ON hourly_data
USING btree (sensors_id);

CREATE INDEX IF NOT EXISTS hourly_data_datetime_idx
ON hourly_data
USING btree (datetime);

CREATE UNIQUE INDEX IF NOT EXISTS hourly_data_sensors_id_datetime_idx
ON hourly_data
USING btree (sensors_id, datetime);

CREATE INDEX IF NOT EXISTS hourly_data_measurands_id_idx
ON hourly_data
USING btree (measurands_id);

CREATE INDEX IF NOT EXISTS hourly_data_measurands_id_datetime_idx
ON hourly_data
USING btree (measurands_id, datetime);

-- not really used but here just in case we need it
CREATE OR REPLACE FUNCTION create_hourly_data_partition(sd date, ed date) RETURNS text AS $$
DECLARE
table_name text := 'hourly_data_'||to_char(sd, 'YYYYMMDD')||||to_char(ed, '_YYYYMMDD');
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS _measurements_internal.%s
          PARTITION OF hourly_data
          FOR VALUES
          FROM (''%s'')
          TO (''%s'');',
          table_name,
          sd,
          ed
          );
   RETURN table_name;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_hourly_data_partition(dt date) RETURNS text AS $$
DECLARE
_table_schema text := '_measurements_internal';
_table_name text := 'hourly_data_'||to_char(dt, 'YYYYMM');
sd date := date_trunc('month', dt);
ed date := date_trunc('month', dt + '1month'::interval);
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS %s.%s
          PARTITION OF hourly_data
          FOR VALUES
          FROM (''%s'')
          TO (''%s'');',
          _table_schema,
          _table_name,
          sd,
          ed
          );
   -- register that table
   INSERT INTO data_table_partitions (
   data_tables_id
   , table_schema
   , table_name
   , start_date
   , end_date)
   SELECT data_tables_id
   , _table_schema
   , _table_name
   , sd
   , ed
   FROM data_tables
   WHERE table_schema = 'public'
   AND table_name = 'hourly_data';
   RETURN _table_name;
END;
$$ LANGUAGE plpgsql;


INSERT INTO data_tables (data_tables_id, table_schema, table_name) VALUES
(2, 'public', 'hourly_data');


WITH dates AS (
SELECT generate_series('2016-01-01'::date, '2024-01-01'::date, '1month'::interval) as dt)
SELECT create_hourly_data_partition(dt::date)
FROM dates;

-- use this to keep track of what hours are stale
-- should be updated on EVERY ingestion
CREATE TABLE IF NOT EXISTS hourly_stats (
 datetime timestamptz PRIMARY KEY
 , added_on timestamptz NOT NULL DEFAULT now()
 , modified_on timestamptz
 , calculated_count int NOT NULL DEFAULT 0
 , calculated_on timestamptz
 , calculated_seconds double precision
 , measurements_count int
 , sensors_count int
 );


-- create a table to help us keep track of what days have been exported
-- we can use the hourly_stats to determine which are outdated
-- basically check and see which days have either not been exported
-- or
CREATE TABLE IF NOT EXISTS daily_stats (
  day date NOT NULL UNIQUE
, sensor_nodes_count bigint NOT NULL
, sensors_count bigint NOT NULL
, hours_count bigint NOT NULL
, measurements_count bigint NOT NULL
, export_path text
, calculated_on timestamp
, initiated_on timestamp
, exported_on timestamp
, metadata jsonb
);


CREATE OR REPLACE FUNCTION has_measurement(timestamptz) RETURNS boolean AS $$
WITH m AS (
SELECT datetime
FROM measurements
WHERE datetime = $1
LIMIT 1)
SELECT COUNT(1) > 0
FROM m;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION has_measurement(date) RETURNS boolean AS $$
WITH m AS (
SELECT datetime
FROM measurements
WHERE datetime > $1
AND datetime <= $1 + '1day'::interval
LIMIT 1)
SELECT COUNT(1) > 0
FROM m;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION has_measurement(int) RETURNS boolean AS $$
WITH m AS (
SELECT datetime
FROM measurements
WHERE sensors_id = $1
LIMIT 1)
SELECT COUNT(1) > 0
FROM m;
$$ LANGUAGE SQL;


-- method to update all the hourly stats
-- based on the measurements table
-- MUCH faster than the groupby method
CREATE OR REPLACE FUNCTION reset_hourly_stats(
  st timestamptz DEFAULT '-infinity'
  , et timestamptz DEFAULT 'infinity'
  )
RETURNS bigint AS $$
WITH first_and_last AS (
SELECT MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
FROM measurements
WHERE datetime >= st
AND datetime <= et
), datetimes AS (
SELECT generate_series(
   date_trunc('hour', datetime_first)
   , date_trunc('hour', datetime_last)
   , '1hour'::interval) as datetime
FROM first_and_last
), inserts AS (
INSERT INTO hourly_stats (datetime, modified_on)
SELECT datetime
, now()
FROM datetimes
WHERE has_measurement(datetime)
ON CONFLICT (datetime) DO UPDATE
SET modified_on = GREATEST(EXCLUDED.modified_on, hourly_stats.modified_on)
RETURNING 1)
SELECT COUNT(1) FROM inserts;
$$ LANGUAGE SQL;



CREATE OR REPLACE FUNCTION initialize_daily_stats(
  sd date DEFAULT '-infinity'
  , ed date DEFAULT 'infinity'
  )
RETURNS bigint AS $$
WITH first_and_last AS (
SELECT MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
FROM measurements
WHERE datetime >= sd
AND datetime <= ed
), datetimes AS (
SELECT generate_series(
   date_trunc('day', datetime_first)
   , date_trunc('day', datetime_last)
   , '1day'::interval) as day
FROM first_and_last
), inserts AS (
INSERT INTO daily_stats (day, sensor_nodes_count, sensors_count, measurements_count, hours_count)
SELECT day::date, -1, -1, -1, -1
FROM datetimes
WHERE has_measurement(day::date)
ON CONFLICT (day) DO NOTHING
RETURNING 1)
SELECT COUNT(1) FROM inserts;
$$ LANGUAGE SQL;



-- Table to hold the list of thresholds that we will
-- need to calculate for each rollup
CREATE SEQUENCE IF NOT EXISTS thresholds_sq START 10;
CREATE TABLE IF NOT EXISTS thresholds (
  thresholds_id int PRIMARY KEY DEFAULT nextval('thresholds_sq')
  , measurands_id int NOT NULL REFERENCES measurands
  , value double precision NOT NULL
  , UNIQUE (measurands_id, value)
);

DROP TABLE IF EXISTS sensor_exceedances;
CREATE TABLE IF NOT EXISTS sensor_exceedances (
  sensors_id int NOT NULL REFERENCES sensors ON DELETE CASCADE
  , threshold_value double precision
  , datetime_latest timestamptz
  , updated_on timestamptz NOT NULL DEFAULT now()
  , UNIQUE(sensors_id, threshold_value)
);
-- add index
CREATE INDEX sensor_exceedances_sensors_id_idx ON sensor_exceedances USING btree (sensors_id);
CREATE INDEX sensor_exceedances_threshold_value ON sensor_exceedances USING btree (threshold_value);


-- a table to track the entities specific sets of thresholds
-- this will allow us to define groups of thresholds for display purposes
-- e.g. epa, who, other orgs
CREATE SEQUENCE IF NOT EXISTS entities_thresholds_sq START 10;
CREATE TABLE IF NOT EXISTS entities_thresholds (
  entities_thresholds_id int PRIMARY KEY DEFAULT nextval('entities_thresholds_sq')
  , entities_id int NOT NULL REFERENCES entities ON DELETE CASCADE
  , thresholds_id int NOT NULL REFERENCES thresholds ON DELETE CASCADE
  , UNIQUE(entities_id, thresholds_id)
);

-- this should be made into a normal table that we manage
-- because this is a lot of overhead for things that dont
-- need to be updated all the time
CREATE MATERIALIZED VIEW sensor_node_daily_exceedances AS
SELECT sy.sensor_nodes_id
, h.measurands_id
, date_trunc('day', datetime - '1sec'::interval) as day
, t.value as threshold_value
, SUM((h.value_avg >= t.value)::int) as exceedance_count
, SUM(value_count) as total_count
, COUNT(*) AS hourly_count
FROM hourly_data h
JOIN sensors s ON (h.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (sy.sensor_systems_id = s.sensor_systems_id)
JOIN thresholds t ON (t.measurands_id = h.measurands_id)
GROUP BY 1,2,3,4;

CREATE UNIQUE INDEX ON sensor_node_daily_exceedances (sensor_nodes_id, measurands_id, threshold_value, day);


-- This could stay a materialized view
-- because we will need to refresh the whole thing all the time
-- we could move the intervals to a table
CREATE MATERIALIZED VIEW sensor_node_range_exceedances AS
WITH intervals AS (
   SELECT UNNEST(ARRAY[1,14,30,90]) as days
)
SELECT sensor_nodes_id
, days
, measurands_id
, threshold_value
, SUM(exceedance_count) as exceedance_count
, SUM(total_count) as total_count
FROM sensor_node_daily_exceedances
, intervals
WHERE day > current_date - days
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX ON sensor_node_range_exceedances (sensor_nodes_id, measurands_id, threshold_value, days);


-- this is the basic function used to rollup an entire day
CREATE OR REPLACE FUNCTION calculate_rollup_daily_stats(day date) RETURNS bigint AS $$
WITH data AS (
   SELECT (datetime - '1sec'::interval)::date as day
   , h.sensors_id
   , sensor_nodes_id
   , value_count
   FROM hourly_data h
   JOIN sensors s ON (h.sensors_id = s.sensors_id)
   JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
   WHERE datetime > day::timestamp
   AND  datetime <= day + '1day'::interval
), inserts AS (
INSERT INTO daily_stats (
  day
, sensor_nodes_count
, sensors_count
, hours_count
, measurements_count
, calculated_on
)
SELECT day
, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
, COUNT(DISTINCT sensors_id) as sensors_count
, COUNT(1) as hours_count
, SUM(value_count) as measurements_count
, current_timestamp
FROM data
GROUP BY day
ON CONFLICT (day) DO UPDATE
SET sensor_nodes_count = EXCLUDED.sensor_nodes_count
, sensors_count = EXCLUDED.sensors_count
, hours_count = EXCLUDED.hours_count
, measurements_count = EXCLUDED.measurements_count
, calculated_on = EXCLUDED.calculated_on
RETURNING measurements_count)
SELECT measurements_count
FROM inserts;
$$ LANGUAGE SQL;

-- Function to rollup a give interval to the hour
-- date_trunc is used to ensure that only hourly data is inserted
-- an hour currently takes about 15-30 seconds to roll up, depending on load
-- we add the hour to the datetime so that its saved as time ending
-- we subtract the second so that a value that is recorded as 2022-01-01 10:00:00
-- and is time ending becomes 2022-01-01 09:59:59, and then trucated to the 9am hour

--\set et '''2023-01-19 16:00:00+00'''::timestamptz
--\set st '''2023-01-19 15:00:00+00'''::timestamptz


CREATE OR REPLACE FUNCTION calculate_hourly_data(st timestamptz, et timestamptz) RETURNS TABLE (
  sensors_count bigint
, measurements_count bigint
) AS $$
WITH inserted AS (
INSERT INTO hourly_data (
  sensors_id
, measurands_id
, datetime
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, calculated_on)
SELECT
  m.sensors_id
, measurands_id
, date_trunc('hour', datetime - '1sec'::interval) + '1hour'::interval as datetime
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, COUNT(1) as value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value) as value_p98
, current_timestamp as calculated_on
FROM measurements m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
WHERE datetime > date_trunc('hour', st)
AND datetime <= date_trunc('hour', et)
GROUP BY 1,2,3
HAVING COUNT(1) > 0
ON CONFLICT (sensors_id, measurands_id, datetime) DO UPDATE
SET first_datetime = EXCLUDED.first_datetime
, last_datetime = EXCLUDED.last_datetime
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, calculated_on = EXCLUDED.calculated_on
RETURNING value_count)
SELECT COUNT(1) as sensors_count
, SUM(value_count) as measurements_count
FROM inserted;
$$ LANGUAGE SQL;


-- A method that includes specifying the sensors_id
CREATE OR REPLACE FUNCTION calculate_hourly_data(
  id int
, st timestamptz
, et timestamptz
) RETURNS TABLE (
  sensors_count bigint
, measurements_count bigint
) AS $$
WITH inserted AS (
INSERT INTO hourly_data (
  sensors_id
, measurands_id
, datetime
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, calculated_on)
SELECT
  m.sensors_id
, measurands_id
, date_trunc('hour', datetime - '1sec'::interval) + '1hour'::interval as datetime
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, COUNT(1) as value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value) as value_p98
, current_timestamp as calculated_on
FROM measurements m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
WHERE m.sensors_id = id
AND datetime > date_trunc('hour', st)
AND datetime <= date_trunc('hour', et)
GROUP BY 1,2,3
HAVING COUNT(1) > 0
ON CONFLICT (sensors_id, measurands_id, datetime) DO UPDATE
SET first_datetime = EXCLUDED.first_datetime
, last_datetime = EXCLUDED.last_datetime
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, calculated_on = EXCLUDED.calculated_on
RETURNING value_count)
SELECT COUNT(1) as sensors_count
, SUM(value_count) as measurements_count
FROM inserted;
$$ LANGUAGE SQL;

-- Some helper functions to make things easier
-- Pass the time ending timestamp to calculate one hour
CREATE OR REPLACE FUNCTION calculate_hourly_data(et timestamptz DEFAULT now() - '1hour'::interval) RETURNS TABLE (
  sensors_count bigint
, measurements_count bigint
) AS $$
SELECT * FROM calculate_hourly_data(et - '1hour'::interval, et);
$$ LANGUAGE SQL;

-- Helper function to record a how day
CREATE OR REPLACE FUNCTION calculate_hourly_data(dt date) RETURNS TABLE (
  sensors_count bigint
, measurements_count bigint
) AS $$
SELECT * FROM calculate_hourly_data(dt::timestamptz, dt + '1day'::interval);
$$ LANGUAGE SQL;


--DROP FUNCTION IF EXISTS update_hourly_data(timestamptz);
CREATE OR REPLACE FUNCTION update_hourly_data(hr timestamptz DEFAULT now() - '1hour'::interval) RETURNS bigint AS $$
DECLARE
nw timestamptz := clock_timestamp();
mc bigint;
BEGIN
WITH inserted AS (
  SELECT COALESCE(measurements_count, 0) as measurements_count
  , COALESCE(sensors_count, 0) as sensors_count
  FROM calculate_hourly_data(hr))
INSERT INTO hourly_stats (
  datetime
, calculated_on
, measurements_count
, sensors_count
, calculated_count
, calculated_seconds)
SELECT date_trunc('hour', hr)
, now()
, measurements_count
, sensors_count
, 1
, EXTRACT(EPOCH FROM clock_timestamp() - nw)
FROM inserted
ON CONFLICT (datetime) DO UPDATE
SET calculated_on = EXCLUDED.calculated_on
, calculated_count = hourly_stats.calculated_count + 1
, measurements_count = EXCLUDED.measurements_count
, sensors_count = EXCLUDED.sensors_count
, calculated_seconds = EXCLUDED.calculated_seconds
RETURNING measurements_count INTO mc;
RETURN mc;
END;
$$ LANGUAGE plpgsql;



-- helpers for measurand_id
CREATE OR REPLACE FUNCTION calculate_hourly_data(id int, et timestamptz) RETURNS TABLE (
  sensors_count bigint
, measurements_count bigint
) AS $$
SELECT calculate_hourly_data(id, et - '1hour'::interval, et);
$$ LANGUAGE SQL;

-- Helper function to record a whole day
CREATE OR REPLACE FUNCTION calculate_hourly_data(id int, dt date) RETURNS TABLE (
  sensors_count bigint
, measurements_count bigint
) AS $$
SELECT calculate_hourly_data(id, dt::timestamptz, dt + '1day'::interval);
$$ LANGUAGE SQL;

-- Simple view for coverage
CREATE OR REPLACE VIEW sensor_hourly_coverage AS
SELECT r.sensors_id
, datetime
, value_count
, (s.metadata->'hourly_frequency')::int as expected
, CASE WHEN value_count >= (s.metadata->'hourly_frequency')::int THEN 100
  ELSE ROUND(value_count/(s.metadata->'hourly_frequency')::int::decimal * 100)
  END as coverage
FROM hourly_data r
JOIN sensors s ON (r.sensors_id = s.sensors_id);

-- simple table to add some time tracking
-- this is to monitor the update_rollups process which
-- is creating too many table locks
CREATE TABLE IF NOT EXISTS performance_log (
  process_name text
, start_datetime timestamptz
, end_datetime timestamptz DEFAULT current_timestamp
);

CREATE OR REPLACE VIEW performance_log_view AS
SELECT process_name
, start_datetime
, end_datetime
, age(end_datetime, start_datetime) as process_time
FROM performance_log;

CREATE OR REPLACE FUNCTION log_performance(text, timestamptz) RETURNS timestamptz AS $$
  INSERT INTO performance_log (process_name, start_datetime, end_datetime)
  VALUES (pg_backend_pid()||'-'||$1, $2, current_timestamp)
  RETURNING end_datetime;
$$ LANGUAGE SQL;


CREATE OR REPLACE PROCEDURE update_hourly_data(lmt int DEFAULT 1000) AS $$
DECLARE
dt timestamptz;
BEGIN
FOR dt IN (
    SELECT datetime
    FROM hourly_stats
    WHERE datetime < now()
    AND (calculated_on IS NULL
    OR calculated_on < COALESCE(modified_on, added_on))
    ORDER BY datetime ASC
    LIMIT lmt)
LOOP
  RAISE NOTICE 'updating hour: %', dt;
  PERFORM update_hourly_data(dt);
  COMMIT;
END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE update_hourly_data(lag interval, lmt int DEFAULT 1000) AS $$
DECLARE
dt timestamptz;
BEGIN
FOR dt IN (
    SELECT datetime
    FROM hourly_stats
    WHERE datetime < now() - lag
    AND (calculated_on IS NULL
    OR calculated_on < COALESCE(modified_on, added_on))
    ORDER BY datetime ASC
    LIMIT lmt)
LOOP
  RAISE NOTICE 'updating hour: %', dt;
  PERFORM update_hourly_data(dt);
  COMMIT;
END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE update_hourly_data_latest(lmt int DEFAULT 1000) AS $$
DECLARE
dt timestamptz;
BEGIN
FOR dt IN (
    SELECT datetime
    FROM hourly_stats
    WHERE datetime < now()
    AND (calculated_on IS NULL
    OR calculated_on < COALESCE(modified_on, added_on))
    ORDER BY datetime DESC
    LIMIT lmt)
LOOP
  RAISE NOTICE 'updating hour: %', dt;
  PERFORM update_hourly_data(dt);
  COMMIT;
END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE update_hourly_data_latest(lag interval, lmt int DEFAULT 1000) AS $$
DECLARE
dt timestamptz;
BEGIN
FOR dt IN (
    SELECT datetime
    FROM hourly_stats
    WHERE datetime < now() - lag
    AND (calculated_on IS NULL
    OR calculated_on < COALESCE(modified_on, added_on))
    ORDER BY datetime DESC
    LIMIT lmt)
LOOP
  RAISE NOTICE 'updating hour: %', dt;
  PERFORM update_hourly_data(dt);
  COMMIT;
END LOOP;
END;
$$ LANGUAGE plpgsql;


DROP TABLE IF EXISTS sensors_rollup_patch;
SELECT sensors_id
, MIN(first_datetime) as first_datetime
, MAX(last_datetime) as last_datetime
, SUM(value_count) as value_count
, AVG(value_avg) as value_avg
, MIN(value_min) as value_min
, MAX(value_max) as value_max
INTO sensors_rollup_patch
FROM hourly_data
GROUP BY sensors_id;

SELECT COUNT(1)
FROM sensors_rollup;

INSERT INTO sensors_rollup (
 sensors_id
 , datetime_first
 , datetime_last
 , value_count
 , value_avg
 , value_min
 , value_max
 , value_latest
)
SELECT s.sensors_id
 , s.first_datetime
 , s.last_datetime
 , s.value_count
 , s.value_avg
 , s.value_min
 , s.value_max
 , m.value
FROM sensors_rollup_patch s
JOIN measurements m ON (s.sensors_id = m.sensors_id AND s.last_datetime = m.datetime)
ON CONFLICT (sensors_id) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, value_count  = EXCLUDED.value_count
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_avg = EXCLUDED.value_avg
, value_latest = COALESCE(sensors_rollup.value_latest, EXCLUDED.value_latest);



-- when was it last updated
DO $$
DECLARE
	__st timestamptz := '2023-05-18 15:00:00+00'::timestamptz;
	__et timestamptz;
	__calculated_on timestamptz := '-infinity';
	__count bigint;
BEGIN
	__st := date_trunc('hour', __st);
	__et := __st + '1hour'::interval;
	SELECT calculated_on INTO __calculated_on
	FROM hourly_stats h
	WHERE h.datetime = __st;
	---
	WITH sensors AS (
	SELECT sensors_id
	FROM measurements
  WHERE datetime > __st
  AND datetime <= __et
	AND added_on > __calculated_on
	GROUP BY sensors_id)
	SELECT COUNT(1) INTO __count
	FROM sensors;
	RAISE NOTICE 'We found % sensors', __count;
END
$$;


CREATE OR REPLACE FUNCTION calculate_hourly_data_partial(st timestamptz) RETURNS TABLE (
  sensors_count bigint
, measurements_count bigint
) AS $$
DECLARE
	--__st timestamptz := '2023-05-18 15:00:00+00'::timestamptz;
	et timestamptz;
	__calculated_on timestamptz := '-infinity';
	__ns bigint;
	__nm bigint;
BEGIN
	st := date_trunc('hour', st);
	et := st + '1hour'::interval;
	SELECT calculated_on INTO __calculated_on
	FROM hourly_stats h
	WHERE h.datetime = st;
	---
	WITH sensors AS (
	  SELECT sensors_id
		FROM measurements
  	WHERE datetime > st
  	AND datetime <= et
		AND added_on > __calculated_on
		GROUP BY sensors_id
	), hourly AS (
		SELECT (stats).measurements_count
		FROM sensors, calculate_hourly_data(sensors_id, st, et) as stats
	) SELECT COUNT(1)
	, SUM(h.measurements_count) INTO __ns, __nm
		FROM hourly h;
	---
	RETURN QUERY
	SELECT COUNT(1) as sensors_count
	, SUM(value_count) as measurements_count
	FROM hourly_data
	WHERE datetime = st;
END;
$$ LANGUAGE plpgsql;
