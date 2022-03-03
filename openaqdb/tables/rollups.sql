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


-- proposal to replace the one above
CREATE TABLE IF NOT EXISTS hourly_rollups (
  sensors_id int NOT NULL --REFERENCES sensors ON DELETE CASCADE
, datetime timestamptz NOT NULL
, first_datetime timestamptz NOT NULL
, last_datetime timestamptz NOT NULL
, value_count int NOT NULL
, value_avg double precision
, value_sd double precision
, value_min double precision
, value_max double precision
, value_p05 double precision
, value_p50 double precision
, value_p95 double precision
, updated_on timestamptz -- last time the sensor was updated
, calculated_on timestamptz-- last time the row rollup was calculated
, UNIQUE(sensors_id, datetime)
);


CREATE INDEX hourly_rollups_sensors_id_idx
ON hourly_rollups
USING btree (sensors_id);

CREATE INDEX hourly_rollups_datetime_idx
ON hourly_rollups
USING btree (datetime);

CREATE INDEX hour_rollups_date_idx
ON hourly_rollups
USING btree ((datetime::date));


DROP INDEX hourly_rollups_sensors_id_idx;
DROP INDEX hourly_rollups_datetime_idx;
ALTER TABLE hourly_rollups
DROP CONSTRAINT hourly_rollups_sensors_id_datetime_key;
ALTER TABLE hourly_rollups
DROP CONSTRAINT hourly_rollups_sensors_id_fkey;


select
  c.relname
, usename
, l.locktype
, application_name
, client_addr
, backend_start
, query_start
, state_change
, query
from pg_locks l
inner join pg_stat_activity psa ON (psa.pid = l.pid)
left outer join pg_class c ON (l.relation = c.oid)
where l.relation = 'sensors'::regclass;

-- Function to rollup a give interval to the hour
-- date_trunc is used to ensure that only hourly data is inserted
-- an hour currently takes about 15-30 seconds to roll up, depending on load
-- we add the hour to the datetime so that its saved as time ending
-- we subtract the second so that a value that is recorded as 2022-01-01 10:00:00
-- and is time ending becomes 2022-01-01 09:59:59, and then trucated to the 9am hour
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(st timestamptz, et timestamptz) RETURNS bigint AS $$
WITH inserted AS (
INSERT INTO hourly_rollups (
  sensors_id
, datetime
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p05
, value_p50
, value_p95
, calculated_on)
SELECT
  sensors_id
, date_trunc('hour', datetime - '1sec'::interval) + '1hour'::interval as datetime
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, COUNT(1) as value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.05) WITHIN GROUP(ORDER BY value) as value_p05
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY value) as value_p95
, current_timestamp as calculated_on
FROM measurements
WHERE datetime > date_trunc('hour', st)
AND datetime <= date_trunc('hour', et)
GROUP BY 1,2
HAVING COUNT(1) > 0
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET first_datetime = EXCLUDED.first_datetime
, last_datetime = EXCLUDED.last_datetime
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_p05 = EXCLUDED.value_p05
, value_p50 = EXCLUDED.value_p50
, value_p95 = EXCLUDED.value_p95
RETURNING 1)
SELECT COUNT(1)
FROM inserted;
$$ LANGUAGE SQL;

-- Some helper functions to make things easier
-- Pass the time ending timestamp to calculate one hour
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(et timestamptz) RETURNS bigint AS $$
SELECT calculate_hourly_rollup(et - '1hour'::interval, et);
$$ LANGUAGE SQL;

-- Helper function to record a how day
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(dt date) RETURNS bigint AS $$
SELECT calculate_hourly_rollup(dt::timestamptz, dt + '1day'::interval);
$$ LANGUAGE SQL;

-- Need to test this out with a table partitioned by

-- Need to update the ingest method so that when measurements
-- are added we update the this table at the same time
-- insert new data but only upsert the updated_on column (will need a new function)
-- the reason is because we will likely not have all the data ready

-- Need to create a function in the open data lambda to do
-- Hourly (given an hour)
-- daily (given the day)
-- pending (all stale hours that have been updated, with limit)

-- Need to create a scheduler to run the new functions
-- hourly runs on the hour
-- cleanup could run each half hour??
-- daily could run for a while as needed
