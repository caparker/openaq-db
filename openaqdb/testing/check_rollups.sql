\set origin '''test-rollups'''

\i testing_schema.sql


SELECT generate_fake_data(
  stations => 8
, origin => :origin
, averaging => '30min'
, period => '1years'
);


CALL run_updates_full();

-- SELECT *
-- FROM testing.canary_rollup_days
-- WHERE subtitle ~* 'tz:0';

-- SELECT *
-- FROM testing.canary_days_compare
-- WHERE site_name ~* 'tz:-8'
-- AND value = 5;

-- SELECT *
-- FROM testing.canary_days_utc
-- WHERE site_name ~* 'tz:5'
-- LIMIT 10;

-- SELECT *
-- FROM testing.canary_days_local
-- WHERE site_name ~* 'tz:5'
-- LIMIT 10;
