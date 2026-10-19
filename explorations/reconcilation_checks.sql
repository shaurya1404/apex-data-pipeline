-- Batch: Audit PostHog's default schema for Batch exports
SELECT *
FROM apex.bronze.batch_events
LIMIT 5;

-- Batch: every landing file row is in bronze (the two will be equal AFTER a recent pipeline run)
SELECT
  (SELECT count(*)
   FROM read_files(
     'abfss://landing@dlstreamprojdev.dfs.core.windows.net/events/',
     format => 'parquet',
     recursiveFileLookup => true,
     pathGlobFilter => '*.parquet*'
   )) AS landing_rows,
  (SELECT count(*) FROM apex.bronze.batch_events) AS bronze_rows;

-- Batch: no file loaded twice (expect no rows)
SELECT _source_file, count(DISTINCT _ingested_at) AS loads
FROM apex.bronze.batch_events
GROUP BY _source_file
HAVING count(DISTINCT _ingested_at) > 1;

-- Stream: no message stored twice (expect no rows)
SELECT partition, offset, count(*)
FROM apex.bronze.stream_events
GROUP BY partition, offset
HAVING count(*) > 1;

-- Events: Batch events won whenever they're available
SELECT e.source, count(*)
FROM apex.silver.events e
JOIN apex.bronze.batch_events b USING (uuid)
GROUP BY e.source;

-- Dimension: No data loss
SELECT 'users' AS t,
       (SELECT count(*) FROM apex.bronze.users) AS expected,
       (SELECT count(*) FROM apex.silver.users) AS actual
UNION ALL
SELECT 'workouts',
       (SELECT count(*) FROM apex.bronze.workouts),
       (SELECT count(*) FROM apex.silver.workouts)
UNION ALL
SELECT 'gyms',
       (SELECT count(*) + count(secondary_place_id) FROM apex.bronze.gyms),
       (SELECT count(*) FROM apex.silver.gyms);