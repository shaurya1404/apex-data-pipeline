-- Audit streamed events from Event Hub in Bronze
SELECT count(*), MIN(enqueued_at) AS Earliest, MAX(enqueued_at) AS Latest 
FROM apex.bronze.stream_events;

-- Audit PostHog's default schema for batch events ingested from blob container into Bronze
SELECT *
FROM apex.bronze.batch_events
LIMIT 5;

-- Audit dimensional data ingested in Bronze
SELECT * FROM apex.bronze.users LIMIT 3;
SELECT * FROM apex.bronze.workouts LIMIT 3;
SELECT * FROM apex.bronze.gyms LIMIT 3;

