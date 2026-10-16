SELECT count(*), MIN(enqueued_at) AS Earliest, MAX(enqueued_at) AS Latest 
FROM apex.bronze.stream_events;