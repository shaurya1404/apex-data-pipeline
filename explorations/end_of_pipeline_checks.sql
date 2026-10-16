-- Bronze -> Silver: Data Loss Check
SELECT 'users' AS t, (SELECT count(*) FROM apex.bronze.users) AS bronze, (SELECT count(*) FROM apex.silver.users) AS silver
UNION ALL
SELECT 'workouts', (SELECT count(*) FROM apex.bronze.workouts), (SELECT count(*) FROM apex.silver.workouts)
UNION ALL
SELECT 'gyms',
       (SELECT count(*) + count(secondary_place_id) FROM apex.bronze.gyms),
       (SELECT count(*) FROM apex.silver.gyms);

-- Silver: Keys are unique
SELECT 'users' AS t, count(*) - count(DISTINCT user_id) AS duplicate_keys FROM apex.silver.users
UNION ALL
SELECT 'workouts', count(*) - count(DISTINCT workout_id) FROM apex.silver.workouts
UNION ALL
SELECT 'gyms', count(*) - count(DISTINCT user_id, gym_slot) FROM apex.silver.gyms;

-- Silver: parse_pgtext_array() works correct
SELECT workout_name, muscle_groups FROM apex.silver.workouts LIMIT 10;

-- Silver: Duplicate workouts based on composite key (user_id, workout_name)
SELECT user_id, workout_name_key, count(*) AS workouts_with_this_name
FROM apex.silver.workouts
GROUP BY user_id, workout_name_key
HAVING count(*) > 1;

-- Gold: Match rates per event with each dimension table
SELECT event_name,
       count(*) AS events,
       count_if(user_id IS NULL)    AS no_user,
       count_if(workout_id IS NULL) AS no_workout,
       count_if(gym_slot IS NULL)   AS no_gym
FROM apex.gold.fact_events
GROUP BY event_name
ORDER BY events DESC;

-- Gold: One fact row per event
SELECT (SELECT count(*) FROM apex.gold.fact_events) AS fact_rows,
       (SELECT count(*) FROM apex.silver.events)    AS silver_rows;

-- Gold: Unique gym slot values in all events
SELECT DISTINCT gym_slot, count(*)
FROM apex.gold.fact_events
WHERE gym_slot IS NOT NULL
GROUP BY gym_slot