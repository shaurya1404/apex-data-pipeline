-- Gyms with the most check-ins and the number of users who have checked in at that gym
SELECT g.place_name, count(*) AS total_check_ins, count(distinct e.user_id) AS total_users
FROM apex.gold.fact_events e
    JOIN apex.silver.gyms g ON e.user_id = g.user_id AND e.gym_slot = g.gym_slot
WHERE e.event_name = 'gym_check_in_completed'
GROUP BY g.place_name
ORDER BY total_check_ins DESC;

-- Daily active users
SELECT event_date_utc, count(DISTINCT user_id) AS active_users
FROM apex.gold.fact_events
WHERE user_id IS NOT NULL
GROUP BY event_date_utc
ORDER BY event_date_utc;

-- Muscle groups ranked based on popularity
WITH exploded AS (
    SELECT explode(w.muscle_groups) AS muscle_group
    FROM apex.gold.fact_events f
    JOIN apex.silver.workouts w ON f.workout_id = w.workout_id
    WHERE f.event_name = 'workout_started'
)
SELECT muscle_group, COUNT(*) AS total_workouts
FROM exploded
GROUP BY muscle_group
ORDER BY total_workouts DESC