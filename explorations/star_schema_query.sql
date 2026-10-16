SELECT g.place_name, count(*) AS total_check_ins, count(distinct e.user_id) AS total_users
FROM apex.gold.fact_events e
    JOIN apex.silver.gyms g ON e.user_id = g.user_id AND e.gym_slot = g.gym_slot
WHERE e.event_name = 'gym_check_in_completed'
GROUP BY g.place_name
ORDER BY total_check_ins DESC