from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Facts: Defining the gold layer as a fact dataset enriched with the three Primary Keys
# Non-identified Events: Events triggered before the user has before the user has a user_id assigned such as oauth_signup_started
@dp.materialized_view(
    name='apex.gold.fact_events',
    comment='One row per app event.'
)
@dp.expect('identified_events_have_users', '(NOT coalesce(is_identified, false)) OR user_id IS NOT NULL')
def fact_events():
    events = spark.read.table("apex.silver.events")

    users = spark.read.table('apex.silver.users').select('user_id')

    # Workout events don't have the workout_id attached to them - using (user_id, workout_name) as a workaround, which is fallible
    latest_first = Window.partitionBy('user_id', 'workout_name_key').orderBy(F.col('updated_at').desc(), F.col('workout_id'))
    workouts = spark.read.table('apex.silver.workouts').withColumn('rank', F.row_number().over(latest_first)).where(F.col('rank') == 1) \
        .select("user_id", "workout_name_key", "workout_id")

    events = events.withColumn("workout_name_key", F.lower(F.trim("workout_name")))
    
    gyms = spark.read.table("apex.silver.gyms").select("user_id", "gym_slot")

    joined = (
        events.join(users, events.distinct_id == users.user_id, 'left') \
            .join(workouts, (events.distinct_id == workouts.user_id) & (events.workout_name_key == workouts.workout_name_key), 'left')
            .join(gyms, (events.distinct_id == gyms.user_id) & (events.gym_slot == gyms.gym_slot), 'left')
    )

    return joined.select(
       # Identity and time
        events["uuid"].alias("event_id"),
        events["event"].alias("event_name"),
        events["event_time"],
        F.to_date(events["event_time"]).alias("event_date_utc"),
        # Foreign keys: populated only when the dimension row exists
        users["user_id"].alias("user_id"),
        workouts["workout_id"].alias("workout_id"),
        gyms["gym_slot"].alias("gym_slot"),
        # Raw PostHog identifier, kept for non-identified events
        events["distinct_id"],
        # Context
        events["session_id"],
        events["is_identified"],
        events["device_name"],
        events["os_version"],
        events["app_version"],
        events["app_build"],
        events["country_code"],
        events["region"],
        events["city"],
        # Event details
        events["reason"],
        events["method"],
        events["permission_type"],
        events["result"],
        events["nudge_type"],
        events["new_goal"],
        events["new_tier"],
        events["is_change"],
        events["tour_id"],
        events["total_steps"],
        events["steps_viewed"],
        events["release_key"],
        events["feature_id"],
        events["step_number"],
        events["total_features"],
        events["elapsed_ms"],
        events["cached_token_present"],
        events["source"]
    )