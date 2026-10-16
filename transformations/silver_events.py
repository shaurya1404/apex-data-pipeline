from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

PROPERTY_FIELDS = {
    "session_id": "$session_id",
    "is_identified": "$is_identified",
    "app_version": "$app_version",
    "app_build": "$app_build",
    "device_name": "$device_name",
    "device_type": "$device_type",
    "os_name": "$os_name",
    "os_version": "$os_version",
    "locale": "$locale",
    "timezone": "$timezone",
    "city": "$geoip_city_name",
    "region": "$geoip_subdivision_1_name",
    "country_code": "$geoip_country_code",
    "workout_name": "workout_name",
    "gym_slot": "gym_slot",
    "is_change": "is_change",
    "reason": "reason",
    "method": "method",
    "missing_email": "missing_email",
    "permission_type": "type",
    "new_goal": "new_goal",
    "new_tier": "new_tier",
    "nudge_type": "nudgeType",
    "result": "result",
    "tour_id": "tour_id",
    "total_steps": "total_steps",
    "steps_viewed": "steps_viewed",
    "release_key": "release_key",
    "total_features": "total_features",
    "feature_id": "feature_id",
    "step_number": "step_number",
    "elapsed_ms": "elapsedMs",
    "cached_token_present": "cachedTokenPresent",
}

CORE_FIELDS = ["uuid", "event", "timestamp", "captured_at", "distinct_id", "person_id"]

STREAM_SCHEMA = StructType(
    [StructField(name, StringType()) for name in CORE_FIELDS + list(PROPERTY_FIELDS)]
)

INT_COLUMNS = [
    "elapsed_ms",
    "new_goal",
    "step_number",
    "total_steps",
    "steps_viewed",
    "total_features",
]
BOOL_COLUMNS = ["is_identified", "is_change", "missing_email", "cached_token_present"]

EXCLUDED_EVENTS = [
    "$set",
    "$identify",
    "signin_stale_token_found",
    "signin_stale_token_deleted",
    "supabase_token_retry",
]


# 1. Conform the stream: parse the JSON body into the final schema.

@dp.temporary_view()
def stream_conformed():
    parsed = spark.readStream.table("apex.bronze.stream_events").withColumn(
        "e", F.from_json("body", STREAM_SCHEMA)
    )
    return parsed.select(
        F.col("e.uuid").alias("uuid"),
        F.col("e.event").alias("event"),
        F.to_timestamp("e.timestamp").alias("event_time"),
        F.to_timestamp("e.captured_at").alias("captured_at"),
        F.col("e.distinct_id").alias("distinct_id"),
        F.col("e.person_id").alias("person_id"),
        *[F.col(f"e.{name}").alias(name) for name in PROPERTY_FIELDS],
        F.lit("stream").alias("source"),
        F.col("_ingested_at"),
    )


# 2. Conform the batch: pull the columns out of the properties map and shape into the final schema.

@dp.temporary_view()
def batch_conformed():
    batch = spark.readStream.table("apex.bronze.batch_events")
    return batch.select(
        F.col("uuid"),
        F.col("event"),
        F.col("timestamp").alias("event_time"),
        F.lit(None).cast("timestamp").alias("captured_at"),
        F.col("distinct_id"),
        F.col("person_id"),
        *[F.col("properties")[key].alias(name) for name, key in PROPERTY_FIELDS.items()],
        F.lit("batch").alias("source"),
        F.col("_ingested_at"),
    )


# 3. Combine, perform data quality check, and cast to corrected types

@dp.temporary_view()
@dp.expect_all_or_drop({
    "uuid_present": "uuid IS NOT NULL",
    "event_name_present": "event IS NOT NULL",
    "event_time_present": "event_time IS NOT NULL",
})
@dp.expect("event_time_not_in_future", "event_time <= current_timestamp() + INTERVAL 1 DAY")
def events_unioned():
    events = spark.readStream.table("stream_conformed").unionByName(
        spark.readStream.table("batch_conformed")
    )
    for name in INT_COLUMNS:
        events = events.withColumn(name, F.col(name).try_cast("bigint"))
    for name in BOOL_COLUMNS:
        events = events.withColumn(name, F.col(name).try_cast("boolean"))
    return (
        events.filter(~F.col("event").isin(EXCLUDED_EVENTS))
        .withColumn(
            "_source_priority",
            F.when(F.col("source") == "batch", 1).otherwise(0),
        )
    )

# 4. Deduplicate into the silver table: one row per uuid, batch copy wins.

dp.create_streaming_table(
    name="apex.silver.events",
    comment=(
        "All PostHog events from the stream and the batch export, typed, scoped "
        "and deduplicated on uuid. When an event exists in both sources, the "
        "batch copy is kept."
    ),
)

dp.create_auto_cdc_flow(
    target="apex.silver.events",
    source="events_unioned",
    keys=["uuid"],
    sequence_by=F.struct("_source_priority", "_ingested_at"),
    except_column_list=["_source_priority"],
    stored_as_scd_type=1,
)