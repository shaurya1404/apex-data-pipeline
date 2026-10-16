from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name='apex.silver.users'
)
def users():
    return (
        spark.read.table('apex.bronze.users').select(
            F.col("id").alias("user_id"),
            F.col("email_hash"),
            F.lower(F.trim("email_domain")).alias("email_domain"),
            F.col("first_name_masked"),
            F.col("last_name_masked"),
            F.col("username_masked"),
            F.col("created_at").cast("timestamp"),
            F.col("updated_at").cast("timestamp"),
            F.col("file_modification_time").alias("loaded_at"),
        )
    )

def parse_string_array(column_name):
    cleaned = F.regexp_replace(F.col(column_name), '''[{}"]''', '')
    return (
        F.when((F.col(column_name).isNull()) | (cleaned == ''), F.array().cast('array<string>')) \
            .otherwise(F.split(cleaned, ','))
    )

@dp.materialized_view(
    name='apex.silver.workouts',
    comment='Workouts per user and per split, with muscle groups parsed'
)
@dp.expect_or_fail('workout_id_present', 'workout_id IS NOT NULL')
@dp.expect_all({'user_id_present': 'user_id IS NOT NULL', 'workout_name_present': 'workout_name IS NOT NULL'})
def workouts():
    return (
        spark.read.table('apex.bronze.workouts').select(
            F.col('id').alias('workout_id'),
            F.col('user_id'),
            F.col('split_id'),
            F.trim('workout_name').alias('workout_name'),
            F.lower(F.trim("workout_name")).alias("workout_name_key"),
            F.trim("day").alias("day_label"),
            F.regexp_extract('day', '(\\d+)', 1).try_cast('int').alias('day_number'),
            F.col('order_index').cast('int'),
            parse_string_array('muscle_groups').alias('muscle_groups'),
            F.col("created_at").cast("timestamp"),
            F.col("updated_at").cast("timestamp"),
            F.col("file_modification_time").alias("loaded_at"),
        )
    )

@dp.materialized_view(
    name='apex.silver.gyms',
    comment="Each user's gym(s) with one row per slot. Exact addresses and coordinates are dropped"
)
@dp.expect_all_or_drop({'user_id_present': 'user_id IS NOT NULL', 'place_id_present': 'place_id IS NOT NULL'})
def gyms():

    shared = [
        F.col('id').alias('gym_record_id'),
        F.col('user_id'),
        F.col('gym_change_count').cast('int'),
        F.col('is_geofencing_active').cast('boolean'),
        F.col('updated_at').cast('timestamp'),
        F.col('file_modification_time').alias('loaded_at')
    ]

    primary = spark.read.table('apex.bronze.gyms').select(
        *shared,
        F.lit('primary').alias('gym_slot'),
        F.col('place_id'),
        F.col('place_name'),
        F.col('created_at').cast('timestamp').alias('slot_set_at')
    )

    secondary = spark.read.table('apex.bronze.gyms').filter(F.col('secondary_place_id').isNotNull()).select(
        *shared,
        F.lit('secondary').alias('gym_slot'),
        F.col('secondary_place_id').alias('place_id'),
        F.col('secondary_place_name').alias('place_name'),
        F.col('secondary_first_set_at').cast('timestamp').alias('slot_set_at')
    )

    return primary.unionByName(secondary)
