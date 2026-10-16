from pyspark import pipelines as dp
from pyspark.sql import functions as F

landing = 'abfss://landing@dlstreamprojdev.dfs.core.windows.net/events'

SENSITIVE_KEYS = [
    "$ip",
    "$geoip_latitude",
    "$geoip_longitude",
    "$geoip_postal_code",
    "$set",
    "$set_once",
]

@dp.table(
    name='apex.bronze.batch_events',
    comment='Ingesting raw events for backfilled data that also runs daily for present data with PII removed'
)

def batch_and_backfill_events():
    return (
        spark.readStream.format('cloudFiles') \
            .option('cloudFiles.format', 'parquet') \
            .load(landing) \
            .drop('person_properties') \
            .withColumn('properties', F.map_filter(F.from_json('properties', 'map<string, string>'),
                                                   lambda key,val: ~key.isin(SENSITIVE_KEYS)))
            .withColumn('_ingested_at', F.current_timestamp())
            .withColumn('_source_file', F.col('_metadata.file_path'))
    )