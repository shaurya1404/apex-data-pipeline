from pyspark import pipelines as dp
from pyspark.sql import functions as F

EH_namespace = 'streaming-project-events'
EH_instance = 'streaming-project-topic'

connection_string = dbutils.secrets.get(scope='apex', key='eventhubs-listen-connection')

@dp.table(
    name='apex.bronze.stream_events',
    comment='Ingesting raw events from Event Hub into the Bronze layer'
)

def ingest_stream_events():
    return (
        spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', f'{EH_namespace}.servicebus.windows.net:9093')
        .option('subscribe', f'{EH_instance}')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'PLAIN')
        .option('kafka.sasl.jaas.config', f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='{connection_string}';")
        .option('startingOffsets', 'earliest')
        .option('failOnDataLoss', 'false')
        .load()
        .select(F.col('value').cast('string').alias('body'), F.col('partition'), F.col('offset'), F.col('timestamp').alias('enqueued_at'), F.current_timestamp().alias('_ingested_at'))
    )