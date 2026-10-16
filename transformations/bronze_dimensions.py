from pyspark import pipelines as dp
from pyspark.sql import functions
from pyspark.sql.functions import col

landing = 'abfss://landing@dlstreamprojdev.dfs.core.windows.net/dimensions'
def all_bronze_dimensions(table_name):
    @dp.materialized_view(
        name = f"apex.bronze.{table_name}",
        comment = f"Bronze table for {table_name} that ingests raw data from Landing into Delta Lake"
    )

    def bronze_dimension():
        return (
            spark.read.format('parquet').load(f"{landing}/{table_name}") \
            .withColumn('file_path', col('_metadata.file_path')) \
            .withColumn('file_modification_time', col('_metadata.file_modification_time'))
        )
for table_name in ['users', 'gyms', 'workouts']:
    all_bronze_dimensions(table_name)