from pyspark.sql import SparkSession
import sys

# Args: output_path
output_path = sys.argv[1]  # e.g., gs://bucket/iceberg_table/

spark = (
    SparkSession.builder
    .appName("IcebergCreateTable")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark3-runtime:1.3.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.gcs_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.gcs_catalog.type", "hadoop")
    .config("spark.sql.catalog.gcs_catalog.warehouse", output_path)
    .getOrCreate()
)

spark.sql(f'''
CREATE OR REPLACE TABLE iceberg_dataset.first_iceberg_table 
(
   id INT,
   name STRING,
   age STRING
)
with connection DEFAULT 
 options(
          'fileFormat'='parquet',
          'tableformat'='iceberg',
          'location'='{output_path}/iceberg_dataset/first_iceberg_table',
          )         
          
USING ICEBERG
''')

spark.stop()