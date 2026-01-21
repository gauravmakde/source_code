from pyspark.sql import SparkSession
import sys

# Args: output_path
output_path = sys.argv[1]  # e.g., gs://bucket/iceberg_table/

spark = (
    SparkSession.builder
    .appName("IcebergWriter")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark3-runtime:1.3.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.gcs_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.gcs_catalog.type", "hadoop")
    .config("spark.sql.catalog.gcs_catalog.warehouse", output_path)
    .config("spark.sql.catalog.gcs_catalog.gcp_project", "wf-mdss-bq")
    .config("spark.sql.catalog.gcs_catalog.gcp_location", "us-central1")
    .getOrCreate()
)

# Sample data
data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
schema = ["id", "name", "age"]

df = spark.createDataFrame(data, schema)
print(df)

# Write as Iceberg table under sample_dataset.sample_iceberg_table
# This creates metadata/ and data/ under the warehouse path

# df.writeTo("gcs_catalog.sample_dataset.sample_iceberg_table").create()
df.write.format("iceberg").mode("append").saveAsTable("gcs_catalog.iceberg_dataset.sample_iceberg_table")

spark.stop()