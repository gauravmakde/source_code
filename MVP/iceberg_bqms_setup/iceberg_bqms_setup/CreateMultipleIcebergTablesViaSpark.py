import sys
import json, re
from pyspark.sql import SparkSession
from google.cloud import storage

def init_spark(sparkproperties):
    catalog_configs = sparkproperties
    builder = SparkSession.builder.appName("IcebergCatalogInitializer")

    for catalog in catalog_configs:
        catalog_name = catalog["catalog_name"]
        warehouse = catalog["warehouse"]
        gcp_project = catalog["gcp_project"]
        gcp_location = catalog["gcp_location"]

        builder = builder\
       .config(f"spark.sql.catalog.{catalog_name}","org.apache.iceberg.spark.SparkCatalog")\
       .config(f"spark.sql.catalog.{catalog_name}.catalog-impl","org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog")\
       .config(f"spark.sql.catalog.{catalog_name}.gcp_project", gcp_project)\
       .config(f"spark.sql.catalog.{catalog_name}.gcp_location", gcp_location)\
       .config(f"spark.sql.catalog.{catalog_name}.warehouse",warehouse)\
       
    spark = builder.getOrCreate()

    for catalog in catalog_configs:
        catalog_name = catalog["catalog_name"]
        
        for database in catalog['databases']:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database};")
            
    return spark

def read_properties_from_gcs(gcs_path):
    storage_client = storage.Client()
    bucket_name, *path_parts = gcs_path.replace("gs://", "").split('/')
    properties_blob_name = "/".join(path_parts)
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(properties_blob_name)

    properties_content = blob.download_as_text()
    return json.loads(properties_content)

def read_sql_files_from_gcs(gcs_path):
    storage_client = storage.Client()
    gcs_path = gcs_path.replace("gs://", "")
    bucket_name, *path_parts = gcs_path.split('/')
    print(f"Bucket Name: {bucket_name}")
    print(f"Path Parts: {path_parts}")
    
    prefix = "/".join(path_parts) + "/" 
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    sql_files = [blob.name for blob in blobs if blob.name.endswith('.sql')]
    
    if not sql_files:
        print(f"No SQL files found in {gcs_path}")

    return bucket_name,sql_files

def execute_sql_files(spark, bucket_name,sql_files):
    successfully_created_tables = []
    storage_client = storage.Client()

    for sql_file in sql_files:
        print(f"Executing file: {sql_file}")
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(sql_file)
            
            sql_content = blob.download_as_text(encoding='utf-8')
            variable_mapping = {
                                "GCS_IcebergBucket": "gs://conversionfactory/VaibhavThorat/ExternalLocation"
                                }
            sql_content = sql_content.format(**variable_mapping)
            print(sql_content)
            spark.sql(f"""{sql_content}""")
            successfully_created_tables.append(sql_file)
            print(f"Successfully executed metadata for {sql_file}")
        except Exception as e:
            print(f"Failed to execute metadata for {sql_file}: {e}")

    return successfully_created_tables

def main(properties_gcs_path, sql_files_gcs_path):
    properties = read_properties_from_gcs(properties_gcs_path)

    spark = init_spark(properties)

    bucket_name,sql_files = read_sql_files_from_gcs(sql_files_gcs_path)

    if not sql_files:
        print(f"No SQL files found in {sql_files_gcs_path}")
        return

    successfully_created_tables = execute_sql_files(spark, bucket_name,sql_files)
    spark.stop()

    if successfully_created_tables:
        print("\nSuccessfully created tables:")
        for table in successfully_created_tables:
            print(table)
    else:
        print("No tables were successfully created.")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        properties_gcs_path = sys.argv[1]
        sql_files_gcs_path = sys.argv[2]
        main(properties_gcs_path, sql_files_gcs_path)
    else:
        print("Usage: run_sql.py <properties_gcs_path> <sql_files_gcs_path>")