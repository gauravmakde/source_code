from pyspark.sql import SparkSession
from google.cloud import bigquery
import sys

project_id=sys.argv[1]
dbname = sys.argv[2]
tblname = sys.argv[3]
gcs_input_path = sys.argv[4]
bigquery_table = f'{project_id}.{dbname}.{tblname}'


def get_single_result(project_id: str, query: str):
    # Initialize a BigQuery client
    print("*"*20)
    print(query)
    print("*"*20)
    client = bigquery.Client(project=project_id)

    query_job = client.query(query) 
    result = query_job.result()  
    row = next(result, None)    
    if row is None:
        return None 
    return row[0]  # Return the first column value

query = rf"""
select 
'SELECT ' ||
ARRAY_TO_STRING(ARRAY_AGG('CAST ( _C' ||
CAST(cast(ORDINAL_POSITION AS INT64) - 1 as STRING) || 
' AS ' ||
CASE 
WHEN TRIM(DATATYPE) IN ('BIGINT','INT64','INT', 'SMALLINT', 'INTEGER', 'TINYINT', 'BYTEINT') THEN 'INTEGER'
WHEN TRIM(DATATYPE) IN ('STRING') THEN 'STRING'
WHEN TRIM(DATATYPE) IN ('FLOAT64') THEN 'FLOAT'
WHEN TRIM(DATATYPE) IN ('NUMERIC','DECIMAL','BIGNUMERIC','BIGDECIMAL') THEN 'NUMERIC'
WHEN TRIM(DATATYPE) IN ('TIMESTAMP','DATETIME') THEN 'TIMESTAMP'
WHEN TRIM(DATATYPE) IN ('DATE') THEN DATATYPE
ELSE DATATYPE
END
 ||
' ) AS ' || COLUMN_NAME
ORDER BY ORDINAL_POSITION),',') || ' FROM Source_data  ;'
FROM
(
SELECT 
A.TABLE_SCHEMA,A.TABLE_NAME,A.COLUMN_NAME,A.ORDINAL_POSITION,regexp_replace(A.Data_type,'\\(.*\\)',' ') AS DATATYPE
from 
{dbname}.INFORMATION_SCHEMA.COLUMNS A
INNER JOIN
  {dbname}.INFORMATION_SCHEMA.TABLES B
ON
  A.table_schema=B.Table_schema
  AND A.Table_name = B.Table_name
  AND B.table_type = 'BASE TABLE'
  WHERE LOWER(A.TABLE_NAME) = LOWER('{tblname}')
  AND LOWER(A.table_schema) = LOWER('{dbname}')
)
"""

load_sql_string = get_single_result(project_id,query)

print(f'Below is the select query: {load_sql_string}')


# Initialize Spark session
spark = SparkSession.builder \
    .appName("GCS_to_BigQuery") \
    .config("spark.sql.parquet.int96AsTimestamp", "false") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .getOrCreate()


# Read the CSV file from GCS
df = spark.read \
    .format("csv") \
    .option("header", "false") \
    .option("delimiter","␟") \
    .option("inferSchema", "true") \
    .load(gcs_input_path)

# Show data for verification (optional)
df.show(10)

df.createOrReplaceTempView("Source_data")

# df=spark.sql ('''
# SELECT 
# CAST ( _C0 AS STRING ) AS rmssku,
# CAST ( _C1 AS STRING ) AS channelcountry,
# CAST ( _C2 AS STRING ) AS channelbrand,
# CAST ( _C3 AS STRING ) AS sellingchannel,
# CAST ( _C4 AS STRING ) AS isonlinepurchasable,
# CAST ( _C5 AS STRING ) AS lastupdatedat
#  FROM Source_data 

# ''')

df=spark.sql(load_sql_string)



# Write data to BigQuery
df.write \
    .format("bigquery") \
    .option("table", bigquery_table) \
    .option("temporaryGcsBucket", "test-data-datametica") \
    .mode("overwrite") \
    .save()

# Stop the Spark session
spark.stop()