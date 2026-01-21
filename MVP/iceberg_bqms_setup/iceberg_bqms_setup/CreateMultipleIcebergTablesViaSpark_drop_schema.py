import sys
import json
from pyspark.sql import SparkSession
from google.cloud import storage

def init_spark(sparkproperties):
    catalog_configs = sparkproperties
    builder = SparkSession.builder.enableHiveSupport()

    for catalog in catalog_configs:
        catalog_name = catalog["catalog_name"]
        warehouse = catalog["warehouse"]
        gcp_project = catalog["gcp_project"]
        gcp_location = catalog["gcp_location"]

        builder = builder \
            .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog") \
            .config(f"spark.sql.catalog.{catalog_name}.gcp_project", gcp_project) \
            .config(f"spark.sql.catalog.{catalog_name}.gcp_location", gcp_location) \
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
    spark = builder.getOrCreate()

    for catalog in catalog_configs:
        catalog_name = catalog["catalog_name"]
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}")
        for database in catalog['databases']:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{database}")

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
            spark.sql("drop database if exists ambit_us_bronze;")
            spark.sql("drop database if exists ambit_dev_us_bronze;")
            spark.sql("drop database if exists ambit_preprod_us_bronze;")
            spark.sql("drop database if exists aptools_preprod_us_bronze;")
            spark.sql("drop database if exists aptools_prod_us_bronze;")
            spark.sql("drop database if exists arclight_us_bronze;")
            spark.sql("drop database if exists arclight_dev_us_bronze;")
            spark.sql("drop database if exists atg_us_bronze;")
            spark.sql("drop database if exists brainstorm_logs_dev_us_bronze;")
            spark.sql("drop database if exists brbac_dev_us_bronze;")
            spark.sql("drop database if exists brbac_prod_us_bronze;")
            spark.sql("drop database if exists caf_us_bronze;")
            spark.sql("drop database if exists caf_agg_us_bronze;")
            spark.sql("drop database if exists capmp_us_bronze;")
            spark.sql("drop database if exists cmn_network_event_data_prod_us_bronze;")
            spark.sql("drop database if exists cmt_prod_us_bronze;")
            spark.sql("drop database if exists commercial_mobility_platform_us_bronze;")
            spark.sql("drop database if exists common_hotspot_us_bronze;")
            spark.sql("drop database if exists comscore_us_bronze;")
            spark.sql("drop database if exists csa_dev_us_bronze;")
            spark.sql("drop database if exists csa_prod_us_bronze;")
            spark.sql("drop database if exists csoc_analytics_us_bronze;")
            spark.sql("drop database if exists databus_us_bronze;")
            spark.sql("drop database if exists databus_dev_us_bronze;")
            spark.sql("drop database if exists dgplat_cs_dev_us_bronze;")
            spark.sql("drop database if exists dgplat_cs_prod_us_bronze;")
            spark.sql("drop database if exists disney_plus_session_qoe_prod_us_bronze;")
            spark.sql("drop database if exists e2e_nofip_no_pii_dev_us_bronze;")
            spark.sql("drop database if exists e2e_nofip_no_pii_prod_us_bronze;")
            spark.sql("drop database if exists e2e_nofip_pii_prod_us_bronze;")
            spark.sql("drop database if exists emerald_cache_us_bronze;")
            spark.sql("drop database if exists engines_prod_us_bronze;")
            spark.sql("drop database if exists exedenetflow_us_bronze;")
            spark.sql("drop database if exists exedenetflow_mgmt_us_bronze;")
            spark.sql("drop database if exists firewall_blocks_prod_us_bronze;")
            spark.sql("drop database if exists fmcs_dev_us_bronze;")
            spark.sql("drop database if exists fmcs_prod_us_bronze;")
            spark.sql("drop database if exists gadberry_us_bronze;")
            spark.sql("drop database if exists gem_data_analytics_dev_us_bronze;")
            spark.sql("drop database if exists gem_data_analytics_preprod_us_bronze;")
            spark.sql("drop database if exists gem_data_analytics_prod_us_bronze;")
            spark.sql("drop database if exists giapps_jira_archive_us_bronze;")
            spark.sql("drop database if exists giapps_jira_archive_dev_us_bronze;")
            spark.sql("drop database if exists giapps_jira_archive_nonprod_us_bronze;")
            spark.sql("drop database if exists gmb_us_bronze;")
            spark.sql("drop database if exists gmb_up_preprod_us_bronze;")
            spark.sql("drop database if exists gms_us_bronze;")
            spark.sql("drop database if exists hybrid_us_bronze;")
            spark.sql("drop database if exists information_schema_us_bronze;")
            spark.sql("drop database if exists ipfix_vs3_prod_us_bronze;")
            spark.sql("drop database if exists jira_datalake_ithelp_us_bronze;")
            spark.sql("drop database if exists kasat_us_bronze;")
            spark.sql("drop database if exists lyoung_us_bronze;")
            spark.sql("drop database if exists marketing_us_residential_us_bronze;")
            spark.sql("drop database if exists merkle_us_bronze;")
            spark.sql("drop database if exists merkle_agg_us_bronze;")
            spark.sql("drop database if exists mit_prod_us_bronze;")
            spark.sql("drop database if exists mit_syslog_firehose_prod_us_bronze;")
            spark.sql("drop database if exists mm_lb_prod_us_bronze;")
            spark.sql("drop database if exists mobility_us_bronze;")
            spark.sql("drop database if exists mobility_analytics_us_bronze;")
            spark.sql("drop database if exists mprocera_us_bronze;")
            spark.sql("drop database if exists network_ops_dev_us_bronze;")
            spark.sql("drop database if exists network_ops_preprod_us_bronze;")
            spark.sql("drop database if exists network_ops_prod_us_bronze;")
            spark.sql("drop database if exists nmsadmin_us_bronze;")
            spark.sql("drop database if exists nsaunders_us_bronze;")
            spark.sql("drop database if exists ops_analytics_prod_us_bronze;")
            spark.sql("drop database if exists perf_us_bronze;")
            spark.sql("drop database if exists perf_agg_us_bronze;")
            spark.sql("drop database if exists pixel_us_bronze;")
            spark.sql("drop database if exists planet_us_bronze;")
            spark.sql("drop database if exists procera_brazil_us_bronze;")
            spark.sql("drop database if exists procera_brazil_agg_us_bronze;")
            spark.sql("drop database if exists provision_us_bronze;")
            spark.sql("drop database if exists provision_agg_us_bronze;")
            spark.sql("drop database if exists rdsotp_us_bronze;")
            spark.sql("drop database if exists reports_us_bronze;")
            spark.sql("drop database if exists rte_dev_us_bronze;")
            spark.sql("drop database if exists run_dmc_prod_us_bronze;")
            spark.sql("drop database if exists san_us_bronze;")
            spark.sql("drop database if exists sandvine_us_bronze;")
            spark.sql("drop database if exists sandvine_agg_us_bronze;")
            spark.sql("drop database if exists scn_data_science_dev_us_bronze;")
            spark.sql("drop database if exists scn_data_science_nda_prod_us_bronze;")
            spark.sql("drop database if exists scn_data_science_prod_us_bronze;")
            spark.sql("drop database if exists smac_us_bronze;")
            spark.sql("drop database if exists smac_agg_us_bronze;")
            spark.sql("drop database if exists smac_preprod_us_bronze;")
            spark.sql("drop database if exists smts_us_bronze;")
            spark.sql("drop database if exists spdb_us_bronze;")
            spark.sql("drop database if exists statics_us_bronze;")
            spark.sql("drop database if exists statpush_us_bronze;")
            spark.sql("drop database if exists statpush_agg_us_bronze;")
            spark.sql("drop database if exists sub360_us_bronze;")
            spark.sql("drop database if exists threatintel_us_bronze;")
            spark.sql("drop database if exists tpe_us_bronze;")
            spark.sql("drop database if exists trackos_us_bronze;")
            spark.sql("drop database if exists udm_us_bronze;")
            spark.sql("drop database if exists us_res_gis_enrichment_us_bronze;")
            spark.sql("drop database if exists v3g_data_us_bronze;")
            spark.sql("drop database if exists v3g_data_raw_us_bronze;")
            spark.sql("drop database if exists vail_us_bronze;")
            spark.sql("drop database if exists vasn_prod_us_bronze;")
            spark.sql("drop database if exists vault_audit_gen_dev_us_bronze;")
            spark.sql("drop database if exists vcds_us_bronze;")
            spark.sql("drop database if exists vcds_agg_us_bronze;")
            spark.sql("drop database if exists vcds_preprod_us_bronze;")
            spark.sql("drop database if exists vega_personalization_err_us_bronze;")
            spark.sql("drop database if exists vega_personalization_uae_dev_us_bronze;")
            spark.sql("drop database if exists vega_personalization_uae_preprod_us_bronze;")
            spark.sql("drop database if exists vega_personalization_uae_prod_us_bronze;")
            spark.sql("drop database if exists video_qoe_dev_us_bronze;")
            spark.sql("drop database if exists video_qoe_preprod_us_bronze;")
            spark.sql("drop database if exists video_qoe_prod_us_bronze;")
            spark.sql("drop database if exists vpe_prod_us_bronze;")
            spark.sql("drop database if exists vwa_us_bronze;")
            spark.sql("drop database if exists whistleout_us_bronze;")
            spark.sql("drop database if exists whiteglove_us_bronze;")
            spark.sql("drop database if exists brbac_prod_eu_bronze;")
            spark.sql("drop database if exists caf_eu_bronze;")
            spark.sql("drop database if exists csa_dev_eu_bronze;")
            spark.sql("drop database if exists csa_prod_eu_bronze;")
            spark.sql("drop database if exists gem_data_analytics_dev_eu_bronze;")
            spark.sql("drop database if exists gfb_kasat_fixed_prod_eu_bronze;")
            spark.sql("drop database if exists information_schema_eu_bronze;")
            spark.sql("drop database if exists ipfix_residential_agg_eu_bronze;")
            spark.sql("drop database if exists ipfix_vs3_prod_eu_bronze;")
            spark.sql("drop database if exists ka_sat_decimators_prod_eu_bronze;")
            spark.sql("drop database if exists mobility_eu_bronze;")
            spark.sql("drop database if exists mprocera_eu_bronze;")
            spark.sql("drop database if exists smts_prod_eu_bronze;")
            spark.sql("drop database if exists spdb_eu_bronze;")
            spark.sql("drop database if exists statpush_eu_bronze;")
            spark.sql("drop database if exists stream_residential_prod_eu_bronze;")
            spark.sql("drop database if exists test_eu_bronze;")
            spark.sql("drop database if exists threatintel_prod_eu_bronze;")
            spark.sql("drop database if exists tpe_prod_eu_bronze;")
            spark.sql("drop database if exists v3g_data_raw_eu_bronze;")
            spark.sql("drop database if exists vasn_prod_eu_bronze;")
           
            successfully_created_tables.append(sql_file)
            print(f"Successfully executed metadata for {sql_file}")
        except Exception as e:
            print(f"Failed to execute metadata for {sql_file}: {e}")
            sys.exit(1)

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