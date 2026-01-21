
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW metadata_move_groups
(
    object_source string,
    object_schema string,
    object_name string,
    schema_object_name string,
    needs_data_transfer string,
    coe_engineer string,
    td_mirroring_status string,
    pelican_testing_result string,
    pelican_errors string,
    table_merge_keys string,
    timestamp_column string,
    is_full_load string,
    refresh_cadence string,
    is_staging string,
    appid string,
    sprint string,
    updated_timestamp Timestamp
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/metadata_move_groups/",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table metadata_move_groups_ldg_output
select  object_source,
          object_schema,
          object_name,
          schema_object_name,
          needs_data_transfer,
          coe_engineer,
          td_mirroring_status,
          pelican_testing_result,
          pelican_errors,
          table_merge_keys,
          timestamp_column,
          is_full_load,
          refresh_cadence,
          is_staging,
          appid,
          sprint,
          updated_timestamp
from metadata_move_groups
;
