
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW manual_object_metadata_capture
(
    schema_name string,
    object_name string,
    schema_object_name string,
    object_type string,
    terameta_appid string,
    nerds_appid string,
    terameta_owner_team_name string,
    terameta_appid_owner string,
    owner_em string,
    empty string,
    empty_2 string,
    key_columns string,
    merge_key_columns string,
    is_full_load string,
    is_staging string,
    refresh_schedule string,
    pelican_audit_columns string,
    pelican_requirements string,
    metadata_complete_status string,
    comments string,
    last_updated date
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/manual_object_metadata_capture/",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table manual_object_metadata_capture_ldg_output
select 
        schema_name,
        object_name,
        schema_object_name,
        object_type,
        terameta_appid,
        nerds_appid,
        terameta_owner_team_name,
        terameta_appid_owner,
        owner_em,
        key_columns,
        merge_key_columns,
        is_full_load,
        is_staging,
        refresh_schedule,
        pelican_audit_columns,
        pelican_requirements,
        metadata_complete_status,
        comments,
        last_updated
from manual_object_metadata_capture
;
