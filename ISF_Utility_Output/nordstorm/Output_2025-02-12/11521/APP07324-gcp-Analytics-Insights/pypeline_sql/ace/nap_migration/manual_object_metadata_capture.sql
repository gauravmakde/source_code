/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_manual_object_metadata_capture_11521_ACE_ENG;
     Task_Name=manual_object_metadata_capture;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (schema_name, object_name),
                    COLUMN (schema_name) ,
                    COLUMN (object_name)
on {techex_t2_schema}.manual_object_metadata_capture_ldg
;

delete
from    {techex_t2_schema}.manual_object_metadata_capture
;

insert into {techex_t2_schema}.manual_object_metadata_capture
select  schema_name,
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
        last_updated,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.manual_object_metadata_capture_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (schema_name, object_name),
                    COLUMN (schema_name), -- column names used for primary index
                    COLUMN (object_name)  -- column names used for partition
on {techex_t2_schema}.manual_object_metadata_capture
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


