/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_metadata_move_groups_11521_ACE_ENG;
     Task_Name=metadata_move_groups;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (object_schema,object_name),
                    COLUMN (object_schema) ,
                    COLUMN (object_name)
on {techex_t2_schema}.metadata_move_groups_ldg
;

delete
from    {techex_t2_schema}.metadata_move_groups
;

insert into {techex_t2_schema}.metadata_move_groups
select    object_source,
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
          updated_timestamp,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.metadata_move_groups_ldg
where object_source <> ','
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (object_schema,object_name),
                    COLUMN (object_schema) ,
                    COLUMN (object_name)
on {techex_t2_schema}.metadata_move_groups
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
