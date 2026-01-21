/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_manual_dag_object_mapping_11521_ACE_ENG;
     Task_Name=manual_dag_object_mapping;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (schema_object_name)
on {techex_t2_schema}.manual_dag_object_mapping_ldg
;

delete
from    {techex_t2_schema}.manual_dag_object_mapping
;

insert into {techex_t2_schema}.manual_dag_object_mapping
select    dag_id,
          git_repo_url,
          last_dagrun,
          app_id,
          team_id,
          dag_status,
          manager,
          director,
          vp,
          application_tier,
          schema_object_name,
          source_sys,
          source_target,
          h2_commit,
          last_updated,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {techex_t2_schema}.manual_dag_object_mapping_ldg
;

-- collect stats on final table
COLLECT STATISTICS  COLUMN (schema_object_name)
on {techex_t2_schema}.manual_dag_object_mapping
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
