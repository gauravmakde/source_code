
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW manual_dag_object_mapping
(
    dag_id string,
    git_repo_url string,
    last_dagrun string,
    app_id string,
    team_id string,
    dag_status string,
    manager string,
    director string,
    vp string,
    application_tier string,
    schema_object_name string,
    source_sys string,
    source_target string,
    h2_commit string,
    last_updated date
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/manual_dag_object_mapping/",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table manual_dag_object_mapping_ldg_output
select  dag_id,
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
        last_updated
from manual_dag_object_mapping
;
