
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW microstrategy_migration_reporting_scope
(
    project string
    , schema_object_name string
    , schema_name string
    , object_name string
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/microstrategy_migration_reporting_scope/",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table microstrategy_migration_reporting_scope_ldg_output
select 
    project
    , schema_object_name 
    , schema_name
    , object_name
from microstrategy_migration_reporting_scope
;



