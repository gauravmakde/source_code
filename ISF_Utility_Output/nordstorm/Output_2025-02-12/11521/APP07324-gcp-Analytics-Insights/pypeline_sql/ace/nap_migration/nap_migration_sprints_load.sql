
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW sprints
(
   databasename string
    , tablename string
    , sprint string
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/sprints.csv",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table nap_migration_sprints_ldg_output
select 
    databasename
    , tablename 
    , sprint 
from sprints
;

