
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW move_groups
(
   databasename string
    , tablename string
    , move_group string
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/move_groups.csv",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table nap_migration_move_groups_ldg_output
select 
    databasename
    , tablename 
    , move_group 
from move_groups
;

