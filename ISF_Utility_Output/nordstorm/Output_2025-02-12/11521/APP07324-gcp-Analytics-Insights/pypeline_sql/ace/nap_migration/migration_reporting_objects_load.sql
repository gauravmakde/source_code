
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW migration_reporting_objects
(
    `Site Name` string
    , `Workbook Name` string
    , schema_object_name string
    , schema_name string
    , table_name string
    , bi_platform string
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/migration_reporting_objects/",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table migration_reporting_objects_ldg_output
select 
    `Site Name` as site_name
    , `Workbook Name` as workbook_name
    , schema_object_name
    , schema_name
    , table_name
    , bi_platform
from migration_reporting_objects
;
