
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW terameta_objects
(
    object_database string,
    object_name string,
    object_database_name string,
    object_kind string,
    object_bus_desc string,
    business_process string,
    terameta_appid string
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/terameta_objects/",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table terameta_objects_ldg_output
select  object_database,
        object_name,
        object_database_name,
        object_kind,
        object_bus_desc,
        business_process,
        terameta_appid
from terameta_objects
;
