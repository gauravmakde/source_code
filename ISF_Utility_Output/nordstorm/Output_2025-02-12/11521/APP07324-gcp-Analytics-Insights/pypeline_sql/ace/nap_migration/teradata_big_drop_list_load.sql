
-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW teradata_big_drop_list
(
    databasename string,
    tablename string, 
    object_type string,
    update_date date,
    comments string
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/nap_migration/teradata_big_drop_list/",
    sep ",",
    header "true"
)
;


-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table teradata_big_drop_list_ldg_output
select  databasename,
        tablename, 
        object_type,
        update_date,
        comments
from teradata_big_drop_list
;
