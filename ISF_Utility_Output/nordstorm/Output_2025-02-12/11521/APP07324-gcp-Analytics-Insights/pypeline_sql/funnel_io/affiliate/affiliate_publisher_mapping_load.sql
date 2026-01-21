-- Reading data from S3 and creating the view
create or replace temporary view affiliate_publisher_mapping_view
(
   publisher_id string
    , encrypted_id string
    , publisher_name string
    , publisher_group string
    , publisher_subgroup string
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/staging/affiliate_publisher_mapping/Affiliate_publisher_mapping.csv",
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE affiliate_publisher_mapping_ldg_output
SELECT
    publisher_id 
    , encrypted_id 
    , publisher_name 
    , publisher_group 
    , publisher_subgroup 
FROM affiliate_publisher_mapping_view
WHERE 1=1
;

