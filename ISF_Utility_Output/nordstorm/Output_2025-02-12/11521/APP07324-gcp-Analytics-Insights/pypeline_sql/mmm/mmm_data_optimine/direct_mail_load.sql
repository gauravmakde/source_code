create or replace temporary view direct_mail_view
(
	start_date string,
	campaign_name string,
	channel_type string,
	bar string,
	sales_channel string,
	region string,
	circulation string,
	cost string,
	store_no string,
	funding_type string,
	funnel_type string
   	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/Direct*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE direct_mail_ldg_output
SELECT
    to_date(start_date,"M/d/yyyy") as start_date,
	campaign_name,
	channel_type,
	bar,
	sales_channel,
	region,
	circulation,
	cost,
	store_no,
	funding_type,
	funnel_type
FROM direct_mail_view
WHERE 1=1
;

