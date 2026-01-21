create or replace temporary view app_push_nord_view

(Business_Unit string,
Campaign string,
Audience_Name string,
Event_Date string,
Send_Date string,
Mobile_App string,
Mobile_Device_OS string,
Push_Content_Name string,
Push_Send_ID float,
Push_Send_Name string,
Push_Send_Type string,
Push_Title string,
Push_Bounces float,
Push_Not_Sent float,
Push_Opens float,
Push_Sends float     	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/app_push_nord.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE app_push_nord_ldg_output
SELECT
Business_Unit,
Campaign,
Audience_Name,
to_date(Event_Date,"M/d/yyyy") as Event_Date,
to_date(Send_Date,"M/d/yyyy") as Send_Date, 
Mobile_App,
Mobile_Device_OS,
Push_Content_Name,
Push_Send_ID,
Push_Send_Name,
Push_Send_Type,
Push_Title,
Push_Bounces,
Push_Not_Sent,
Push_Opens,
Push_Sends
FROM app_push_nord_view
WHERE 1=1
;

