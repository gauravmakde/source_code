create or replace temporary view app_push_view

(Audience_Name string, 
Audience_Type string,
Campaign_Type string,
Send_Date string,
Mobile_Device_OS string,
Push_Send_Name string,
Push_Send_Type string,
Push_Content_Name string,
Push_Title string,
Campaign_Code string,
Mobile_App string,
Push_Sends string,
Push_Deliveries string,
Push_Opens string,
Push_Delivery_Rate string,
Push_Open_Rate string,
Push_Bounce_Rate string      	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/app_push.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE app_push_ldg_output
SELECT
    Audience_Name, 
Audience_Type,
Campaign_Type,
to_date(Send_Date,"M/d/yyyy") as Send_Date,
Mobile_Device_OS,
Push_Send_Name,
Push_Send_Type,
Push_Content_Name,
Push_Title,
Campaign_Code,
Mobile_App,
Push_Sends,
Push_Deliveries,
Push_Opens,
Push_Delivery_Rate,
Push_Open_Rate,
Push_Bounce_Rate
FROM app_push_view
WHERE 1=1
;

