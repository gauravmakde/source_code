SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_loyalty_redemption_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_loyalty_redemption_teradata_job;'
FOR SESSION VOLATILE;

ET;

CREATE VOLATILE multiset TABLE loyalty_redemption (  
          week_start_day_date                     DATE FORMAT 'MM/DD/YYYY',
          note_type                               VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
          redemption_business_unit_desc           VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
          redeemed_next_30days                    INTEGER,
          redeemed_next_60days                    INTEGER,
          redeemed_next_90days                    INTEGER,
          redeemed_next_120days                   INTEGER,
          note_more120                            INTEGER,
          not_redeemed                            INTEGER
) ON COMMIT PRESERVE ROWS; 

ET;

INSERT INTO loyalty_redemption  
SELECT  
    cal.week_start_day_date,  
    lr.note_type,  
    CASE  
        WHEN lr.redemption_channel = 'OFFPRICE ONLINE' THEN 'R.COM'  
        WHEN lr.redemption_channel = 'FULL LINE' THEN 'NORDSTROM STORE'  
        WHEN lr.redemption_channel = 'RACK' THEN 'RACK STORE'  
        ELSE lr.redemption_channel  
    END AS redemption_business_unit_desc,  
    COUNT(DISTINCT CASE WHEN ABS(lr.note_issue_date - lr.redemption_date) <= 30 THEN lr.note_num ELSE NULL END) AS redeemed_next_30days,  
    COUNT(DISTINCT CASE WHEN ABS(lr.note_issue_date - lr.redemption_date) > 30 AND ABS(lr.note_issue_date - lr.redemption_date) <= 60 THEN lr.note_num ELSE NULL END) AS redeemed_next_60days,  
    COUNT(DISTINCT CASE WHEN ABS(lr.note_issue_date - lr.redemption_date) > 60 AND ABS(lr.note_issue_date - lr.redemption_date) <= 90 THEN lr.note_num ELSE NULL END) AS redeemed_next_90days,  
    COUNT(DISTINCT CASE WHEN ABS(lr.note_issue_date - lr.redemption_date) > 90 AND ABS(lr.note_issue_date - lr.redemption_date) <= 120 THEN lr.note_num ELSE NULL END) AS redeemed_next_120days,  
    COUNT(DISTINCT CASE WHEN ABS(lr.note_issue_date - lr.redemption_date) > 120 THEN lr.note_num ELSE NULL END) AS note_more120,  
    COUNT(DISTINCT CASE WHEN lr.redemption_date IS NULL THEN lr.note_num ELSE NULL END) AS not_redeemed  
    FROM  
        prd_nap_usr_vws.loyalty_note_hdr_fact_vw lr  
    LEFT JOIN  
        prd_nap_usr_vws.day_cal_454_dim cal ON lr.redemption_week_num = cal.week_idnt  
    WHERE  
        lr.redemption_week_num BETWEEN (  
            SELECT cal.week_idnt  
            FROM prd_nap_usr_vws.day_cal_454_dim cal  
            WHERE day_date = (current_date - 14)
        ) AND (  
            SELECT cal.week_idnt  
            FROM prd_nap_usr_vws.day_cal_454_dim cal  
            WHERE day_date = (current_date - 1)
        )  
        AND lr.issued_currency_code NOT IN ('CAD')  
    GROUP BY 1,2,3;

ET;

--DELETING AND INSERTING DATA IN THE LANDING TABLE
DELETE FROM {proto_schema}.MMM_LOYALTY_REDEMPTION_LDG ALL;

ET;

INSERT INTO {proto_schema}.MMM_LOYALTY_REDEMPTION_LDG
select
week_start_day_date,                     
note_type,                               
redemption_business_unit_desc,           
redeemed_next_30days,                    
redeemed_next_60days,                    
redeemed_next_90days,                    
redeemed_next_120days,                   
note_more120,                            
not_redeemed,                            
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from loyalty_redemption;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;