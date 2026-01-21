SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_customer_satisfaction_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_customer_satisfaction_teradata_job;'
FOR SESSION VOLATILE;

ET;

CREATE VOLATILE multiset TABLE customer_satisfaction (  
        vendor                                VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        category                              VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        subcategory                           VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        submission_type	                      VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        channel_country                       VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        channel_brand                         VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        survey_response_id                    VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        experience_date                       DATE FORMAT 'YYYY-MM-DD',
        question_label                        VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        answer_value                          VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
) ON COMMIT PRESERVE ROWS; 

ET;

insert into customer_satisfaction
WITH t1 AS (
SELECT
    store_num
    , store_short_name
    , store_type_code
    , CASE  WHEN store_type_code = 'VS'
            THEN CAST(REGEXP_SUBSTR(store_abbrev_name, '[0-9]+', 1, 1) AS INTEGER)
            ELSE store_num
    END AS store_num_actual
FROM {db_env}_NAP_USR_VWS.STORE_DIM
)
, t2 AS (
SELECT
    t1.store_num
    , t1.store_short_name
    , t1.store_type_code
    , t1.store_num_actual
    , sd.store_short_name AS store_short_name_actual
    , sd.store_type_code AS store_type_code_actual
    , sd.store_close_date
FROM t1
LEFT JOIN {db_env}_nap_usr_vws.store_dim AS sd
    ON t1.store_num_actual = sd.store_num
)
SELECT
    'Medallia' AS vendor
    , csrf.category
    , csrf.subcategory
    , csrf.submission_type
    , csrf.channel_country
    , CASE  WHEN t2.store_type_code_actual IN ('FL', 'NL')
            THEN 'NORDSTROM'
            WHEN t2.store_type_code_actual IN ('RK')
            THEN 'NORDSTROM RACK'
            ELSE csrf.channel_brand
    END AS channel_brand
    , csrqaf.survey_response_id
    , COALESCE(csrf.business_day_date, csrf.delivery_confirm_email_sent_date, csrf.pickup_confirm_email_sent_date, csrf.response_date_pacific) AS experience_date
    , 'Satisfaction - Overall' AS question_label
    , csrqaf.answer AS answer_value
FROM {db_env}_nap_usr_vws.customer_survey_response_qa_fact as csrqaf
left join {db_env}_nap_usr_vws.customer_survey_response_fact as csrf
    ON csrqaf.survey_response_id = csrf.survey_response_id
LEFT JOIN t2
    ON csrf.store_num = t2.store_num_actual
WHERE 1 = 1
    AND csrqaf.answer IS NOT NULL
    AND csrf.channel_country = 'US'
    AND csrqaf.question_id LIKE '%@__sat_scale5%' ESCAPE '@'
    AND experience_date between (current_date - 14) and (current_date - 1);

ET;
--DELETING AND INSERTING DATA IN THE LANDING TABLE
DELETE FROM {proto_schema}.MMM_CUSTOMER_SATISFACTION_LDG ALL;

ET;

INSERT INTO {proto_schema}.MMM_CUSTOMER_SATISFACTION_LDG
select
vendor,                                
category,		                          
subcategory,	                          
submission_type,	                      
channel_country,                         
channel_brand,                         
survey_response_id,                        
experience_date,                       
question_label,                        
answer_value,     
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from customer_satisfaction;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;