{teradata_autocommit_on};

SET QUERY_BAND = '
App_ID=app04216; DAG_ID=marketed_user_weekly_load_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketed_user_weekly_load_job;'
FOR SESSION VOLATILE;


EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('marketed_user_FACT','{db_env}_NAP_FCT' ,'marketed_user_weekly_load_6761_DAS_MARKETING_das_marketing_insights','read_stg_to_fct_job_1',1,'LOAD_START','',current_timestamp,'MARKETED_USER');

DELETE FROM {db_env}_NAP_FCT.MARKETED_USER_FACT 
WHERE mobile_number IN (SELECT DISTINCT mobile_number FROM {db_env}_NAP_STG.MARKETED_USER_STG)
   OR email IN (SELECT DISTINCT email FROM {db_env}_NAP_STG.MARKETED_USER_STG)
   OR acp_id IN (SELECT DISTINCT acp_id FROM {db_env}_NAP_STG.MARKETED_USER_STG);
 
INSERT INTO {db_env}_NAP_FCT.MARKETED_USER_FACT
WITH MOBILE AS (  
    SELECT DISTINCT  
        IL.MOBILE_NUMBER, 
        CUST.TELEPHONE_NUMBER_TOKEN_VALUE,  
        CUST.unique_source_id  
    FROM  
        {db_env}_NAP_USR_VWS.MARKETED_USER_STG IL 
    LEFT JOIN  
        {db_env}_NAP_USR_VWS.CUSTOMER_OBJ_TELEPHONE_DIM CUST  
    ON  
        CUST.TELEPHONE_NUMBER_TOKEN_VALUE = IL.MOBILE_NUMBER
    WHERE CUST.customer_source = 'icon'
),
/*Get icon customer's email*/
EMAIL AS (  
    SELECT DISTINCT 
        IL.EMAIL,  
        CUST.EMAIL_ADDRESS_TOKEN_VALUE,  
        CUST.unique_source_id  
    FROM  
        {db_env}_NAP_USR_VWS.MARKETED_USER_STG IL  
    LEFT JOIN  
        {db_env}_NAP_USR_VWS.CUSTOMER_OBJ_EMAIL_DIM CUST  
    ON  
        CUST.EMAIL_ADDRESS_TOKEN_VALUE = IL.EMAIL  
    WHERE CUST.customer_source = 'icon'    
),  
/*Get icon customer's email*/
ACP_ID AS (  
    SELECT DISTINCT 
        IL.acp_id,  
        AACX.CUST_SOURCE || '::' || AACX.CUST_ID as unique_source_id
    FROM  
        {db_env}_NAP_USR_VWS.MARKETED_USER_STG IL  
    LEFT JOIN  
        {db_env}_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF AACX
    ON  
        AACX.acp_id = IL.acp_id  
    WHERE AACX.cust_source = 'icon' AND IL.mobile_number is NULL and IL.email is NULL 
),  
/*Get icon customer's loyaltyid from mobile*/
MOBILE_with_lylty AS (  
    SELECT  
        a.MOBILE_NUMBER,  
        a.TELEPHONE_NUMBER_TOKEN_VALUE,  
        a.unique_source_id,  
        lylty.program_index_id AS loyalty_id
    FROM  
        MOBILE a  
    LEFT JOIN  
        {db_env}_NAP_USR_VWS.CUSTOMER_OBJ_PROGRAM_DIM lylty  
    ON  
        a.unique_source_id = lylty.unique_source_id
    WHERE lylty.customer_source = 'icon' and lylty.program_index_name = 'MTLYLTY'
),
/*Get icon customer's loyaltyid from email*/
EMAIL_with_lylty AS (  
    SELECT  
        a.EMAIL,  
        a.EMAIL_ADDRESS_TOKEN_VALUE,  
        a.unique_source_id,  
        lylty.program_index_id AS loyalty_id
    FROM  
        EMAIL a  
    LEFT JOIN  
        {db_env}_NAP_USR_VWS.CUSTOMER_OBJ_PROGRAM_DIM lylty  
    ON  
        a.unique_source_id = lylty.unique_source_id
    WHERE lylty.customer_source = 'icon' and lylty.program_index_name = 'MTLYLTY'
),
/*Get icon customer's loyaltyid from email*/
ACP_ID_with_lylty AS (  
    SELECT  
        a.acp_id,   
        a.unique_source_id,  
        lylty.program_index_id AS loyalty_id
    FROM  
        ACP_ID a  
    LEFT JOIN  
        {db_env}_NAP_USR_VWS.CUSTOMER_OBJ_PROGRAM_DIM lylty  
    ON  
        a.unique_source_id = lylty.unique_source_id
    WHERE lylty.customer_source = 'icon' and lylty.program_index_name = 'MTLYLTY'
),
/*you can either find the unique_source_id by using email or phone. 
 * so if unique_source_id exists in both table, we can just keep one */
MOBILE_FILTER_OUT_COMMON AS (  
    SELECT     
        m.MOBILE_NUMBER,  
        m.TELEPHONE_NUMBER_TOKEN_VALUE,  
        m.unique_source_id,  
        m.loyalty_id
    FROM  
        MOBILE_with_lylty m  
    WHERE  
        m.unique_source_id NOT IN (  
            SELECT  
                e.unique_source_id  
            FROM  
                EMAIL_with_lylty e  
        )  
),  
/* Union mobile and email table so we wont miss any acp_ids*/
WITH_LYLTY_ICON AS (  
    SELECT  
        CAST(mobile_number AS VARCHAR(255)) AS mobile_number,  
        CAST(telephone_number_token_value AS VARCHAR(255)) AS telephone_number_token_value,  
        CAST(NULL AS VARCHAR(255)) AS email,  
        CAST(NULL AS VARCHAR(255)) AS email_address_token_value,  
        unique_source_id,  
        loyalty_id
    FROM  
        MOBILE_FILTER_OUT_COMMON
    UNION ALL  
    SELECT  
        CAST(NULL AS VARCHAR(255)) AS mobile_number,  
        CAST(NULL AS VARCHAR(255)) AS telephone_number_token_value,  
        CAST(email AS VARCHAR(255)) AS email,  
        CAST(email_address_token_value AS VARCHAR(255)) AS email_address_token_value,  
        unique_source_id,  
        loyalty_id
    FROM  
        EMAIL_with_lylty
),
/* aggregate both icon_id and loyalty_id in one table*/
WITH_LYLTY_ICON_2 AS (
        SELECT  
        DISTINCT 
    r.mobile_number,
    r.email,
    t.unique_source_id,
    t.loyalty_id
        FROM {db_env}_NAP_USR_VWS.MARKETED_USER_STG r left join WITH_LYLTY_ICON t on (r.mobile_number = t.mobile_number OR r.email = t.email)
),
/* aggregate both icon_id and acp_id and loyalty_id in one table*/
WITH_ACP_ICON_LTLY AS(
    SELECT 
        a.mobile_number,
        a.email,
        a.loyalty_id,
        a.unique_source_id,
        b.acp_id
        FROM WITH_LYLTY_ICON_2 a 
        LEFT JOIN
        {db_env}_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF b
        ON a.unique_source_id = b.CUST_SOURCE || '::' || b.CUST_ID
),
/* aggregate with the events that has null mobile number and null email*/
WITH_FINAL_ACP_ICON_LTLY AS(
    SELECT
        mobile_number,
        email,
        acp_id,
        loyalty_id,
        unique_source_id
    FROM
        WITH_ACP_ICON_LTLY
    UNION ALL
    SELECT
        CAST(NULL AS VARCHAR(255)) AS mobile_number,  
        CAST(NULL AS VARCHAR(255)) AS email, 
        acp_id,
        loyalty_id,
        unique_source_id
    FROM
        ACP_ID_with_lylty
)
SELECT  
        mobile_number,
        email,
        acp_id,
        loyalty_id,
        unique_source_id,
        current_date as dw_batch_date,
        current_timestamp(0) as dw_sys_load_tmstp,
        current_timestamp(0) as dw_sys_updt_tmstp 
FROM WITH_FINAL_ACP_ICON_LTLY;

-- to refresh staging table everyday
DELETE FROM {db_env}_NAP_STG.MARKETED_USER_STG ALL;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('MARKETED_USER_FACT','{db_env}_NAP_FCT' ,'marketed_user_weekly_load_6761_DAS_MARKETING_das_marketing_insights','read_stg_to_fct_job_1',2,'LOAD_END','',current_timestamp,'MARKETED_USER');
 

SET QUERY_BAND = NONE FOR SESSION;
