{teradata_autocommit_on};

SET QUERY_BAND = '
App_ID=app04216; DAG_ID=icon_lounge_weekly_load_6761_DAS_MARKETING_das_marketing_insights; Task_Name=icon_lounge_weekly_load_job;'
FOR SESSION VOLATILE;


EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('icon_lounge_FACT','{db_env}_NAP_FCT' ,'icon_lounge_weekly_load_6761_DAS_MARKETING_das_marketing_insights','read_stg_to_fct_job_1',1,'LOAD_START','',current_timestamp,'ICON_LOUNGE');

DELETE FROM {db_env}_NAP_FCT.ICON_LOUNGE_FACT WHERE mobile_number IN (SELECT DISTINCT mobile_number from {db_env}_NAP_STG.ICON_LOUNGE_STG);;
 
INSERT INTO {db_env}_NAP_FCT.ICON_LOUNGE_FACT
WITH MOBILE AS (  
    SELECT DISTINCT  
        IL.MOBILE_NUMBER, 
        CUST.TELEPHONE_NUMBER_TOKEN_VALUE,  
        CUST.unique_source_id  
    FROM  
        {db_env}_NAP_USR_VWS.ICON_LOUNGE_STG IL 
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
        {db_env}_NAP_USR_VWS.ICON_LOUNGE_STG IL  
    LEFT JOIN  
        {db_env}_NAP_USR_VWS.CUSTOMER_OBJ_EMAIL_DIM CUST  
    ON  
        CUST.EMAIL_ADDRESS_TOKEN_VALUE = IL.EMAIL  
    WHERE CUST.customer_source = 'icon'    
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
    r.*,
    t.unique_source_id,
    t.loyalty_id
        FROM {db_env}_NAP_USR_VWS.ICON_LOUNGE_STG r left join WITH_LYLTY_ICON t on (r.mobile_number = t.mobile_number OR r.email = t.email)
),
/* aggregate both icon_id and acp_id and loyalty_id in one table*/
WITH_ACP_ICON_LTLY AS(
        SELECT 
        a.*,
        b.acp_id
        FROM WITH_LYLTY_ICON_2 a 
        LEFT JOIN
        {db_env}_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF b
        ON a.unique_source_id = b.CUST_SOURCE || '::' || b.CUST_ID
)
SELECT  
        loyalty_id,
        acp_id,
        unique_source_id,
        visit_date,
        loyalty_status,
        first_name,
        last_name,
        mobile_number,
        email,
	    purchased_pass, 	
	    visiting_city, 	
	    visiting_state, 	
	    guests,
        current_date as dw_batch_date,
        current_timestamp(0) as dw_sys_load_tmstp,
        current_timestamp(0) as dw_sys_updt_tmstp 
FROM WITH_ACP_ICON_LTLY;

-- to refresh staging table everyday
DELETE FROM {db_env}_NAP_STG.ICON_LOUNGE_STG ALL;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('ICON_LOUNGE_FACT','{db_env}_NAP_FCT' ,'icon_lounge_weekly_load_6761_DAS_MARKETING_das_marketing_insights','read_stg_to_fct_job_1',2,'LOAD_END','',current_timestamp,'ICON_LOUNGE');
 

SET QUERY_BAND = NONE FOR SESSION;
