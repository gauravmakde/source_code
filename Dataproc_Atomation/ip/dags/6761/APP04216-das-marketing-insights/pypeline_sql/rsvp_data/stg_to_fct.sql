{teradata_autocommit_on};

SET QUERY_BAND = '
App_ID=app04216; DAG_ID=rsvp_onetime_load_6761_DAS_MARKETING_das_marketing_insights; Task_Name=rsvp_onetime_load_job;'
FOR SESSION VOLATILE;


EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('RSVP_FACT','{db_env}_NAP_FCT' ,'rsvp_onetime_load_6761_DAS_MARKETING_das_marketing_insights','read_stg_to_fct_job_1',1,'LOAD_START','',current_timestamp,'RSVP');

DELETE FROM {db_env}_NAP_FCT.RSVP_FACT
WHERE transaction_id IN (SELECT DISTINCT transaction_id from {db_env}_NAP_STG.RSVP_STG);
 
INSERT INTO {db_env}_NAP_FCT.RSVP_FACT
WITH MOBILE AS (  
    SELECT DISTINCT 
        RF.transaction_id,
        RF.MOBILE,  
        CUST.TELEPHONE_NUMBER_TOKEN_VALUE,  
        CUST.unique_source_id  
    FROM  
        {db_env}_NAP_BASE_VWS.RSVP_STG RF  
    LEFT JOIN  
        {db_env}_NAP_BASE_VWS.CUSTOMER_OBJ_TELEPHONE_DIM CUST  
    ON  
        CUST.TELEPHONE_NUMBER_TOKEN_VALUE = RF.MOBILE  
    WHERE CUST.customer_source = 'icon'
),
/*Get icon customer's email*/
EMAIL AS (  
    SELECT DISTINCT 
        RF.transaction_id,
        RF.EMAIL,  
        CUST.EMAIL_ADDRESS_TOKEN_VALUE,  
        CUST.unique_source_id  
    FROM  
        {db_env}_NAP_BASE_VWS.RSVP_STG RF
    LEFT JOIN  
        {db_env}_NAP_BASE_VWS.CUSTOMER_OBJ_EMAIL_DIM CUST  
    ON  
        CUST.EMAIL_ADDRESS_TOKEN_VALUE = RF.EMAIL  
    WHERE CUST.customer_source = 'icon'
),
/*Get icon customer's acpid*/
MOBILE_with_acpid AS (  
    SELECT  
        a.transaction_id, 
        a.MOBILE,  
        a.TELEPHONE_NUMBER_TOKEN_VALUE,  
        a.unique_source_id,  
        ACP.CUST_SOURCE,  
        ACP.CUST_ID,
        ACP.acp_id
    FROM  
        MOBILE a  
    LEFT JOIN  
        {db_env}_NAP_BASE_VWS.ACP_ANALYTICAL_CUST_XREF ACP  
    ON  
        a.unique_source_id = ACP.CUST_SOURCE || '::' || ACP.CUST_ID 
    WHERE acp.cust_source = 'icon'
),
/*Get icon customer's acpid*/
EMAIL_with_acpid AS (  
    SELECT  
        a.transaction_id,
        a.EMAIL,  
        a.EMAIL_ADDRESS_TOKEN_VALUE,  
        a.unique_source_id,  
        ACP.CUST_SOURCE,  
        ACP.CUST_ID,
        ACP.acp_id
    FROM  
        EMAIL a  
    LEFT JOIN  
        {db_env}_NAP_BASE_VWS.ACP_ANALYTICAL_CUST_XREF ACP  
    ON  
        a.unique_source_id = ACP.CUST_SOURCE || '::' || ACP.CUST_ID 
    WHERE acp.cust_source = 'icon'
),
/*you can either find the unique_source_id by using email or phone. 
 * so if unique_source_id exists in both table, we can just keep one */
MOBILE_FILTER_OUT_COMMON AS (  
    SELECT  
        m.transaction_id,     
        m.MOBILE,  
        m.TELEPHONE_NUMBER_TOKEN_VALUE,  
        m.unique_source_id,  
        m.CUST_SOURCE,  
        m.CUST_ID,  
        m.acp_id
    FROM  
        MOBILE_with_acpid m  
    WHERE  
        m.unique_source_id NOT IN (  
            SELECT  
                e.unique_source_id  
            FROM  
                EMAIL_with_acpid e  
        )  
),
/* Union mobile and email table so we wont miss any acp_ids*/
AGG_TBL AS (  
    SELECT  
        CAST(mobile AS VARCHAR(255)) AS mobile,  
        CAST(telephone_number_token_value AS VARCHAR(255)) AS telephone_number_token_value,  
        CAST(NULL AS VARCHAR(255)) AS email,  
        CAST(NULL AS VARCHAR(255)) AS email_address_token_value,  
        unique_source_id,  
        cust_source,  
        cust_id,
        acp_id,
        transaction_id
    FROM  
        MOBILE_FILTER_OUT_COMMON
    UNION  
    SELECT  
        CAST(NULL AS VARCHAR(255)) AS mobile,  
        CAST(NULL AS VARCHAR(255)) AS telephone_number_token_value,  
        CAST(email AS VARCHAR(255)) AS email,  
        CAST(email_address_token_value AS VARCHAR(255)) AS email_address_token_value,  
        unique_source_id,  
        cust_source,  
        cust_id,
        acp_id,
        transaction_id
    FROM  
        EMAIL_with_acpid 
),
/* aggregate both icon_id and acp_id and loyalty_id in one table*/
AGG_TBL_WITH_LTY AS(
    SELECT 
    b.acp_loyalty_id as loyalty_id,
    a.acp_id,
    a.unique_source_id,                       
    a.transaction_id
    FROM AGG_TBL a 
    LEFT JOIN
    {db_env}_NAP_BASE_VWS.ANALYTICAL_CUSTOMER b
    ON a.acp_id = b.acp_id
),
FINAL_TBL AS (
    SELECT
    b.loyalty_id,
    b.acp_id,
    b.unique_source_id,
    a.event_name,
    a.event_location,
    a.event_date,                           
    a.transaction_id,
    a.first_name,
    a.last_name,
    a.email,
    a.mobile,
    a.how_did_you_hear,
    a.attended
    FROM {db_env}_NAP_BASE_VWS.RSVP_STG a
    LEFT JOIN AGG_TBL_WITH_LTY b on a.transaction_id = b.transaction_id
)
SELECT
       loyalty_id,
        acp_id,
        unique_source_id,
        event_name,
        event_location,
        SUBSTR(event_date, 0, 11) as event_date,
        SUBSTR(event_date,12) as event_start_time,                               
        transaction_id,
        first_name,
        last_name,
        email,
        mobile,
        how_did_you_hear,
        attended,
        current_date as dw_batch_date,
        current_timestamp(0) as dw_sys_load_tmstp,
        current_timestamp(0) as dw_sys_updt_tmstp 
FROM FINAL_TBL;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('RSVP_FACT','{db_env}_NAP_FCT' ,'rsvp_onetime_load_6761_DAS_MARKETING_das_marketing_insights','read_stg_to_fct_job_1',2,'LOAD_END','',current_timestamp,'RSVP');
 

SET QUERY_BAND = NONE FOR SESSION;
