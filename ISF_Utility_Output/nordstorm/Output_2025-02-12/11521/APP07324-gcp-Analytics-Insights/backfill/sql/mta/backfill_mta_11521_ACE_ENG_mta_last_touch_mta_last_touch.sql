SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_11521_ACE_ENG;
     Task_Name=mta_last_touch;'
     FOR SESSION VOLATILE;



DELETE FROM T2DL_DAS_MTA.mta_last_touch
WHERE 1=1
AND order_date_pacific BETWEEN date'2024-05-16' AND date'2024-05-22';

-- LOADING PRODUCTION TABLE FROM STAGING TABLE
INSERT INTO T2DL_DAS_MTA.mta_last_touch

SELECT DISTINCT
    order_number
    , mktg_type
    , utm_channel
    , order_date_pacific
	, sp_campaign
	, utm_campaign
	, utm_content
	, utm_source
	, utm_term
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM T2DL_DAS_MTA.mta_last_touch_ldg

WHERE 1=1
    AND order_date_pacific BETWEEN date'2024-05-16' AND date'2024-05-22'
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE T2DL_DAS_MTA.mta_last_touch_ldg;

COLLECT STATISTICS COLUMN(order_date_pacific), COLUMN(order_number) ON T2DL_DAS_MTA.mta_last_touch;

SET QUERY_BAND = NONE FOR SESSION;