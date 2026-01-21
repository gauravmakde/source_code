SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=sku_item_added_11521_ACE_ENG;
     Task_Name=sku_item_added;'
     FOR SESSION VOLATILE;

DELETE 
FROM    {scaled_events_t2_schema}.sku_item_added
WHERE   event_date_pacific >= {start_date}
AND     event_date_pacific <= {end_date}
;


INSERT INTO {scaled_events_t2_schema}.sku_item_added
SELECT  event_date_pacific 
        , channelcountry
        , channelbrand
        , experience
        , rms_sku_num
        , quantity
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM	{scaled_events_t2_schema}.sku_item_added_ldg
;

DROP TABLE {scaled_events_t2_schema}.sku_item_added_ldg;

COLLECT STATISTICS  COLUMN (rms_sku_num), 
                    COLUMN (event_date_pacific)
ON {scaled_events_t2_schema}.sku_item_added;
SET QUERY_BAND = NONE FOR SESSION;