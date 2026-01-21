SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_sku_item_added_11521_ACE_ENG;
     Task_Name=ddl_sku_item_added;'
     FOR SESSION VOLATILE;

/******************************************************************************

Table definition for T2DL_DAS_SCALED_EVENTS.sku_item_added

*******************************************************************************/ 

CREATE MULTISET TABLE {scaled_events_t2_schema}.sku_item_added 
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    (
     event_date_pacific     DATE
    ,channelcountry         varchar(2) compress ('US','CA')
    ,channelbrand           varchar(16) compress ('NORDSTROM','NORDSTROM_RACK')
    ,experience             varchar(16) compress ('ANDROID_APP','DESKTOP_WEB','IOS_APP','MOBILE_WEB','UNDETERMINED')
    ,rms_sku_num            varchar(25)
    ,quantity               integer compress
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX(rms_sku_num)
PARTITION BY RANGE_N(event_date_pacific BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;
SET QUERY_BAND = NONE FOR SESSION;