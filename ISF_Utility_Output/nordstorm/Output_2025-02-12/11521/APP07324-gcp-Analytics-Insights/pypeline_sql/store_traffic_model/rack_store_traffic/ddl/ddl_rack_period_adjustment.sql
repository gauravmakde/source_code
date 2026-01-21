SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_rack_period_adjustment_11521_ACE_ENG;
     Task_Name=ddl_rack_period_adjustment;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_period_adjustment
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 01/30/2023

Notes: 
--This tables has details on additional event based adjustments applied to traffic estimation model
--the values for this table can be read from t3dl_ace_corp.rack_period_adjustment
Change log: 1/19/22, added in region column

*/

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.rack_period_adjustment
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(
    period_description varchar (50)
    , period_type varchar(15)
    , event_type varchar (25)
    , region varchar(30)
    , intercept DECIMAL(20,5) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , slope DECIMAL(20,5) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , start_date DATE
    , end_date DATE
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
UNIQUE PRIMARY INDEX (period_description, event_type, region, start_date, end_date)
;


-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.rack_period_adjustment IS 'Event based adjustments for rack store traffic';
 

SET QUERY_BAND = NONE FOR SESSION;