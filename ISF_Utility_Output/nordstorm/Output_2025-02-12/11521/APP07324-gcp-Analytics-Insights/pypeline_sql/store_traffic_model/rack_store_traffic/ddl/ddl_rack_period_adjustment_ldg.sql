SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_period_adjustment_11521_ACE_ENG;
     Task_Name=ddl_rack_period_adjustment_ldg;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_period_adjustment
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 01/30/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'rack_period_adjustment_ldg', OUT_RETURN_MSG);

create multiset table {fls_traffic_model_t2_schema}.rack_period_adjustment_ldg
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
    )
UNIQUE PRIMARY INDEX (period_description, event_type, region, start_date, end_date);
 

SET QUERY_BAND = NONE FOR SESSION;