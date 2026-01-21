SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_local_covid_traffic_adjustment_11521_ACE_ENG;
     Task_Name=ddl_fls_local_covid_traffic_adjustment_ldg;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.fls_local_covid_traffic_adjustment
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'fls_local_covid_traffic_adjustment_ldg', OUT_RETURN_MSG);

create multiset table {fls_traffic_model_t2_schema}.fls_local_covid_traffic_adjustment_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    store_number INTEGER,
    covid_mltplr DECIMAL(20,15) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
    start_date DATE,
    end_date DATE
    )
UNIQUE PRIMARY INDEX (store_number, start_date);
 

SET QUERY_BAND = NONE FOR SESSION;