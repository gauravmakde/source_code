SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=corrected_modeled_traffic_11521_ACE_ENG;
     Task_Name=ddl_corrected_modeled_traffic_ldg;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.corrected_modeled_traffic
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'corrected_modeled_traffic_ldg', OUT_RETURN_MSG);

create multiset table {fls_traffic_model_t2_schema}.corrected_modeled_traffic_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    store_number              INTEGER NOT NULL
    , day_date                DATE
    , corrected_wifi           INTEGER
    , corrected_traffic         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00  

    )
UNIQUE PRIMARY INDEX(store_number, day_date);
 

SET QUERY_BAND = NONE FOR SESSION;