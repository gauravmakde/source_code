SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_rn_nsg_wifi_coef_11521_ACE_ENG;
     Task_Name=ddl_fls_rn_nsg_wifi_coef_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.fls_rn_nsg_wifi_coef
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Created/Modified: 03/03/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'fls_rn_nsg_wifi_coef_ldg', OUT_RETURN_MSG);

create multiset table {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_coef_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    store_number          INTEGER,
    intercept             DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    rn_wifi_slope         DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    holiday_dt            DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    weekend_dt            DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    holiday_rn_wifi_slope DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    weekend_rn_wifi_slope DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    r_squared             DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00
    )
UNIQUE PRIMARY INDEX (store_number)
;

SET QUERY_BAND = NONE FOR SESSION;