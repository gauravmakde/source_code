SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=cust_jwn_trip_bucket_lkp_11521_ACE_ENG;
     Task_Name=cust_jwn_trip_bucket_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_jwn_trip_bucket_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 06/28/2023

Notes:
-- Trip Bucket Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.cust_jwn_trip_bucket_lkp
;

INSERT INTO {str_t2_schema}.cust_jwn_trip_bucket_lkp
    SELECT DISTINCT cust_jwn_trip_bucket_desc
                   ,ROW_NUMBER() OVER (ORDER BY cust_jwn_trip_bucket_desc  ASC ) AS cust_jwn_trip_bucket_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM
         (SELECT DISTINCT COALESCE(case when cust_jwn_trips < 10 then '0'||cast(cust_jwn_trips as varchar(1))||' trips'
                                        when cust_jwn_trips < 30 then cast(cust_jwn_trips as varchar(2))||' trips'
                                        else '30+ trips' end, 'Unknown') as cust_jwn_trip_bucket_desc  
            from T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes)a ;

SET QUERY_BAND = NONE FOR SESSION;