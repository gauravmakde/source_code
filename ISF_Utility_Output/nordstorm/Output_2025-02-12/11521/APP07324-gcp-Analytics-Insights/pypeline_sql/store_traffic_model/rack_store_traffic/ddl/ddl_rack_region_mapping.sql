SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_rack_region_mapping_11521_ACE_ENG;
     Task_Name=ddl_rack_region_mapping;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_region_mapping
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 01/30/2023

Notes: 
--This table has details on region mapping used for apply adjustments to the model to changes in wifi usage behavior
--The data for this table can be copied from T3DL_ACE_CORP.rack_region_mapping

*/

 CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.rack_region_mapping
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
   (
    region_group varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , region varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , clbr_start_date DATE
    , clbr_end_date DATE
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (region, clbr_start_date, clbr_end_date);


-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.rack_region_mapping IS 'Region mapping used for apply adjustments to the rack store traffic model to changes in wifi usage behavior';
 


SET QUERY_BAND = NONE FOR SESSION;