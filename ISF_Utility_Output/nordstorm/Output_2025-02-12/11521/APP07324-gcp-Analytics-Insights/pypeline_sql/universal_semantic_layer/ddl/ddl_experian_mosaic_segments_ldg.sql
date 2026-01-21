/*

T2/Table Name: T2DL_DAS_USL.experian_mosaic_segments_ldg
Team/Owner: Customer Analytics/ Brian McGrane
Date Created/Modified: 05/31/2024

This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/



SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_usl_experian_mosaic_segments_11521_ACE_ENG;
     Task_Name=ddl_experian_mosaic_segments_ldg;'
     FOR SESSION VOLATILE;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{usl_t2_schema}', 'experian_mosaic_segments_ldg', OUT_RETURN_MSG);

create multiset table {usl_t2_schema}.experian_mosaic_segments_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
mosaic_segment_id varchar(4) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_segment varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_segment_household_pct decimal(5,4)
,mosaic_segment_pop_pct decimal(5,4)
,mosaic_segment_pop integer
,mosaic_group_id char(1) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_group varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_group_summary varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_segment_features varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_segment_summary varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_segment_description varchar(3000) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_group_howtomarket varchar(1000) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_segment_link varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_segment_field_name varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
,mosaic_group_field_name varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
) PRIMARY INDEX(mosaic_segment_id);

SET QUERY_BAND = NONE FOR SESSION;





