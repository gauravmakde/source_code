/*

T2/Table Name: T2DL_DAS_USL.experian_mosaic_segments
Team/Owner: Customer Analytics/ Brian McGrane
Date Created/Modified: 05/31/2024

*/
SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_usl_experian_mosaic_segments_11521_ACE_ENG;
     Task_Name=ddl_experian_mosaic_segments;'
     FOR SESSION VOLATILE;




CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{usl_t2_schema}', 'experian_mosaic_segments', OUT_RETURN_MSG);	

create multiset table {usl_t2_schema}.experian_mosaic_segments
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
,dw_sys_load_tmstp  TIMESTAMP NOT NULL
) PRIMARY INDEX(mosaic_segment_id);

-- Table Comment (STANDARD)
COMMENT ON  {usl_t2_schema}.experian_mosaic_segments IS 'Details of Experian Mosaic Segments and Groups, at Segment grain (Segments are subsets of Groups). Sourced from the file MosaicUSA_Handbook.pdf, copyright 2019, www.segmentationportal.com';

COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_id           IS '1 letter and 2 digit ID of Mosaic Segment. The letter is the ID of the overarching Mosaic Group.';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment              IS 'Mosaic Segment Name';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_household_pct IS 'Percent of US Households that fall into this Mosaic Segment (all segments add to 100%)';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_pop_pct      IS 'Percent of US Population that fall into this Mosaic Segment (all segments add to 100%)';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_pop          IS 'Count of people in the Mosaic Segment based on US Population of 341,814,420';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_group_id             IS 'Single-letter ID of Mosaic Group';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_group                IS 'Mosaic Group Name';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_group_summary        IS 'Sentence describing the people in the Mosaic Group';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_features     IS 'Words and phrases associated with the Mosaic Segment';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_summary      IS 'Sentence describing the people in the Mosaic Segment';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_description  IS 'Detailed multi-paragraph description of Mosaic Segment';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_group_howtomarket    IS 'Suggestions for how to market to this Mosaic Group';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_link         IS 'PDF URL to more information about Mosaic Segment';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_segment_field_name   IS 'Segment name formatted for future join purposes';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.mosaic_group_field_name     IS 'Group name formatted for future join purposes';
COMMENT ON {usl_t2_schema}.experian_mosaic_segments.dw_sys_load_tmstp           IS 'Data load timestamp';

SET QUERY_BAND = NONE FOR SESSION;
