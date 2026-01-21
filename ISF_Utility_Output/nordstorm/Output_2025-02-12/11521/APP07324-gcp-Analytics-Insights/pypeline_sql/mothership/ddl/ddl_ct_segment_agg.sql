SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=ddl_mothership_ct_segment_agg_11521_ACE_ENG;
     Task_Name=ddl_ct_segment_agg;'
     FOR SESSION VOLATILE;

/*
T2DL_DAS_MOTHERSHIP.CT_SEGMENT_AGG
Description: get segmentation model results but only keep rows where segment changes. Fewer rows = faster join later
this is also the slowest step in the mothership pipeline, so if we run it ahead of time then the morning pipeline is faster
Contacts: Matthew Bond, Analytics
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'CT_SEGMENT_AGG', OUT_RETURN_MSG);

 CREATE MULTISET TABLE {mothership_t2_schema}.CT_SEGMENT_AGG,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
        acp_id         	              VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
				scored_date                	  DATE,
        predicted_segment         	  VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
        seg_lag         	            VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
        seg_lead         	            VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
		    dw_sys_load_tmstp             TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
     PRIMARY INDEX (acp_id,scored_date);

SET QUERY_BAND = NONE FOR SESSION;