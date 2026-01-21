/*
Ops Scorecard DDL
Author: Soutrik Saha
Date Created: 1/6/23
Date Last Updated: 30/11/23

Datalab: 
Creates Table: 
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'WEEKLY_OPS_STANDUP', OUT_RETURN_MSG);

CREATE MULTISET TABLE {environment_schema}.WEEKLY_OPS_STANDUP
  ,FALLBACK
  ,NO BEFORE JOURNAL
  ,NO AFTER JOURNAL
  ,CHECKSUM = DEFAULT
  ,DEFAULT MERGEBLOCKRATIO
  ,MAP = TD_MAP1
(
  OPS_NAME	 	            VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,BANNER	 	              VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,CHANNEL	 	            VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,METRIC_NAME	 	        VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,FISCAL_NUM	 	          INTEGER
  ,FISCAL_DESC	 	        VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,LABEL	 	              VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC -- Chart Plot Axis (x-axis)
  ,ROLLING_FISCAL_IND	 	  INTEGER -- identifier for Rolling & Forward weeks 
  ,PLAN_OP	 	            FLOAT
  ,PLAN_CP	 	            FLOAT
  ,TY	 	                  FLOAT
  ,LY	 	                  FLOAT
  ,UPDATED_WEEK	 	        INTEGER -- most recent completed week 
  ,update_timestamp	 	    TIMESTAMP(6) WITH TIME ZONE 
  ,DAY_DATE               DATE                --Added new column to accomodate Holiday OPs Slides
) PRIMARY INDEX (OPS_NAME,BANNER,METRIC_NAME,FISCAL_NUM,LABEL);

