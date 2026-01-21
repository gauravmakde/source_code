   /*
Purpose:        Creates empty tables in {{environment_schema}} for Item Performance
                    item_intent_lookup

Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING 
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Rasagnya Avala
Date Created/Modified: 07/25/2024

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'item_intent_lookup{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.item_intent_lookup{env_suffix} ,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      plan_week INTEGER,
      plan_month varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
      plan_year INTEGER,
      banner CHAR(15)  CHARACTER SET UNICODE NOT CASESPECIFIC, 
      channel_country CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC, 
      channel_num INTEGER,  
      channel_desc VARCHAR(46) character set unicode not casespecific, 
      dept_num INTEGER,
      rms_style_num VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sku_nrf_color_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      vpn VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier_color VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
      intended_plan_type VARCHAR(35) CHARACTER SET UNICODE NOT CASESPECIFIC,
      intended_season VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      intended_exit_month_year VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      intended_lifecycle_type VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      scaled_event VARCHAR(35) CHARACTER SET UNICODE NOT CASESPECIFIC,
      holiday_or_celebration VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( plan_week ,rms_style_num, dept_num ,channel_num)
PARTITION BY RANGE_N(plan_week  BETWEEN 201901 AND 202552 EACH 1);

GRANT SELECT ON {environment_schema}.item_intent_lookup{env_suffix} TO PUBLIC;
