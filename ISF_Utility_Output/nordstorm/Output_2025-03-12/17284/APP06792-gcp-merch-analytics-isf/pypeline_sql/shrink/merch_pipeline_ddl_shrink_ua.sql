/* Create Two Tables that show shrink and a detailed and aggregated level */

-- Shrink Detail Weekly: Includes shrink c/r/u down to GL reference numbers
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'shrink_unit_adj_daily', OUT_RETURN_MSG);

CREATE MULTISET TABLE {environment_schema}.shrink_unit_adj_daily ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      day_date DATE FORMAT 'YY/MM/DD',
      week_end_day_date DATE FORMAT 'YY/MM/DD',
      location_num INTEGER,
      channel_num INTEGER,
      pi_count VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      gl_ref_no VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tran_code BYTEINT,
      shrink_category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      reason_description VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DEPARTMENT_NUM INTEGER,
      CLASS_NUM INTEGER,
      SUBCLASS_NUM INTEGER,
      brand_name VARCHAR(100) CHARACTER SET UNICODE UPPERCASE NOT CASESPECIFIC,
      supplier VARCHAR(100) CHARACTER SET UNICODE UPPERCASE NOT CASESPECIFIC,
      RMS_SKU_NUM VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
      shrink_cost DECIMAL(38,9),
      shrink_retail DECIMAL(38,9),
      shrink_units DECIMAL(38,0))
PRIMARY INDEX ( day_date ,location_num ,gl_ref_no ,DEPARTMENT_NUM ,
SUBCLASS_NUM ,brand_name );

GRANT SELECT ON {environment_schema}.shrink_unit_adj_daily TO PUBLIC;
