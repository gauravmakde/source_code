/* Create Two Tables that show shrink and a detailed and aggregated level */

-- Shrink Detail Weekly: Includes shrink c/r/u down to GL reference numbers
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'shrink_detail_weekly', OUT_RETURN_MSG);

CREATE MULTISET TABLE {environment_schema}.shrink_detail_weekly ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      week_end_day_date DATE FORMAT 'YY/MM/DD',
      location_num INTEGER,
      channel_num INTEGER,
      gl_ref_no VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      shrink_category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      reason_description VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DEPARTMENT_NUM INTEGER,
      CLASS_NUM INTEGER,
      SUBCLASS_NUM INTEGER,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      shrink_cost DECIMAL(38,9),
      shrink_retail DECIMAL(38,9),
      shrink_units DECIMAL(38,0))
PRIMARY INDEX ( week_end_day_date ,location_num ,gl_ref_no ,DEPARTMENT_NUM ,
SUBCLASS_NUM ,brand_name );

GRANT SELECT ON {environment_schema}.shrink_detail_weekly TO PUBLIC;


-- Shrink Summary Weekly: Includes sales & shrink c/r/u down to store/brand/week
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'shrink_summary_weekly', OUT_RETURN_MSG);

CREATE MULTISET TABLE {environment_schema}.shrink_summary_weekly ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      store_name VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      region_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC,
      banner VARCHAR(14) CHARACTER SET UNICODE NOT CASESPECIFIC,
      week_454_label VARCHAR(33) CHARACTER SET LATIN NOT CASESPECIFIC,
      week_idnt INTEGER,
      fiscal_week_num SMALLINT,
      month_454_label VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC,
      month_idnt INTEGER,
      quarter_label VARCHAR(17) CHARACTER SET LATIN NOT CASESPECIFIC,
      half_label VARCHAR(15) CHARACTER SET LATIN NOT CASESPECIFIC,
      fiscal_year_num INTEGER,
      ty_ly_lly_ind VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_region VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      shrink_cup_group VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      week_end_day_date DATE FORMAT 'YY/MM/DD',
      location_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_num INTEGER,
      shrink_category VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      department_num INTEGER,
      class_num INTEGER,
      subclass_num INTEGER,
      brand_name VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      shrink_c DECIMAL(38,9),
      shrink_u DECIMAL(38,0),
      shrink_r DECIMAL(38,9),
      sales_c DECIMAL(38,9),
      sales_u INTEGER,
      sales_r DECIMAL(38,9))
PRIMARY INDEX ( week_end_day_date ,location_num ,department_num ,
subclass_num ,brand_name );

GRANT SELECT ON {environment_schema}.shrink_summary_weekly TO PUBLIC;
