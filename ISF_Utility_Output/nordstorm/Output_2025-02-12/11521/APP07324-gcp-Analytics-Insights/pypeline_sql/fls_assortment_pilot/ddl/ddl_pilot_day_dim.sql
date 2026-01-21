SET QUERY_BAND = 'App_ID=APP07324;
    DAG_ID=ddl_fls_assortment_pilot_11521_ACE_ENG;
    Task_Name=ddl_pilot_day_dim;'
    FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_ccs_categories.pilot_day_dim
Team/Owner: Merch Insights / Thomas Peterson
Date Created/Modified: 6/19/2024
*/

-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'pilot_day_dim', OUT_RETURN_MSG);

CREATE MULTISET TABLE {shoe_categories_t2_schema}.pilot_day_dim 
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      day_date DATE FORMAT 'YY/MM/DD',
      day_num INTEGER,
      day_abrv VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      day_index INTEGER,
      first_day_of_week_flag BYTEINT,
      last_day_of_week_flag BYTEINT,
      week_num INTEGER,
      week_end_date DATE FORMAT 'YY/MM/DD',
      week_label VARCHAR(15) CHARACTER SET LATIN NOT CASESPECIFIC,
      week_index INTEGER,
      first_week_of_month_flag BYTEINT,
      last_week_of_month_flag BYTEINT,
      month_num INTEGER,
      month_abrv VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      month_label VARCHAR(14) CHARACTER SET LATIN NOT CASESPECIFIC,
      month_index INTEGER,
      quarter_num INTEGER,
      quarter_label VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      half_num INTEGER,
      half_label VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      year_num INTEGER,
      year_label VARCHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      true_week_num INTEGER,
	  dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )
PRIMARY INDEX ( day_num );

SET QUERY_BAND = NONE FOR SESSION;