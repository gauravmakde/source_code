SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ccs_kpi_monthly_11521_ACE_ENG;
     Task_Name=ddl_ccs_kpi_monthly;'
     FOR SESSION VOLATILE;


/*

T2/Table Name: t2dl_das_ccs_categories.ccs_kpi_monthly
Team/Owner: Merchandising Insights
Date Created/Modified: 4/29/2024

Note:
-- Purpose: customer metrics on CCS Category and department levels
-- Cadence: Runs weekly on Sunday at 15:30 
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'ccs_kpi_monthly', OUT_RETURN_MSG);
create multiset table {shoe_categories_t2_schema}.ccs_kpi_monthly
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	data_level VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
    banner VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
    ccs_role VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
    ccs_subcategory VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
    department_label VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
    ty_ly_lly_ind VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
    trip_count INTEGER COMPRESS ,
    total_customer_count INTEGER COMPRESS ,
    aqc_customer_count INTEGER COMPRESS ,
    retained_customer_count INTEGER COMPRESS ,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL

    )
PRIMARY INDEX ( banner ,ccs_role ,ccs_subcategory ,department_label ,
ty_ly_lly_ind );

SET QUERY_BAND = NONE FOR SESSION;  



