/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=your_app_id;
     DAG_ID=your_dag_name_11521_ACE_ENG;
     Task_Name=your_sql_file_name_without_extension;'
     FOR SESSION VOLATILE;

/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'final_table_name', OUT_RETURN_MSG);

create multiset table {t2_schema}.final_table_name
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    column_1            date
    ,column_2           varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,column_3           char(10) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,column_4           integer compress
    ,column_5           numeric(12,2) compress
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(column_1)
partition by range_n(column_1 BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

-- Table Comment (STANDARD)
COMMENT ON  {t2_schema}.final_table_name IS 'Description of table';
-- Column comments (OPTIONAL)
COMMENT ON  {t2_schema}.final_table_name.column_1 IS 'column description';
COMMENT ON  {t2_schema}.final_table_name.column_2 IS 'column description';
COMMENT ON  {t2_schema}.final_table_name.column_3 IS 'column description';
COMMENT ON  {t2_schema}.final_table_name.column_4 IS 'column description';
COMMENT ON  {t2_schema}.final_table_name.column_5 IS 'column description';


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (column_name), -- column names used for primary index
                    COLUMN (column_name)  -- column names used for partition
on {t2_schema}.final_table_name;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;