SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=teradata_objects_daily_11521_ACE_ENG;
     Task_Name=teradata_objects_daily;'
     FOR SESSION VOLATILE;


-- Create back-up table from old table definition
create multiset table {techex_t2_schema}.teradata_objects_daily_backup
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio 
(
        day_date                  DATE FORMAT 'YYYY-MM-DD'
        , databasename              varchar (64) CHARACTER SET UNICODE NOT CASESPECIFIC
        , tablename                 varchar (80) CHARACTER SET UNICODE NOT CASESPECIFIC
        , tablekind                 varchar (1) CHARACTER SET UNICODE NOT CASESPECIFIC
        , creatorname               varchar (40) CHARACTER SET UNICODE NOT CASESPECIFIC
        , createtimestamp           timestamp
        , tablesize                 integer
        , tableskew                 integer        
        , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    ) 
PRIMARY INDEX(day_date, databasename, tablename)
partition by range_n(day_date between date '2021-01-01' and date '2025-12-31' each interval '1' day);


-- Insert old table data into back-up table.
INSERT INTO {techex_t2_schema}.teradata_objects_daily_backup
SELECT *
FROM  T2DL_DAS_TECHEX.teradata_objects_daily;

-- Will drop old table after verifying data.
-- Will create table with new table definition via the DDL airflow dag.

SET QUERY_BAND = NONE FOR SESSION;