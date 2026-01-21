SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=teradata_objects_daily_11521_ACE_ENG;
     Task_Name=teradata_objects_daily;'
     FOR SESSION VOLATILE;


-- Will create table with new table definition via the DDL airflow dag.

-- Swapping tablesize/tableskew column on insert because there were in the
-- wrong order on the old table
INSERT INTO {techex_t2_schema}.teradata_objects_daily
	(	day_date
		, databasename
		, tablename
		, tablekind
		, creatorname
		, createtimestamp
		, tablesize_gb
        , tableskew
		, dw_sys_load_tmstp
		)
SELECT 	day_date
		, databasename
		, tablename
		, tablekind
		, creatorname
		, createtimestamp
		, tableskew
        , tablesize
		, dw_sys_load_tmstp    
FROM 	{techex_t2_schema}.teradata_objects_daily_backup
;

-- Will drop backup table after verifying data.


SET QUERY_BAND = NONE FOR SESSION;