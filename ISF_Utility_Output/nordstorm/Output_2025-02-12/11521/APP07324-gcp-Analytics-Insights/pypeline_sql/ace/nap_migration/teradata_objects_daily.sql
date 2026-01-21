SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=teradata_objects_daily_11521_ACE_ENG;
     Task_Name=teradata_objects_daily;'
     FOR SESSION VOLATILE;



DELETE 
FROM    {techex_t2_schema}.teradata_objects_daily
WHERE   day_date between {start_date} and {end_date}
;

-- Join query log info to daily table snapshot.  Using a full outer join for the cases where the 
-- object being queried doesn't exist in DBC.TablesV
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
SELECT 	dc.day_date
		, tv.DataBaseName
		, tv.tablename
		, tv.tablekind
		, tv.creatorname
		, tv.createtimestamp
		, SUM(ts.CURRENTPERM)/(1024.0*1024.0*1024.0) as tablesize_gb
		, CAST((100 - (AVG(ts.CURRENTPERM)/MAX(ts.CURRENTPERM)*100.0)) AS FLOAT) as tableskew
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp    
FROM 	DBC.TablesV tv
LEFT JOIN DBC.tablesizeV ts ON tv.databasename=ts.databasename AND tv.tablename=ts.tablename
CROSS JOIN prd_nap_usr_vws.DAY_CAL dc
WHERE 	dc.day_date BETWEEN {start_date} AND {end_date}
GROUP BY 1,2,3,4,5,6,9
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (day_date), -- column names used for primary index
                    COLUMN (DataBaseName),  -- column names used for partition
                    COLUMN (tablename),
                    COLUMN (day_date, DataBaseName, tablename)
on {techex_t2_schema}.teradata_objects_daily;


SET QUERY_BAND = NONE FOR SESSION;