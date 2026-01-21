SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=teradata_objects_daily_11521_ACE_ENG;
     Task_Name=teradata_objects_daily;'
     FOR SESSION VOLATILE;



DELETE 
FROM    T2DL_DAS_TECHEX.teradata_objects_daily
WHERE   day_date between '2024-05-01' and '2024-06-06'
;

-- Join query log info to daily table snapshot.  Using a full outer join for the cases where the 
-- object being queried doesn't exist in DBC.TablesV
INSERT INTO T2DL_DAS_TECHEX.teradata_objects_daily
SELECT 	dc.day_date
		, tv.DataBaseName
		, tv.tablename
		, tv.tablekind
		, tv.creatorname
		, tv.createtimestamp
		, CAST((100 - (AVG(ts.CURRENTPERM)/MAX(ts.CURRENTPERM)*100)) AS INTEGER) as tableskew
		, SUM(ts.CURRENTPERM)/(1024*1024*1024) as tablesize
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp    
FROM 	DBC.TablesV tv
LEFT JOIN DBC.tablesize ts ON tv.databasename=ts.databasename AND tv.tablename=ts.tablename
CROSS JOIN prd_nap_usr_vws.DAY_CAL dc
WHERE 	dc.day_date BETWEEN '2024-05-01' AND '2024-06-06'
GROUP BY 1,2,3,4,5,6,9
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (day_date), -- column names used for primary index
                    COLUMN (DataBaseName),  -- column names used for partition
                    COLUMN (tablename),
                    COLUMN (day_date, DataBaseName, tablename)
on T2DL_DAS_TECHEX.teradata_objects_daily;


SET QUERY_BAND = NONE FOR SESSION;