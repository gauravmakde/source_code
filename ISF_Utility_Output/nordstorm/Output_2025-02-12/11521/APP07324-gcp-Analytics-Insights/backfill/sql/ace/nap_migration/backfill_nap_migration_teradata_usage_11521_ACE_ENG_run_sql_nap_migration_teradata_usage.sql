SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_teradata_usage_11521_ACE_ENG;
     Task_Name=nap_migration_teradata_usage;'
     FOR SESSION VOLATILE;


-- Identify all tables in Teradata, along with their size and skew.  Using a cross join to day_cal
-- to get a daily snapshot for our lookback window.
CREATE multiset volatile table all_tables as ( 
SELECT dc.day_date
		, tv.DataBaseName
		, tv.tablename
		, tv.tablekind
		, tv.creatorname
		, tv.createtimestamp
		, CAST((100 - (AVG(ts.CURRENTPERM)/MAX(ts.CURRENTPERM)*100)) AS INTEGER) as tableskew
		, SUM(ts.CURRENTPERM)/(1024*1024*1024) as tablesize
FROM 	DBC.TablesV tv
LEFT JOIN DBC.tablesize ts ON tv.databasename=ts.databasename AND tv.tablename=ts.tablename
CROSS JOIN prd_nap_usr_vws.DAY_CAL dc
WHERE 	tv.TableKind IN ('V', 'T', 'O')
AND		dc.day_date BETWEEN '2024-04-15' AND '2024-05-17'
--AND		dc.day_date BETWEEN current_date-31 AND current_date-1
--AND tv.DataBaseName LIKE ANY ('labs_%', 'T2DL_%', 'T3DL_%', 'DL_ACEBI%', 'DL_CMA_%', 'DL_SCM_%')
--AND tv.TableName NOT LIKE 'priv_%'
GROUP BY 1,2,3,4,5,6
) with data primary index (day_date, databasename, tablename) on commit preserve rows ;


-- Identify querylog information (counts and users) for lookback period.
CREATE multiset volatile table all_queries as ( 
SELECT DISTINCT 
			a.logdate
			, a.objectdatabasename
			, a.objecttablename
			, a.objecttype
			, b.statementtype
			, b.username
			, count(distinct a.queryid) as query_count
			, sum(ampcputime) as ampcputime
	FROM 	pdcrinfo.dbqlobjtbl_hst a
	INNER JOIN pdcrinfo.dbqlogtbl_hst b
	ON 		a.queryid=b.queryid
	--AND 	a.procid=b.procid
	--AND		a.logdate=b.logdate
	WHERE 	a.logdate BETWEEN '2024-04-15' AND '2024-05-17'
	AND 	b.logdate BETWEEN '2024-04-15' AND '2024-05-17'
	--WHERE 	a.logdate BETWEEN current_date-31 AND current_date-1
	--AND 	b.logdate BETWEEN current_date-31 AND current_date-1
	--AND a.objectdatabasename LIKE ANY ('labs_%', 'T2DL_%', 'T3DL_%', 'DL_ACEBI%', 'DL_CMA_%', 'DL_SCM_%')
	AND 	a.objecttype IN ('Tab','Viw')
	AND 	b.numsteps <> 0
	AND 	b.username NOT IN
			('TDPUSER', 'Crashdumps', 'tdwm', 'DBC',
		    'LockLogShredder', 'TDMaps', 'Sys_Calendar', 'SysAdmin',
		    'SystemFe', 'External_AP', 'All', 'console', 'Default',
		    'ViewPoint', 'PUBLIC', 'TD_ESTIMATOR', 'END_USERS', 'EXTUSER',
		    'DBASTATS', 'SYSDBA', 'DBABKP', 'PDCRACCESS', 'PDCRADMIN')
GROUP BY 1,2,3,4,5,6
) with data primary index (logdate, objectdatabasename, objecttablename) on commit preserve rows ;


DELETE 
FROM    T2DL_DAS_TECHEX.nap_migration_teradata_usage
WHERE   day_date between '2024-04-15' and '2024-05-17'
;

-- Join query log info to daily table snapshot.  Using a full outer join for the cases where the 
-- object being queried doesn't exist in DBC.TablesV
INSERT INTO T2DL_DAS_TECHEX.nap_migration_teradata_usage
SELECT 	COALESCE(obj.day_date, qo.logdate) as day_date
		, COALESCE(obj.DataBaseName, qo.objectdatabasename) AS databasename
		, COALESCE(obj.tablename, qo.objecttablename) AS tablename
		, obj.tablekind
		, CASE WHEN qo.query_count>=1 then 1 else 0 end AS active
		, obj.creatorname
		, obj.createtimestamp
		, obj.tablesize
		, obj.tableskew
		, qo.query_count
		, qo.ampcputime
		--, qo.objecttype
		, qo.statementtype
		, qo.username
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp    
FROM	all_tables obj
FULL OUTER JOIN all_queries qo
	ON 	obj.DatabaseName = qo.objectdatabasename
	AND obj.tablename = qo. objecttablename
	AND	obj.day_date = qo.logdate
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (day_date), -- column names used for primary index
                    COLUMN (databasename),  -- column names used for partition
                    COLUMN (tablename),
                    COLUMN (day_date, databasename, tablename)
on T2DL_DAS_TECHEX.nap_migration_teradata_usage;


SET QUERY_BAND = NONE FOR SESSION;