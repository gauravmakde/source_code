SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=teradata_query_activity_daily_11521_ACE_ENG;
     Task_Name=teradata_query_activity_daily;'
     FOR SESSION VOLATILE;



DELETE 
FROM    T2DL_DAS_TECHEX.teradata_query_activity_daily
WHERE   logdate between '2024-07-01' and '2024-07-17'
;

-- Join query log info to daily table snapshot.  Using a full outer join for the cases where the 
-- object being queried doesn't exist in DBC.TablesV
INSERT INTO T2DL_DAS_TECHEX.teradata_query_activity_daily
SELECT  a.logdate
		, a.objectdatabasename
		, a.objecttablename
		, a.objecttype
		, b.statementtype
		, b.username
		, count(distinct a.queryid) as query_count
          , CURRENT_TIMESTAMP as dw_sys_load_tmstp  
FROM 	pdcrinfo.dbqlobjtbl_hst a
INNER JOIN pdcrinfo.dbqlogtbl_hst b ON a.queryid=b.queryid AND a.procid=b.procid
WHERE 	a.logdate BETWEEN '2024-07-01' AND '2024-07-17'
AND 	b.logdate BETWEEN '2024-07-01' AND '2024-07-17'
--AND 	b.numsteps <> 0
--AND  a.objectcolumnname is NULL
GROUP BY 1,2,3,4,5,6
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (logdate), -- column names used for primary index
                    COLUMN (objectdatabasename),  -- column names used for partition
                    COLUMN (objecttablename),
                    COLUMN (logdate, objectdatabasename, objecttablename)
on T2DL_DAS_TECHEX.teradata_query_activity_daily;


SET QUERY_BAND = NONE FOR SESSION;