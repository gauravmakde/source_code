/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_teradata_objects_metadata_11521_ACE_ENG;
     Task_Name=teradata_objects_metadata;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/

/*********************************************************************************
 * TD OBJECTS SNAPSHOT
 *********************************************************************************/

-- 60 day query activity
CREATE MULTISET VOLATILE TABLE teradata_query_activity AS (
SELECT  a.logdate
		, a.objectdatabasename
		, a.objecttablename
		, a.objecttype
		, b.statementtype
		, b.username
		, COUNT(DISTINCT a.queryid) AS query_count
        , CURRENT_TIMESTAMP AS dw_sys_load_tmstp  
FROM 	pdcrinfo.dbqlobjtbl_hst a
INNER JOIN pdcrinfo.dbqlogtbl_hst b ON a.queryid=b.queryid AND a.procid=b.procid
WHERE 	a.logdate  >= current_date()-61
AND 	b.logdate  >= current_date()-61
AND 	b.numsteps <> 0
AND  	a.objectcolumnname IS NULL
GROUP BY 1,2,3,4,5,6
) WITH DATA
PRIMARY INDEX(objectdatabasename, objecttablename)
PARTITION BY RANGE_N(logdate BETWEEN DATE '2024-01-01' AND DATE '2024-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS;
;


CREATE MULTISET VOLATILE TABLE teradata_objects AS (
SELECT	tv.databasename
		, tv.tablename
		, tv.tablekind 
		, tv.commentstring
		, tv.creatORname
		, tv.createtimestamp
		, tv.lastaccesstimestamp 
		, sv.RowCount 
		, sv.LastCollectTimeStamp
		, SUM(ts.CURRENTPERM)/(1024.0*1024.0*1024.0) AS tablesize_gb
FROM	DBC.tablesV tv
LEFT JOIN DBC.tablesizeV ts ON tv.databasename=ts.databasename AND tv.tablename=ts.tablename
LEFT JOIN dbc.statsv sv ON tv.databasename=sv.databasename AND tv.tablename=sv.tablename AND sv.statsid =0
GROUP BY 1,2,3,4,5,6,7,8,9
) WITH DATA
PRIMARY INDEX(DataBaseName, tablename)
ON COMMIT PRESERVE ROWS;
;


CREATE MULTISET VOLATILE TABLE teradata_objects_snapshot AS (
SELECT	tod.*
		, tqa.query_count_60_days
		, tqa.DISTINCT_users_60_days
		, tqa.max_query_date
		, CURRENT_DATE() AS snapshot_date
FROM 	teradata_objects tod
LEFT JOIN (	SELECT 	objectdatabasename
					, objecttablename
					, sum(query_count) AS query_count_60_days
					, COUNT(DISTINCT username) AS DISTINCT_users_60_days
					, MAX(logdate) AS max_query_date
			FROM 	teradata_query_activity
			GROUP BY 1,2 ) tqa ON tod.databasename = tqa.objectdatabasename AND tod.tablename = tqa.objecttablename
) WITH DATA
PRIMARY INDEX(DataBaseName, tablename)
ON COMMIT PRESERVE ROWS;
;


/********************************************************************************* 
* INDEXES
 *********************************************************************************/


-- Create a volatile table to store concatenated column names AND access counts
-- drop table indexes_base
CREATE VOLATILE TABLE indexes_base AS (
SELECT	databasename,
	    tablename,
	    columnname,
	    indexnumber,
	    IndexType,
	    MAX(i.accesscount) AS AccessCount
FROM	DBC.IndicesV i
GROUP BY	databasename,
		    tablename,
		    columnname,
		    indexnumber,
		    IndexType
) WITH DATA PRIMARY INDEX (databasename, tablename) ON COMMIT PRESERVE ROWS;

/*
 
SELECT 	DISTINCT indextype
FROM	indexes_base
WHERE	indexnumber = 1

 */


CREATE VOLATILE TABLE teradata_indexes_snapshot AS (
SELECT 	pi.databasename
		, pi.tablename
		, CASE 
             WHEN pi.indextype = 'P' THEN 'Nonpartitioned primary index'
             WHEN pi.indextype = 'Q' THEN 'Partitioned primary index'
             WHEN pi.indextype = 'A' THEN 'Primary AMP index'
             WHEN pi.indextype = 'K' THEN 'Primary key'
             WHEN pi.indextype = 'U' THEN 'Unique constraint'
             ELSE pi.indextype
             END AS IndexType
		, pi.primary_index
		, si.additional_index_count
		, si.index_array
		, CURRENT_DATE() AS snapshot_date
FROM 	-- PRIMARY INDEX
		(	SELECT	DatabaseName,
				    TableName,
					indextype
					, TRIM(Trailing ',' FROM (XmlAgg(TRIM(columnname) || ',' ORDER BY columnname) (VARCHAR(10000)))) AS primary_index  
			FROM  indexes_base
			WHERE indexnumber = 1
			GROUP BY 1,2,3 ) pi 
LEFT JOIN  -- ADDITIONAL INDEXES
		(	SELECT	DatabaseName,
				    TableName
				    , COUNT(DISTINCT indexnumber) AS additional_index_count
					, ARRAY_AGG( secondary_index ORDER BY indexnumber, NEW index_array()) AS index_array
			FROM  (	SELECT	DatabaseName,
						    TableName,
							indexnumber
							, TRIM(Trailing ',' FROM (XmlAgg(TRIM(columnname) || ',' ORDER BY columnname) (VARCHAR(10000)))) AS secondary_index  
					FROM	indexes_base
					WHERE 	indexnumber >1
					GROUP BY 1,2,3 ) a 
			GROUP BY 1,2) si ON pi.databasename = si.databasename AND pi.tablename = si.tablename
) WITH DATA
PRIMARY INDEX (databasename, tablename)
ON COMMIT PRESERVE ROWS;
;



/*********************************************************************************
 * TIMESTAMP COLUMNS
 *********************************************************************************/

CREATE MULTISET VOLATILE TABLE teradata_timestamps_snapshot AS (
SELECT	DatabaseName
	    , TableName
	    , COUNT(DISTINCT columnname) AS timestamp_count
		, TRIM(Trailing ',' FROM (XmlAgg(TRIM(columnname) || ',' ORDER BY columnname) (VARCHAR(10000)))) AS timestamp_columns
		, CURRENT_DATE() AS snapshot_date 
FROM 	dbc.columnsv
WHERE 	columntype in ('tz','ts','sz')   --- excluding ansi time AND interval column types
GROUP BY 1,2
) WITH DATA
PRIMARY INDEX (databasename, tablename)
ON COMMIT PRESERVE ROWS;
;


/*
CREATE VOLATILE TABLE columns_base AS (
SELECT  DatabaseName,
        TableName,
        ColumnName,
        CASE ColumnType
            WHEN 'DA' THEN 'Date'
            WHEN 'AT' THEN 'ANSI Time'
            WHEN 'TS' THEN 'Timestamp'
            WHEN 'TZ' THEN 'ANSI Time With Time Zone'
            WHEN 'SZ' THEN 'Timestamp With Time Zone'
            WHEN 'YR' THEN 'Interval Year'
            WHEN 'YM' THEN 'Interval Year To Month'
            WHEN 'MO' THEN 'Interval Month'
            WHEN 'DY' THEN 'Interval Day'
            WHEN 'DH' THEN 'Interval Day To Hour'
            WHEN 'DM' THEN 'Interval Day To Minute'
            WHEN 'DS' THEN 'Interval Day To Second'
            WHEN 'HR' THEN 'Interval Hour'
            WHEN 'HM' THEN 'Interval Hour To Minute'
            WHEN 'HS' THEN 'Interval Hour To Second'
            WHEN 'MI' THEN 'Interval Minute'
            WHEN 'MS' THEN 'Interval Minute To Second'
            WHEN 'SC' THEN 'Interval Second'
            END AS DataType
FROM    DBC.ColumnsV
WHERE   ColumnType in ('DA', 'AT', 'TS', 'TZ', 'SZ', 'YR', 'YM', 'MO',
        'DY', 'DH', 'DM', 'DS', 'HR', 'HM', 'HS', 'MI', 'MS', 'SC')
) WITH DATA PRIMARY INDEX (databasename, tablename) ON COMMIT PRESERVE ROWS
;*/



/*********************************************************************************
 * CREATE VIEW
 *********************************************************************************/

delete
from    {techex_t2_schema}.teradata_objects_metadata
;

insert into {techex_t2_schema}.teradata_objects_metadata
SELECT 	tos.databasename
		, tos.tablename
		, CASE WHEN tos.tablekind in ('T','O') THEN 'TABLE' ELSE 'VIEW' END AS object_type
		, tos.commentstring
		, tos.creatorname
		, tos.createtimestamp
		, tos.rowcount
		, tos.lastcollecttimestamp
		, tos.tablesize_gb
		, tos.lastaccesstimestamp
		, tos.query_count_60_days
		, tos.DISTINCT_users_60_days
		, COALESCE(mg.appid, tmo.terameta_appid) AS best_app_id
		, tis.indextype AS primary_indextype
		, tis.primary_index
		, tis.additional_index_count
		, tis.index_array AS additional_index_array
		, tts.timestamp_count
		, tts.timestamp_columns		
		, tos.snapshot_date AS object_snapshot_date
		, tos.exclude_flag
		, tos.exclude_type
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
-- identify list of objects.  Including only tables AND views.  Exclusions based ON requirements		
FROM (	SELECT  tos.*
				, CASE WHEN tos.databasename like 'PRD_A%'  -- exclude aedw
						or tos.databasename like 'PRD_PROC%' -- exclude aedw
						or tos.databasename like 'ACCOPR_AW_WRK'  -- exclude aedw
						or tos.databasename like 'T3DL%'  -- exclude T3s
						or tos.databasename like 'PRD_MA_BADO'  -- exclude madm
						or tos.databasename like 'ARTS_%'  -- exclude madm
						or tos.databasename like 'prd_usr_vws' -- exclude madm
						or tos.databasename like '%_STG'
						or tos.databasename like '%_LDG'
						or tos.tablename like 'ZZZ_%'
						or tos.tablename like '%_drop_20240911'
						or tos.tablename like 'TPT_%'
						or tablename like '%TEMP%'
						or tablename like '%TMP%'
						or tablename like '%ml__fl_stag%'
						or tos.tablename like '%_ERR'
						or tos.tablename like '%_LDG'
						or tos.tablename like '%_STG'
						or CHARACTER_LENGTH(databasename) = 4
						THEN 'Y' END AS exclude_flag
					, CASE WHEN tos.databasename like 'PRD_A%'   -- exclude aedw
							or tos.databasename like 'PRD_PROC%' -- exclude aedw
							or tos.databasename like 'ACCOPR_AW_WRK' 
								THEN 'AEDW'-- exclude aedw
							WHEN tos.databasename like 'T3DL%'
								THEN 'T3DL'-- exclude T3s
							WHEN tos.databasename like 'PRD_MA_BADO'  -- exclude madm
							or tos.databasename like 'ARTS_%'  -- exclude madm
							or tos.databasename like 'prd_usr_vws' -- exclude madm
								THEN 'MADM'
							WHEN tos.databasename like '%_STG'
							or tos.databasename like '%_LDG'
								THEN 'STG'
							WHEN tos.tablename like 'ZZZ_%'
							or tos.tablename like '%_drop_20240911'
								THEN 'INACTIVE'
							WHEN tos.tablename like 'TPT_%'
							or tablename like '%ml__fl_stag%'
							or tos.tablename like '%_ERR'
								THEN 'SYSTEM'
							WHEN tablename like '%TEMP%'
							or tablename like '%TMP%'
								THEN 'TEMP'
							WHEN tos.tablename like '%_LDG'
							or tos.tablename like '%_STG'
								THEN 'STG'
							WHEN CHARACTER_LENGTH(databasename) = 4 
								THEN 'USER'
							END AS exclude_type
		FROM 	teradata_objects_snapshot tos
		WHERE 	tablekind in ('T','O','V')  -- include tables AND views		
		) tos		

-- Idenfity best app id using metadata move groups table AS primary AND teramata objects AS secondary.  Maxing app id incase of dupes		
LEFT JOIN (	SELECT	object_schema 
					, object_name 
					, MAX(CASE WHEN appid like 'app%' THEN appid ELSE NULL END) appid
			FROM {techex_t2_schema}.metadata_move_groups 
			GROUP BY 1,2 ) mg ON tos.databasename = mg.object_schema AND tos.tablename = mg.object_name	
LEFT JOIN (	SELECT	object_database
					, object_name
					, MAX(CASE WHEN terameta_appid like 'app%' THEN terameta_appid ELSE NULL END) terameta_appid
			FROM	{techex_t2_schema}.terameta_objects
			GROUP BY 1,2) tmo ON tos.databasename = tmo.object_database AND tos.tablename = tmo.object_name			

LEFT JOIN teradata_indexes_snapshot tis ON tos.databasename = tis.databasename AND tos.tablename = tis.tablename			
LEFT JOIN teradata_timestamps_snapshot tts ON tos.databasename = tts.databasename AND tos.tablename = tts.tablename			
;


-- collect stats on final table
COLLECT STATISTICS  COLUMN (databasename,tablename),
                    COLUMN (databasename),
                    COLUMN (tablename) 
on {techex_t2_schema}.teradata_objects_metadata
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


