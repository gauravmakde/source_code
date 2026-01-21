/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_big_drop_scope_metadata_11521_ACE_ENG;
     Task_Name=big_drop_scope_metadata;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

*/

CREATE VOLATILE TABLE tables_base AS (
SELECT  
        tv.databasename
        , tv.tablename
        , tv.tablekind
        , CASE WHEN bd.tablename IS NOT NULL THEN 'Y' ELSE NULL END AS big_drop_flag
        , CASE WHEN bd.databasename IN ('dba_work', 'PDCRDATA', 'PDCRINFO','DBC')
                OR bd.databasename LIKE 'PRD_MA_%'
                OR bd.databasename LIKE 'PRD_A%'
                OR bd.databasename LIKE 'PREPROD_%'
                OR bd.databasename LIKE 'PROTO_%'
                OR bd.databasename LIKE 'DEV_%'	
                OR bd.databasename LIKE 'T2DL_DAS_TECHEX'
                OR bd.databasename LIKE 'T2DL_DAS_BIE_DEV'
                THEN 'Y' ELSE NULL END AS out_of_scope
        , bd.comments as bigdrop_comment
        , bd.update_date as bigdrop_update_date
        , sv.rowcount as rowcount_current
        , sv.lastcollecttimestamp
        , SUM(ts.CURRENTPERM)/(1024.0*1024.0*1024.0) as tablesize_gb 	
FROM 	dbc.tablesv tv
LEFT JOIN DBC.tablesizeV ts ON tv.databasename=ts.databasename AND tv.tablename=ts.tablename
LEFT JOIN T2DL_DAS_TECHEX.teradata_big_drop_list bd ON tv.databasename = bd.databasename AND tv.tablename = bd.tablename
LEFT JOIN dbc.statsv sv ON tv.databasename=sv.databasename AND tv.tablename=sv.tablename AND sv.statsid =0
WHERE 	tv.tablekind IN ('T','V','O')
GROUP BY 1,2,3,4,5,6,7,8,9
) WITH DATA
PRIMARY INDEX(databasename, tablename)
ON COMMIT PRESERVE ROWS;
;

COLLECT STATISTICS  COLUMN (tablename) on tables_base;

CREATE VOLATILE TABLE tmp_reporting_objects AS (                   
select	site_name as site_name
        , workbook_name as workbook_name
        , table_name as tablename
        , 'tableau' as platform               
from 	T2DL_DAS_TECHEX.migration_reporting_objects  
UNION
select	project as site_name
        , project as workbook_name
        , object_name as tablename
        , 'microstrategy' as platform
from 	T2DL_DAS_TECHEX.microstrategy_migration_reporting_scope                   
) WITH DATA
PRIMARY INDEX(tablename)
ON COMMIT PRESERVE ROWS;
;   


CREATE VOLATILE TABLE tmp_terameta_objects AS (  
SELECT  object_name
        , terameta_appid
FROM (	SELECT  object_name
                , terameta_appid
                , ROW_NUMBER() OVER (PARTITION BY object_name ORDER BY COUNT(*) DESC) rn
        WHERE	terameta_appid IS NOT NULL		
        FROM	T2DL_DAS_TECHEX.terameta_objects
        GROUP BY 1,2 ) a 
WHERE	rn=1
) WITH DATA
PRIMARY INDEX(object_name)
ON COMMIT PRESERVE ROWS;
; 


CREATE VOLATILE TABLE tmp_manual_dag_object_mapping AS ( 
select	STRTOK(schema_object_name,'.',2) AS target_tablename
        , dag_id   
        , MIN(app_id) AS app_id       
from T2DL_DAS_TECHEX.manual_dag_object_mapping
WHERE 	source_target like '%targ%'
GROUP BY 1,2
) WITH DATA
PRIMARY INDEX(target_tablename)
ON COMMIT PRESERVE ROWS;
; 
                        

CREATE VOLATILE TABLE tmp_metadata_move_groups AS ( 
select	distinct object_name
        , last_value(table_merge_keys IGNORE NULLS) over (partition by object_name order by table_merge_keys DESC rows unbounded preceding) AS merge_key_columns
        , last_value(is_full_load IGNORE NULLS) over (partition by object_name order by is_full_load DESC rows unbounded preceding) AS is_full_load
        , last_value(refresh_cadence IGNORE NULLS) over (partition by object_name order by refresh_cadence DESC rows unbounded preceding) AS refresh_cadence
        , last_value(is_staging IGNORE NULLS) over (partition by object_name order by is_staging DESC rows unbounded preceding) AS is_staging
        , last_value(appid IGNORE NULLS) over (partition by object_name order by appid DESC rows unbounded preceding) AS app_id
        , last_value(sprint IGNORE NULLS) over (partition by object_name order by sprint DESC rows unbounded preceding) AS sprint
from T2DL_DAS_TECHEX.metadata_move_groups
) WITH DATA
PRIMARY INDEX(object_name)
ON COMMIT PRESERVE ROWS;
; 

CREATE VOLATILE TABLE tmp_manual_object_metadata_capture AS ( 
select DISTINCT object_name
        , last_value(nerds_appid IGNORE NULLS) over (partition by object_name order by nerds_appid DESC rows unbounded preceding) AS nerds_appid
        , last_value(key_columns IGNORE NULLS) over (partition by object_name order by key_columns DESC rows unbounded preceding) AS key_columns
        , last_value(merge_key_columns IGNORE NULLS) over (partition by object_name order by merge_key_columns DESC rows unbounded preceding) AS merge_key_columns
        , last_value(is_full_load IGNORE NULLS) over (partition by object_name order by is_full_load DESC rows unbounded preceding) AS is_full_load
        , last_value(is_staging IGNORE NULLS) over (partition by object_name order by is_staging DESC rows unbounded preceding) AS is_staging
        , last_value(pelican_audit_columns IGNORE NULLS) over (partition by object_name order by pelican_audit_columns DESC rows unbounded preceding) AS pelican_audit_columns
        , last_value(pelican_requirements IGNORE NULLS) over (partition by object_name order by pelican_requirements DESC rows unbounded preceding) AS pelican_requirements
        , last_value(refresh_schedule IGNORE NULLS) over (partition by object_name order by refresh_schedule DESC rows unbounded preceding) AS refresh_schedule
        , last_value(comments IGNORE NULLS) over (partition by object_name order by comments DESC rows unbounded preceding) AS comments
from T2DL_DAS_TECHEX.manual_object_metadata_capture
) WITH DATA
PRIMARY INDEX(object_name)
ON COMMIT PRESERVE ROWS;
; 




delete
from    {techex_t2_schema}.big_drop_scope_metadata
;

insert into {techex_t2_schema}.big_drop_scope_metadata
SELECT	a.*	
        , n.group_manager
        , n.group_director
        , n.group_vp
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
--select count(*)		
FROM (
        SELECT  DISTINCT
                tv.databasename
                , tv.tablename
                , tv.tablekind
                , tv.big_drop_flag
                , tv.out_of_scope
                , tv.bigdrop_comment
                , tv.bigdrop_update_date
                , omc.key_columns
                , coalesce(omc.merge_key_columns, mg.merge_key_columns) as merge_key_columns
                , coalesce(omc.is_full_load, mg.is_full_load)           as is_full_load
                , coalesce(omc.is_staging, mg.is_staging)               as is_staging
                , omc.pelican_audit_columns
                , omc.pelican_requirements
                , coalesce(omc.refresh_schedule, mg.refresh_cadence)    as refresh_schedule
                , omc.comments
                , dom.dag_id                                            as dag_id
                , coalesce(mg.sprint, 'Big Drop')                     as migration_phase
                , coalesce(dom.app_id, mg.app_id, omc.nerds_appid, tmo.terameta_appid)  as app_id
                , tom.rowcount as rowcount_snapshot
                , tv.rowcount_current
                , tv.tablesize_gb
                , tv.lastcollecttimestamp
                , tom.primary_index as teradata_index
                , tom.timestamp_count
                , tom.timestamp_columns
                , rep.platform
                , rep.site_name
                , rep.workbook_name	
        FROM 	tables_base tv
        LEFT JOIN T2DL_DAS_TECHEX.teradata_objects_metadata tom on tv.databasename = tom.databasename AND tv.tablename = tom.tablename
        
        -- Idenfity best app id using metadata move groups table AS primary AND teramata objects AS secondary.  Maxing app id incase of dupes		
        LEFT JOIN tmp_terameta_objects tmo ON tv.tablename = tmo.object_name	
                        
        LEFT JOIN tmp_metadata_move_groups mg ON tv.tablename = mg.object_name	

        LEFT JOIN tmp_manual_dag_object_mapping dom ON tv.tablename = dom.target_tablename		

        LEFT JOIN tmp_manual_object_metadata_capture omc on tv.tablename = omc.object_name
                        
        LEFT JOIN  tmp_reporting_objects rep on tv.tablename = rep.tablename  
                        
) A 
left join T2DL_DAS_TECHEX.techex_appid_nerds_info n on a.app_id = n.app_id
;


-- collect stats on final table
COLLECT STATISTICS  COLUMN (databasename,tablename),
                    COLUMN (databasename),
                    COLUMN (tablename)
on {techex_t2_schema}.big_drop_scope_metadata;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
