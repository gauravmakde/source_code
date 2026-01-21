SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=nap_migration_planning_vw_11521_ACE_ENG;
     Task_Name=nap_migration_planning_vw;'
     FOR SESSION VOLATILE;


REPLACE VIEW {techex_t2_schema}.nap_migration_planning_vw
AS
LOCK ROW FOR ACCESS
SELECT  tu.*
        , mg.move_group
        , s.sprint
        , tfi.first_name
        , tfi.last_name
        , tfi.manager
        , tfi.email
        , tfi.active AS active_employee
FROM  (SELECT 	COALESCE(obj.day_date, qo.logdate) as day_date
				, COALESCE(obj.DataBaseName, qo.objectdatabasename) AS databasename
				, COALESCE(obj.tablename, qo.objecttablename) AS tablename
				, obj.tablekind
				, CASE WHEN qo.query_count>=1 then 1 else 0 end AS active
				, obj.creatorname
				, obj.createtimestamp
				, obj.tablesize_gb
				, obj.tableskew
				, qo.query_count
				, qo.objecttype
				, qo.statementtype
				, qo.username   
		FROM	(	SELECT	*
					FROM	{techex_t2_schema}.teradata_objects_daily) obj
		FULL OUTER JOIN (	SELECT	*
							FROM	{techex_t2_schema}.teradata_query_activity_daily ) qo
			ON 	obj.DatabaseName = qo.objectdatabasename
			AND obj.tablename = qo. objecttablename
			AND	obj.day_date = qo.logdate)  tu
LEFT JOIN T2DL_DAS_TECHEX.nap_migration_move_groups mg ON tu.databasename = mg.databasename
                                                        AND tu.tablename = mg.tablename
LEFT JOIN T2DL_DAS_TECHEX.nap_migration_sprints s ON tu.databasename = s.databasename
                                                    AND tu.tablename = s.tablename
LEFT JOIN T2DL_DAS_TECHEX.techex_fetch_info tfi ON tu.username = tfi.lan_id
;

SET QUERY_BAND = NONE FOR SESSION;