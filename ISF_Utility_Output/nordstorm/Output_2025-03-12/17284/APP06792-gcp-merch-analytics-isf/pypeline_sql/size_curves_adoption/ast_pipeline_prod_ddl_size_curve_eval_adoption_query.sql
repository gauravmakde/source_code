/*
ast_pipeline_prod_ddl_size_curve_eval_adoption_query.sql
Author: Paria Avij
project: Size curve evaluation
Purpose: Create a table for size curve dashboard monthly adoption view
Date Created: 12/26/23
Datalab: t2dl_das_size,

*/

REPLACE VIEW {environment_schema}.adoption_metrics_vw AS
LOCK ROW FOR ACCESS
WITH adoption AS (
    SELECT
         a.fiscal_month
        ,banner
        ,CASE WHEN channel_id = 110 THEN 'FLS'
              WHEN channel_id = 210 THEN 'RACK'
              WHEN channel_id = 250 THEN 'NRHL'
              WHEN channel_id = 120 THEN 'N.COM'
              ELSE NULL 
              END AS chnl_idnt
        ,dept_id
        ,size_profile
        ,SUM(rcpt_units) AS rcpt_units
    FROM t2dl_das_size.adoption_metrics a
    GROUP BY 1,2,3,4,5
)
SELECT
     a.fiscal_month AS month_idnt
    ,a.banner
    ,a.chnl_idnt
    ,a.dept_id
	,COALESCE(SUM(CASE WHEN size_profile = 'ARTS' THEN rcpt_units END), 0) AS arts_curecs_rcpt_u
	,COALESCE(SUM(CASE WHEN size_profile = 'EXISTING CURVE' THEN rcpt_units END), 0) AS existing_curves_rcpt_u
	,COALESCE(SUM(CASE WHEN size_profile = 'NONE' THEN rcpt_units end), 0) AS none_curves_rcpt_u
	,COALESCE(SUM(CASE WHEN size_profile = 'OTHER' THEN rcpt_units end), 0) AS other_curves_rcpt_u
	,COALESCE(SUM(CASE WHEN size_profile = 'DSA' THEN rcpt_units END), 0) AS dsa_curves_rcpt_u
	,SUM(rcpt_unitS) AS curves_rcpt_u
	,COALESCE(dsa_curves_rcpt_u / NULLIFZERO(curves_rcpt_u * 1.0000), 0) AS dsa_adoption_rate
FROM adoption a
WHERE dept_id IN (SELECT DISTINCT department_id FROM t2dl_das_size.size_curve_baseline_hist) 
GROUP BY 1,2,3,4
;