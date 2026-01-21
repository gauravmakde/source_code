/******************************************************************************
** START: complete_fiscal_week_vw
******************************************************************************/
/******************************************************************************
** FILE: pmo_pipeline_prod_ddl_complete_fiscal_week_vw.sql
** TABLE: T2DL_DAS_STRATEGIC_BRAND_MANAGEMENT_REPORTING.complete_fiscal_week_vw
**
** AUTHOR:
**      - Asiyah Fox (asiyah.fox@nordstrom.com)
******************************************************************************/

REPLACE VIEW {environment_schema}.complete_fiscal_week_vw 
AS
LOCK ROW FOR ACCESS
SELECT DISTINCT
	month_idnt
	,month_start_day_date
	,month_start_day_idnt
	,month_end_day_date
	,month_end_day_idnt
	,week_idnt
	,week_start_day_date
	,week_start_day_idnt
	,week_end_day_date
	,week_end_day_idnt
FROM prd_nap_usr_vws.day_cal_454_dim
WHERE day_date =
	(SELECT MAX(week_end_day_date) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date <= CURRENT_DATE)
;
GRANT SELECT ON {environment_schema}.complete_fiscal_week_vw TO PUBLIC WITH GRANT OPTION;

/******************************************************************************
** END: complete_fiscal_week_vw
******************************************************************************/