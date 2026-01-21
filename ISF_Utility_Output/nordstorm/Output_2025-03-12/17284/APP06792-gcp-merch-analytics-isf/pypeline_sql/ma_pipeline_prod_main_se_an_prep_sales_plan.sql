/******************************************************************************
** FILE: ma_pipeline_prod_main_se_an_prep_sales_plan.sql
** TABLE: {environment_schema}.anniv_sales_plan_22_staging
**
** DESCRIPTION:
**      - Table that contains the financial sales plans. This data is provided by business
**      - PARAMS:   
**          - environment_schema = Schema data will reside in
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
******************************************************************************/

DELETE FROM {environment_schema}.anniv_sales_plan_22_staging;

INSERT INTO {environment_schema}.anniv_sales_plan_22_staging
SELECT country
	,channel_type
	,department
	,sales_plan_dollars
	,current_timestamp as process_tmstp
FROM t3dl_ace_mch.anniv_sales_plan_22_staging 
GROUP BY 1,2,3,4,5;

COLLECT STATS
	PRIMARY INDEX ( country, channel_type, department )
	,COLUMN ( country, channel_type, department )
		on {environment_schema}.anniv_sales_plan_22_staging;