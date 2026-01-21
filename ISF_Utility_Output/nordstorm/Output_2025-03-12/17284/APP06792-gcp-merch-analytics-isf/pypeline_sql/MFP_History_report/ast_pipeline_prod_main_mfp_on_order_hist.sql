--main script that when it runs, either everyday or weekly to update the table 

/*
ast_pipeline_prod_main_mfp_on_order_hist.sql
Author: Paria Avij
Date Created: 01/24/23

Datalab: t2dl_das_open_to_buy
Updates Tables:
    - merch_on_order_hist
*/




CREATE MULTISET VOLATILE TABLE merch_on_order_history_plans AS (
SELECT
     cal.month_idnt AS snapshot_plan_month_idnt
    ,cal.month_start_day_date AS snapshot_start_day_date
    ,PURCHASE_ORDER_NUMBER 
    ,RMS_SKU_NUM
    ,EPM_SKU_NUM
	,STORE_NUM
	,WEEK_NUM
	,SHIP_LOCATION_ID
	,RMS_CASEPACK_NUM
	,EPM_CASEPACK_NUM
	,ORDER_FROM_VENDOR_ID
	,ANTICIPATED_PRICE_TYPE
	,ORDER_CATEGORY_CODE
	,PO_TYPE
	,ORDER_TYPE
	,PURCHASE_TYPE
	,STATUS
	,START_SHIP_DATE
	,END_SHIP_DATE
	,OTB_EOW_DATE
	,FIRST_APPROVAL_DATE
	,LATEST_APPROVAL_DATE
	,FIRST_APPROVAL_EVENT_TMSTP_PACIFIC
	,LATEST_APPROVAL_EVENT_TMSTP_PACIFIC
	,ANTICIPATED_RETAIL_AMT
	,TOTAL_EXPENSES_PER_UNIT_CURRENCY
	,QUANTITY_ORDERED
	,QUANTITY_CANCELED
	,QUANTITY_RECEIVED
	,QUANTITY_OPEN
	,UNIT_COST_AMT
	,TOTAL_EXPENSES_PER_UNIT_AMT
	,TOTAL_DUTY_PER_UNIT_AMT
	,UNIT_ESTIMATED_LANDING_COST
	,TOTAL_ESTIMATED_LANDING_COST
	,TOTAL_ANTICIPATED_RETAIL_AMT
	,DW_BATCH_DATE
	,DW_SYS_LOAD_TMSTP
	,CURRENT_TIMESTAMP AS rcd_update_timestamp
FROM prd_nap_usr_vws.merch_on_order_fact_vw  p
JOIN prd_nap_usr_vws.day_cal_454_dim cal
  ON current_date = cal.day_date
WHERE week_num >= (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
)
WITH DATA
PRIMARY INDEX (snapshot_plan_month_idnt, week_num, RMS_SKU_NUM, STORE_NUM)
ON COMMIT PRESERVE ROWS
;



DELETE FROM {environment_schema}.merch_on_order_hist_plans{env_suffix} WHERE snapshot_plan_month_idnt = (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date);

INSERT INTO {environment_schema}.merch_on_order_hist_plans{env_suffix}
SELECT * 
FROM merch_on_order_history_plans
WHERE snapshot_plan_month_idnt = (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
;

