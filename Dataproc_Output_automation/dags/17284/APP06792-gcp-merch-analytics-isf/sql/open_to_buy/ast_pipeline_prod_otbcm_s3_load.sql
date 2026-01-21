--Developed by:Isaac Wallick 12/1/2022 This cript imports the ab cm data that is used in otb reporting

---Create stg table for CM_OTB_DETAIL.CSV file

Declare OUT_MESSAGE string;

CREATE TABLE IF NOT EXISTS `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.ab_cm_orders_stg (
channel STRING,
nrf_color_code STRING,
style_id STRING,
style_group_id STRING,
class_name STRING,
subclass_name STRING,
npg_ind STRING,
dept_id STRING,
plan_season_id STRING,
plan_key STRING,
supp_id STRING,
supp_name STRING,
fiscal_month_id STRING,
otb_eow_date STRING,
org_id STRING,
vpn STRING,
vpn_desc STRING,
class_id STRING,
subclass_id STRING,
supp_color STRING,
import_country_code STRING,
banner STRING,
plan_type STRING,
po_key STRING,
rcpt_units STRING,
unit_cost_us STRING,
unit_cost_ca STRING,
spcl_unit_cost_us STRING,
spcl_unit_cost_ca STRING,
ttl_cost_us STRING,
ttl_cost_ca STRING,
unit_rtl_us STRING,
unit_rtl_ca STRING,
spcl_unit_rtl_us STRING,
spcl_unit_rtl_ca STRING,
rcpt_rtl_type STRING,
ttl_rtl_us STRING,
ttl_rtl_ca STRING,
plan_commit_ind STRING,
cm_dollars STRING,
po_number STRING,
plan_season_desc STRING,
last_committed_otb_eow_date STRING
) ; --CLUSTER BY style_id, nrf_color_code;


CALL `{{params.gcp_project_id}}`.SYS_MGMT.S3_TPT_LOAD ('`{{params.gcp_project_id}}`.t2dl_das_open_to_buy','AB_CM_ORDERS_STG','us-east-1','cm-dollars','extract/','CM_OTB_DETAIL.CSV','7C',OUT_MESSAGE);


--COLLECT STATS 	PRIMARY INDEX(style_id ,nrf_color_code) 	,COLUMN(style_id ) 	,COLUMN(nrf_color_code) 		ON t2dl_das_open_to_buy.AB_CM_ORDERS_STG