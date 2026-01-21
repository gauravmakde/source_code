SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=wac_channel_dim_load;
Task_Name=wac_channel_load_stage_work_load;'
FOR SESSION VOLATILE;

ET;

--Delete to load new records
DELETE FROM {db_env}_NAP_STG.WEIGHTED_AVERAGE_COST_CHANNEL_KEYS_WRK ALL;

--Insert keys into work table
LOCK TABLE {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_VTW FOR ACCESS
INSERT INTO {db_env}_NAP_STG.WEIGHTED_AVERAGE_COST_CHANNEL_KEYS_WRK 
(
sku_num,
sku_num_type
)
SELECT
DISTINCT sku_num,
sku_num_type
FROM
{db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_VTW;

--Delete VTW to load new and modified records
NONSEQUENCED VALIDTIME
DELETE FROM {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW ALL;

--Insert new and modified WAC records into VTW at sku\channel\date level
LOCK TABLE {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_DATE_DIM FOR ACCESS
LOCK TABLE {db_env}_NAP_STG.WEIGHTED_AVERAGE_COST_CHANNEL_KEYS_WRK FOR ACCESS
INSERT INTO {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW
(		sku_num 
		,sku_num_type
		,channel_num 		
		,weighted_average_cost_currency_code
		,weighted_average_cost
		,eff_begin_dt
		,eff_end_dt
		,dw_sys_updt_tmstp
		,dw_sys_load_tmstp		
)
SELECT 
		src.sku_num
		,src.sku_num_type		
		,src.channel_num
		,src.weighted_average_cost_currency_code
		,src.weighted_average_cost
		,BEGIN("VALIDTIME") AS begin_dt
		,END("VALIDTIME") AS end_dt
		,Current_Timestamp(0) AS dw_sys_updt_tmstp
		,Current_Timestamp(0) AS dw_sys_load_tmstp
FROM
(
SEQUENCED VALIDTIME 
SELECT	NORMALIZE sku_num
		,sku_num_type		
		,channel_num 
		,MAX(weighted_average_cost_currency_code) AS weighted_average_cost_currency_code
		,average(weighted_average_cost) AS weighted_average_cost
from
(
SEQUENCED VALIDTIME 
SELECT	NORMALIZE  
		 wac_date.sku_num
		,wac_date.sku_num_type
		,org_store.store_num 
		,COALESCE(org_store.channel_num,-1) AS channel_num
		,wac_date.weighted_average_cost_currency_code
		,wac_date.weighted_average_cost
		,wac_date.eff_begin_dt
		,PERIOD(eff_begin_dt, eff_end_dt) as eff_period
FROM  {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_DATE_DIM   wac_date
JOIN {db_env}_NAP_STG.WEIGHTED_AVERAGE_COST_CHANNEL_KEYS_WRK key_wrk
ON key_wrk.sku_num = wac_date.sku_num
AND key_wrk.sku_num_type = wac_date.sku_num_type
JOIN {db_env}_NAP_BASE_VWS.STORE_DIM org_store
ON org_store.store_num = wac_date.location_num
)sub
GROUP BY sku_num
		,sku_num_type		
		,channel_num )src;

--insert new and modified WAC records into VTW at sku\channel\date for banner level logic
INSERT INTO {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW
(		sku_num 
		,sku_num_type
		,channel_num 		
		,weighted_average_cost_currency_code
		,weighted_average_cost
		,eff_begin_dt
		,eff_end_dt
		,dw_sys_updt_tmstp
		,dw_sys_load_tmstp		
)
SELECT
		wac_banner.sku_num
		,wac_banner.sku_num_type
		,wac_banner.channel_num
		,wac_banner.weighted_average_cost_currency_code
		,wac_banner.weighted_average_cost
		,wac_banner.eff_begin_dt
		,wac_banner.eff_end_dt
		,Current_Timestamp(0) AS dw_sys_updt_tmstp
		,Current_Timestamp(0) AS dw_sys_load_tmstp
FROM
(
SELECT	 
		 wac_chnl.sku_num 
		,wac_chnl.sku_num_type
		,org_banner.channel_num 		
		,wac_chnl.weighted_average_cost_currency_code
		,wac_chnl.weighted_average_cost
		,wac_chnl.eff_begin_dt
		,wac_chnl.eff_end_dt						
FROM {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW wac_chnl
JOIN {db_env}_NAP_BASE_VWS.ORG_CHANNEL_BANNER_RANGED_WAC_DIM_VW org_banner
ON org_banner.ranged_channel_num  = wac_chnl.channel_num
) wac_banner
LEFT JOIN {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW wac_chnl_banner
ON wac_banner.sku_num=wac_chnl_banner.sku_num
AND wac_banner.sku_num_type=wac_chnl_banner.sku_num_type
AND wac_banner.channel_num=wac_chnl_banner.channel_num
WHERE wac_chnl_banner.sku_num IS NULL;
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;