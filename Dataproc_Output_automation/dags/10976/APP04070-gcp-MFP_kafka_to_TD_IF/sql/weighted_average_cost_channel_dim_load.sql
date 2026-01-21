SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=wac_channel_dim_load;
Task_Name=wac_channel_load_stage_dim_load;'
FOR SESSION VOLATILE;

ET;

--Delete the exisitng records from CHANNEL DIM table
LOCK TABLE {db_env}_NAP_STG.WEIGHTED_AVERAGE_COST_CHANNEL_KEYS_WRK  FOR ACCESS
NONSEQUENCED VALIDTIME
DELETE FROM {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_DIM AS TGT,
{db_env}_NAP_STG.WEIGHTED_AVERAGE_COST_CHANNEL_KEYS_WRK  WRK
WHERE TGT.SKU_NUM = WRK.SKU_NUM
AND TGT.SKU_NUM_TYPE = WRK.SKU_NUM_TYPE;

--Insert all recrods except the first  into TEMPORAL CHANNEL DIM table
LOCK TABLE {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW FOR ACCESS
INSERT INTO {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_DIM
( 	 	sku_num 
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
		 wrk.sku_num 
		,wrk.sku_num_type
		,wrk.channel_num 
		,wrk.weighted_average_cost_currency_code	
		,wrk.weighted_average_cost		
		,wrk.eff_begin_dt
		,wrk.eff_end_dt
		,Current_Timestamp(0) AS dw_sys_updt_tmstp
		,Current_Timestamp(0) AS dw_sys_load_tmstp		
FROM {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW wrk
QUALIFY ROW_NUMBER() OVER( PARTITION BY 
		        wrk.sku_num 
			   ,wrk.sku_num_type
			   ,wrk.channel_num 	 
				ORDER BY wrk.eff_begin_dt ASC) > 1;

--Insert the first  into TEMPORAL CHANNEL DIM table
LOCK TABLE {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW FOR ACCESS
INSERT INTO {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_DIM
( 	 	sku_num 
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
		 wrk.sku_num 
		,wrk.sku_num_type
		,wrk.channel_num 
		,wrk.weighted_average_cost_currency_code	
		,wrk.weighted_average_cost		
		,'2000-01-01'
		,wrk.eff_end_dt
		,Current_Timestamp(0) AS dw_sys_updt_tmstp
		,Current_Timestamp(0) AS dw_sys_load_tmstp		
FROM {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_VTW wrk
QUALIFY ROW_NUMBER() OVER( PARTITION BY 
		        wrk.sku_num 
			   ,wrk.sku_num_type
			   ,wrk.channel_num 	 
				ORDER BY wrk.eff_begin_dt ASC) = 1;
ET;

--Refresh statistics
COLLECT STATISTICS ON {db_env}_NAP_DIM.WEIGHTED_AVERAGE_COST_CHANNEL_DIM;
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;