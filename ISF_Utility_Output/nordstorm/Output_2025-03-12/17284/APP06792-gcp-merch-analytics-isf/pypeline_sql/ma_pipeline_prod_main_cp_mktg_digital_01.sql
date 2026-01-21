/*
Contribution Profit - Marketing
Authors: KP Tryon, Sara Riker, Sara Scott
Datalab: t2dl_das_contribution_margin

Updates:
2022.03.07 - Initial ETL for Marketing Digital Costs Build Created
2022.03.07 - Marketing Costs (Digital initiated)

*/

insert overwrite table spark_temp_cp_mktg_digital_trans_stg 
select distinct order_number 
	,order_channel 
	,order_channelcountry 
	,mktg_type 
	,finance_rollup 
	,finance_detail 
	,marketing_type 
	,order_date_pacific 
from ace_bie_etl.mta_scored_demand_dev
where cast(order_date_pacific as date) between (current_date - interval '1' day) and current_date;

