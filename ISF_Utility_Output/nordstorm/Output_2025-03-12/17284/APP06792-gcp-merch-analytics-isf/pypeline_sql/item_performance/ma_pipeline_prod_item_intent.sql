/*
Purpose:        Inserts data in {{environment_schema}} tables for Item Performance Reporting
                    item_intent_lookup
Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING (prod)
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):      Rasagnya Avala
*/

DELETE FROM {environment_schema}.item_intent_lookup{env_suffix} ALL;
INSERT INTO {environment_schema}.item_intent_lookup{env_suffix}

select k.*
from 
(select cast(CONCAT(plan_year, trim(case when length(SUBSTRING(plan_week FROM POSITION('_' IN plan_week) + 1)) = 1 
then 0 || SUBSTRING(plan_week FROM POSITION('_' IN plan_week) + 1) 
else SUBSTRING(plan_week FROM POSITION('_' IN plan_week) + 1) end)) as integer) as plan_week_calc
, plan_month
, plan_year
, channel_brand as banner
, channel_country
, case when selling_channel = 'ONLINE' and channel_brand = 'NORDSTROM_RACK' then 250 
       when selling_channel = 'ONLINE' and channel_brand = 'NORDSTROM' then 120 
       when selling_channel = 'STORE' and channel_brand = 'NORDSTROM_RACK' then 210 
       when selling_channel = 'STORE' and channel_brand = 'NORDSTROM' then 110 
       end as channel_num
, case when selling_channel = 'ONLINE' and channel_brand = 'NORDSTROM_RACK' then '250, OFFPRICE ONLINE' 
       when selling_channel = 'ONLINE' and channel_brand = 'NORDSTROM' then '120, N.COM' 
       when selling_channel = 'STORE' and channel_brand = 'NORDSTROM_RACK' then '210, RACK STORES' 
       when selling_channel = 'STORE' and channel_brand = 'NORDSTROM' then '110, FULL LINE STORES' 
       end as channel_desc
, cast(dept_num as integer) as dept_num
, rms_style_num
, sku_nrf_color_num
, vpn
, supplier_color
, intended_plan_type
, intended_season
, intended_exit_month_year
, intended_lifecycle_type
, scaled_event
, holiday_or_celebration
, CURRENT_TIMESTAMP as dw_sys_load_tmstp
QUALIFY DENSE_RANK() OVER (ORDER BY plan_week desc) < 57
from PRD_NAP_USR_VWS.ITEM_INTENT_PLAN_FACT_ENHANCED_SMART_MARKDOWN_VW item) k
-- rolling 52 weeks 
inner join (select
      distinct week_idnt, month_idnt
  from prd_nap_usr_vws.day_cal_454_dim a
  where week_end_day_date <= current_date
  QUALIFY ROW_NUMBER() OVER (ORDER BY week_idnt DESC) <= 52*7) cal on cal.week_idnt = k.plan_week_calc;

COLLECT STATS PRIMARY INDEX(plan_week ,rms_style_num, dept_num ,channel_num) ON {environment_schema}.item_intent_lookup{env_suffix};