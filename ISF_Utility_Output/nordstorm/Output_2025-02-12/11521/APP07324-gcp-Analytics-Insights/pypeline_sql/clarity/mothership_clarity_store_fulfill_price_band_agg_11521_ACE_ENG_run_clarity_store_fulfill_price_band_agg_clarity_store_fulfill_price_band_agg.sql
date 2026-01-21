SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_clarity_store_fulfill_price_band_agg_11521_ACE_ENG;
     Task_Name=clarity_store_fulfill_price_band_agg;'
     FOR SESSION VOLATILE;

/*
PRD_NAP_JWN_METRICS_USR_VWS.store_fulfill_price_band_agg
Description - table of digital store fulfill (sff) reported demand split by item price band
Contacts: Matthew Bond, Analytics
*/

--daily refresh schedule, needs to pull last 40 days to cover any changes made by audit of last FM AND demand cancels changing over time (if that doesn't bog down the system)


--make intermediate table for canceled items in demand calculations
CREATE MULTISET VOLATILE TABLE price_type AS (
select
rms_sku_num
, (eff_begin_tmstp + INTERVAL '1' SECOND) as eff_begin_tmstp --add 1 second to prevent duplication of items that were ordered exactly when the price type changed. yes, it can happen
, eff_end_tmstp
,case when ownership_retail_price_type_code = 'CLEARANCE' then 'Clearance'
    when selling_retail_price_type_code = 'CLEARANCE' then 'Clearance'
    when selling_retail_price_type_code = 'PROMOTION' then 'Promotion'
    when selling_retail_price_type_code = 'REGULAR' then 'Regular Price'
    else null end as price_type
,store_num
,case when ppd.channel_brand= 'NORDSTROM_RACK' and ppd.selling_channel = 'STORE' and ppd.channel_country = 'CA' THEN 'RACK CANADA'
    when ppd.channel_brand= 'NORDSTROM_RACK' and ppd.selling_channel = 'STORE' and ppd.channel_country = 'US' THEN 'RACK'
    when ppd.channel_brand= 'NORDSTROM_RACK' and ppd.selling_channel = 'ONLINE' and ppd.channel_country = 'US' THEN 'OFFPRICE ONLINE'
    when ppd.channel_brand= 'NORDSTROM' and ppd.selling_channel = 'STORE' and ppd.channel_country = 'CA' THEN 'FULL LINE CANADA'
    when ppd.channel_brand= 'NORDSTROM' and ppd.selling_channel = 'STORE' and ppd.channel_country = 'US' THEN 'FULL LINE'
    when ppd.channel_brand= 'NORDSTROM' and ppd.selling_channel = 'ONLINE' and ppd.channel_country = 'CA' THEN 'N.CA'
    when ppd.channel_brand= 'NORDSTROM' and ppd.selling_channel = 'ONLINE' and ppd.channel_country = 'US' THEN 'N.COM'
    else null end as business_unit_desc
from PRD_NAP_BASE_VWS.PRODUCT_PRICE_TIMELINE_DIM ppd
where eff_end_tmstp >= cast(current_date()-30 as date) and eff_begin_tmstp <= cast(current_date() as date)
and ppd.selling_channel = 'ONLINE'
and (ownership_retail_price_type_code = 'CLEARANCE' or selling_retail_price_type_code in ('CLEARANCE','PROMOTION'))
)
WITH DATA PRIMARY INDEX(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON price_type;


CREATE MULTISET VOLATILE TABLE jdmv AS (
SELECT
demand_date
,case when business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM' else business_unit_desc end as business_unit_desc
--,fulfilled_from_location
,line_item_currency_code
,inventory_business_model
,case when division_name like 'INACT%' then 'OTHER'
    when division_name in ('DIRECT', 'DISCONTINUED DEPTS', 'INVENTORY INTEGRITY TEST', 'NPG NEW BUSINESS', 'MERCH PROJECTS') then 'OTHER'
    when division_name = 'CORPORATE' then 'OTHER/NON-MERCH'
    when division_name is null then 'OTHER/NON-MERCH'
    else division_name end as division_name
,case when price_type = 'Regular Price' then 'Regular' else price_type end as price_type
,remote_selling_ind
,case when fulfilled_from_location_type in ('UNKNOWN_VALUE', 'DC', 'FC') then 'FC Routed'
    when fulfilled_from_location_type = 'Store' then 'Store Fulfill'
    when fulfilled_from_location_type = 'Vendor (Drop Ship)' then 'Dropship'
    else fulfilled_from_location_type end as fulfilled_from_location_type
,delivery_method
,rms_sku_num
,demand_tmstp_pacific
,jwn_reported_demand_ind
,demand_units
,jwn_reported_demand_usd_amt
from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_DEMAND_METRIC_VW jdmv
where 1=1
and demand_date between current_date()-30 and current_date()
and demand_date between '2021-01-31' and current_date-1
and zero_value_unit_ind ='N' --remove Gift with Purchase and Beauty (Smart) Sample
and business_unit_desc in ('OFFPRICE ONLINE', 'N.COM')
and not (business_unit_country='CA' and business_day_date > '2023-02-25')-- fiscal month 202301 and prior
and fulfilled_from_location_type = 'Store'
)
WITH DATA NO PRIMARY INDEX
INDEX(rms_sku_num, business_unit_desc, demand_tmstp_pacific, price_type) ON COMMIT PRESERVE ROWS;

collect statistics column (rms_sku_num, business_unit_desc, demand_tmstp_pacific, price_type) on jdmv;


CREATE MULTISET VOLATILE TABLE jdmv_joined AS (
SELECT
jdmv.demand_date
,jdmv.business_unit_desc
--,jdmv.fulfilled_from_location
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,cast(case when jdmv.price_type <> 'NOT_APPLICABLE' then jdmv.price_type
    when ptc.price_type is not null then ptc.price_type
    else 'Regular' end as varchar(20)) AS price_type
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.jwn_reported_demand_ind
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
from jdmv
left join price_type AS ptc
    on jdmv.rms_sku_num = ptc.rms_sku_num and jdmv.business_unit_desc = ptc.business_unit_desc and jdmv.demand_tmstp_pacific between ptc.eff_begin_tmstp and ptc.eff_end_tmstp and jdmv.price_type = 'NOT_APPLICABLE'
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE bands AS (
SELECT
jdmv.demand_date
,jdmv.business_unit_desc
,jdmv.division_name
,jdmv.price_type
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,case when jwn_reported_demand_usd_amt between 0.01 and 5.00 then '0-5'
    when jwn_reported_demand_usd_amt between 5.01 and 10 then '5-10'
    when jwn_reported_demand_usd_amt between 10.01 and 15 then '10-15'
    when jwn_reported_demand_usd_amt between 15.01 and 20 then '15-20'
    when jwn_reported_demand_usd_amt between 20.01 and 25 then '20-25'
    when jwn_reported_demand_usd_amt between 25.01 and 30 then '25-30'
    when jwn_reported_demand_usd_amt between 30.01 and 35 then '30-35'
    when jwn_reported_demand_usd_amt between 35.01 and 40 then '35-40'
    when jwn_reported_demand_usd_amt between 40.01 and 45 then '40-45'
    when jwn_reported_demand_usd_amt between 45.01 and 50 then '45-50'
    when jwn_reported_demand_usd_amt between 50.01 and 55 then '50-55'
    when jwn_reported_demand_usd_amt between 55.01 and 60 then '55-60'
    when jwn_reported_demand_usd_amt between 60.01 and 65 then '60-65'
    when jwn_reported_demand_usd_amt between 65.01 and 70 then '65-70'
    when jwn_reported_demand_usd_amt between 70.01 and 75 then '70-75'
    when jwn_reported_demand_usd_amt between 75.01 and 80 then '75-80'
    when jwn_reported_demand_usd_amt between 80.01 and 85 then '80-85'
    when jwn_reported_demand_usd_amt between 85.01 and 90 then '85-90'
    when jwn_reported_demand_usd_amt between 90.01 and 95 then '90-95'
    when jwn_reported_demand_usd_amt between 95.01 and 100 then '95-100'
    when jwn_reported_demand_usd_amt between 100.01 and 200 then '100-200'
    when jwn_reported_demand_usd_amt between 200.01 and 300 then '200-300'
    when jwn_reported_demand_usd_amt between 300.01 and 400 then '300-400'
    when jwn_reported_demand_usd_amt between 400.01 and 500 then '400-500'
    else 'above 500' end as reported_demand_usd_price_band
,sum(jdmv.jwn_reported_demand_usd_amt) as reported_demand_usd_amt
,sum(case when jwn_reported_demand_ind = 'Y' then jdmv.demand_units else 0 end) as reported_demand_units
,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from jdmv_joined as jdmv
group by 1,2,3,4,5,6,7
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


DELETE FROM PRD_NAP_DSA_AI_BASE_VWS.store_fulfill_price_bands_agg WHERE demand_date BETWEEN current_date()-30 AND current_date();

INSERT INTO PRD_NAP_DSA_AI_BASE_VWS.store_fulfill_price_bands_agg
(
  demand_date
  ,business_unit_desc
  ,division_name
  ,price_type
  ,fulfilled_from_location_type
  ,delivery_method
  ,reported_demand_usd_price_band
  ,reported_demand_usd_amt
  ,reported_demand_units
  ,dw_sys_load_tmstp
  )
  SELECT
  demand_date
  ,business_unit_desc
  ,division_name
  ,price_type
  ,fulfilled_from_location_type
  ,delivery_method
  ,reported_demand_usd_price_band
  ,reported_demand_usd_amt
  ,reported_demand_units
  ,dw_sys_load_tmstp
 FROM
bands;


SET QUERY_BAND = NONE FOR SESSION;