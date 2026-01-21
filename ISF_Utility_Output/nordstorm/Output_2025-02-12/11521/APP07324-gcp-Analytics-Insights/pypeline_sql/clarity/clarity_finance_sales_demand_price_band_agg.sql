SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_clarity_finance_sales_demand_price_band_agg_11521_ACE_ENG;
     Task_Name=clarity_finance_sales_demand_price_band_agg;'
     FOR SESSION VOLATILE; 

/*
PRD_NAP_JWN_METRICS_USR_VWS.finance_sales_demand_price_band_agg
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
where eff_end_tmstp >= cast({start_date} as date) and eff_begin_tmstp <= cast({end_date} as date)
and (ownership_retail_price_type_code = 'CLEARANCE' or selling_retail_price_type_code in ('CLEARANCE','PROMOTION'))
)
WITH DATA PRIMARY INDEX(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON price_type;


CREATE MULTISET VOLATILE TABLE jdmv AS (
SELECT
demand_date
,business_unit_desc
,channel
--,fulfilled_from_location
,line_item_currency_code
,inventory_business_model
,price_type
,remote_selling_ind
,fulfilled_from_location_type
,delivery_method
,rms_sku_num
,demand_tmstp_pacific
,jwn_reported_demand_ind
,demand_units
,jwn_reported_demand_usd_amt
,jwn_gross_demand_usd_amt
from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_DEMAND_METRIC_VW jdmv
where 1=1
and demand_date between {start_date} and {end_date}
and demand_date between '2021-01-31' and current_date-1
and zero_value_unit_ind ='N' --remove Gift with Purchase and Beauty (Smart) Sample
and not (business_unit_country='CA' and business_day_date > '2023-02-25')-- fiscal month 202301 and prior
)
WITH DATA NO PRIMARY INDEX
INDEX(rms_sku_num, business_unit_desc, demand_tmstp_pacific, price_type) ON COMMIT PRESERVE ROWS;

collect statistics column (rms_sku_num, business_unit_desc, demand_tmstp_pacific, price_type) on jdmv;


CREATE MULTISET VOLATILE TABLE jdmv_joined AS (
SELECT
jdmv.demand_date
,jdmv.business_unit_desc
,jdmv.channel
--,jdmv.fulfilled_from_location
,jdmv.line_item_currency_code
,cast(case when jdmv.price_type <> 'NOT_APPLICABLE' then jdmv.price_type
    when ptc.price_type is not null then ptc.price_type
    else 'Regular Price' end as varchar(20)) AS price_type
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.jwn_reported_demand_ind
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.jwn_gross_demand_usd_amt
from jdmv
left join price_type AS ptc
    on jdmv.rms_sku_num = ptc.rms_sku_num and jdmv.business_unit_desc = ptc.business_unit_desc and jdmv.demand_tmstp_pacific between ptc.eff_begin_tmstp and ptc.eff_end_tmstp and jdmv.price_type = 'NOT_APPLICABLE'
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE g_bands AS (
SELECT
jdmv.demand_date
,jdmv.business_unit_desc
,jdmv.channel
,jdmv.price_type
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,case when jwn_gross_demand_usd_amt between 0.01 and 5.00 then '0-5'
    when jwn_gross_demand_usd_amt between 5.01 and 10 then '5-10'
    when jwn_gross_demand_usd_amt between 10.01 and 15 then '10-15'
    when jwn_gross_demand_usd_amt between 15.01 and 20 then '15-20'
    when jwn_gross_demand_usd_amt between 20.01 and 25 then '20-25'
    when jwn_gross_demand_usd_amt between 25.01 and 30 then '25-30'
    when jwn_gross_demand_usd_amt between 30.01 and 35 then '30-35'
    when jwn_gross_demand_usd_amt between 35.01 and 40 then '35-40'
    when jwn_gross_demand_usd_amt between 40.01 and 45 then '40-45'
    when jwn_gross_demand_usd_amt between 45.01 and 50 then '45-50'
    when jwn_gross_demand_usd_amt between 50.01 and 55 then '50-55'
    when jwn_gross_demand_usd_amt between 55.01 and 60 then '55-60'
    when jwn_gross_demand_usd_amt between 60.01 and 65 then '60-65'
    when jwn_gross_demand_usd_amt between 65.01 and 70 then '65-70'
    when jwn_gross_demand_usd_amt between 70.01 and 75 then '70-75'
    when jwn_gross_demand_usd_amt between 75.01 and 80 then '75-80'
    when jwn_gross_demand_usd_amt between 80.01 and 85 then '80-85'
    when jwn_gross_demand_usd_amt between 85.01 and 90 then '85-90'
    when jwn_gross_demand_usd_amt between 90.01 and 95 then '90-95'
    when jwn_gross_demand_usd_amt between 95.01 and 100 then '95-100'
    when jwn_gross_demand_usd_amt between 100.01 and 200 then '100-200'
    when jwn_gross_demand_usd_amt between 200.01 and 300 then '200-300'
    when jwn_gross_demand_usd_amt between 300.01 and 400 then '300-400'
    when jwn_gross_demand_usd_amt between 400.01 and 500 then '400-500'
    else 'above 500' end as gross_demand_usd_price_band
,sum(jdmv.jwn_gross_demand_usd_amt) as gross_demand_usd_amt
,sum(jdmv.demand_units) as gross_demand_units
from jdmv_joined as jdmv
group by 1,2,3,4,5,6,7
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE r_bands AS (
SELECT
jdmv.demand_date
,jdmv.business_unit_desc
,jdmv.channel
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
from jdmv_joined as jdmv
group by 1,2,3,4,5,6,7
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jogmv AS (
SELECT
business_day_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,service_type
,jwn_fulfilled_demand_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,product_return_ind
,jwn_operational_gmv_ind
,operational_gmv_usd_amt
,operational_gmv_units
from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_OPERATIONAL_GMV_METRIC_VW jogmv
where 1=1
and business_day_date between {start_date} AND {end_date}
and business_day_date between '2021-01-31' and current_date-1
and zero_value_unit_ind ='N' --remove Gift with Purchase and Beauty (Smart) Sample
and not (business_unit_country='CA' and business_day_date > '2023-02-25')-- fiscal month 202301 and prior
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE f_bands AS (
SELECT
business_day_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,case when jwn_fulfilled_demand_usd_amt between 0.01 and 5.00 then '0-5'
    when jwn_fulfilled_demand_usd_amt between 5.01 and 10 then '5-10'
    when jwn_fulfilled_demand_usd_amt between 10.01 and 15 then '10-15'
    when jwn_fulfilled_demand_usd_amt between 15.01 and 20 then '15-20'
    when jwn_fulfilled_demand_usd_amt between 20.01 and 25 then '20-25'
    when jwn_fulfilled_demand_usd_amt between 25.01 and 30 then '25-30'
    when jwn_fulfilled_demand_usd_amt between 30.01 and 35 then '30-35'
    when jwn_fulfilled_demand_usd_amt between 35.01 and 40 then '35-40'
    when jwn_fulfilled_demand_usd_amt between 40.01 and 45 then '40-45'
    when jwn_fulfilled_demand_usd_amt between 45.01 and 50 then '45-50'
    when jwn_fulfilled_demand_usd_amt between 50.01 and 55 then '50-55'
    when jwn_fulfilled_demand_usd_amt between 55.01 and 60 then '55-60'
    when jwn_fulfilled_demand_usd_amt between 60.01 and 65 then '60-65'
    when jwn_fulfilled_demand_usd_amt between 65.01 and 70 then '65-70'
    when jwn_fulfilled_demand_usd_amt between 70.01 and 75 then '70-75'
    when jwn_fulfilled_demand_usd_amt between 75.01 and 80 then '75-80'
    when jwn_fulfilled_demand_usd_amt between 80.01 and 85 then '80-85'
    when jwn_fulfilled_demand_usd_amt between 85.01 and 90 then '85-90'
    when jwn_fulfilled_demand_usd_amt between 90.01 and 95 then '90-95'
    when jwn_fulfilled_demand_usd_amt between 95.01 and 100 then '95-100'
    when jwn_fulfilled_demand_usd_amt between 100.01 and 200 then '100-200'
    when jwn_fulfilled_demand_usd_amt between 200.01 and 300 then '200-300'
    when jwn_fulfilled_demand_usd_amt between 300.01 and 400 then '300-400'
    when jwn_fulfilled_demand_usd_amt between 400.01 and 500 then '400-500'
    else 'above 500' end as fulfilled_demand_usd_price_band
,sum(jogmv.jwn_fulfilled_demand_usd_amt) as fulfilled_demand_usd_amt
,sum(jogmv.line_item_quantity) as fulfilled_demand_units
from jogmv
where 1=1
and jwn_fulfilled_demand_ind = 'Y'
group by 1,2,3,4,5,6,7
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE ret_bands AS (
SELECT
business_day_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,case when abs(operational_gmv_usd_amt) between 0.01 and 5.00 then '0-5'
    when abs(operational_gmv_usd_amt) between 5.01 and 10 then '5-10'
    when abs(operational_gmv_usd_amt) between 10.01 and 15 then '10-15'
    when abs(operational_gmv_usd_amt) between 15.01 and 20 then '15-20'
    when abs(operational_gmv_usd_amt) between 20.01 and 25 then '20-25'
    when abs(operational_gmv_usd_amt) between 25.01 and 30 then '25-30'
    when abs(operational_gmv_usd_amt) between 30.01 and 35 then '30-35'
    when abs(operational_gmv_usd_amt) between 35.01 and 40 then '35-40'
    when abs(operational_gmv_usd_amt) between 40.01 and 45 then '40-45'
    when abs(operational_gmv_usd_amt) between 45.01 and 50 then '45-50'
    when abs(operational_gmv_usd_amt) between 50.01 and 55 then '50-55'
    when abs(operational_gmv_usd_amt) between 55.01 and 60 then '55-60'
    when abs(operational_gmv_usd_amt) between 60.01 and 65 then '60-65'
    when abs(operational_gmv_usd_amt) between 65.01 and 70 then '65-70'
    when abs(operational_gmv_usd_amt) between 70.01 and 75 then '70-75'
    when abs(operational_gmv_usd_amt) between 75.01 and 80 then '75-80'
    when abs(operational_gmv_usd_amt) between 80.01 and 85 then '80-85'
    when abs(operational_gmv_usd_amt) between 85.01 and 90 then '85-90'
    when abs(operational_gmv_usd_amt) between 90.01 and 95 then '90-95'
    when abs(operational_gmv_usd_amt) between 95.01 and 100 then '95-100'
    when abs(operational_gmv_usd_amt) between 100.01 and 200 then '100-200'
    when abs(operational_gmv_usd_amt) between 200.01 and 300 then '200-300'
    when abs(operational_gmv_usd_amt) between 300.01 and 400 then '300-400'
    when abs(operational_gmv_usd_amt) between 400.01 and 500 then '400-500'
    else 'above 500' end as actual_product_returns_usd_price_band
,sum(case when service_type <> 'Last Chance' then operational_gmv_usd_amt else 0 end) as actual_product_returns_usd_amt
,sum(case when service_type <> 'Last Chance' then line_item_quantity else 0 end) as actual_product_returns_units
from jogmv
where 1=1
and product_return_ind = 'Y'
group by 1,2,3,4,5,6,7
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE gmv_bands AS (
SELECT
business_day_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,case when abs(operational_gmv_usd_amt) between 0.01 and 5.00 then '0-5'
    when abs(operational_gmv_usd_amt) between 5.01 and 10 then '5-10'
    when abs(operational_gmv_usd_amt) between 10.01 and 15 then '10-15'
    when abs(operational_gmv_usd_amt) between 15.01 and 20 then '15-20'
    when abs(operational_gmv_usd_amt) between 20.01 and 25 then '20-25'
    when abs(operational_gmv_usd_amt) between 25.01 and 30 then '25-30'
    when abs(operational_gmv_usd_amt) between 30.01 and 35 then '30-35'
    when abs(operational_gmv_usd_amt) between 35.01 and 40 then '35-40'
    when abs(operational_gmv_usd_amt) between 40.01 and 45 then '40-45'
    when abs(operational_gmv_usd_amt) between 45.01 and 50 then '45-50'
    when abs(operational_gmv_usd_amt) between 50.01 and 55 then '50-55'
    when abs(operational_gmv_usd_amt) between 55.01 and 60 then '55-60'
    when abs(operational_gmv_usd_amt) between 60.01 and 65 then '60-65'
    when abs(operational_gmv_usd_amt) between 65.01 and 70 then '65-70'
    when abs(operational_gmv_usd_amt) between 70.01 and 75 then '70-75'
    when abs(operational_gmv_usd_amt) between 75.01 and 80 then '75-80'
    when abs(operational_gmv_usd_amt) between 80.01 and 85 then '80-85'
    when abs(operational_gmv_usd_amt) between 85.01 and 90 then '85-90'
    when abs(operational_gmv_usd_amt) between 90.01 and 95 then '90-95'
    when abs(operational_gmv_usd_amt) between 95.01 and 100 then '95-100'
    when abs(operational_gmv_usd_amt) between 100.01 and 200 then '100-200'
    when abs(operational_gmv_usd_amt) between 200.01 and 300 then '200-300'
    when abs(operational_gmv_usd_amt) between 300.01 and 400 then '300-400'
    when abs(operational_gmv_usd_amt) between 400.01 and 500 then '400-500'
    else 'above 500' end as op_gmv_usd_price_band
,sum(jogmv.operational_gmv_usd_amt) as op_gmv_usd_amt
,sum(jogmv.operational_gmv_units) as op_gmv_units
from jogmv
where 1=1
and jwn_operational_gmv_ind = 'Y'
group by 1,2,3,4,5,6,7
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE combo AS (
SELECT
demand_date as tran_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,gross_demand_usd_price_band as price_band
,gross_demand_usd_amt
,gross_demand_units
,cast(null as decimal (32,8)) as reported_demand_usd_amt
,cast(null as integer) as reported_demand_units
,cast(null as decimal (32,8)) as fulfilled_demand_usd_amt
,cast(null as integer) as fulfilled_demand_units
,cast(null as decimal (32,8)) as actual_product_returns_usd_amt
,cast(null as integer) as actual_product_returns_units
,cast(null as decimal (32,8)) as op_gmv_usd_amt
,cast(null as integer) as op_gmv_units
from g_bands
  UNION ALL
SELECT
demand_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,reported_demand_usd_price_band as price_band
,cast(null as decimal (32,8)) as gross_demand_usd_amt
,cast(null as integer) as gross_demand_units
,reported_demand_usd_amt
,reported_demand_units
,cast(null as decimal (32,8)) as fulfilled_demand_usd_amt
,cast(null as integer) as fulfilled_demand_units
,cast(null as decimal (32,8)) as actual_product_returns_usd_amt
,cast(null as integer) as actual_product_returns_units
,cast(null as decimal (32,8)) as op_gmv_usd_amt
,cast(null as integer) as op_gmv_units
from r_bands
  UNION ALL
SELECT
business_day_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,fulfilled_demand_usd_price_band as price_band
,cast(null as decimal (32,8)) as gross_demand_usd_amt
,cast(null as integer) as gross_demand_units
,cast(null as decimal (32,8)) as reported_demand_usd_amt
,cast(null as integer) as reported_demand_units
,fulfilled_demand_usd_amt
,fulfilled_demand_units
,cast(null as decimal (32,8)) as actual_product_returns_usd_amt
,cast(null as integer) as actual_product_returns_units
,cast(null as decimal (32,8)) as op_gmv_usd_amt
,cast(null as integer) as op_gmv_units
from f_bands 
  UNION ALL
SELECT
business_day_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,actual_product_returns_usd_price_band as price_band
,cast(null as decimal (32,8)) as gross_demand_usd_amt
,cast(null as integer) as gross_demand_units
,cast(null as decimal (32,8)) as reported_demand_usd_amt
,cast(null as integer) as reported_demand_units
,cast(null as decimal (32,8)) as fulfilled_demand_usd_amt
,cast(null as integer) as fulfilled_demand_units
,actual_product_returns_usd_amt
,actual_product_returns_units
,cast(null as decimal (32,8)) as op_gmv_usd_amt
,cast(null as integer) as op_gmv_units
from ret_bands 
  UNION ALL
SELECT
business_day_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,op_gmv_usd_price_band as price_band
,cast(null as decimal (32,8)) as gross_demand_usd_amt
,cast(null as integer) as gross_demand_units
,cast(null as decimal (32,8)) as reported_demand_usd_amt
,cast(null as integer) as reported_demand_units
,cast(null as decimal (32,8)) as fulfilled_demand_usd_amt
,cast(null as integer) as fulfilled_demand_units
,cast(null as decimal (32,8)) as actual_product_returns_usd_amt
,cast(null as integer) as actual_product_returns_units
,op_gmv_usd_amt
,op_gmv_units
from gmv_bands
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;

 
CREATE MULTISET VOLATILE TABLE combo_agg AS (
SELECT
tran_date
,business_unit_desc
,channel
,price_type
,fulfilled_from_location_type
,delivery_method
,price_band
,sum(gross_demand_usd_amt) as gross_demand_usd_amt
,sum(gross_demand_units) as gross_demand_units
,sum(reported_demand_usd_amt) as reported_demand_usd_amt
,sum(reported_demand_units) as reported_demand_units
,sum(fulfilled_demand_usd_amt) as fulfilled_demand_usd_amt
,sum(fulfilled_demand_units) as fulfilled_demand_units
,sum(actual_product_returns_usd_amt) as actual_product_returns_usd_amt
,sum(actual_product_returns_units) as actual_product_returns_units
,sum(op_gmv_usd_amt) as op_gmv_usd_amt
,sum(op_gmv_units) as op_gmv_units
,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from combo
group by 1,2,3,4,5,6,7
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


DELETE FROM {clarity_schema}.finance_sales_demand_price_bands_agg WHERE tran_date BETWEEN {start_date} AND {end_date};

INSERT INTO {clarity_schema}.finance_sales_demand_price_bands_agg
(
  tran_date
  ,business_unit_desc
  ,channel
  ,price_type
  ,fulfilled_from_location_type
  ,delivery_method
  ,price_band
  ,gross_demand_usd_amt
  ,gross_demand_units
  ,reported_demand_usd_amt
  ,reported_demand_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,dw_sys_load_tmstp
  )
  SELECT
  tran_date
  ,business_unit_desc
  ,channel
  ,price_type
  ,fulfilled_from_location_type
  ,delivery_method
  ,price_band
  ,gross_demand_usd_amt
  ,gross_demand_units
  ,reported_demand_usd_amt
  ,reported_demand_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,dw_sys_load_tmstp
 FROM 
combo_agg;


SET QUERY_BAND = NONE FOR SESSION;