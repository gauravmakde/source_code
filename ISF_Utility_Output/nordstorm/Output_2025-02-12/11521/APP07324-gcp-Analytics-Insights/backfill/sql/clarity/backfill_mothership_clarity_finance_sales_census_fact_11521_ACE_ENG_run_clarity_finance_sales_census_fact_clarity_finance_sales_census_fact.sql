SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_clarity_finance_sales_census_fact_11521_ACE_ENG;
     Task_Name=clarity_finance_sales_census_fact;'
     FOR SESSION VOLATILE; 

/*
PRD_NAP_JWN_METRICS_USR_VWS.finance_sales_census_fact
Description - Creates a 4 box (n.com, r.com, FULL LINE, RACK) table of daily demand split by store, deliver_method, department number, loyalty status, and census geography 
Full documenation: https://confluence.nordstrom.com/x/M4fITg
Contacts: Matthew Bond, Analytics

SLA: none
source data (JWN_CLARITY_TRANSACTION_FACT) finishes around 3-4am, this job starts at 10:05am
weekly refresh schedule (Sunday morning), needs to pull at least last 40 days to cover any changes made by audit of last FM AND demand cancels changing over time- pulls 60 days
*/


CREATE MULTISET VOLATILE TABLE jogmv AS (
SELECT
business_day_date
,demand_date
,dc.year_num
,business_unit_desc
,intent_store_num as store_num
,dept_num
,case when delivery_method in ('BOPUS', 'Store Take', 'Charge Send') then delivery_method
    when order_platform_type = 'Direct to Customer (DTC)' then order_platform_type
    when delivery_method like 'Ship %' then 'Shipped'
    else 'Other' end as delivery_method
,case when loyalty_status in ('UNKNOWN_VALUE', 'NOT_APPLICABLE') then null else loyalty_status end as loyalty_status
,acp_id
,order_num
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_OPERATIONAL_GMV_METRIC_VW jogmv
inner join PRD_NAP_BASE_VWS.DAY_CAL dc on dc.day_date = jogmv.business_day_date
where 1=1
and business_day_date between date'2022-01-30' AND date'2023-01-28'
and business_day_date between '2021-01-31' and current_date-1
and zero_value_unit_ind ='N' --remove Gift with Purchase and Beauty (Smart) Sample
and not (business_unit_country='CA' and business_day_date > '2023-02-25')-- fiscal month 202301 and prior
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jogmv_order_not_null AS (
SELECT
business_day_date
,demand_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,order_num
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv
where order_num is not null
)
WITH DATA PRIMARY INDEX(order_num,demand_date) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jogmv_order_null AS (
SELECT
business_day_date
,year_num
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,acp_id
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv
where order_num is null
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jogmv_order_null_acp_null AS (
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,acp_id
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null
where acp_id is null
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jogmv_order_null_acp_not_null AS (
SELECT
business_day_date
,year_num
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,acp_id
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null
where acp_id is not null
)
WITH DATA PRIMARY INDEX (year_num, acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (year_num, acp_id) ON jogmv_order_null_acp_not_null;


CREATE MULTISET VOLATILE TABLE jogmv_order_null_acp_not_null_census1 AS (
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,jogmv.acp_id
,state_fips_code,county_fips_code,tract_code,block_code
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null_acp_not_null as jogmv
left join T2DL_DAS_REAL_ESTATE.acp_census as census on census.acp_id = jogmv.acp_id and census.year_num = jogmv.year_num
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jogmv_order_null_acp_not_null_census_not_null AS (
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,state_fips_code,county_fips_code,tract_code,block_code
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null_acp_not_null_census1 as jogmv
where tract_code is not null and block_code is not null
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jogmv_order_null_acp_not_null_census_null AS (
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,acp_id
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null_acp_not_null_census1 as jogmv
where tract_code is null and block_code is null
)
WITH DATA PRIMARY INDEX (acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (acp_id) ON jogmv_order_null_acp_not_null_census_null;
    

CREATE MULTISET VOLATILE TABLE census_agg AS (
select acp_id, state_fips_code, county_fips_code, tract_code, block_code
from T2DL_DAS_REAL_ESTATE.acp_census
QUALIFY Row_Number() OVER (PARTITION BY acp_id ORDER BY year_num DESC) = 1
    --pick most recent row per acp_id
)
WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (acp_id) ON census_agg;


CREATE MULTISET VOLATILE TABLE jogmv_order_null_acp_not_null_census2 AS (
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,state_fips_code,county_fips_code,tract_code,block_code
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null_acp_not_null_census_null as jogmv
left join census_agg on census_agg.acp_id = jogmv.acp_id
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


/*
We want only ship to store items so that we can remove them (since they are just stores, not customer addresses)
(bopus is already null in census data source)
*/
CREATE MULTISET VOLATILE TABLE oldf AS (
SELECT
distinct order_num, order_line_id
FROM PRD_NAP_BASE_VWS.ORDER_LINE_DETAIL_FACT AS oldf
WHERE 1=1
and (PROMISE_TYPE_CODE = 'SHIP_TO_STORE' or DESTINATION_NODE_NUM is not null)
AND order_date_pacific between date'2022-01-30'-180 and date'2023-01-28'
-- need to account for gap between demand and fulfillment/return (business day date)
)
WITH DATA PRIMARY INDEX(order_num, order_line_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (order_num, order_line_id) ON oldf;


CREATE MULTISET VOLATILE TABLE census AS (
select order_num, order_line_id, order_date_pacific, state_fips_code, county_fips_code, tract_code, block_code
from T2DL_DAS_CUSTOMER.ORDER_LINE_POSTAL_ADDRESS_GEOCODER_LKUP_VW
where 1=1
AND order_date_pacific between date'2022-01-30'-180 and date'2023-01-28'
-- need to account for gap between demand and fulfillment/return (business day date)
and tract_code is not null
and match_address in ('Exact', 'Non_Exact') --remove null/no_match
    UNION ALL
select order_num, order_line_id, order_date_pacific, state_fips_code, county_fips_code, tract_code, block_code
from PRD_NAP_BASE_VWS.ORDER_LINE_POSTAL_ADDRESS_GEOCODER_LKUP
where 1=1
AND order_date_pacific between date'2022-01-30'-180 and date'2023-01-28'
-- need to account for gap between demand and fulfillment/return (business day date)
and order_date_pacific > '2024-08-27'
and tract_code is not null
and match_address in ('Exact', 'Non_Exact') --remove null/no_match
)
WITH DATA PRIMARY INDEX(order_num, order_line_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (order_num, order_line_id) ON census;


/*
Remove ship to store line items so that we can remove them (since they are just stores, not customer addresses)
(bopus is already null in census data source)
*/
CREATE MULTISET VOLATILE TABLE census2 AS (
select distinct order_num, order_date_pacific, state_fips_code, county_fips_code, tract_code, block_code
from census
where order_line_id not in (select order_line_id from oldf) --remove ship to store line items (bopus is already null in census data source)
-- this means that 100% sts orders will have no geo info, and partial sts orders will have geo info based on shipping address
)
WITH DATA PRIMARY INDEX(order_num,order_date_pacific) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (order_num,order_date_pacific) ON census2;


CREATE MULTISET VOLATILE TABLE jogmv_order_not_null_census AS (
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,state_fips_code,county_fips_code,tract_code,block_code
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_not_null as jogmv
left join census2 on jogmv.order_num = census2.order_num and jogmv.demand_date = census2.order_date_pacific
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE combo AS (
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,state_fips_code
,county_fips_code
,tract_code
,block_code
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null_acp_not_null_census_not_null
      UNION ALL
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,state_fips_code
,county_fips_code
,tract_code
,block_code
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null_acp_not_null_census2
     UNION ALL
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,state_fips_code
,county_fips_code
,tract_code
,block_code
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_not_null_census
    UNION ALL
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,cast(null as varchar(10)) as state_fips_code
,cast(null as varchar(10)) as county_fips_code
,cast(null as varchar(10)) as tract_code
,cast(null as varchar(10)) as block_code
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv_order_null_acp_null
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE combo_agg AS (
SELECT
business_day_date
,business_unit_desc
,store_num
,dept_num
,delivery_method
,loyalty_status
,state_fips_code
,county_fips_code
,tract_code
,block_code
,sum(case when jwn_fulfilled_demand_ind = 'Y' then jwn_fulfilled_demand_usd_amt else 0 end) as fulfilled_demand_usd_amt
,sum(case when jwn_fulfilled_demand_ind = 'Y' then line_item_quantity else 0 end) as fulfilled_demand_units
,sum(case when jwn_operational_gmv_ind = 'Y' then operational_gmv_usd_amt else 0 end) as op_gmv_usd_amt
,sum(case when jwn_operational_gmv_ind = 'Y' then operational_gmv_units else 0 end) as op_gmv_units
,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from combo
group by 1,2,3,4,5,6,7,8,9,10
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


DELETE FROM PRD_NAP_DSA_AI_BASE_VWS.finance_sales_census_fact WHERE business_day_date BETWEEN date'2022-01-30' AND date'2023-01-28';

INSERT INTO PRD_NAP_DSA_AI_BASE_VWS.finance_sales_census_fact
(
    business_day_date
    ,business_unit_desc
    ,store_num
    ,dept_num
    ,delivery_method
    ,loyalty_status
    ,state_fips_code
    ,county_fips_code
    ,tract_code
    ,block_code
    ,fulfilled_demand_usd_amt
    ,fulfilled_demand_units
    ,op_gmv_usd_amt
    ,op_gmv_units
    ,dw_sys_load_tmstp
  )
  SELECT
    business_day_date
    ,business_unit_desc
    ,store_num
    ,dept_num
    ,delivery_method
    ,loyalty_status
    ,state_fips_code
    ,county_fips_code
    ,tract_code
    ,block_code
    ,fulfilled_demand_usd_amt
    ,fulfilled_demand_units
    ,op_gmv_usd_amt
    ,op_gmv_units
    ,dw_sys_load_tmstp
 FROM 
COMBO_agg;


SET QUERY_BAND = NONE FOR SESSION;