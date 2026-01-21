SET QUERY_BAND = 'App_ID=APP08219;
     DAG_ID=precast_daily_11521_ACE_ENG;
     Task_Name=precast_mothership_daily;'
     FOR SESSION VOLATILE;

/*
Table Name: T2DL_DAS_FORECASTING_SOLUTIONS.precast_daily
Owner: Selina Song, Sierra Broussard 
Date Modified: 06/01/2023

Notes: 
-- PreCast is a collection of highly aggregated KPI time series. It is formatted in a way to allow flexibility 
in the number and type of KPIs while also providing a structure for automated forecasting.
-- Update cadence: daily
-- Lookback window: 60 days, to match up with mothership

1. Incremental functionality available in ISF - features are updated on schedule. DELETE FROM criteria to be set for all available features.
2. New feature integration - Backfill/History dag specifically created to load historical data for new feature. SQL script would persist in repo. DAG would be created as needed.
3. When onboarding new features in the future, feature_id will start from current max(feature_id) + 1, and then use dense_rank()
to generate. 
4. The file name is different from the table name for distinction purpose. This one is purely from mothership, and we will have other scripts to pull other metrics but to the same table.
*/ 


-- demand and sales
CREATE MULTISET VOLATILE TABLE demand_sales AS (
with temp_fsdf1 as
(select tran_date as day_date,
        business_unit_desc_bopus_in_digital_demand,
        cast('total' as VARCHAR(256)) as dimension_type,
        cast('TOTAL' as VARCHAR(256)) as feature_dimension,
        sum(demand_amt_bopus_in_digital) as demand_digital_bopus_amt, -- attribute bopus to digital
        sum(demand_canceled_amt) as cancels_amt,
        sum(demand_units_bopus_in_digital) demand_digital_bopus_unit,
        sum(demand_canceled_units) cancels_units,
        sum(gross_merch_sales_amt) as gross_merch_amt,
        sum(merch_returns_amt) as returns_amt,
        sum(net_merch_sales_amt) as net_merch_amt,
        sum(gross_operational_sales_amt) as gross_op_amt,
        sum(net_operational_sales_amt) as net_op_amt,
        sum(gross_merch_sales_units) as gross_merch_unit,
        sum(merch_returns_units) as returns_unit,
        sum(net_merch_sales_units) as net_merch_unit,
        sum(gross_operational_sales_units) as gross_op_unit,
        sum(net_operational_sales_units) as net_op_unit
from T2DL_DAS_MOTHERSHIP.FINANCE_SALES_DEMAND_FACT 
where tran_date between {start_date} and {end_date} 
    and business_unit_desc_bopus_in_digital_demand in ('N.COM','OFFPRICE ONLINE','FULL LINE','RACK') --US only
group by 1,2,3,4
union
select tran_date as day_date,
        business_unit_desc_bopus_in_digital_demand,
        'merch_division' as dimension_type,
        merch_division as feature_dimension,
        sum(demand_amt_bopus_in_digital) as demand_digital_bopus_amt,
        sum(demand_canceled_amt) as cancels_amt,
        sum(demand_units_bopus_in_digital) demand_digital_bopus_unit,
        sum(demand_canceled_units) cancels_units,
        sum(gross_merch_sales_amt) as gross_merch_amt,
        sum(merch_returns_amt) as returns_amt,
        sum(net_merch_sales_amt) as net_merch_amt,
        sum(gross_operational_sales_amt) as gross_op_amt,
        sum(net_operational_sales_amt) as net_op_amt,
        sum(gross_merch_sales_units) as gross_merch_unit,
        sum(merch_returns_units) as returns_unit,
        sum(net_merch_sales_units) as net_merch_unit,
        sum(gross_operational_sales_units) as gross_op_unit,
        sum(net_operational_sales_units) as net_op_unit
from T2DL_DAS_MOTHERSHIP.FINANCE_SALES_DEMAND_FACT 
where tran_date between {start_date} and {end_date}
    and business_unit_desc_bopus_in_digital_demand in ('N.COM','OFFPRICE ONLINE','FULL LINE','RACK')
    and merch_division in ('ACCESSORIES','APPAREL','BEAUTY','DESIGNER','HOME','MERCH PROJECTS','SHOES') -- only keep main divisions
group by 1,2,3,4
),
temp_fsdf2 as (
SELECT
    day_date,
    business_unit_desc_bopus_in_digital_demand as business_unit_desc,
        dimension_type,
        feature_dimension,
        sum(demand_digital_bopus_amt) as demand_digital_bopus_amt,
        sum(cancels_amt) as cancels_amt,
        sum(demand_digital_bopus_unit) demand_digital_bopus_unit,
        sum(cancels_units) cancels_units,
        sum(gross_merch_amt) as gross_merch_amt,
        sum(returns_amt) as returns_amt,
        sum(net_merch_amt) as net_merch_amt,
        sum(gross_op_amt) as gross_op_amt,
        sum(net_op_amt) as net_op_amt,
        sum(gross_merch_unit) as gross_merch_unit,
        sum(returns_unit) as returns_unit,
        sum(net_merch_unit) as net_merch_unit,
        sum(gross_op_unit) as gross_op_unit,
        sum(net_op_unit) as net_op_unit
from temp_fsdf1
    where feature_dimension is not null
group by 1,2,3,4
    )
SELECT *
from temp_fsdf2 UNPIVOT(feature_value FOR feature_name IN (
        demand_digital_bopus_amt,
        cancels_amt,
        demand_digital_bopus_unit,
        cancels_units,
        gross_merch_amt,
        returns_amt,
        net_merch_amt,
        gross_op_amt,
        net_op_amt,
        gross_merch_unit,
        returns_unit,
        net_merch_unit,
        gross_op_unit,
        net_op_unit
)) as temp_fsdf_pivot)
WITH DATA 
PRIMARY INDEX(day_date) 
ON COMMIT PRESERVE ROWS;

-- orders and transaction
CREATE MULTISET VOLATILE TABLE orders_trans AS (
with temp_sdotf1 as
(select tran_date as day_date,
        business_unit_desc,
        cast('total' as VARCHAR(256)) as dimension_type,
        cast('TOTAL' as VARCHAR(256)) as feature_dimension,
        sum(orders) as orders,
        sum(visitors) as udv,
        sum(viewing_visitors) as viewing_udv,
        sum(adding_visitors) as adding_udv,
        sum(ordering_visitors) as ordering_udv,
        sum(store_purchase_trips) as purchase_trips,
        sum(store_traffic) as store_traffic,
        sum(transactions_total) as transactions
from T2DL_DAS_MOTHERSHIP.SALES_DEMAND_ORDERS_TRAFFIC_FACT
where tran_date between {start_date} and {end_date} 
    and business_unit_desc in ('N.COM','OFFPRICE ONLINE','FULL LINE','RACK')
group by 1,2,3,4
),
temp_sdotf2 as (
    SELECT
    day_date,
    business_unit_desc,
        dimension_type,
        feature_dimension,
        sum(orders) as orders,
        sum(udv) as udv,
        sum(viewing_udv) as viewing_udv,
        sum(adding_udv) as adding_udv,
        sum(ordering_udv) as ordering_udv,
        sum(purchase_trips) as purchase_trips,
        sum(store_traffic) as store_traffic,
        sum(transactions) as transactions
from temp_sdotf1
    where feature_dimension is not null
group by 1,2,3,4
    )
SELECT *
from temp_sdotf2 UNPIVOT(feature_value FOR feature_name IN (
        orders,
        udv,
        viewing_udv,
        adding_udv,
        ordering_udv,
        purchase_trips,
        store_traffic,
        transactions
)) as temp_fsdf_pivot)
WITH DATA 
PRIMARY INDEX(day_date) 
ON COMMIT PRESERVE ROWS;

-- delete any overlapping records from destination table prior to INSERT of new data
DELETE 
FROM    {forecasting_solutions_t2_schema}.precast_daily
WHERE   day_date between {start_date} and {end_date};


INSERT INTO {forecasting_solutions_t2_schema}.precast_daily
with temp_final as (
    SELECT day_date,
            business_unit_desc,
            dimension_type,
            feature_dimension,
            feature_name,
            feature_value
    from demand_sales
    union all
    SELECT day_date,
            business_unit_desc,
            dimension_type,
            feature_dimension,
            feature_name,
            feature_value
    from orders_trans
)
SELECT 
    day_date,
    business_unit_desc,
    dimension_type,
    feature_dimension,
    feature_name,
    feature_value,
    DENSE_RANK() OVER(ORDER BY business_unit_desc, feature_name, dimension_type, feature_dimension) feature_id,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from temp_final;


COLLECT STATISTICS COLUMN(day_date), COLUMN(business_unit_desc), COLUMN(dimension_type), COLUMN(feature_dimension),
    COLUMN(feature_id) ON {forecasting_solutions_t2_schema}.precast_daily;
COLLECT STATISTICS COLUMN (PARTITION , day_date) ON {forecasting_solutions_t2_schema}.precast_daily;
COLLECT STATISTICS COLUMN (PARTITION) ON {forecasting_solutions_t2_schema}.precast_daily;

SET QUERY_BAND = NONE FOR SESSION;