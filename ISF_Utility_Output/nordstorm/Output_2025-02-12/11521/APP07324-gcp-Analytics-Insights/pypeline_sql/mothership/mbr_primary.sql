SET QUERY_BAND = 'App_ID=APP08717;
     DAG_ID=mothership_mbr_primary_11521_ACE_ENG;
     Task_Name=mbr_primary;'
     FOR SESSION VOLATILE;
 
/*

Table Name: t2dl_das_mothership.mbr_primary
Team/Owner: tech_ffp_analytics/Matthew Bond/Charlie Taylor
Date Modified: 09/27/2023

Notes:
MBR = finance Monthly Business Review tableau dashboard: https://tableau.nordstrom.com/#/site/AS/views/MBRdash/DigitalMBRPriorMonths?:iid=2
creates intermediate data table to make dashboard run quickly, the primary table is used to power the dashboard
*/


create multiset volatile table wk_53_realign as
(select fiscal_year_num+1 as fiscal_year_num_ly, max(fiscal_week_num) max_wk_num_ly
from prd_nap_usr_vws.day_cal_454_dim ty
group by 1
where fiscal_year_num between 2021 and (select fiscal_year_num from prd_nap_usr_vws.day_cal_454_dim where day_date = current_date)
)with data primary index(fiscal_year_num_ly) on commit preserve rows;

--drop table fm;
create multiset volatile table fm as
(select
       distinct ty.fiscal_year_num,
       month_idnt as month_num,
       month_abrv as month_desc,
       week_idnt as week_num,
       max_wk_num_ly,
       case when max_wk_num_ly = 52 and fiscal_week_num < 53 then week_num - 100
            when max_wk_num_ly = 52 and fiscal_week_num = 53 then week_num - 52
            when max_wk_num_ly = 53 then week_num - 99
       end as ly_week_num,
       month_num - 100 as ly_month_num
from prd_nap_usr_vws.day_cal_454_dim ty
    join wk_53_realign ly on ty.fiscal_year_num = ly.fiscal_year_num_ly
where month_idnt between
    202101
    and
    (case when (select fiscal_month_num from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date = current_date) = 1
          then (select month_idnt-89 from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date = current_date)
          when (select fiscal_month_num from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date = current_date) > 1
          then (select month_idnt-1 from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date = current_date) end)
)
with data primary index(week_num) on commit preserve rows;

-- pulling last week of each fiscal month, utilized for metrics measured at months end (i.e. LOS CCs)
create multiset volatile table lw_by_mth as
(select
  month_num
, month_desc
, max(week_num) as last_week_of_mnth
from fm
group by 1,2
)with data primary index(month_num,last_week_of_mnth)on commit preserve rows;


-- building cutoff table
-- year cutoff is used to help create smooth transitions between fiscal years in dashboard
create multiset volatile table year_cutoff as
(select distinct fiscal_year_num year_num,
        month_idnt month_num,
        -- when 1st month of current year, return 1st month of current year
        CASE WHEN fiscal_month_num = 1
                  AND fiscal_year_num = (select max(fiscal_year_num) from fm)
                  THEN month_idnt -- minimum month from prev year
        -- when first month of a prior year, return 1st month of current year
             WHEN fiscal_month_num = 1
                  AND fiscal_year_num < (select max(fiscal_year_num) from fm)
                  THEN (select max(fiscal_year_num)*100+1 from fm)
        -- when 2nd-12th month of a prior year, return 1st month of current year
             WHEN fiscal_month_num > 1
                  AND fiscal_year_num < (select max(fiscal_year_num) from fm)
                  THEN (select max(fiscal_year_num)*100+1 from fm )
        -- else (when 2nd-12th month of current year) return current month of year
             ELSE month_idnt
        END as cutoff
  from prd_nap_usr_vws.day_cal_454_dim
  where month_idnt between 202101 and (select max(month_num) from fm)
)with data primary index(month_num,year_num,cutoff)on commit preserve rows;

create multiset volatile table bopus as
(
select
 week_num
, month_num
, case when business_unit_desc_bopus_in_store_demand = 'OFFPRICE ONLINE' then 'R.COM' else business_unit_desc_bopus_in_store_demand end as bu
, sum(gross_merch_sales_amt) as gross_sales_excl_bopus
, sum(demand_canceled_amt) as canceled_demand_excl_bopus
from T2DL_DAS_MOTHERSHIP.finance_sales_demand_fact as fsdf
LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL as w
    ON fsdf.tran_date = w.day_date
where (month_num in (select distinct month_num from fm))
and customer_journey <> 'BOPUS'
and business_unit_desc_bopus_in_store_demand in ('N.COM', 'OFFPRICE ONLINE')
group by 1,2,3
) with data primary index(month_num,bu) on commit preserve rows;

create multiset volatile table sales as
(
select
  week_num
, month_num
, left(cast(month_num  as varchar(6)),4) as fy
, case when sdotf.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM' else sdotf.business_unit_desc end as business_unit_desc
, sum(demand_amt_excl_bopus + bopus_attr_digital_amt) as demand
, sum(demand_amt_excl_bopus) as demand_excl_bopus
, sum(demand_units_excl_bopus + bopus_attr_digital_units) as units
, sum(orders) as orders
, sum(visitors) as udv
, sum(ordering_visitors) as ordering_udv
, sum(gross_merch_sales_amt) as gross_merch_sales_amt
, sum(merch_returns_amt) as merch_returns_amt
, sum(net_merch_sales_amt) as net_merch_sales_amt
from T2DL_DAS_MOTHERSHIP.SALES_DEMAND_ORDERS_TRAFFIC_FACT as sdotf
left join PRD_NAP_USR_VWS.DAY_CAL as dt on dt.day_date = sdotf.tran_date
left join PRD_NAP_USR_VWS.STORE_DIM as sd on sd.store_num = sdotf.store_num
where 1=1
and sd.business_unit_desc = sdotf.business_unit_desc --removes DTC orders that have FLS store numbers (not 808)
-- DTC should be in stores (DTC sales and demand are in stores) and is counted in purchase_trips already. This logic is copied from how finance pulls to the mothership excel
and sdotf.business_unit_desc in ('N.COM', 'OFFPRICE ONLINE') --'N.CA',
and sdotf.store_num not in (141, 173)--trunk club
and (month_num in (select distinct month_num from fm) or month_num in (select distinct month_num-100 from fm))
group by 1,2,3,4
) with data primary index(month_num,business_unit_desc) on commit preserve rows;

--drop table combo;
create multiset volatile table combo as
(
SELECT
  sales.week_num
, sales.month_num
, left(cast(sales.month_num  as varchar(6)),4) as fy
, sales.business_unit_desc as source_site
, demand
, demand_excl_bopus
, units
, canceled_demand_excl_bopus
, orders
, udv
, ordering_udv
, gross_merch_sales_amt
, bopus.gross_sales_excl_bopus
, merch_returns_amt
, net_merch_sales_amt
from sales
left join bopus
on bopus.week_num = sales.week_num and bopus.bu = sales.business_unit_desc
) with data primary index(month_num,source_site) on commit preserve rows;

--drop table ly;
create multiset volatile table ly as (
select
  fm.week_num
, fm.month_num
, source_site
, demand as demand_ly
, demand_excl_bopus as demand_excl_bopus_ly
, orders as orders_ly
, udv as udv_ly
, ordering_udv as ordering_udv_ly
, net_merch_sales_amt as net_merch_sales_amt_ly
, units as units_ly
from fm
    join combo on combo.week_num = fm.ly_week_num
where combo.month_num in (select month_num from fm) or combo.month_num in (select ly_month_num from fm)
) with data primary index(month_num,source_site) on commit preserve rows;


create multiset volatile table grow as
(
select
  ty.month_num
, ty.source_site as box
, sum(udv) udv
, sum(udv_ly) udv_ly
, sum(ordering_udv) ordering_udv
, sum(ordering_udv_ly) ordering_udv_ly
, sum(canceled_demand_excl_bopus) canceled_demand_excl_bopus
, sum(units) units
, sum(units_ly) units_ly
, sum(demand_excl_bopus) demand_excl_bopus
, sum(demand_excl_bopus_ly) demand_excl_bopus_ly
, sum(demand) demand
, sum(demand_ly) demand_ly
, sum(gross_sales_excl_bopus) gross_sales_excl_bopus
, sum(gross_merch_sales_amt) gross_merch_sales_amt
, sum(merch_returns_amt) merch_returns_amt
, sum(net_merch_sales_amt) net_merch_sales_amt
, sum(net_merch_sales_amt_ly) net_merch_sales_amt_ly
, sum(orders) orders
, sum(orders_ly)  orders_ly
from combo as ty
left join ly
on ty.week_num = ly.week_num and ty.source_site = ly.source_site
where ty.month_num in (select distinct month_num from fm)
group by 1,2
) with data primary index(month_num,box) on commit preserve rows;

create multiset volatile table car as
(--cust acquisition (ntn) and retention
select a.report_period as month_num,
    case when box = 'NCOM' then 'N.COM' else 'R.COM' end as box,
    sum(new_customer) new_customer,
    sum(retained_customer) retained_customer,
    sum(react_customer) react_customer,
    sum(total_customer) total_customer,
    sum(total_trips) total_trips
from dl_cma_cmbr.month_num_buyerflow_segment a
where box in ('NCOM','NRHL')
    and country <> 'CA'
and (report_period in (select distinct month_num from fm))
group by 1,2
) with data primary index(month_num,box) on commit preserve rows;


-- nsr prep table, code was provided by MTA team to pull low funnel marketing spend
-- pulling in numerator, denominator and aggregated nsr to facilitate individual & multi-month dashboard views
create multiset volatile table nsr_prep as
(SELECT month_num,
        nordstrom_nsr_numerator,
        nordstrom_nsr_denominator,
        rack_nsr_numerator,
        rack_nsr_denominator,
        Nordstrom_nsr,
        Nordstrom_Rack_nsr
FROM (SELECT month_num,
             SUM(CASE WHEN order_channel IN ('NCOM','FLS') THEN attributed_pred_net ELSE 0 END) AS nordstrom_nsr_numerator,
             SUM(CASE WHEN arrived_channel IN ('NCOM') AND funnel_type = 'low' THEN cost ELSE 0 END) AS nordstrom_nsr_denominator,
             SUM(CASE WHEN order_channel IN ('RCOM','RACK') THEN attributed_pred_net ELSE 0 END) AS rack_nsr_numerator,
             SUM(CASE WHEN arrived_channel IN ('RCOM') AND funnel_type = 'low' THEN cost ELSE 0 END) AS rack_nsr_denominator,
             (SUM(CASE WHEN order_channel IN ('NCOM','FLS') THEN attributed_pred_net ELSE 0 END)) /(nullifzero (SUM(CASE WHEN arrived_channel IN ('NCOM') AND funnel_type = 'low' THEN cost ELSE 0 END))) AS Nordstrom_nsr,
             (SUM(CASE WHEN order_channel IN ('RCOM','RACK') THEN attributed_pred_net ELSE 0 END)) /(nullifzero (SUM(CASE WHEN arrived_channel IN ('RCOM') AND funnel_type = 'low' THEN cost ELSE 0 END))) AS Nordstrom_Rack_nsr
      FROM T2DL_DAS_MTA.mta_ssa_cost_agg_vw AS mta
        LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM AS w ON mta.activity_date_pacific = w.day_date
      WHERE (month_num IN (SELECT distinct month_num FROM fm))
      AND   channelcountry = 'US'
      AND   marketing_type = 'paid'
      AND   (arrived_channel IN ('NCOM','RCOM') OR (arrived_channel = 'NULL' AND order_channel IN ('FLS','NCOM','RACK','RCOM')))
      GROUP BY 1) AS a) WITH data PRIMARY INDEX (month_num) ON COMMIT preserve rows;


-- unioning low funnel nsr actuals data at the business unit (box) level
create multiset volatile table nsr as
(select month_num,
       'N.COM' as box,
        nordstrom_nsr_numerator as nsr_numerator,
        nordstrom_nsr_denominator as nsr_denominator,
        nordstrom_nsr as nsr
from nsr_prep
UNION
select month_num,
       'R.COM' as box,
        rack_nsr_numerator as nsr_numerator,
        rack_nsr_denominator as nsr_denominator,
        nordstrom_rack_nsr as nsr
from nsr_prep
) with data primary index(month_num,box) on commit preserve rows;


-- repeating the low funnel NSR prep table process, this time for plan data
CREATE multiset volatile TABLE nsr_plan_prep
AS
(SELECT month_num,
        nordstrom_nsr_plan_numerator,
        nordstrom_nsr_plan_denominator,
        rack_nsr_plan_numerator,
        rack_nsr_plan_denominator,
        Nordstrom_nsr_plan,
        Nordstrom_Rack_nsr_plan
FROM (SELECT month_idnt as month_num,
             SUM(CASE WHEN order_channel IN ('NCOM','FLS') THEN fcst_attributed_pred_net ELSE 0 END) AS nordstrom_nsr_plan_numerator,
             SUM(CASE WHEN order_channel IN ('NCOM') THEN fcst_cost ELSE 0 END) AS nordstrom_nsr_plan_denominator,
             SUM(CASE WHEN order_channel IN ('RCOM','RACK') THEN fcst_attributed_pred_net ELSE 0 END) AS rack_nsr_plan_numerator,
             SUM(CASE WHEN order_channel IN ('RCOM')  THEN fcst_cost ELSE 0 END) AS rack_nsr_plan_denominator,
             (SUM(CASE WHEN order_channel IN ('NCOM','FLS') THEN fcst_attributed_pred_net ELSE 0 END)) /(nullifzero (SUM(CASE WHEN order_channel IN ('NCOM')  THEN fcst_cost ELSE 0 END))) AS Nordstrom_nsr_plan,
             (SUM(CASE WHEN order_channel IN ('RCOM','RACK') THEN fcst_attributed_pred_net ELSE 0 END)) /(nullifzero (SUM(CASE WHEN order_channel IN ('RCOM') THEN fcst_cost ELSE 0 END))) AS Nordstrom_Rack_nsr_plan
      FROM T2DL_DAS_MTA.mta_ssa_cost_fcst_agg_vw AS mta
        LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM AS w ON mta.activity_date_pacific = w.day_date
      WHERE (month_num IN (SELECT distinct month_num FROM fm))
      AND   channelcountry = 'US'
      AND   marketing_type = 'paid'
      AND   (arrived_channel IN ('NCOM','RCOM','RACK') OR (arrived_channel = 'NULL' AND order_channel IN ('FLS','NCOM','RACK','RCOM')))
      GROUP BY 1) AS a) WITH data PRIMARY INDEX (month_num) ON COMMIT preserve rows;


create multiset volatile table nsr_plan as
(select month_num,
       'N.COM' as box,
       nordstrom_nsr_plan_numerator as nsr_plan_numerator,
       nordstrom_nsr_plan_denominator as nsr_plan_denominator,
       nordstrom_nsr_plan as nsr_plan
from nsr_plan_prep
UNION
select month_num,
       'R.COM' as box,
       rack_nsr_plan_numerator as nsr_plan_numerator,
       rack_nsr_plan_denominator as nsr_plan_denominator,
       nordstrom_rack_nsr_plan as nsr_plan
from nsr_plan_prep) with data primary index(month_num,box) on commit preserve rows;


-- similar to NSR, for TWIST (regular price items) we need to pull in pre-calculated & numerator/denominator values
-- to accommodate single & multi-month views in the dashboard
create multiset volatile table twist as
(
select
    month_idnt month_num
        , case when channel = 'FULL_LINE' then 'N.COM' else 'R.COM' end as box
    -- twist defined as:
        , sum (pf.product_views*pf.pct_instock)/sum(case when pf.pct_instock is not null then pf.product_views end) as twist
        , sum (pf.product_views*pf.pct_instock) twist_numerator
        , sum (case when pf.pct_instock is not null then pf.product_views end) as twist_denominator
from t2dl_das_product_funnel.product_price_funnel_daily pf
    inner join PRD_NAP_USR_VWS.day_cal_454_dim dc
on dc.day_date = pf.event_date_pacific
where pf.channelcountry in ('US')
  and dc.month_idnt in (select distinct month_num from fm)
    and current_price_type = 'R'
group by 1, 2
)with data primary index(month_num,box)on commit preserve rows;


create multiset volatile table sff as
(
select x1.month_num,
       x1.box box,
       x3.sff_demand_units_bopus_in_digital units_sff_shipped,
       x2.units_ds_shipped units_ds_shipped,
       x1.total_demand_units_bopus_in_digital units_shipped,
       1.0000*units_sff_shipped/units_shipped as pct_sff_units_shipped,
       1.0000*units_ds_shipped/units_shipped as pct_ds_units_shipped
from
(select
    month_idnt month_num
    ,  case when business_unit_desc_bopus_in_digital_demand = 'OFFPRICE ONLINE' then 'R.COM'
       else business_unit_desc_bopus_in_digital_demand
       end as box
    , sum(demand_units_bopus_in_digital) as total_demand_units_bopus_in_digital
from T2DL_DAS_MOTHERSHIP.finance_sales_demand_fact fsdf
left join PRD_NAP_USR_VWS.day_cal_454_dim dc
    on dc.day_date = fsdf.tran_date
where box in ('N.COM', 'R.COM')
group by 1,2) x1
left join
(select
      month_idnt month_num
    , case when business_unit_desc_bopus_in_digital_demand = 'OFFPRICE ONLINE' then 'R.COM'
       else business_unit_desc_bopus_in_digital_demand
       end as box
    , sum(demand_units_bopus_in_digital) as units_ds_shipped
from T2DL_DAS_MOTHERSHIP.finance_sales_demand_fact fsdf
left join PRD_NAP_USR_VWS.day_cal_454_dim dc
    on dc.day_date = fsdf.tran_date
where customer_journey in ('ShipToStore_from_DropShip','ShipToHome_from_DropShip')
    and box in ('N.COM', 'R.COM')
    group by 1,2) x2
on x1.month_num = x2.month_num
    and x1.box = x2.box
left join
(select
    month_idnt month_num
    ,  case when business_unit_desc_bopus_in_digital_demand = 'OFFPRICE ONLINE' then 'R.COM'
       else business_unit_desc_bopus_in_digital_demand
       end as box
    , sum(demand_units_bopus_in_digital) as sff_demand_units_bopus_in_digital
from T2DL_DAS_MOTHERSHIP.finance_sales_demand_fact fsdf
left join PRD_NAP_USR_VWS.day_cal_454_dim dc
    on dc.day_date = fsdf.tran_date
where customer_journey in ('ShipToStore_from_StoreFulfill','ShipToHome_from_StoreFulfill')
    and box in ('N.COM', 'R.COM')
group by 1,2) x3
on x1.month_num = x3.month_num
    and x1.box = x3.box
where 1=1
    and x1.box in ('N.COM', 'R.COM')
    and (x1.month_num in (select distinct month_num from fm))
)with data primary index(month_num,box)on commit preserve rows;

create multiset volatile table backlog as  -- backlog: use last week number for MBR
(SELECT
  Fiscal_Year*100 + Fiscal_Month as month_num
, Fiscal_Year*100 + Fiscal_Week as week_num
, case when ss_Business_Unit_21_Day_Labor = 'NRHL' then 'R.COM' else 'N.COM' end as box
, sum(actual_backlog) as fc_backlog
FROM T2DL_SCA_VWS.DAILY_OUTBOUND_FCT_VW dof
where ss_Business_Unit_21_Day_Labor in ('NCOM', 'NRHL')
    and week_num in (select last_week_of_mnth from lw_by_mth)
    and DOW = 'Sat'
group by 1,2,3
)with data primary index(month_num,box)on commit preserve rows;


create multiset volatile table p90 as
(--sushanth has an explanation for why it's slightly different than the old dashboard- this data needs an update but it's good enough.
-- changed the data source from weekly to daily to align with P90 dashboard
select
case when business_unit = 'NCOM' then 'N.COM' else 'R.COM' end as box
, (cycle_year*100 + CYCLE_MONTH) as month_num
, AVG(p90) as P90
from T2DL_SCA_VWS.PACKAGE_P50P90_DAILY_NODE_VW
where KPI = 'Click_to_deliver'
and (month_num in (select distinct month_num from fm))
and node_type = 'all_up'
and node = 'all_up'
and KPI_COHORT = 3
group by 1,2
) with data primary index(month_num,box) on commit preserve rows;

-- creating summary table
create multiset volatile table mbr_summary_table as
(select
  grow.month_num as fm
, grow.box
, car.new_customer
, car.retained_customer
, car.react_customer
, car.total_customer
, car.total_trips
, nsr_numerator
, nsr_denominator
, nsr
, twist_numerator
, twist_denominator
, twist
, sff.units_shipped
, sff.units_sff_shipped
, sff.units_ds_shipped
, P90
, fc_backlog
, lag (new_customer,1,null) over (partition by car.box order by car.month_num) as new_customer_lag
, lag (retained_customer,1,null) over (partition by car.box order by car.month_num) as retained_customer_lag
, lag (react_customer,1,null) over (partition by car.box order by car.month_num) as react_customer_lag
, lag (total_customer,1,null) over (partition by car.box order by car.month_num) as total_customer_lag
, lag (total_trips,1,null) over (partition by car.box order by car.month_num) as total_trips_lag
, lag (nsr,1,null) over (partition by nsr.box order by nsr.month_num) as nsr_lag
, lag (twist,1,null) over (partition by twist.box order by twist.month_num) as twist_lag
, lag (units_shipped,1,null) over (partition by sff.box order by sff.month_num) as units_shipped_lag
, lag (units_sff_shipped,1,null) over (partition by sff.box order by sff.month_num) as units_sff_shipped_lag
, lag (units_ds_shipped,1,null) over (partition by sff.box order by sff.month_num) as units_ds_shipped_lag
, lag (P90,1,null) over (partition by p90.box order by p90.month_num) as p90_lag
, lag (fc_backlog,1,null) over (partition by backlog.box order by backlog.month_num) as fc_backlog_lag
, udv, udv_ly, ordering_udv, ordering_udv_ly
, units, units_ly
, demand_excl_bopus, demand, demand_ly, demand_excl_bopus_ly
, gross_sales_excl_bopus, gross_merch_sales_amt
, merch_returns_amt, net_merch_sales_amt, net_merch_sales_amt_ly
, orders, orders_ly
, lag (udv,1,null) over (partition by grow.box order by grow.month_num) as udv_lag
, lag (udv_ly,1,null) over (partition by grow.box order by grow.month_num) as udv_ly_lag
, lag (ordering_udv,1,null) over (partition by grow.box order by grow.month_num) as ordering_udv_lag
, lag (ordering_udv_ly,1,null) over (partition by grow.box order by grow.month_num) as ordering_udv_ly_lag
, lag (canceled_demand_excl_bopus,1,null) over (partition by grow.box order by grow.month_num) as canceled_demand_excl_bopus_lag
, lag (canceled_demand_excl_bopus,2,null) over (partition by grow.box order by grow.month_num) as canceled_demand_excl_bopus_lag2 --for cancel rate calculation, which is lagged 1 week
, lag (units,1,null) over (partition by grow.box order by grow.month_num) as units_lag
, lag (units_ly,1,null) over (partition by grow.box order by grow.month_num) as units_ly_lag
, lag (demand_excl_bopus,1,null) over (partition by grow.box order by grow.month_num) as demand_excl_bopus_lag
, lag (demand_excl_bopus,2,null) over (partition by grow.box order by grow.month_num) as demand_excl_bopus_lag2 --for cancel rate calculation, which is lagged 1 week
, lag (demand_excl_bopus_ly,1,null) over (partition by grow.box order by grow.month_num) as demand_excl_bopus_ly_lag
, lag (demand,1,null) over (partition by grow.box order by grow.month_num) as demand_lag
, lag (demand_ly,1,null) over (partition by grow.box order by grow.month_num) as demand_ly_lag
, lag (gross_sales_excl_bopus,1,null) over (partition by grow.box order by grow.month_num) as gross_sales_excl_bopus_lag
, lag (gross_merch_sales_amt,1,null) over (partition by grow.box order by grow.month_num) as gross_merch_sales_amt_lag
, lag (merch_returns_amt,1,null) over (partition by grow.box order by grow.month_num) as merch_returns_amt_lag
, lag (net_merch_sales_amt,1,null) over (partition by grow.box order by grow.month_num) as net_merch_sales_amt_lag
, lag (net_merch_sales_amt_ly,1,null) over (partition by grow.box order by grow.month_num) as net_merch_sales_amt_ly_lag
, lag (orders,1,null) over (partition by grow.box order by grow.month_num) as orders_lag
, lag (orders_ly,1,null) over (partition by grow.box order by grow.month_num) as orders_ly_lag
, nsr_plan_numerator, nsr_plan_denominator, nsr_plan
from grow
left join car
on grow.month_num = car.month_num and grow.box = car.box
left join nsr
on grow.month_num = nsr.month_num and grow.box = nsr.box
left join nsr_plan
on grow.month_num = nsr_plan.month_num and grow.box = nsr_plan.box
left join twist
on grow.month_num = twist.month_num and grow.box = twist.box
left join sff
on grow.month_num = sff.month_num and grow.box = sff.box
left join p90
on grow.month_num = p90.month_num and grow.box = p90.box
left join backlog
on grow.month_num = backlog.month_num and grow.box = backlog.box
) with data primary index(fm,box) on commit preserve rows;


--turn the data from vertical to horizontal to make easier for DDL and save engineering resources
create multiset volatile table pre_insert as (
SELECT fm, box, feature_name, feature_value
from mbr_summary_table UNPIVOT(feature_value FOR feature_name IN (
    new_customer, retained_customer, react_customer, total_customer, total_trips
    , nsr_numerator, nsr_denominator, nsr, twist_numerator, twist_denominator, twist, P90, fc_backlog
    , units_shipped, units_sff_shipped, units_ds_shipped
    , new_customer_lag, retained_customer_lag, react_customer_lag, total_customer_lag, total_trips_lag
    , nsr_lag, twist_lag, p90_lag, fc_backlog_lag
    , units_shipped_lag, units_sff_shipped_lag, units_ds_shipped_lag
    , udv, udv_ly, ordering_udv, ordering_udv_ly
    , units, units_ly
    , demand_excl_bopus, demand, demand_ly, demand_excl_bopus_ly
    , gross_sales_excl_bopus, gross_merch_sales_amt
    , merch_returns_amt, net_merch_sales_amt, net_merch_sales_amt_ly
    , orders, orders_ly
    , udv_lag, udv_ly_lag, ordering_udv_lag, ordering_udv_ly_lag
    , canceled_demand_excl_bopus_lag, canceled_demand_excl_bopus_lag2
    , units_lag, units_ly_lag
    , demand_excl_bopus_lag, demand_excl_bopus_lag2, demand_excl_bopus_ly_lag
    , demand_lag, demand_ly_lag
    , gross_sales_excl_bopus_lag, gross_merch_sales_amt_lag, merch_returns_amt_lag
    , net_merch_sales_amt_lag, net_merch_sales_amt_ly_lag
    , orders_lag, orders_ly_lag
    , nsr_plan
)) as temp_pivot
) with data primary index(fm,box) on commit preserve rows;




DELETE FROM {mothership_t2_schema}.mbr_primary;

--------------------------------------------------------------------
/* 
final table creation
*/
--T2DL_DAS_MOTHERSHIP.mbr_primary

INSERT INTO {mothership_t2_schema}.mbr_primary
SELECT fm as month_num, box as business_unit_desc, feature_name, feature_value, CURRENT_TIMESTAMP as dw_sys_load_tmstp
from pre_insert
 ;

COLLECT STATISTICS COLUMN(month_num), COLUMN(business_unit_desc), COLUMN(feature_name) ON {mothership_t2_schema}.mbr_primary;


SET QUERY_BAND = NONE FOR SESSION;

