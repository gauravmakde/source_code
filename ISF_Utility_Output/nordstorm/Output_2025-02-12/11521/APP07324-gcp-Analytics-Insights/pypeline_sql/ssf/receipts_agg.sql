/*

Table Name: t2dl_das_store_sales_forecasting.receipts_agg
Team/Owner: tech_ffp_analytics/Matthew Bond

Notes:
creates intermediate table aggregating historic and plan weekly receipts units by store for the store sales forecast
https://tableau.nordstrom.com/#/site/AS/views/StoreSalesForecast/StoreVSChannel?:iid=1

*/


SET QUERY_BAND = 'App_ID=APP08569;
     DAG_ID=ssf_receipts_agg_11521_ACE_ENG;
     Task_Name=receipts_agg;'
     FOR SESSION VOLATILE;

--DELETING ALL ROWS FROM LAST RUN BEFORE TABLE GETS LOADED WITH DATA FROM THIS RUN */
DELETE
FROM {ssf_t2_schema}.receipts_agg
;


INSERT INTO {ssf_t2_schema}.receipts_agg
with pct_wk as (
--get % monthly units to each channel by week, then multiply to split T2DL_DAS_LOCATION_PLANNING.LOC_PLAN_PRD_VW
-- channel week plan to estimate units allocation
--method suggested by Emily Simanton
select
distinct week_idnt, month_num, chnl_idnt,
sum(rcpt_need_u) over (partition by week_idnt, chnl_idnt) as rcpt_u_wk,
sum(rcpt_need_u) over (partition by month_num, chnl_idnt) as rcpt_u_mth,
rcpt_u_wk/rcpt_u_mth as pct_rcpt_wk
from T2DL_DAS_APT_COST_REPORTING.category_channel_cost_plans_weekly as cccpw
left join PRD_NAP_USR_VWS.DAY_CAL as dc on cccpw.week_idnt = dc.week_num
where chnl_idnt in (110, 210)
and week_idnt >= (select week_num from PRD_NAP_USR_VWS.DAY_CAL where day_date = current_date)
and week_idnt <= (select week_num from PRD_NAP_USR_VWS.DAY_CAL where day_date = current_date)+100
-- and week_idnt between 202301 and 202313 --for testing
)
, plan as (
select
loc_idnt, mth_idnt, chnl_idnt,
sum(rcpt_plan) as plan_units_month
from T2DL_DAS_LOCATION_PLANNING.LOC_PLAN_PRD_VW
where mth_idnt >= (select month_num from PRD_NAP_USR_VWS.DAY_CAL where day_date = current_date)
and mth_idnt <= (select month_num from PRD_NAP_USR_VWS.DAY_CAL where day_date = current_date)+100
and loc_idnt in (select store_num from PRD_NAP_USR_VWS.STORE_DIM where store_type_code in ('FL', 'RK'))
and loc_idnt < 800
and loc_idnt <> 57 --asos nordstrom, strange store
--and loc_idnt in (1,17,420,660) --for testing
group by 1,2,3
)
--nap receipts actuals (missing 2019-2020 and 2021 data no good)
select store_num, week_num as wk_idnt,
sum(receipts_total_units_ty) as receipts_units
from PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_AGG_FACT
where 1=1 
--and store_num in (1,17,420,660) --for testing
and week_num between 202101 and (select week_num from PRD_NAP_USR_VWS.DAY_CAL where day_date = current_date)-1 -- in (201901, 202001, 202101) --
group by 1,2
    UNION ALL
--nap receipts plan
select
loc_idnt, week_idnt as wk_idnt,
round(sum(plan_units_month*pct_rcpt_wk),0) as receipts_units
from plan
full outer join pct_wk on pct_wk.month_num = plan.mth_idnt and pct_wk.chnl_idnt = plan.chnl_idnt
where week_idnt >= (select week_num from PRD_NAP_USR_VWS.DAY_CAL where day_date = current_date)
group by 1,2
;


COLLECT STATISTICS COLUMN(store_num), COLUMN(wk_idnt) ON {ssf_t2_schema}.receipts_agg;


SET QUERY_BAND = NONE FOR SESSION;  