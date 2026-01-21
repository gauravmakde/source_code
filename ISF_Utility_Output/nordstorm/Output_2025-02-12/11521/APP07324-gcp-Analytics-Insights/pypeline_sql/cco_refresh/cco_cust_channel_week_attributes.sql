SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
Task_Name=run_cco_cust_channel_week_attributes;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Week/Channel-level attributes table for the CCO project.
 *
 *
 * The steps involved are:
 *
 * I) Derive Customer/Week/Channel-level attributes from cco_line_items table
 *
 * II) Join everything together:
 *  1) Customer/channel/week-level derived attributes from (I)
 *  2) Customer/week-level attributes from cco_cust_week_attributes table
 *  3) Customer/channel/week-level Buyer-Flow & AARE attributes from
 *     cco_buyer_flow_cust_channel_week table
 *  4) JWN net sales rank
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


/************************************************************************************/
/************************************************************************************
 * PART I) Derive Customer/Week/Channel-level attributes from CCO line-items table
 ************************************************************************************/
/************************************************************************************/

create multiset volatile table cco_strategy_line_item_extract ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,date_shopped DATE
  ,week_idnt INTEGER
  ,banner VARCHAR(9) COMPRESS ('NORDSTROM', 'RACK')
  ,channel VARCHAR(19)
  ,store_num INTEGER
  ,gross_sales DECIMAL(38,2)
  ,gross_incl_gc DECIMAL(38,2)
  ,return_amt DECIMAL(38,2)
  ,net_sales DECIMAL(38,2)
  ,gross_items INTEGER
  ,return_items INTEGER
  ,net_items INTEGER
  ,event_anniversary INTEGER
  ,event_holiday INTEGER
  ,npg_flag INTEGER
  ,div_num INTEGER COMPRESS
  ,marketplace_flag INTEGER
) primary index (acp_id, week_idnt, channel) on commit preserve rows;

insert into cco_strategy_line_item_extract
select acp_id
  ,date_shopped
  ,week_idnt
  ,banner
  ,channel
  ,store_num
  ,gross_sales
  ,gross_incl_gc
  ,return_amt
  ,net_sales
  ,gross_items
  ,return_items
  ,net_items
  ,event_anniversary
  ,event_holiday
  ,case when npg_flag = 'Y' then 1 else 0 end as npg_flag
  ,div_num
  ,marketplace_flag as cust_marketplace_flag
from t2dl_das_strategy.cco_line_items cli
join prd_nap_usr_vws.day_cal_454_dim cal
  on cal.day_date = cli.date_shopped
where reporting_year_shopped is not null;

collect statistics
column (acp_id, week_idnt, channel)
on cco_strategy_line_item_extract;


/************************************************************************************
 * PART I-a) Create a temp Customer/Channel/Week-level table
 ************************************************************************************/

/*********** PART I-a-i) Insert the rows for Channels as "Channels" *****************/
create multiset volatile table cust_chan_level_derived_nopi_chan ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
  ,banner VARCHAR(20)
  ,cust_chan_gross_sales DECIMAL(38,2)
  ,cust_chan_return_amt DECIMAL(38,2)
  ,cust_chan_net_sales DECIMAL(38,2)
  ,cust_chan_trips INTEGER
  ,cust_chan_gross_items INTEGER
  ,cust_chan_return_items INTEGER
  ,cust_chan_net_items INTEGER
  ,cust_chan_anniversary INTEGER
  ,cust_chan_holiday INTEGER
  ,cust_chan_npg INTEGER
  ,cust_chan_div_count INTEGER
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_net_sales_shoes DECIMAL(38,2)
  ,cust_jwn_net_sales_beauty DECIMAL(38,2)
  ,cust_jwn_net_sales_designer DECIMAL(38,2)
  ,cust_jwn_net_sales_accessories DECIMAL(38,2)
  ,cust_jwn_net_sales_home DECIMAL(38,2)
  ,cust_jwn_net_sales_merch_projects DECIMAL(38,2)
  ,cust_jwn_net_sales_leased_boutiques DECIMAL(38,2)
  ,cust_jwn_net_sales_other_non_merch DECIMAL(38,2)
  ,cust_jwn_net_sales_restaurant DECIMAL(38,2)
  ,cust_jwn_transaction_apparel_ind INTEGER
  ,cust_jwn_transaction_shoes_ind INTEGER
  ,cust_jwn_transaction_beauty_ind INTEGER
  ,cust_jwn_transaction_designer_ind INTEGER
  ,cust_jwn_transaction_accessories_ind INTEGER
  ,cust_jwn_transaction_home_ind INTEGER
  ,cust_jwn_transaction_merch_projects_ind INTEGER
  ,cust_jwn_transaction_leased_boutiques_ind INTEGER
  ,cust_jwn_transaction_other_non_merch_ind INTEGER
  ,cust_jwn_transaction_restaurant_ind INTEGER
) no primary index on commit preserve rows;

insert into cust_chan_level_derived_nopi_chan
select acp_id
  ,week_idnt
  ,channel
  ,banner
  ,sum(gross_sales) cust_chan_gross_sales
  ,sum(return_amt) cust_chan_return_amt
  ,sum(net_sales) cust_chan_net_sales
  ,count(distinct case when gross_incl_gc > 0
                       then acp_id||store_num||date_shopped
                       else null end) cust_chan_trips
  ,sum(gross_items) cust_chan_gross_items
  ,sum(return_items) cust_chan_return_items
  ,sum(gross_items - return_items) cust_chan_net_items
  ,max(event_anniversary) cust_chan_anniversary
  ,max(event_holiday) cust_chan_holiday
  ,max(npg_flag) cust_chan_npg
  ,count(distinct div_num) cust_chan_div_count
  ,sum(case when div_num = 351 then net_sales else 0 end) net_sales_apparel
  ,sum(case when div_num = 310 then net_sales else 0 end) net_sales_shoes
  ,sum(case when div_num = 340 then net_sales else 0 end) net_sales_beauty
  ,sum(case when div_num = 341 then net_sales else 0 end) net_sales_designer
  ,sum(case when div_num = 360 then net_sales else 0 end) net_sales_accessories
  ,sum(case when div_num = 365 then net_sales else 0 end) net_sales_home
  ,sum(case when div_num = 370 then net_sales else 0 end) net_sales_merch_projects
  ,sum(case when div_num = 800 then net_sales else 0 end) net_sales_leased_boutiques
  ,sum(case when div_num = 900 then net_sales else 0 end) net_sales_other_non_merch
  ,sum(case when div_num = 70  then net_sales else 0 end) net_sales_restaurant
  ,sum(case when div_num = 351 then 1 else 0 end) transaction_apparel_ind
  ,sum(case when div_num = 310 then 1 else 0 end) transaction_shoes_ind
  ,sum(case when div_num = 340 then 1 else 0 end) transaction_beauty_ind
  ,sum(case when div_num = 341 then 1 else 0 end) transaction_designer_ind
  ,sum(case when div_num = 360 then 1 else 0 end) transaction_accessories_ind
  ,sum(case when div_num = 365 then 1 else 0 end) transaction_home_ind
  ,sum(case when div_num = 370 then 1 else 0 end) transaction_merch_projects_ind
  ,sum(case when div_num = 800 then 1 else 0 end) transaction_leased_boutiques_ind
  ,sum(case when div_num = 900 then 1 else 0 end) transaction_other_non_merch_ind
  ,sum(case when div_num = 70  then 1 else 0 end) transaction_restaurant_ind
from cco_strategy_line_item_extract
group by 1,2,3,4;


/*********** PART I-a-ii) Insert the rows for Banners as "Channels" *****************/
create multiset volatile table cust_chan_level_derived_nopi_banner ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
  ,cust_chan_gross_sales DECIMAL(38,2)
  ,cust_chan_return_amt DECIMAL(38,2)
  ,cust_chan_net_sales DECIMAL(38,2)
  ,cust_chan_trips INTEGER
  ,cust_chan_gross_items INTEGER
  ,cust_chan_return_items INTEGER
  ,cust_chan_net_items INTEGER
  ,cust_chan_anniversary INTEGER
  ,cust_chan_holiday INTEGER
  ,cust_chan_npg INTEGER
  ,cust_chan_div_count INTEGER
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_net_sales_shoes DECIMAL(38,2)
  ,cust_jwn_net_sales_beauty DECIMAL(38,2)
  ,cust_jwn_net_sales_designer DECIMAL(38,2)
  ,cust_jwn_net_sales_accessories DECIMAL(38,2)
  ,cust_jwn_net_sales_home DECIMAL(38,2)
  ,cust_jwn_net_sales_merch_projects DECIMAL(38,2)
  ,cust_jwn_net_sales_leased_boutiques DECIMAL(38,2)
  ,cust_jwn_net_sales_other_non_merch DECIMAL(38,2)
  ,cust_jwn_net_sales_restaurant DECIMAL(38,2)
  ,cust_jwn_transaction_apparel_ind INTEGER
  ,cust_jwn_transaction_shoes_ind INTEGER
  ,cust_jwn_transaction_beauty_ind INTEGER
  ,cust_jwn_transaction_designer_ind INTEGER
  ,cust_jwn_transaction_accessories_ind INTEGER
  ,cust_jwn_transaction_home_ind INTEGER
  ,cust_jwn_transaction_merch_projects_ind INTEGER
  ,cust_jwn_transaction_leased_boutiques_ind INTEGER
  ,cust_jwn_transaction_other_non_merch_ind INTEGER
  ,cust_jwn_transaction_restaurant_ind INTEGER
) no primary index on commit preserve rows;

insert into cust_chan_level_derived_nopi_banner
select acp_id
  ,week_idnt
  ,case when banner = 'NORDSTROM' then '5) Nordstrom Banner'
        when banner = 'RACK' then '6) Rack Banner'
        else null end channel
  ,sum(cust_chan_gross_sales) cust_chan_gross_sales
  ,sum(cust_chan_return_amt) cust_chan_return_amt
  ,sum(cust_chan_net_sales) cust_chan_net_sales
  ,sum(cust_chan_trips) cust_chan_trips
  ,sum(cust_chan_gross_items) cust_chan_gross_items
  ,sum(cust_chan_return_items) cust_chan_return_items
  ,sum(cust_chan_net_items) cust_chan_net_items
  ,max(cust_chan_anniversary) cust_chan_anniversary
  ,max(cust_chan_holiday) cust_chan_holiday
  ,max(cust_chan_npg) cust_chan_npg
  ,sum(cust_chan_div_count) cust_chan_div_count
  ,sum(cust_jwn_net_sales_apparel) net_sales_apparel
  ,sum(cust_jwn_net_sales_shoes) net_sales_shoes
  ,sum(cust_jwn_net_sales_beauty) net_sales_beauty
  ,sum(cust_jwn_net_sales_designer) net_sales_designer
  ,sum(cust_jwn_net_sales_accessories) net_sales_accessories
  ,sum(cust_jwn_net_sales_home) net_sales_home
  ,sum(cust_jwn_net_sales_merch_projects) net_sales_merch_projects
  ,sum(cust_jwn_net_sales_leased_boutiques) net_sales_leased_boutiques
  ,sum(cust_jwn_net_sales_other_non_merch) net_sales_other_non_merch
  ,sum(cust_jwn_net_sales_restaurant) net_sales_restaurant
  ,max(cust_jwn_transaction_apparel_ind) transaction_apparel_ind
  ,max(cust_jwn_transaction_shoes_ind) transaction_shoes_ind
  ,max(cust_jwn_transaction_beauty_ind) transaction_beauty_ind
  ,max(cust_jwn_transaction_designer_ind) transaction_designer_ind
  ,max(cust_jwn_transaction_accessories_ind) transaction_accessories_ind
  ,max(cust_jwn_transaction_home_ind) transaction_home_ind
  ,max(cust_jwn_transaction_merch_projects_ind) transaction_merch_projects_ind
  ,max(cust_jwn_transaction_leased_boutiques_ind) transaction_leased_boutiques_ind
  ,max(cust_jwn_transaction_other_non_merch_ind) transaction_other_non_merch_ind
  ,max(cust_jwn_transaction_restaurant_ind) transaction_restaurant_ind
from cust_chan_level_derived_nopi_chan
group by 1,2,3;


/*********** PART I-a-iii) Insert the rows for JWN as "Channel" *********************/
create multiset volatile table cust_chan_level_derived_nopi_jwn ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
  ,cust_chan_gross_sales DECIMAL(38,2)
  ,cust_chan_return_amt DECIMAL(38,2)
  ,cust_chan_net_sales DECIMAL(38,2)
  ,cust_chan_trips INTEGER
  ,cust_chan_gross_items INTEGER
  ,cust_chan_return_items INTEGER
  ,cust_chan_net_items INTEGER
  ,cust_chan_anniversary INTEGER
  ,cust_chan_holiday INTEGER
  ,cust_chan_npg INTEGER
  ,cust_chan_div_count INTEGER
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_net_sales_shoes DECIMAL(38,2)
  ,cust_jwn_net_sales_beauty DECIMAL(38,2)
  ,cust_jwn_net_sales_designer DECIMAL(38,2)
  ,cust_jwn_net_sales_accessories DECIMAL(38,2)
  ,cust_jwn_net_sales_home DECIMAL(38,2)
  ,cust_jwn_net_sales_merch_projects DECIMAL(38,2)
  ,cust_jwn_net_sales_leased_boutiques DECIMAL(38,2)
  ,cust_jwn_net_sales_other_non_merch DECIMAL(38,2)
  ,cust_jwn_net_sales_restaurant DECIMAL(38,2)
  ,cust_jwn_transaction_apparel_ind INTEGER
  ,cust_jwn_transaction_shoes_ind INTEGER
  ,cust_jwn_transaction_beauty_ind INTEGER
  ,cust_jwn_transaction_designer_ind INTEGER
  ,cust_jwn_transaction_accessories_ind INTEGER
  ,cust_jwn_transaction_home_ind INTEGER
  ,cust_jwn_transaction_merch_projects_ind INTEGER
  ,cust_jwn_transaction_leased_boutiques_ind INTEGER
  ,cust_jwn_transaction_other_non_merch_ind INTEGER
  ,cust_jwn_transaction_restaurant_ind INTEGER
) no primary index on commit preserve rows;

insert into cust_chan_level_derived_nopi_jwn
select acp_id
  ,week_idnt
  ,'7) JWN' channel
  ,sum(cust_chan_gross_sales) cust_chan_gross_sales
  ,sum(cust_chan_return_amt) cust_chan_return_amt
  ,sum(cust_chan_net_sales) cust_chan_net_sales
  ,sum(cust_chan_trips) cust_chan_trips
  ,sum(cust_chan_gross_items) cust_chan_gross_items
  ,sum(cust_chan_return_items) cust_chan_return_items
  ,sum(cust_chan_net_items) cust_chan_net_items
  ,max(cust_chan_anniversary) cust_chan_anniversary
  ,max(cust_chan_holiday) cust_chan_holiday
  ,max(cust_chan_npg) cust_chan_npg
  ,sum(cust_chan_div_count) cust_chan_div_count
  ,sum(cust_jwn_net_sales_apparel) net_sales_apparel
  ,sum(cust_jwn_net_sales_shoes) net_sales_shoes
  ,sum(cust_jwn_net_sales_beauty) net_sales_beauty
  ,sum(cust_jwn_net_sales_designer) net_sales_designer
  ,sum(cust_jwn_net_sales_accessories) net_sales_accessories
  ,sum(cust_jwn_net_sales_home) net_sales_home
  ,sum(cust_jwn_net_sales_merch_projects) net_sales_merch_projects
  ,sum(cust_jwn_net_sales_leased_boutiques) net_sales_leased_boutiques
  ,sum(cust_jwn_net_sales_other_non_merch) net_sales_other_non_merch
  ,sum(cust_jwn_net_sales_restaurant) net_sales_restaurant
  ,max(cust_jwn_transaction_apparel_ind) transaction_apparel_ind
  ,max(cust_jwn_transaction_shoes_ind) transaction_shoes_ind
  ,max(cust_jwn_transaction_beauty_ind) transaction_beauty_ind
  ,max(cust_jwn_transaction_designer_ind) transaction_designer_ind
  ,max(cust_jwn_transaction_accessories_ind) transaction_accessories_ind
  ,max(cust_jwn_transaction_home_ind) transaction_home_ind
  ,max(cust_jwn_transaction_merch_projects_ind) transaction_merch_projects_ind
  ,max(cust_jwn_transaction_leased_boutiques_ind) transaction_leased_boutiques_ind
  ,max(cust_jwn_transaction_other_non_merch_ind) transaction_other_non_merch_ind
  ,max(cust_jwn_transaction_restaurant_ind) transaction_restaurant_ind
from cust_chan_level_derived_nopi_banner
group by 1,2,3;


create multiset volatile table cust_chan_level_derived_nopi ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
  ,cust_chan_gross_sales DECIMAL(38,2)
  ,cust_chan_return_amt DECIMAL(38,2)
  ,cust_chan_net_sales DECIMAL(38,2)
  ,cust_chan_trips INTEGER
  ,cust_chan_gross_items INTEGER
  ,cust_chan_return_items INTEGER
  ,cust_chan_net_items INTEGER
  ,cust_chan_anniversary INTEGER
  ,cust_chan_holiday INTEGER
  ,cust_chan_npg INTEGER
  ,cust_chan_div_count INTEGER
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_net_sales_shoes DECIMAL(38,2)
  ,cust_jwn_net_sales_beauty DECIMAL(38,2)
  ,cust_jwn_net_sales_designer DECIMAL(38,2)
  ,cust_jwn_net_sales_accessories DECIMAL(38,2)
  ,cust_jwn_net_sales_home DECIMAL(38,2)
  ,cust_jwn_net_sales_merch_projects DECIMAL(38,2)
  ,cust_jwn_net_sales_leased_boutiques DECIMAL(38,2)
  ,cust_jwn_net_sales_other_non_merch DECIMAL(38,2)
  ,cust_jwn_net_sales_restaurant DECIMAL(38,2)
  ,cust_jwn_transaction_apparel_ind INTEGER
  ,cust_jwn_transaction_shoes_ind INTEGER
  ,cust_jwn_transaction_beauty_ind INTEGER
  ,cust_jwn_transaction_designer_ind INTEGER
  ,cust_jwn_transaction_accessories_ind INTEGER
  ,cust_jwn_transaction_home_ind INTEGER
  ,cust_jwn_transaction_merch_projects_ind INTEGER
  ,cust_jwn_transaction_leased_boutiques_ind INTEGER
  ,cust_jwn_transaction_other_non_merch_ind INTEGER
  ,cust_jwn_transaction_restaurant_ind INTEGER
) no primary index on commit preserve rows;

insert into cust_chan_level_derived_nopi
select acp_id
  ,week_idnt
  ,channel
  ,cust_chan_gross_sales
  ,cust_chan_return_amt
  ,cust_chan_net_sales
  ,cust_chan_trips
  ,cust_chan_gross_items
  ,cust_chan_return_items
  ,cust_chan_net_items
  ,cust_chan_anniversary
  ,cust_chan_holiday
  ,cust_chan_npg
  ,cust_chan_div_count
  ,cust_jwn_net_sales_apparel
  ,cust_jwn_net_sales_shoes
  ,cust_jwn_net_sales_beauty
  ,cust_jwn_net_sales_designer
  ,cust_jwn_net_sales_accessories
  ,cust_jwn_net_sales_home
  ,cust_jwn_net_sales_merch_projects
  ,cust_jwn_net_sales_leased_boutiques
  ,cust_jwn_net_sales_other_non_merch
  ,cust_jwn_net_sales_restaurant
  ,cust_jwn_transaction_apparel_ind
  ,cust_jwn_transaction_shoes_ind
  ,cust_jwn_transaction_beauty_ind
  ,cust_jwn_transaction_designer_ind
  ,cust_jwn_transaction_accessories_ind
  ,cust_jwn_transaction_home_ind
  ,cust_jwn_transaction_merch_projects_ind
  ,cust_jwn_transaction_leased_boutiques_ind
  ,cust_jwn_transaction_other_non_merch_ind
  ,cust_jwn_transaction_restaurant_ind
from cust_chan_level_derived_nopi_chan
union all
select *
from cust_chan_level_derived_nopi_banner
union all
select *
from cust_chan_level_derived_nopi_jwn;

collect statistics
column (week_idnt),
column (acp_id, channel),
column (acp_id, week_idnt, channel)
on cust_chan_level_derived_nopi;


/*********** build weeks table for same week a year ago and a year hence
 *    (hence making sense looking forward from past years)
 ***********/
create multiset volatile table cust_wk_ly_ny as (
select distinct wks.week_idnt as curr_week_idnt
  ,lag(wks.week_idnt, 52) over(order by wks.week_idnt) as prev_52wk_week_idnt
  ,lead(wks.week_idnt, 52) over(order by wks.week_idnt) as next_52wk_week_idnt
from
  (
    select distinct week_idnt
    from prd_nap_usr_vws.day_cal_454_dim
    where day_date between current_date - 1830 and current_date + 730
  ) wks
) with data primary index (curr_week_idnt) on commit preserve rows;

collect statistics
column (curr_week_idnt),
column (prev_52wk_week_idnt),
column (next_52wk_week_idnt)
on cust_wk_ly_ny;


create multiset volatile table cust_chan_level_derived ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
  ,cust_chan_gross_sales DECIMAL(38,2)
  ,cust_chan_return_amt DECIMAL(38,2)
  ,cust_chan_net_sales DECIMAL(38,2)
  ,cust_chan_trips INTEGER
  ,cust_chan_gross_items INTEGER
  ,cust_chan_return_items INTEGER
  ,cust_chan_net_items INTEGER
  ,cust_chan_anniversary INTEGER
  ,cust_chan_holiday INTEGER
  ,cust_chan_npg INTEGER
  ,cust_chan_div_count INTEGER
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_net_sales_shoes DECIMAL(38,2)
  ,cust_jwn_net_sales_beauty DECIMAL(38,2)
  ,cust_jwn_net_sales_designer DECIMAL(38,2)
  ,cust_jwn_net_sales_accessories DECIMAL(38,2)
  ,cust_jwn_net_sales_home DECIMAL(38,2)
  ,cust_jwn_net_sales_merch_projects DECIMAL(38,2)
  ,cust_jwn_net_sales_leased_boutiques DECIMAL(38,2)
  ,cust_jwn_net_sales_other_non_merch DECIMAL(38,2)
  ,cust_jwn_net_sales_restaurant DECIMAL(38,2)
  ,cust_jwn_transaction_apparel_ind INTEGER
  ,cust_jwn_transaction_shoes_ind INTEGER
  ,cust_jwn_transaction_beauty_ind INTEGER
  ,cust_jwn_transaction_designer_ind INTEGER
  ,cust_jwn_transaction_accessories_ind INTEGER
  ,cust_jwn_transaction_home_ind INTEGER
  ,cust_jwn_transaction_merch_projects_ind INTEGER
  ,cust_jwn_transaction_leased_boutiques_ind INTEGER
  ,cust_jwn_transaction_other_non_merch_ind INTEGER
  ,cust_jwn_transaction_restaurant_ind INTEGER
  ,cust_chan_net_sales_wkly DECIMAL(38,2) COMPRESS
  ,cust_chan_net_sales_wkny DECIMAL(38,2) COMPRESS
  ,cust_chan_anniversary_wkly INTEGER COMPRESS
  ,cust_chan_holiday_wkly INTEGER COMPRESS
) primary index (acp_id, week_idnt, channel) on commit preserve rows;

insert into cust_chan_level_derived
select ty.acp_id
  ,ty.week_idnt
  ,ty.channel
  ,ty.cust_chan_gross_sales
  ,ty.cust_chan_return_amt
  ,ty.cust_chan_net_sales
  ,ty.cust_chan_trips
  ,ty.cust_chan_gross_items
  ,ty.cust_chan_return_items
  ,ty.cust_chan_net_items
  ,ty.cust_chan_anniversary
  ,ty.cust_chan_holiday
  ,ty.cust_chan_npg
  ,ty.cust_chan_div_count
  ,ty.cust_jwn_net_sales_apparel
  ,ty.cust_jwn_net_sales_shoes
  ,ty.cust_jwn_net_sales_beauty
  ,ty.cust_jwn_net_sales_designer
  ,ty.cust_jwn_net_sales_accessories
  ,ty.cust_jwn_net_sales_home
  ,ty.cust_jwn_net_sales_merch_projects
  ,ty.cust_jwn_net_sales_leased_boutiques
  ,ty.cust_jwn_net_sales_other_non_merch
  ,ty.cust_jwn_net_sales_restaurant
  ,ty.cust_jwn_transaction_apparel_ind
  ,ty.cust_jwn_transaction_shoes_ind
  ,ty.cust_jwn_transaction_beauty_ind
  ,ty.cust_jwn_transaction_designer_ind
  ,ty.cust_jwn_transaction_accessories_ind
  ,ty.cust_jwn_transaction_home_ind
  ,ty.cust_jwn_transaction_merch_projects_ind
  ,ty.cust_jwn_transaction_leased_boutiques_ind
  ,ty.cust_jwn_transaction_other_non_merch_ind
  ,ty.cust_jwn_transaction_restaurant_ind
  ,ly.cust_chan_net_sales as cust_chan_net_sales_wkly
  ,ny.cust_chan_net_sales as cust_chan_net_sales_wkny
  ,ly.cust_chan_anniversary as cust_chan_anniversary_wkly
  ,ly.cust_chan_holiday as cust_chan_holiday_wkly
from cust_chan_level_derived_nopi ty
join cust_wk_ly_ny lynywks
  on ty.week_idnt = lynywks.curr_week_idnt
left join cust_chan_level_derived_nopi ly
  on ty.acp_id = ly.acp_id
  and ty.channel = ly.channel
  and ly.week_idnt = lynywks.prev_52wk_week_idnt
left join cust_chan_level_derived_nopi ny
  on ty.acp_id = ny.acp_id
  and ty.channel = ny.channel
  and ny.week_idnt = lynywks.next_52wk_week_idnt;

collect statistics
column (acp_id, week_idnt),
column (acp_id, week_idnt, channel)
on cust_chan_level_derived;


/************************************************************************************
 * PART I-b) Determine JWN net sales rank by customer
 ************************************************************************************/

create multiset volatile table cust_jwn_rank as (
select acp_id
  ,week_idnt
  ,cust_chan_net_sales
  ,row_number() over(partition by week_idnt order by cust_chan_net_sales desc) as rank_record
from cust_chan_level_derived
where channel = '7) JWN'
qualify rank_record <= 500000
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics
column (acp_id, week_idnt)
on cust_jwn_rank;


/************************************************************************************/
/************************************************************************************
 * PART II) Join derived attributes with customer/week-level & Buyer-Flow attributes
 *          at a customer/channel/week-level
 ************************************************************************************/
/************************************************************************************/

/**** create NOPI to optimize joins; then block fastload into empty master table ****/
create multiset volatile table cco_cust_channel_week_attributes_stg ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,channel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_chan_gross_sales DECIMAL(38,2)
  ,cust_chan_return_amt DECIMAL(38,2)
  ,cust_chan_net_sales DECIMAL(38,2)
  ,cust_chan_trips INTEGER
  ,cust_chan_gross_items INTEGER
  ,cust_chan_return_items INTEGER
  ,cust_chan_net_items INTEGER
  ,cust_chan_anniversary BYTEINT
  ,cust_chan_holiday BYTEINT
  ,cust_chan_npg BYTEINT
  ,cust_chan_singledivision BYTEINT
  ,cust_chan_multidivision BYTEINT
  ,cust_chan_net_sales_ly DECIMAL(38,2) COMPRESS
  ,cust_chan_net_sales_ny DECIMAL(38,2) COMPRESS
  ,cust_chan_anniversary_ly INTEGER COMPRESS
  ,cust_chan_holiday_ly INTEGER COMPRESS
  ,cust_chan_buyer_flow VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_chan_acquired_aare BYTEINT COMPRESS
  ,cust_chan_activated_aare BYTEINT COMPRESS
  ,cust_chan_retained_aare BYTEINT COMPRESS
  ,cust_chan_engaged_aare BYTEINT COMPRESS
  ,cust_gender VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_age SMALLINT COMPRESS
  ,cust_lifestage VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_age_group VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_nms_market VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_dma VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_dma_rank VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_loyalty_type VARCHAR(14) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_loyalty_level VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_loy_member_enroll_dt DATE COMPRESS
  ,cust_loy_cardmember_enroll_dt DATE COMPRESS
  ,cust_acquired_this_year BYTEINT COMPRESS
  ,cust_acquisition_date DATE COMPRESS
  ,cust_acquisition_fiscal_year SMALLINT COMPRESS
  ,cust_acquisition_channel VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_acquisition_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_acquisition_brand VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_tenure_bucket_months VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_tenure_bucket_years VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_activation_date DATE COMPRESS
  ,cust_activation_channel VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_activation_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_channel_count INTEGER
  ,cust_channel_combo VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_banner_count INTEGER
  ,cust_banner_combo VARCHAR(17) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,cust_employee_flag INTEGER
  ,cust_jwn_trip_bucket VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_jwn_net_spend_bucket VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_jwn_gross_sales DECIMAL(38,2)
  ,cust_jwn_return_amt DECIMAL(38,2)
  ,cust_jwn_net_sales DECIMAL(38,2)
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_net_sales_shoes DECIMAL(38,2)
  ,cust_jwn_net_sales_beauty DECIMAL(38,2)
  ,cust_jwn_net_sales_designer DECIMAL(38,2)
  ,cust_jwn_net_sales_accessories DECIMAL(38,2)
  ,cust_jwn_net_sales_home DECIMAL(38,2)
  ,cust_jwn_net_sales_merch_projects DECIMAL(38,2)
  ,cust_jwn_net_sales_leased_boutiques DECIMAL(38,2)
  ,cust_jwn_net_sales_other_non_merch DECIMAL(38,2)
  ,cust_jwn_net_sales_restaurant DECIMAL(38,2)
  ,cust_jwn_transaction_apparel_ind INTEGER
  ,cust_jwn_transaction_shoes_ind INTEGER
  ,cust_jwn_transaction_beauty_ind INTEGER
  ,cust_jwn_transaction_designer_ind INTEGER
  ,cust_jwn_transaction_accessories_ind INTEGER
  ,cust_jwn_transaction_home_ind INTEGER
  ,cust_jwn_transaction_merch_projects_ind INTEGER
  ,cust_jwn_transaction_leased_boutiques_ind INTEGER
  ,cust_jwn_transaction_other_non_merch_ind INTEGER
  ,cust_jwn_transaction_restaurant_ind INTEGER
  ,cust_jwn_trips INTEGER
  ,cust_jwn_gross_items INTEGER
  ,cust_jwn_return_items INTEGER
  ,cust_jwn_net_items INTEGER
  ,cust_tender_nordstrom BYTEINT
  ,cust_tender_nordstrom_note BYTEINT
  ,cust_tender_3rd_party_credit BYTEINT
  ,cust_tender_debit_card BYTEINT
  ,cust_tender_gift_card BYTEINT
  ,cust_tender_cash BYTEINT
  ,cust_tender_paypal BYTEINT
  ,cust_tender_check BYTEINT
  ,cust_svc_group_exp_delivery BYTEINT
  ,cust_svc_group_order_pickup BYTEINT
  ,cust_svc_group_selling_relation BYTEINT
  ,cust_svc_group_remote_selling BYTEINT
  ,cust_svc_group_alterations BYTEINT
  ,cust_svc_group_in_store BYTEINT
  ,cust_svc_group_restaurant BYTEINT
  ,cust_service_free_exp_delivery BYTEINT
  ,cust_service_next_day_pickup BYTEINT
  ,cust_service_same_day_bopus BYTEINT
  ,cust_service_curbside_pickup BYTEINT
  ,cust_service_style_boards BYTEINT
  ,cust_service_gift_wrapping BYTEINT
  ,cust_service_pop_in BYTEINT
  ,cust_marketplace_flag BYTEINT
  ,cust_platform_desktop BYTEINT
  ,cust_platform_MOW BYTEINT
  ,cust_platform_IOS BYTEINT
  ,cust_platform_Android BYTEINT
  ,cust_platform_POS BYTEINT
  ,cust_anchor_brand BYTEINT
  ,cust_strategic_brand BYTEINT
  ,cust_store_customer BYTEINT
  ,cust_digital_customer BYTEINT
  ,cust_clv_jwn DECIMAL(14,4) COMPRESS
  ,cust_clv_fp DECIMAL(14,4) COMPRESS
  ,cust_clv_op DECIMAL(14,4) COMPRESS
  ,cust_jwn_top500k BYTEINT
  ,cust_jwn_top1k BYTEINT
) no primary index on commit preserve rows;

insert into cco_cust_channel_week_attributes_stg
select a.acp_id
  ,a.week_idnt
  ,a.channel
  ,a.cust_chan_gross_sales
  ,a.cust_chan_return_amt
  ,a.cust_chan_net_sales
  ,a.cust_chan_trips
  ,a.cust_chan_gross_items
  ,a.cust_chan_return_items
  ,a.cust_chan_net_items
  ,a.cust_chan_anniversary
  ,a.cust_chan_holiday
  ,a.cust_chan_npg
  ,case when a.cust_chan_div_count = 1 then 1 else 0 end as cust_chan_singledivision
  ,case when a.cust_chan_div_count > 1 then 1 else 0 end as cust_chan_multidivision
  ,a.cust_chan_net_sales_wkly
  ,a.cust_chan_net_sales_wkny
  ,a.cust_chan_anniversary_wkly
  ,a.cust_chan_holiday_wkly
  ,b.buyer_flow cust_chan_buyer_flow
  ,b.aare_acquired cust_chan_acquired_aare
  ,b.aare_activated cust_chan_activated_aare
  ,b.aare_retained cust_chan_retained_aare
  ,b.aare_engaged cust_chan_engaged_aare
  ,c.cust_gender
  ,c.cust_age
  ,c.cust_lifestage
  ,c.cust_age_group
  ,c.cust_nms_market
  ,c.cust_dma
  ,c.cust_country
  ,c.cust_dma_rank
  ,c.cust_loyalty_type
  ,c.cust_loyalty_level
  ,c.cust_loy_member_enroll_dt
  ,c.cust_loy_cardmember_enroll_dt
  ,b.aare_acquired cust_acquired_this_year
  ,c.cust_acquisition_date
  ,c.cust_acquisition_fiscal_year
  ,c.cust_acquisition_channel
  ,c.cust_acquisition_banner
  ,c.cust_acquisition_brand
  ,c.cust_tenure_bucket_months
  ,c.cust_tenure_bucket_years
  ,c.cust_activation_date
  ,c.cust_activation_channel
  ,c.cust_activation_banner
  ,c.cust_engagement_cohort
  ,c.cust_channel_count
  ,c.cust_channel_combo
  ,c.cust_banner_count
  ,c.cust_banner_combo
  ,c.cust_employee_flag
  ,c.cust_jwn_trip_bucket
  ,c.cust_jwn_net_spend_bucket
  ,c.cust_jwn_gross_sales
  ,c.cust_jwn_return_amt
  ,c.cust_jwn_net_sales
  ,a.cust_jwn_net_sales_apparel
  ,a.cust_jwn_net_sales_shoes
  ,a.cust_jwn_net_sales_beauty
  ,a.cust_jwn_net_sales_designer
  ,a.cust_jwn_net_sales_accessories
  ,a.cust_jwn_net_sales_home
  ,a.cust_jwn_net_sales_merch_projects
  ,a.cust_jwn_net_sales_leased_boutiques
  ,a.cust_jwn_net_sales_other_non_merch
  ,a.cust_jwn_net_sales_restaurant
  ,case when a.cust_jwn_transaction_apparel_ind > 0 then 1 else 0 end cust_jwn_transaction_apparel_ind
  ,case when a.cust_jwn_transaction_shoes_ind > 0 then 1 else 0 end cust_jwn_transaction_shoes_ind
  ,case when a.cust_jwn_transaction_beauty_ind > 0 then 1 else 0 end cust_jwn_transaction_beauty_ind
  ,case when a.cust_jwn_transaction_designer_ind > 0 then 1 else 0 end cust_jwn_transaction_designer_ind
  ,case when a.cust_jwn_transaction_accessories_ind > 0 then 1 else 0 end cust_jwn_transaction_accessories_ind
  ,case when a.cust_jwn_transaction_home_ind > 0 then 1 else 0 end cust_jwn_transaction_home_ind
  ,case when a.cust_jwn_transaction_merch_projects_ind > 0 then 1 else 0 end cust_jwn_transaction_merch_projects_ind
  ,case when a.cust_jwn_transaction_leased_boutiques_ind > 0 then 1 else 0 end cust_jwn_transaction_leased_boutiques_ind
  ,case when a.cust_jwn_transaction_other_non_merch_ind > 0 then 1 else 0 end cust_jwn_transaction_other_non_merch_ind
  ,case when a.cust_jwn_transaction_restaurant_ind > 0 then 1 else 0 end cust_jwn_transaction_restaurant_ind
  ,c.cust_jwn_trips
  ,c.cust_jwn_gross_items
  ,c.cust_jwn_return_items
  ,c.cust_jwn_net_items
  ,c.cust_tender_nordstrom
  ,c.cust_tender_nordstrom_note
  ,c.cust_tender_3rd_party_credit
  ,c.cust_tender_debit_card
  ,c.cust_tender_gift_card
  ,c.cust_tender_cash
  ,c.cust_tender_paypal
  ,c.cust_tender_check
  ,c.cust_svc_group_exp_delivery
  ,c.cust_svc_group_order_pickup
  ,c.cust_svc_group_selling_relation
  ,c.cust_svc_group_remote_selling
  ,c.cust_svc_group_alterations
  ,c.cust_svc_group_in_store
  ,c.cust_svc_group_restaurant
  ,c.cust_service_free_exp_delivery
  ,c.cust_service_next_day_pickup
  ,c.cust_service_same_day_bopus
  ,c.cust_service_curbside_pickup
  ,c.cust_service_style_boards
  ,c.cust_service_gift_wrapping
  ,c.cust_service_pop_in
  ,c.cust_marketplace_flag
  ,c.cust_platform_desktop
  ,c.cust_platform_MOW
  ,c.cust_platform_IOS
  ,c.cust_platform_Android
  ,c.cust_platform_POS
  ,c.cust_anchor_brand
  ,c.cust_strategic_brand
  ,c.cust_store_customer
  ,c.cust_digital_customer
  ,c.cust_clv_jwn
  ,c.cust_clv_fp
  ,c.cust_clv_op
  ,case when d.rank_record <= 500000 then 1 else 0 end as cust_jwn_top500k
  ,case when d.rank_record <= 1000 then 1 else 0 end as cust_jwn_top1k
from cust_chan_level_derived a
left join {cco_t2_schema}.cco_buyer_flow_cust_channel_week b
  on a.acp_id = b.acp_id
  and a.week_idnt = b.week_idnt
  and a.channel = b.channel
left join {cco_t2_schema}.cco_cust_week_attributes c
  on a.acp_id = c.acp_id
  and a.week_idnt = c.week_idnt
left join cust_jwn_rank d
  on a.acp_id = d.acp_id
  and a.week_idnt = d.week_idnt;


delete from {cco_t2_schema}.cco_cust_channel_week_attributes all;

insert into {cco_t2_schema}.cco_cust_channel_week_attributes
select stg.*
     , current_timestamp(6) as dw_sys_load_tmstp
from cco_cust_channel_week_attributes_stg as stg;


SET QUERY_BAND = NONE FOR SESSION;
