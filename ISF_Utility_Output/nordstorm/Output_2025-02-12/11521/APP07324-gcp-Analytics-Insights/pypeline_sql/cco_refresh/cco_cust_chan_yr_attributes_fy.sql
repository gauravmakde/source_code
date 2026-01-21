SET QUERY_BAND = 'App_ID=APP08240;
   DAG_ID=cco_tables_11521_ACE_ENG;
   Task_Name=run_cco_job_7_cco_cust_chan_yr_attributes_fy;'
   FOR SESSION VOLATILE;



/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Year/Channel-level table for the CCO project   
 *  (intended to be used alongisde t2dl_das_strategy.cco_line_items in order to:
 *    a) build any aggregated Tableau Sandboxes (for Strategy to self-serve)
 *    b) answer any more nuanced questions (that Strategy can't do with the Tableau Sandboxes)
 *
 * The Steps involved are:

 * I) Derive Customer/Year/Channel-level attributes from line-item table
 *
 * II) Join everything together:
 *  1) customer/channel/year-level derived attributes from 1
 *  2) customer/year-level attributes from 05_CCO_cust_year_table.sql
 *  3) customer/channel/year-level Buyer-Flow & AARE attributes from 04_Buyer_flow_Grant.sql
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************

March 2023 migrated to IsF */

/************************************************************************************/
/************************************************************************************
 * PART I) Derive Customer/Year/Channel-level attributes from line-item table
 ************************************************************************************/
/************************************************************************************/

create MULTISET volatile table  cco_strategy_line_item_extract ,--NO FALLBACK , 
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,date_shopped DATE
    ,fiscal_year_shopped VARCHAR(10)
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
) primary index (acp_id,fiscal_year_shopped,channel) on commit preserve rows;

 insert into cco_strategy_line_item_extract
 select
 acp_id 
    ,date_shopped 
    ,fiscal_year_shopped
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
    from
    {cco_t2_schema}.cco_line_items
    ;
    
    collect statistics
    column (acp_id,fiscal_year_shopped,channel)
    on
    cco_strategy_line_item_extract;

/************************************************************************************
 * PART I-a-i Create a temp Customer/Channel/Year-level table
 ************************************************************************************/

 /********* PART I-a-i) Create empty table *********/
 --drop table cust_chan_level_derived;
create MULTISET volatile table  cust_chan_level_derived ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,fiscal_year_shopped VARCHAR(10)
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
    ,cust_chan_net_sales_ly DECIMAL(38,2)
    ,cust_chan_net_sales_ny DECIMAL(38,2)
    ,cust_chan_anniversary_ly INTEGER
    ,cust_chan_holiday_ly INTEGER

) primary index (acp_id,fiscal_year_shopped,channel) on commit preserve rows;


 /********* PART II-a-i) Create empty table *********/
 --drop table cust_chan_level_derived_nopi;
create MULTISET volatile table  cust_chan_level_derived_nopi ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,fiscal_year_shopped VARCHAR(10)
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

) NO PRIMARY INDEX on commit preserve rows;

 /********* PART I-a-ii) Insert the rows for Channels as "Channels" *********/
insert into cust_chan_level_derived_nopi
select acp_id
  ,fiscal_year_shopped
  ,channel
  ,sum(gross_sales) cust_chan_gross_sales
  ,sum(return_amt) cust_chan_return_amt
  ,sum(net_sales) cust_chan_net_sales
  ,count(distinct case when gross_incl_gc > 0 then acp_id||store_num||date_shopped else null end) cust_chan_trips
  ,sum(gross_items) cust_chan_gross_items
  ,sum(return_items) cust_chan_return_items
  ,sum(gross_items - return_items) cust_chan_net_items
  ,max(event_anniversary) cust_chan_anniversary
  ,max(event_holiday) cust_chan_holiday
  ,max(npg_flag) cust_chan_npg
  ,count(distinct div_num) cust_chan_div_count
from cco_strategy_line_item_extract
group by 1,2,3
 

 /********* PART I-a-iii) Insert the rows for Banners as "Channels" *********/
union all
select acp_id
  ,fiscal_year_shopped
  ,case when banner = 'NORDSTROM' then '5) Nordstrom Banner'
        when banner = 'RACK' then '6) Rack Banner'
        else null end channel
  ,sum(gross_sales) cust_chan_gross_sales
  ,sum(return_amt) cust_chan_return_amt
  ,sum(net_sales) cust_chan_net_sales
  ,count(distinct case when gross_incl_gc > 0 then acp_id||store_num||date_shopped else null end) cust_chan_trips
  ,sum(gross_items) cust_chan_gross_items
  ,sum(return_items) cust_chan_return_items
  ,sum(gross_items - return_items) cust_chan_net_items
  ,max(event_anniversary) cust_chan_anniversary
  ,max(event_holiday) cust_chan_holiday
  ,max(npg_flag) cust_chan_npg
  ,count(distinct div_num) cust_chan_div_count
from cco_strategy_line_item_extract
group by 1,2,3
 

/********* PART I-a-iv) Insert the rows for JWN as "Channel" *********/
union all
select acp_id
  ,fiscal_year_shopped
  ,'7) JWN' channel
  ,sum(gross_sales) cust_chan_gross_sales
  ,sum(return_amt) cust_chan_return_amt
  ,sum(net_sales) cust_chan_net_sales
  ,count(distinct case when gross_incl_gc > 0 then acp_id||store_num||date_shopped else null end) cust_chan_trips
  ,sum(gross_items) cust_chan_gross_items
  ,sum(return_items) cust_chan_return_items
  ,sum(gross_items - return_items) cust_chan_net_items
  ,max(event_anniversary) cust_chan_anniversary
  ,max(event_holiday) cust_chan_holiday
  ,max(npg_flag) cust_chan_npg
  ,count(distinct div_num) cust_chan_div_count
from cco_strategy_line_item_extract
group by 1,2,3
;

insert into cust_chan_level_derived 
select
  acp_id
  ,fiscal_year_shopped
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
  ,lag(cust_chan_net_sales) over (partition by acp_id,channel order by fiscal_year_shopped asc) as cust_chan_net_sales_ly
  ,lead(cust_chan_net_sales) over (partition by acp_id,channel order by fiscal_year_shopped asc) as cust_chan_net_sales_ny
  ,lag(cust_chan_anniversary) over (partition by acp_id,channel order by fiscal_year_shopped asc) as cust_chan_anniversary_ly
  ,lag(cust_chan_holiday) over (partition by acp_id,channel order by fiscal_year_shopped asc) as cust_chan_holiday_ly
from
cust_chan_level_derived_nopi;

collect statistics
column (acp_id,fiscal_year_shopped,channel)
on
cust_chan_level_derived;

 /********* PART I-b) Determine JWN net sales rank by customer *********/
create multiset volatile table cust_jwn_rank as (
select
    acp_id
    ,fiscal_year_shopped
    ,cust_chan_net_sales
    ,ROW_NUMBER() OVER(PARTITION BY fiscal_year_shopped ORDER BY cust_chan_net_sales DESC) AS rank_record
    from
    cust_chan_level_derived

where channel = '7) JWN'
qualify rank_record <= 500000
) with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

/************************************************************************************/
/************************************************************************************
 * PART II) Process Experian likelihoods into scores; score logic from Musa Katuli
            * with process edits by Brian McGrane
 ************************************************************************************/
/************************************************************************************/
create MULTISET volatile table experian_likelihood_scores as (
select
  acp_id
  ,case when (social_media_predictions_facebook_usage_likelihood is null or social_media_predictions_facebook_usage_likelihood = 'Unknown') then null else
        ( cast( case
            when social_media_predictions_facebook_usage_likelihood   Like '%extremely%' then 5
            when social_media_predictions_facebook_usage_likelihood   Like '%highly%' then 4
            when social_media_predictions_facebook_usage_likelihood   Like '%very%' then 3
            when social_media_predictions_facebook_usage_likelihood IN('likely','unlikely') then 2
            when social_media_predictions_facebook_usage_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  social_media_predictions_facebook_usage_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  social_media_predictions_facebook_usage_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_social_media_predictions_facebook_usage_score
  ,case when (social_media_predictions_instagram_usage_likelihood is null or social_media_predictions_instagram_usage_likelihood = 'Unknown') then null else
        ( cast( case
            when social_media_predictions_instagram_usage_likelihood   Like '%extremely%' then 5
            when social_media_predictions_instagram_usage_likelihood   Like '%highly%' then 4
            when social_media_predictions_instagram_usage_likelihood   Like '%very%' then 3
            when social_media_predictions_instagram_usage_likelihood IN('likely','unlikely') then 2
            when social_media_predictions_instagram_usage_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  social_media_predictions_instagram_usage_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  social_media_predictions_instagram_usage_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_social_media_predictions_instagram_usage_score
  ,case when (social_media_predictions_pinterest_usage_likelihood is null or social_media_predictions_pinterest_usage_likelihood = 'Unknown') then null else
        ( cast( case
            when social_media_predictions_pinterest_usage_likelihood   Like '%extremely%' then 5
            when social_media_predictions_pinterest_usage_likelihood   Like '%highly%' then 4
            when social_media_predictions_pinterest_usage_likelihood   Like '%very%' then 3
            when social_media_predictions_pinterest_usage_likelihood IN('likely','unlikely') then 2
            when social_media_predictions_pinterest_usage_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  social_media_predictions_pinterest_usage_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  social_media_predictions_pinterest_usage_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_social_media_predictions_pinterest_usage_score
  ,case when (social_media_predictions_linkedin_usage_likelihood is null or social_media_predictions_linkedin_usage_likelihood = 'Unknown') then null else
        ( cast( case
            when social_media_predictions_linkedin_usage_likelihood   Like '%extremely%' then 5
            when social_media_predictions_linkedin_usage_likelihood   Like '%highly%' then 4
            when social_media_predictions_linkedin_usage_likelihood   Like '%very%' then 3
            when social_media_predictions_linkedin_usage_likelihood IN('likely','unlikely') then 2
            when social_media_predictions_linkedin_usage_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  social_media_predictions_linkedin_usage_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  social_media_predictions_linkedin_usage_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_social_media_predictions_linkedin_usage_score
  ,case when (social_media_predictions_twitter_usage_likelihood is null or social_media_predictions_twitter_usage_likelihood = 'Unknown') then null else
        ( cast( case
            when social_media_predictions_twitter_usage_likelihood   Like '%extremely%' then 5
            when social_media_predictions_twitter_usage_likelihood   Like '%highly%' then 4
            when social_media_predictions_twitter_usage_likelihood   Like '%very%' then 3
            when social_media_predictions_twitter_usage_likelihood IN('likely','unlikely') then 2
            when social_media_predictions_twitter_usage_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  social_media_predictions_twitter_usage_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  social_media_predictions_twitter_usage_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_social_media_predictions_twitter_usage_score
  ,case when (social_media_predictions_snapchat_usage_likelihood is null or social_media_predictions_snapchat_usage_likelihood = 'Unknown') then null else
        ( cast( case
            when social_media_predictions_snapchat_usage_likelihood   Like '%extremely%' then 5
            when social_media_predictions_snapchat_usage_likelihood   Like '%highly%' then 4
            when social_media_predictions_snapchat_usage_likelihood   Like '%very%' then 3
            when social_media_predictions_snapchat_usage_likelihood IN('likely','unlikely') then 2
            when social_media_predictions_snapchat_usage_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  social_media_predictions_snapchat_usage_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  social_media_predictions_snapchat_usage_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_social_media_predictions_snapchat_usage_score
  ,case when (social_media_predictions_youtube_usage_likelihood is null or social_media_predictions_youtube_usage_likelihood = 'Unknown') then null else
        ( cast( case
            when social_media_predictions_youtube_usage_likelihood   Like '%extremely%' then 5
            when social_media_predictions_youtube_usage_likelihood   Like '%highly%' then 4
            when social_media_predictions_youtube_usage_likelihood   Like '%very%' then 3
            when social_media_predictions_youtube_usage_likelihood IN('likely','unlikely') then 2
            when social_media_predictions_youtube_usage_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  social_media_predictions_youtube_usage_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  social_media_predictions_youtube_usage_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_social_media_predictions_youtube_usage_score
  ,case when (truetouch_strategy_brand_loyalists_likelihood is null or truetouch_strategy_brand_loyalists_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_brand_loyalists_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_brand_loyalists_likelihood   Like '%highly%' then 4
            when truetouch_strategy_brand_loyalists_likelihood   Like '%very%' then 3
            when truetouch_strategy_brand_loyalists_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_brand_loyalists_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_brand_loyalists_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_brand_loyalists_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_brand_loyalists_score
  ,case when (truetouch_strategy_deal_seekers_likelihood is null or truetouch_strategy_deal_seekers_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_deal_seekers_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_deal_seekers_likelihood   Like '%highly%' then 4
            when truetouch_strategy_deal_seekers_likelihood   Like '%very%' then 3
            when truetouch_strategy_deal_seekers_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_deal_seekers_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_deal_seekers_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_deal_seekers_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_deal_seekers_score
  ,case when (truetouch_strategy_moment_shoppers_likelihood is null or truetouch_strategy_moment_shoppers_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_moment_shoppers_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_moment_shoppers_likelihood   Like '%highly%' then 4
            when truetouch_strategy_moment_shoppers_likelihood   Like '%very%' then 3
            when truetouch_strategy_moment_shoppers_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_moment_shoppers_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_moment_shoppers_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_moment_shoppers_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_moment_shoppers_score
  ,case when (truetouch_strategy_mainstream_adopters_likelihood is null or truetouch_strategy_mainstream_adopters_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_mainstream_adopters_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_mainstream_adopters_likelihood   Like '%highly%' then 4
            when truetouch_strategy_mainstream_adopters_likelihood   Like '%very%' then 3
            when truetouch_strategy_mainstream_adopters_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_mainstream_adopters_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_mainstream_adopters_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_mainstream_adopters_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_mainstream_adopters_score
  ,case when (truetouch_strategy_novelty_seekers_likelihood is null or truetouch_strategy_novelty_seekers_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_novelty_seekers_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_novelty_seekers_likelihood   Like '%highly%' then 4
            when truetouch_strategy_novelty_seekers_likelihood   Like '%very%' then 3
            when truetouch_strategy_novelty_seekers_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_novelty_seekers_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_novelty_seekers_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_novelty_seekers_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_novelty_seekers_score
  ,case when (truetouch_strategy_organic_and_natural_likelihood is null or truetouch_strategy_organic_and_natural_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_organic_and_natural_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_organic_and_natural_likelihood   Like '%highly%' then 4
            when truetouch_strategy_organic_and_natural_likelihood   Like '%very%' then 3
            when truetouch_strategy_organic_and_natural_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_organic_and_natural_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_organic_and_natural_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_organic_and_natural_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_organic_and_natural_score
  ,case when (truetouch_strategy_quality_matters_likelihood is null or truetouch_strategy_quality_matters_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_quality_matters_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_quality_matters_likelihood   Like '%highly%' then 4
            when truetouch_strategy_quality_matters_likelihood   Like '%very%' then 3
            when truetouch_strategy_quality_matters_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_quality_matters_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_quality_matters_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_quality_matters_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_quality_matters_score
  ,case when (truetouch_strategy_recreational_shoppers_likelihood is null or truetouch_strategy_recreational_shoppers_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_recreational_shoppers_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_recreational_shoppers_likelihood   Like '%highly%' then 4
            when truetouch_strategy_recreational_shoppers_likelihood   Like '%very%' then 3
            when truetouch_strategy_recreational_shoppers_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_recreational_shoppers_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_recreational_shoppers_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_recreational_shoppers_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_recreational_shoppers_score
  ,case when (truetouch_strategy_savvy_researchers_likelihood is null or truetouch_strategy_savvy_researchers_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_savvy_researchers_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_savvy_researchers_likelihood   Like '%highly%' then 4
            when truetouch_strategy_savvy_researchers_likelihood   Like '%very%' then 3
            when truetouch_strategy_savvy_researchers_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_savvy_researchers_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_savvy_researchers_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_savvy_researchers_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_savvy_researchers_score
  ,case when (truetouch_strategy_trendsetters_likelihood is null or truetouch_strategy_trendsetters_likelihood = 'Unknown') then null else
        ( cast( case
            when truetouch_strategy_trendsetters_likelihood   Like '%extremely%' then 5
            when truetouch_strategy_trendsetters_likelihood   Like '%highly%' then 4
            when truetouch_strategy_trendsetters_likelihood   Like '%very%' then 3
            when truetouch_strategy_trendsetters_likelihood IN('likely','unlikely') then 2
            when truetouch_strategy_trendsetters_likelihood   Like '%somewhat%' then 1
            else 0
        end as float)
            * case when  truetouch_strategy_trendsetters_likelihood  Like'%unlikely%' then -1 else 1 end
            + case when  truetouch_strategy_trendsetters_likelihood  Like '%unlikely%' then 5.5 else 4.5 end
        )* 0.1 end as cust_current_truetouch_strategy_trendsetters_score
from
    prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim
) with data primary index (acp_id) on commit preserve rows;


/************************************************************************************/
/************************************************************************************
 * PART III) Join derived attributes with customer/year-level & Buyer-Flow attributes
 *    at a customer/channel/year-level
 ************************************************************************************/
/************************************************************************************/


--insert into t2dl_das_strategy.cco_cust_chan_yr_attributes_fy
--explain

DELETE FROM {cco_t2_schema}.cco_cust_chan_yr_attributes_fy ALL;

INSERT INTO {cco_t2_schema}.cco_cust_chan_yr_attributes_fy 
select distinct
   a.acp_id
  ,a.fiscal_year_shopped
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
    -- ^ need both single and multi because 1% of customers are zero-division (alterations etc)
  ,a.cust_chan_net_sales_ly
  ,a.cust_chan_net_sales_ny
  ,a.cust_chan_anniversary_ly
  ,a.cust_chan_holiday_ly
  ,b.buyer_flow cust_chan_buyer_flow
  ,b.aare_acquired cust_chan_acquired_aare
  ,b.aare_activated cust_chan_activated_aare
  ,b.aare_retained cust_chan_retained_aare
  ,b.aare_engaged cust_chan_engaged_aare
  ,c.cust_gender
  ,c.cust_age
  ,c.cust_lifestage
  ,c.cust_age_group
  ,c.cust_NMS_market
  ,c.cust_dma
  ,c.cust_country
  ,c.cust_dma_rank
  ,c.cust_loyalty_type
  ,c.cust_loyalty_level
  ,c.cust_loy_member_enroll_dt
  ,c.cust_loy_cardmember_enroll_dt
  --,c.cust_contr_margin_decile
  --,c.cust_contr_margin_amt
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
  ,c.cust_jwn_net_sales_apparel
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
  --,c.cust_event_ctr
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
  ,h.mcv_net_1year_fp+h.mcv_net_1year_op as cust_next_year_net_spend_jwn
  ,d.RFM_1YEAR_SEGMENT as cust_current_rfm_1year_segment
  ,d.RFM_4YEAR_SEGMENT as cust_current_rfm_4year_segment
  ,CASE WHEN last_order_date_jwn>CURRENT_DATE() THEN NULL ELSE CAST(MONTHS_BETWEEN(CURRENT_DATE(),d.last_order_date_jwn) as INTEGER) END as cust_current_months_since_last_shopped_jwn
  ,CASE WHEN last_order_date_fp>CURRENT_DATE() THEN NULL ELSE CAST(MONTHS_BETWEEN(CURRENT_DATE(),d.last_order_date_fp) as INTEGER) END as cust_current_months_since_last_shopped_fp
  ,CASE WHEN last_order_date_op>CURRENT_DATE() THEN NULL ELSE CAST(MONTHS_BETWEEN(CURRENT_DATE(),d.last_order_date_op) as INTEGER) END as cust_current_months_since_last_shopped_op
  ,CASE WHEN last_order_date_nstores>CURRENT_DATE() THEN NULL ELSE CAST(MONTHS_BETWEEN(CURRENT_DATE(),d.last_order_date_nstores) as INTEGER) END as cust_current_months_since_last_shopped_nstores
  ,CASE WHEN last_order_date_ncom>CURRENT_DATE() THEN NULL ELSE CAST(MONTHS_BETWEEN(CURRENT_DATE(),d.last_order_date_ncom) as INTEGER) END as cust_current_months_since_last_shopped_ncom
  ,CASE WHEN last_order_date_rstores>CURRENT_DATE() THEN NULL ELSE CAST(MONTHS_BETWEEN(CURRENT_DATE(),d.last_order_date_rstores) as INTEGER) END as cust_current_months_since_last_shopped_rstores
  ,CASE WHEN last_order_date_rcom>CURRENT_DATE() THEN NULL ELSE CAST(MONTHS_BETWEEN(CURRENT_DATE(),d.last_order_date_rcom) as INTEGER) END as cust_current_months_since_last_shopped_rcom
  ,e.martial_status as cust_current_marital_status
  ,e.household_adult_count as cust_current_household_adult_count
  ,e.household_children_count as cust_current_household_children_count
  ,e.household_person_count as cust_current_household_person_count
  ,e.occupation_type as cust_current_occupation_type
  ,e.education_model_likelihood as cust_current_education_model_likelihood
  ,cast(e.household_discretionary_spend_estimate_spend_on_apparel as integer) as cust_current_household_discretionary_spend_estimate_spend_on_apparel
  ,cast(e.household_discretionary_spend_estimate_score as integer) as cust_current_household_discretionary_spend_estimate_score
  ,cast(e.household_estimated_income_national_percentile as integer) as cust_current_household_estimated_income_national_percentile
  ,cast(e.household_estimated_income_amount_in_thousands as integer) as cust_current_household_estimated_income_amount_in_thousands
  ,CASE WHEN e.household_estimated_income_range in ('$1,000-$14,999', '$15,000-$24,999', '$25,000-$34,999', '$35,000-$49,999', '$50,000-$74,999') THEN '1) < $75,000' 
        WHEN e.household_estimated_income_range in ('$75,000-$99,999') THEN '2) $75,000 - $99,999'
        WHEN e.household_estimated_income_range in ('$100,000-$124,999', '$125,000-$149,999') THEN '3) $100,000-$149,999'
        WHEN e.household_estimated_income_range in ('$150,000-$174,999', '$175,000-$199,999') THEN '4) $150,000-$199,999'
        WHEN e.household_estimated_income_range in ('$200,000-$249,999', '$250,000+') THEN '5) $200,000+'
        ELSE 'Unknown' END
        as cust_current_household_estimated_income_range 
  ,cast(e.household_buyer_young_adult_clothing_shopper_likelihood_rank_asc as integer) as cust_current_household_buyer_young_adult_clothing_shoppers_likelihood
  ,cast(e.household_buyer_prestige_makeup_user_likelihood_rank_asc as integer) as cust_current_household_buyer_prestige_makeup_user_likelihood
  ,cast(e.household_buyer_luxury_store_shoppers_likelihood_rank_asc as integer) as cust_current_household_buyer_luxury_store_shoppers_likelihood
  ,cast(e.household_buyer_loyalty_card_user_likelihood_rank_asc as integer) as cust_current_household_buyer_loyalty_card_user_likelihood
  ,cast(e.household_buyer_online_overall_propensity_model_rank_desc as integer) as cust_current_household_buyer_online_overall_propensity
  ,cast(e.household_buyer_retail_overall_propensity_model_rank_desc as integer) as cust_current_household_buyer_retail_overall_propensity
  ,cast(e.household_buyer_consumer_expenditure_apparel_propensity_model_rank_desc as integer) as cust_current_household_buyer_consumer_expenditure_apparel_propensity
  ,cast(e.household_buyer_online_apparel_propensity_model_rank_desc as integer) as cust_current_household_buyer_online_apparel_propensity
  ,cast(e.household_buyer_retail_apparel_propensity_model_rank_desc as integer) as cust_current_household_buyer_retail_apparel_propensity
  ,cast(e.household_buyer_consumer_expenditure_shoes_propensity_model_rank_desc as integer) as cust_current_household_buyer_consumer_expenditure_shoes_propensity
  ,cast(e.household_buyer_online_shoe_propensity_model_rank_desc as integer) as cust_current_household_buyer_online_shoes_propensity
  ,cast(e.household_buyer_retail_shoes_propensity_model_rank_desc as integer) as cust_current_household_buyer_retail_shoes_propensity
  ,e.social_media_predictions_facebook_usage_likelihood as cust_current_social_media_predictions_facebook_usage_likelihood
  ,e.social_media_predictions_instagram_usage_likelihood as cust_current_social_media_predictions_instagram_usage_likelihood
  ,e.social_media_predictions_pinterest_usage_likelihood as cust_current_social_media_predictions_pinterest_usage_likelihood
  ,e.social_media_predictions_linkedin_usage_likelihood as cust_current_social_media_predictions_linkedin_usage_likelihood
  ,e.social_media_predictions_twitter_usage_likelihood as cust_current_social_media_predictions_twitter_usage_likelihood
  ,e.social_media_predictions_snapchat_usage_likelihood as cust_current_social_media_predictions_snapchat_usage_likelihood
  ,e.social_media_predictions_youtube_usage_likelihood as cust_current_social_media_predictions_youtube_usage_likelihood
  ,e.retail_shopper_type as cust_current_retail_shopper_type
  ,e.household_mosaic_lifestyle_segment as cust_current_household_mosaic_lifestyle_segment
  ,j.mosaic_group as cust_current_household_mosaic_lifestyle_group
  ,j.mosaic_group_summary as cust_current_household_mosaic_lifestyle_group_summary
  ,e.truetouch_strategy_brand_loyalists_likelihood as cust_current_truetouch_strategy_brand_loyalists_likelihood
  ,e.truetouch_strategy_deal_seekers_likelihood as cust_current_truetouch_strategy_deal_seekers_likelihood
  ,e.truetouch_strategy_moment_shoppers_likelihood as cust_current_truetouch_strategy_moment_shoppers_likelihood
  ,e.truetouch_strategy_mainstream_adopters_likelihood as cust_current_truetouch_strategy_mainstream_adopters_likelihood
  ,e.truetouch_strategy_novelty_seekers_likelihood as cust_current_truetouch_strategy_novelty_seekers_likelihood
  ,e.truetouch_strategy_organic_and_natural_likelihood as cust_current_truetouch_strategy_organic_and_natural_likelihood
  ,e.truetouch_strategy_quality_matters_likelihood as cust_current_truetouch_strategy_quality_matters_likelihood
  ,e.truetouch_strategy_recreational_shoppers_likelihood as cust_current_truetouch_strategy_recreational_shoppers_likelihood
  ,e.truetouch_strategy_savvy_researchers_likelihood as cust_current_truetouch_strategy_savvy_researchers_likelihood
  ,e.truetouch_strategy_trendsetters_likelihood as cust_current_truetouch_strategy_trendsetters_likelihood
  ,cast(f.affinity_accessories as integer) as cust_current_affinity_accessories
  ,cast(f.affinity_active as integer) as cust_current_affinity_active
  ,cast(f.affinity_apparel as integer) as cust_current_affinity_apparel
  ,cast(f.affinity_beauty as integer) as cust_current_affinity_beauty
  ,cast(f.affinity_designer as integer) as cust_current_affinity_designer
  ,cast(f.affinity_home as integer) as cust_current_affinity_home
  ,cast(f.affinity_kids as integer) as cust_current_affinity_kids
  ,cast(f.affinity_mens as integer) as cust_current_affinity_mens
  ,cast(f.affinity_shoes as integer) as cust_current_affinity_shoes
  ,cast(f.affinity_womens as integer) as cust_current_affinity_womens
  ,g.predicted_ct_segment as cust_current_predicted_core_target_segment
  ,i.cust_current_social_media_predictions_facebook_usage_score
  ,i.cust_current_social_media_predictions_instagram_usage_score
  ,i.cust_current_social_media_predictions_pinterest_usage_score
  ,i.cust_current_social_media_predictions_linkedin_usage_score
  ,i.cust_current_social_media_predictions_twitter_usage_score
  ,i.cust_current_social_media_predictions_snapchat_usage_score
  ,i.cust_current_social_media_predictions_youtube_usage_score
  ,i.cust_current_truetouch_strategy_brand_loyalists_score
  ,i.cust_current_truetouch_strategy_deal_seekers_score
  ,i.cust_current_truetouch_strategy_moment_shoppers_score
  ,i.cust_current_truetouch_strategy_mainstream_adopters_score
  ,i.cust_current_truetouch_strategy_novelty_seekers_score
  ,i.cust_current_truetouch_strategy_organic_and_natural_score
  ,i.cust_current_truetouch_strategy_quality_matters_score
  ,i.cust_current_truetouch_strategy_recreational_shoppers_score
  ,i.cust_current_truetouch_strategy_savvy_researchers_score
  ,i.cust_current_truetouch_strategy_trendsetters_score
  ,case when k.rank_record <= 500000 then 1 else 0 end as cust_jwn_top500k
  ,case when k.rank_record <= 1000 then 1 else 0 end as cust_jwn_top1k
  ,current_timestamp(6) as dw_sys_load_tmstp
  ,current_timestamp(6) as dw_sys_updt_tmstp
from cust_chan_level_derived a
left join 
  {cco_t2_schema}.cco_buyer_flow_fy b  
  on a.acp_id=b.acp_id 
  and a.channel=b.channel 
  and a.fiscal_year_shopped=b.fiscal_year_shopped
left join 
  {cco_t2_schema}.cco_customer_level_attributes_fy c 
  on a.acp_id=c.acp_id 
  and a.fiscal_year_shopped=c.fiscal_year_shopped
left join
  T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS d
  on a.acp_id = d.acp_id
left join
  prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim e
  on a.acp_id = e.acp_id
left join
  T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_MERCH f
  on a.acp_id = f.acp_id
left join
  t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment g
  on a.acp_id = g.acp_id
left join
  t2dl_das_cal.customer_attributes_scores h
  on a.acp_id = h.acp_id
left join
  experian_likelihood_scores i
on a.acp_id = i.acp_id
left join
  t2dl_das_usl.experian_mosaic_segments j
on e.household_mosaic_lifestyle_segment = j.mosaic_segment
left join
  cust_jwn_rank k
on a.acp_id=k.acp_id
and a.fiscal_year_shopped=k.fiscal_year_shopped;

collect statistics
column  (acp_id),
column  (fiscal_year_shopped),
column  (channel),
column  (cust_gender),
column  (cust_age),
column  (cust_lifestage),
column  (cust_age_group),
column  (cust_NMS_market),
column  (cust_dma),
column  (cust_country),
column  (cust_dma_rank),
column  (cust_loyalty_type),
column  (cust_loyalty_level),
column  (cust_employee_flag),
column  (acp_id, fiscal_year_shopped, channel)
on
{cco_t2_schema}.cco_cust_chan_yr_attributes_fy;

SET QUERY_BAND = NONE FOR SESSION;
