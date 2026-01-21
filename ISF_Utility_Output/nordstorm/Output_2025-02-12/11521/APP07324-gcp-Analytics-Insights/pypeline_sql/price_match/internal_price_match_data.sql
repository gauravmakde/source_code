/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09035;
     DAG_ID=internal_price_match_data_11521_ACE_ENG;
     Task_Name=internal_price_match_data;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_PRICE_MATCHING
Team/Owner: Nicole Miao
Date Created/Modified: Dec 4, 2023

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

*/


create multiset volatile table promo_dim_extract as
(select distinct event_id
                ,enticement_tags as event_name
                ,trim(promo_ledger_id) as promo_id
                ,promo_name
  from prd_nap_usr_vws.product_promotion_timeline_dim
  where trim(event_id) in ('856', '1121', '1234')
)with data primary index (promo_id) on commit preserve rows;


create multiset volatile table price_match_promos as
(select tbl_source, event_id,  event_name,  promo_id, promo_name
  from (select 'nap' as tbl_source, event_id,  event_name,  promo_id, promo_name
         from promo_dim_extract  --prd_nap_usr_vws.product_promotion_timeline_dim table starts in 2021
        union all
        select tbl_source, 
                cast(event_id as varchar(25)), 
                cast(event_name as varchar(2000)), 
                cast(promo_id as varchar(25)), 
                cast(promo_name as varchar(2000))
        from {price_matching_t2_schema}.PROMOTION_EVENT_HISTORICAL_DATA t3  --t3dl_ace_bie.promo_event_desc_hist as t3  --t3dl_ace_bie.promo_event_desc_hist goes back further
        where event_id in (856, 1121, 1234)
          and not exists (select 1 from promo_dim_extract as pdx where pdx.promo_id = trim(t3.promo_id)) ) as tbl
 qualify row_number() over (partition by promo_id order by event_name nulls last) = 1
)with data primary index(promo_id) on commit preserve rows;



create multiset volatile table customer_tran_line as
(select distinct dtl.acp_id
                ,dtl.global_tran_id
                , case when dtl.line_item_activity_type_desc = 'SALE' and coalesce(dtl.nonmerch_fee_code, '-999') <> '6666' then global_tran_id else null end as sale_order_num
                , case when dtl.line_item_activity_type_desc = 'RETURN' and coalesce(dtl.nonmerch_fee_code, '-999') <> '6666' then global_tran_id else null end as return_order_num
                ,business_unit_desc
                ,dtl.line_item_seq_num
--                 ,dtl.upc_num
                ,dtl.sku_num
                ,case when coalesce(dtl.line_net_usd_amt,0) >= 0 then coalesce(dtl.order_date,dtl.tran_date) else dtl.tran_date end as activity_date
                ,case when dtl.line_item_activity_type_desc = 'SALE' and coalesce(dtl.nonmerch_fee_code, '-999') <> '6666' then dtl.line_net_usd_amt else 0 end as shipped_sales
                ,case when dtl.line_item_activity_type_desc = 'SALE' and coalesce(dtl.nonmerch_fee_code, '-999') <> '6666' then dtl.line_item_regular_price else 0 end as regular_sales
                ,case when dtl.line_item_activity_type_desc = 'SALE' and coalesce(dtl.nonmerch_fee_code, '-999') <> '6666' then dtl.line_item_promo_usd_amt else 0 end as promo_sales

                ,(case when dtl.line_item_activity_type_desc = 'RETURN' and coalesce(dtl.nonmerch_fee_code, '-999') <> '6666' then dtl.line_net_usd_amt*-1 else 0 end) as return_sales
                ,(case when dtl.line_item_activity_type_desc = 'SALE' and coalesce(dtl.nonmerch_fee_code, '-999') <> '6666' then dtl.line_item_quantity else 0 end) as shipped_qty
	            ,(case when dtl.line_item_activity_type_desc = 'RETURN' and coalesce(dtl.nonmerch_fee_code, '-999') <> '6666' then dtl.line_item_quantity else 0 end) as return_qty
                ,case when pr.promo_id is not null and dtl.line_item_activity_type_code = 's' and dtl.line_net_usd_amt > 0 and dtl.upc_num is not null and dtl.line_item_merch_nonmerch_ind = 'merch' then 1 else 0 end price_match_ind
                ,case when pr.promo_id is not null and dtl.line_item_activity_type_code = 's' and dtl.line_net_usd_amt > 0 and dtl.upc_num is not null and dtl.line_item_merch_nonmerch_ind = 'merch' then promo_name else NULL end promo_name
    from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
    left join prd_nap_usr_vws.store_dim st on dtl.intent_store_num = st.store_num
    left join promo_dim_extract as pr
        on coalesce(dtl.line_item_promo_id ,'X'||substring(dtl.transaction_identifier,12,6)) = pr.promo_id
    left join prd_nap_usr_vws.day_cal dc on dc.day_date = dtl.business_day_date
    where case when coalesce(dtl.line_net_usd_amt,0) >= 0 then coalesce(dtl.order_date,dtl.tran_date) else dtl.tran_date end between {start_date} and {end_date}
    and dtl.acp_id is not null
    and dtl.global_tran_id is not null
    and dtl.line_item_seq_num is not null
    and business_unit_desc in ('RACK', 'N.COM', 'FULL LINE', 'OFFPRICE ONLINE') --'OFFPRICE ONLINE',
)with data primary index(global_tran_id,line_item_seq_num) on commit preserve rows;




--- NTN Table
create multiset volatile table ntn_tbl as (
     select distinct acp_id,
                    year_num as acquired_year,
--                     aare_chnl_code,
                    case when aare_chnl_code = 'NCOM' then 'N.COM'
                         when aare_chnl_code = 'NRHL' then 'OFFPRICE ONLINE'
                         when aare_chnl_code = 'RACK' then 'RACK'
                         when aare_chnl_code = 'FLS' then 'FULL LINE' end as aare_chnl_code,
                    aare_status_date,
                    aare_status_date+365 as activated_window
    from prd_nap_usr_vws.CUSTOMER_NTN_STATUS_FACT ca
    left join prd_nap_usr_vws.day_cal dc
        on dc.day_date = ca.aare_status_date
    where 1 = 1
        and month_num >= 202201
) with data primary index (acp_id, aare_status_date) on commit preserve rows;



create multiset volatile table pricematch_sku_sales_daily2 as (
    select a.*,
           case when n.acp_id is not null then 'NTN' else 'EXISTING' end as acquired_customers
    from customer_tran_line a
    left join ntn_tbl n on a.acp_id = n.acp_id and a.activity_date = n.aare_status_date and a.business_unit_desc = n.aare_chnl_code
) with data primary index (global_tran_id,line_item_seq_num) on commit preserve rows;
collect statistics on pricematch_sku_sales_daily2 column (global_tran_id,line_item_seq_num) ;






/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/

delete from {price_matching_t2_schema}.INTERNAL_PRICE_MATCHING
WHERE   activity_date >= {start_date}
AND     activity_date <= {end_date};

insert into {price_matching_t2_schema}.INTERNAL_PRICE_MATCHING 
  select pm.*
    , CURRENT_TIMESTAMP as dw_sysload_tmstp
  from pricematch_sku_sales_daily2 pm
;


COLLECT STATISTICS  COLUMN(global_tran_id,line_item_seq_num,activity_date),
                    COLUMN(global_tran_id),
                    COLUMN(line_item_seq_num),
                    COLUMN(activity_date)
on  {price_matching_t2_schema}.INTERNAL_PRICE_MATCHING;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


