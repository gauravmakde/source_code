SET QUERY_BAND = 'App_ID=APP08159;
     DAG_ID=cust_service_11521_ACE_ENG;
     Task_Name=cust_service_month;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {service_eng_t2_schema}.cust_service_month
Team：Customer Analytics - Styling & Strategy
Date Created: Mar. 12th 2023 

Note:
-- Update Cadence: Monthly (after end of each fiscal month)

*/

/*----------------------------------------------------------------------
--A. Date Handle & Transactions / Returns & Services Tagging
----------------------------------------------------------------------*/

/***************************************************************************************/
/***************************************************************************************/
/********* Step 1: create realigned fiscal calendar                            *********/
/***************************************************************************************/
/***************************************************************************************/

/***************************************************************************************/
/********* Step 1-a: Going back to start of 2009, find all years with a "53rd week" *********/
/***************************************************************************************/
--drop table week_53_yrs;
create MULTISET volatile table week_53_yrs as (
select year_num
  ,rank() over (order by year_num desc) recency_rank
from
  (
  select distinct year_num
  from PRD_NAP_USR_VWS.DAY_CAL
  where week_of_fyr = 53
    and day_date between date'2009-01-01' and current_date+365 
  ) x 
) with data primary index(year_num) on commit preserve rows;

/***************************************************************************************/
/********* Step 1-b: Count the # years with a "53rd week" *********/
/***************************************************************************************/
--drop table week_53_yr_count;
create MULTISET volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

/***************************************************************************************/
/********* Step 1-c: Create an empty table *********/
/***************************************************************************************/
--drop table realigned_fiscal_calendar;
create MULTISET volatile table realigned_fiscal_calendar ,NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO 
(
  day_date date 
  ,day_num integer 
  ,day_desc varchar(30)
  ,week_num integer
  ,week_desc varchar(30)
  ,month_num integer
  ,month_short_desc varchar(30)
  ,quarter_num integer
  ,halfyear_num integer
  ,year_num integer
  ,month_454_num integer
  ,year_454_num integer
) primary index (day_date) on commit preserve rows;

/***************************************************************************************/
/********* Step 1-d: insert data into realigned calendar 
 *  (any weeks before the latest 53rd week should get started 7 days late,
 *   any weeks before the 2nd-latest 53rd week year get started 14 days late,
 *   any weeks before the 3rd-latest 53rd week year get started 21 days late,
 *   any weeks before the 4th-latest 53rd week year get started 28 days late,
 * *********/
 /***************************************************************************************/
insert into realigned_fiscal_calendar
select
  case when year_num > (select year_num from week_53_yrs where recency_rank=1) then day_date
        when (select year_count from week_53_yr_count) = 1 and year_num <= (select year_num from week_53_yrs where recency_rank=1) then day_date + 7
        when (select year_count from week_53_yr_count) = 2 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
        when (select year_count from week_53_yr_count) = 2 and year_num <= (select year_num from week_53_yrs where recency_rank=2) then day_date + 14
        when (select year_count from week_53_yr_count) = 3 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
        when (select year_count from week_53_yr_count) = 3 and year_num >  (select year_num from week_53_yrs where recency_rank=3) then day_date + 14
        when (select year_count from week_53_yr_count) = 3 and year_num <= (select year_num from week_53_yrs where recency_rank=3) then day_date + 21
        when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
        when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=3) then day_date + 14
        when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=4) then day_date + 21
        when (select year_count from week_53_yr_count) = 4 and year_num <= (select year_num from week_53_yrs where recency_rank=4) then day_date + 28
        else null end day_date
  ,day_num
  ,day_desc
  ,week_num
  ,week_desc
  ,month_num
  ,month_short_desc
  ,quarter_num
  ,halfyear_num
  ,year_num
  ,month_454_num
  ,year_num year_454_num
from PRD_NAP_USR_VWS.DAY_CAL
where day_date between date'2009-01-01' and current_date+365 
  and week_of_fyr <> 53
;

COLLECT STATISTICS COLUMN (MONTH_NUM) ON     realigned_fiscal_calendar;
COLLECT STATISTICS COLUMN (DAY_DATE) ON     realigned_fiscal_calendar;

--DROP TABLE get_months;
CREATE multiset volatile TABLE get_months AS (
select 
  distinct earliest_minus_3
  ,earliest_act_mo  

  ,last_complete_mo-200 part1_end_mo
  ,case when mod(last_complete_mo,100)=12 then last_complete_mo-111 else last_complete_mo-199 end part2_start_mo
  ,last_complete_mo-100 part2_end_mo
  ,case when mod(last_complete_mo,100)=12 then last_complete_mo-11 else last_complete_mo-99 end part3_start_mo
  ,last_complete_mo

from
  ( select distinct current_date todays_date
       ,month_num todays_month
       ,case when mod(month_num,100)=1 then month_num-89 else month_num-1 end last_complete_mo
       ,case when mod(month_num,100)=12 then month_num-211 else month_num-299 end earliest_act_mo
       ,case when mod(month_num,100) in (1,2) then month_num-390 else month_num-302 end earliest_minus_3
    from realigned_fiscal_calendar a
    where day_date=(current_date /*- 25*/)
    --where day_date='2020-02-25'
  ) x
) with data primary index(last_complete_mo) on commit preserve rows;


--drop table date_range;
CREATE multiset volatile TABLE date_range AS (
select 
   min(case when month_num = (select earliest_minus_3 from get_months) then day_date else null end) earliest_dt
  ,min(case when month_num = (select earliest_act_mo from get_months) then day_date else null end) earliest_act_dt
  
  ,max(case when month_num = (select part1_end_mo from get_months) then day_date else null end) part1_end_dt
  ,min(case when month_num = (select part2_start_mo from get_months) then day_date else null end) part2_start_dt
  ,max(case when month_num = (select part2_end_mo from get_months) then day_date else null end) part2_end_dt
  ,min(case when month_num = (select part3_start_mo from get_months) then day_date else null end) part3_start_dt
  
  ,max(case when month_num = (select last_complete_mo from get_months) then day_date else null end) latest_mo_dt

from realigned_fiscal_calendar
) with data primary index(latest_mo_dt) on commit preserve rows;

/*----------------------------------------------------------------------
--B-4) Pull Line-item-level PURCHASES & RETURNS from Transaction table - sales_and_returns_fact
  (includes flags for Various Services)
----------------------------------------------------------------------*/

CREATE multiset volatile TABLE sls_olf_combined_pre AS (
SELECT DISTINCT a.global_tran_id
               ,a.line_item_seq_num
               ,lpad(coalesce(trim(a.sku_num),'HI_MOM'),15,'0') padded_sku -- need?
               ,a.acp_id
               ,coalesce(a.order_date,a.tran_date) purch_dt
               ,cal.month_num purch_mo
               ,a.business_day_date
               ,a.shipped_usd_sales gross_sales
               ,case when a.line_item_order_type like 'CustInit%' 
                          and a.line_item_fulfillment_type = 'StorePickUp'
                          and a.business_unit_desc IN ('N.COM', 'N.CA','OFFPRICE ONLINE','TRUNK CLUB') then a.ringing_store_num
                          -- not sure on the 3rd above; original: a.data_source_code = 'COM' - Do we use it in later query: only up to C: Line-item table with Service Flags
                     else a.intent_store_num end as reporting_store_num
              ,case when a.line_item_order_type like 'CustInit%' 
                         and a.line_item_fulfillment_type = 'StorePickUp'
                         and a.business_unit_desc IN ('N.COM', 'N.CA','OFFPRICE ONLINE','TRUNK CLUB') then '2) n.com'
                         -- not sure on the 3rd above; original: a.data_source_code = 'COM' - Do we use it in later query: only up to C:Line-item table with Service Flags
                    when a.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then '1) NS'
                    when a.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then '2) n.com'
                    when a.business_unit_desc in ('RACK', 'RACK CANADA') then '3) RS'
                    when a.business_unit_desc in ('OFFPRICE ONLINE') then '4) nr.com'
                    else null end as reporting_channel
               ,a.intent_store_num
               ,a.destination_node_num destination_store
               ,a.destination_zip_code DESTINATION_ZIP_CODE
               --,a.ringing_store_num
               --,a.transaction_identifier - no transaction identifier in the new table - might not needed
              ,case when upper(trim(a.business_unit_desc)) in ('FULL LINE CANADA','N.CA','RACK CANADA') then 'CA' else 'US' end country
              ,a.commission_slsprsn_num
              ,coalesce(a.order_num,'#'||a.global_tran_id) order_num -- need?

              ,case when a.nonmerch_fee_code='6666' then 1 else 0 end bought_giftcard
              
              -- Remote Sell Categories (SB, SL, Private Styling, & TC)
              ,case when boards.remote_sell_swimlane in ('STYLEBOARD_ATTRIBUTED','STYLEBOARD_PRIVATE_ATTRIBUTED','PERSONAL_REQUEST_A_LOOK_ATTRIB', 'PERSONAL_REQUEST_A_LOOK_ATTRIBUTED','REQUEST_A_LOOK_ATTRIBUTED','CHAT_BOARD_ATTRIBUTED') then 'A01) Style Boards' else null end STYLE_BOARD_SERVICE
              ,case when boards.remote_sell_swimlane in ('STYLELINK_ATTRIBUTED','STYLEBOARD_PUBLIC_ATTRIBUTED') then 'A02) Style Links' else null end STYLE_LINK_SERVICE
              ,case when boards.remote_sell_swimlane in ('PRIVATE_STYLING_ATTRIBUTED','ECF_ATTRIBUTED') then 'A03) Style Board OR Style Link' else null end as PRIVATE_STYLING_SERVICE
              -- ,case when boards.remote_sell_swimlane = 'STYLEBOARD_ATTRIBUTED' then 'A01) Style Boards' else null end STYLE_BOARD_SERVICE
              -- ,case when boards.remote_sell_swimlane = 'STYLELINK_ATTRIBUTED' then 'A02) Style Links' else null end STYLE_LINK_SERVICE
              -- ,case when boards.remote_sell_swimlane = 'PRIVATE_STYLING_ATTRIBUTED' then 'A03) Style Board OR Style Link' else null end as PRIVATE_STYLING_SERVICE
              ,case when boards.remote_sell_swimlane = 'TRUNK_CLUB' then 'A04) Nordstrom Trunks' else null end TRUNK_SERVICE

             ,case when a.shipped_usd_sales>0 then r.RESTAURANT_SERVICE else null end RESTAURANT_SERVICE

              -- N2U Service
              ,case when a.upc_num = '439027332977' then 'A05) Nordstrom To You' else null end N2U_SERVICE
              ,case when a.upc_num = '439027332984' then 'A06) Nordstrom To You - Closet' else null end N2U_CLOSET_SERVICE

              -- Alteration Fee Code Service (will get to NSEAM in later query)
               ,case when a.nonmerch_fee_code in ('1803','1926')   then 'C02) Non-Nordstrom Alterations'
                     when a.nonmerch_fee_code in ('108','809','116','272','817','663') then 'C01) Nordstrom Alterations'
                     when a.nonmerch_fee_code in ('3855','3786')   then 'C03) Rushed Alterations'
                     when a.nonmerch_fee_code = '647' then 'C04) Personalized Alterations'
                     when a.nonmerch_fee_code in ('8571','8463')   then 'F04) Shoe Shine'
                     else null end FEE_CODE_SERVICES

              -- Beauty Service
              ,bsc.BEAUTY_SERVICE

              -- Selling Relationship Service: fields in the new table also come from prd_nap_hr_usr_vws.hr_worker_dim
              ,case when a.payroll_dept IN ('1196') then 'B04) Personal Stylist'
                    when a.payroll_dept IN ('1205') then 'B01) Independent Stylist'
                    when a.payroll_dept IN ('1404') then 'B02) Beauty Stylist'
                    when a.hr_es_ind IN ('Y') then 'B03) Emerging Stylist'
                    else null end SELLING_SERVICE

              -- In-Store Styling Service
              ,case when (substring(coalesce(SELLING_SERVICE,'XYZ'),1,3) IN ('B01','B03','B04')
                         and not(COALESCE(a.line_item_order_type,'XYZ') = 'CustInitWebOrder' and COALESCE(a.line_item_fulfillment_type,'XYZ') = 'StorePickUp')
                         and a.shipped_usd_sales>=0 and COALESCE(a.item_source,'XYZ') <> 'SB_SALESEMPINIT' 
                         and substring(a.business_unit_desc,1,4) IN ('FULL')) then 'A07) In-Store Styling' else null end InStore_Styling_SERVICE
               -- For tagging In-Store Styling: a.line_item_order_type, a.line_item_fulfillment_type, a.item_source, a.business_unit_desc

              -- Expedited Delivery and Fulfillment Service (fields in the new table also come from Order Line Detail Fact)
              ,case when trim(upper(a.promise_type_code)) = 'SAME_DAY_COURIER' then 'D04) Same Day Delivery'
                    when a.requested_level_of_service_code = '07' then 'E04) Next Day BOPUS' --Part of "Next-Day Pickup"
                    when upper(a.delivery_method_code) = 'PICK' then 'E03) Same Day BOPUS'
                    when a.requested_level_of_service_code = '11' then 'E05) Next Day Ship to Store' --Part of "Next-Day Pickup"
                    when a.destination_node_num > 0 and upper(coalesce(a.delivery_method_code,'NONE')) <> 'PICK' and a.requested_level_of_service_code = '42' then 'D02) Free Expedited Ship to Store' --Part of "Next-Day Pickup"
                    when a.requested_level_of_service_code = '42' then 'D01) Free Expedited Delivery'
                    when a.destination_node_num > 0 and upper(coalesce(a.delivery_method_code,'NONE')) <> 'PICK' then 'E02) Ship to Store'
                    when substring(upper(a.promise_type_code),1,3) in ('ONE','TWO','EXP') or upper(a.promise_type_code) like '%BUSINESSDAY%' then 'D05) Paid Expedited Delivery'
                    else null end as FULFILLMENT_SERVICE
              ,case when upper(trim(a.curbside_pickup_ind))='Y' then 'E01) Curbside Pickup' else null end CURBSIDE_SERVICE

FROM T2DL_DAS_SALES_RETURNS.sales_and_returns_fact a
left join prd_nap_usr_vws.store_dim st2 on a.return_ringing_store_num=st2.store_num
-- Item Delivery Method
--left join T2DL_DAS_ITEM_DELIVERY.item_delivery_method_funnel_daily idm on a.order_line_id = idm.order_line_id
--Remote Sell Swimlane
left join (
      select distinct remote_sell_swimlane
                     ,cast(lpad(trim(upc_num),32,'0')as varchar(32)) as upc_num
	               ,cast(global_tran_id as bigint) global_tran_id
	from T2DL_DAS_REMOTE_SELLING.remote_sell_transactions 
  where remote_sell_swimlane like '%ATTRIB%' or remote_sell_swimlane in ('TRUNK_CLUB','ECF_ATTRIBUTED')
	--where remote_sell_swimlane in ('STYLEBOARD_ATTRIBUTED','STYLEBOARD_PRIVATE_ATTRIBUTED','PERSONAL_REQUEST_A_LOOK_ATTRIB', 'PERSONAL_REQUEST_A_LOOK_ATTRIBUTED','REQUEST_A_LOOK_ATTRIBUTED','STYLELINK_ATTRIBUTED','STYLEBOARD_PUBLIC_ATTRIBUTED','PRIVATE_STYLING_ATTRIBUTED','ECF_ATTRIBUTED','TRUNK_CLUB')
	and upc_num is not null
	and global_tran_id is not null
) boards
on trim(a.global_tran_id) = trim(boards.global_tran_id) and cast(lpad(trim(a.upc_num),32,'0')as varchar(32)) = boards.upc_num
-- Restaurant Services
left join (
      select distinct dept_num dept_num
                     --,org_dept_long_desc dept_name
                     ,case when dept_num in (113,188,571,698) then 'G02) Coffee'
                           when dept_num in (568,692,715)     then 'G03) Bar'
                           else 'G01) Food' end RESTAURANT_SERVICE
      from PRD_NAP_USR_VWS.DEPARTMENT_DIM   --This will change when view goes away
      --where record_active_flag='Y'
      WHERE division_num=70
) r
on coalesce(a.merch_dept_num,-1*a.intent_store_num) = r.dept_num
-- Beauty Services
left join (
      select distinct lpad(coalesce(trim(rms_sku_num)
                     ,'HI_MOM'),15,'0') rms_sku_num -- maybe not needed any more
                     ,channel_country
                     ,case when dept_num = 102 then 'F01) Spa'
                           when dept_num = 497 and (class_num in (10,11) or style_desc like '%NAIL%') then 'F02) Nail Bar'
                           when dept_num = 497 and class_num in (12,14) then 'F03) Eyebrows'
                           else null end BEAUTY_SERVICE
      from prd_nap_usr_vws.PRODUCT_sku_dim
      where (dept_num = '102' or (dept_num = '497' and (class_num in (10,11,12,14) 
             or style_desc like '%NAIL%') ) ) 
) bsc 
on padded_sku=bsc.rms_sku_num and country=bsc.channel_country
join realigned_fiscal_calendar cal on coalesce(a.order_date,a.tran_date)=cal.day_date
--left join realigned_fiscal_calendar cal2 on coalesce(a.return_date,a.business_day_date+20000)=cal2.day_date
where a.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB') -- limit to FL
and a.business_day_date between (SELECT earliest_dt FROM date_range) AND (SELECT latest_mo_dt FROM date_range)
and (case when a.shipped_usd_sales >0 then coalesce(a.order_date,a.tran_date) else a.tran_date end) 
between (select earliest_dt FROM date_range) and (select latest_mo_dt FROM date_range)
and a.shipped_usd_sales >= 0
and coalesce(a.employee_discount_usd_amt,0)=0
and a.acp_id is not null
) with data primary index(global_tran_id,line_item_seq_num) on commit preserve rows;

CREATE multiset volatile TABLE sls_olf_combined_returns AS (
  SELECT DISTINCT a.global_tran_id
               ,a.line_item_seq_num
               -- Return Service
               ,coalesce(a.return_date,a.business_day_date+20000) return_date
               ,cal2.month_num return_mo
               ,coalesce(-1*a.return_usd_amt,0) return_usd_amt
               ,a.return_ringing_store_num return_store
               ,case when substring(upper(st2.business_unit_desc),1,4) in ('FULL','RACK') 
                         and a.upc_num is not null then 'I_) STORE RETURNS' else null end STORE_RETURN_SERVICE -- derive based on the above 2 fields
FROM T2DL_DAS_SALES_RETURNS.sales_and_returns_fact a
left join prd_nap_usr_vws.store_dim st2 on a.return_ringing_store_num=st2.store_num
left join realigned_fiscal_calendar cal2 on coalesce(a.return_date,a.business_day_date+20000)=cal2.day_date
WHERE coalesce(a.return_date,a.business_day_date+20000) between (SELECT earliest_dt FROM date_range) AND (SELECT latest_mo_dt FROM date_range)
--and a.shipped_usd_sales >= 0
and coalesce(a.employee_discount_usd_amt,0)=0
--and acp_id is not null
) with data primary index(global_tran_id,line_item_seq_num) on commit preserve rows;

CREATE multiset volatile TABLE sls_olf_combined AS (
  SELECT a.*
        ,b.return_date
        ,b.return_mo
        ,b.return_usd_amt
        ,b.return_store
        ,b.STORE_RETURN_SERVICE
  FROM sls_olf_combined_pre a
  left join sls_olf_combined_returns b
  on a.global_tran_id = b.global_tran_id
  and a.line_item_seq_num = b.line_item_seq_num
  --and a.gross_sales=(-1*b.return_usd_amt)
) with data primary index(global_tran_id,line_item_seq_num) on commit preserve rows;

/*----------------------------------------------------------------------*/
/*----------------------------------------------------------------------
--C: Finalize Line-item table with Service flags
----------------------------------------------------------------------*/
/*----------------------------------------------------------------------*/
create multiset volatile table service_line_item_detail as 
(select distinct b.global_tran_id
  ,b.line_item_seq_num
  ,b.padded_sku
  ,b.acp_id
  ,b.reporting_store_num
  ,b.reporting_channel
  ,b.intent_store_num
  ,b.destination_store
  ,b.purch_dt
  ,b.purch_mo
  ,b.business_day_date
  ,case when b.bought_giftcard=0 then b.gross_sales else 0 end gross_usd_amt
  ,b.return_date
  ,b.return_mo
  ,case when b.bought_giftcard=0 then coalesce(b.return_usd_amt,0) else 0 end return_usd_amt
  ,b.return_store
  , 
    --(case when b.PUBLIC_LOOKS_TRANS is not null then 1 else 0 end ) +
    --(case when b.SALESPERSON_VIDEOS_TRAN is not null then 1 else 0 end ) +
    cast((
    (case when b.PRIVATE_STYLING_SERVICE is not null then 1 else 0 end) + 
    (case when b.STYLE_BOARD_SERVICE is not null then 1 else 0 end) + 
    (case when b.STYLE_LINK_SERVICE is not null then 1 else 0 end) +
    (case when b.TRUNK_SERVICE is not null then 1 else 0 end) + 
    (case when b.N2U_SERVICE is not null then 1 else 0 end) + 
    (case when b.N2U_CLOSET_SERVICE is not null then 1 else 0 end) +
    (case when b.InStore_Styling_SERVICE is not null then 1 else 0 end) +
    --removing digital stylist
    --(case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='A06' then 1 else 0 end ) +
    --Eventually will add A07 thru A12 (new Remote-Selling Services)
    (case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='B01' then 1 else 0 end ) +
    (case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='B02' then 1 else 0 end ) +
    (case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='B03' then 1 else 0 end ) +
    (case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='B04' then 1 else 0 end ) +
    (case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='C01' then 1 else 0 end ) +
    (case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='C02' then 1 else 0 end ) +
    (case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='C03' then 1 else 0 end ) +
    (case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='C04' then 1 else 0 end ) +
    (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D01' then 1 else 0 end ) +
    (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D02' then 1 else 0 end ) +
    (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D04' then 1 else 0 end ) +
    (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D05' then 1 else 0 end ) +
    (case when b.CURBSIDE_SERVICE is not null then 1 else 0 end ) +
    (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='E02' then 1 else 0 end ) +
    (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='E03' then 1 else 0 end ) +
    (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='E04' then 1 else 0 end ) +
    (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='E05' then 1 else 0 end ) +
    --(case when substring(coalesce(b.STORE_RETURN_SERVICE,'XYZ'),1,3)='E08' then 1 else 0 end ) +
    (case when substring(coalesce(b.BEAUTY_SERVICE,'XYZ'),1,3)='F01' then 1 else 0 end ) +
    (case when substring(coalesce(b.BEAUTY_SERVICE,'XYZ'),1,3)='F02' then 1 else 0 end ) +
    (case when substring(coalesce(b.BEAUTY_SERVICE,'XYZ'),1,3)='F03' then 1 else 0 end ) +
    (case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='F04' then 1 else 0 end ) +
    (case when substring(coalesce(b.RESTAURANT_SERVICE,'XYZ'),1,3)='G01' then 1 else 0 end ) +
    (case when substring(coalesce(b.RESTAURANT_SERVICE,'XYZ'),1,3)='G02' then 1 else 0 end ) +
    (case when substring(coalesce(b.RESTAURANT_SERVICE,'XYZ'),1,3)='G03' then 1 else 0 end )) as int) service_count
  /*,case when service_count>0 and substring(coalesce(b.STORE_RETURN_SERVICE,'XYZ'),1,3)='E08' 
        then service_count - 1 else service_count end svc_count_omit_ret
  */
  ,case when service_count>0 then 1 else 0 end Any_service
  ,cast((case when b.bought_giftcard=1 then 0
            when service_count>0 then (cast(b.gross_sales as float) / cast(service_count as float)) 
              else b.gross_sales end) as numeric(20,2)) gross_usd_amt_svc_split
  ,cast((case when b.bought_giftcard=1 then 0
              when service_count>0 
                 then (cast( (coalesce(b.return_usd_amt,0)) as float)  /  cast(service_count as float))
              else (coalesce(b.return_usd_amt,0)) end) as numeric(20,2)) return_usd_amt_svc_split
  ,(case when (b.PRIVATE_STYLING_SERVICE is not null
              or b.STYLE_BOARD_SERVICE is not null
          --or b.PUBLIC_LOOKS_TRANS is not null
          --or b.SALESPERSON_VIDEOS_TRAN is not null
              or b.STYLE_LINK_SERVICE is not null 
              or b.TRUNK_SERVICE is not null
              or b.N2U_SERVICE is not null
              or b.N2U_CLOSET_SERVICE is not null
              or b.InStore_Styling_SERVICE is not null) 
          then 1 else 0 end) +
     (case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,1)='B'
          then 1 else 0 end) +
     (case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,1)='C'
          then 1 else 0 end) +  
     (case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,1)='D'
          then 1 else 0 end ) +
     (case when (substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,1)='E' 
          or b.CURBSIDE_SERVICE is not null
          or substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D02')
          --or b.STORE_RETURN_SERVICE is not null) 
          then 1 else 0 end) +
     (case when (substring(coalesce(b.BEAUTY_SERVICE,'XYZ'),1,1)='F'
              or substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,1)='F') 
          then 1 else 0 end) +
     (case when substring(coalesce(b.RESTAURANT_SERVICE,'XYZ'),1,1)='G'
          then 1 else 0 end)
    as service_group_count
  ,cast((case when service_group_count>0 
                  then cast(b.gross_sales as float) /  cast(service_group_count as float)
              else b.gross_sales end) as numeric(20,2)) gross_usd_amt_svcgrp_split
  ,cast((case when service_group_count>0 
                then (cast((coalesce(b.return_usd_amt,0)) as float) /  cast(service_group_count as float)) 
            else (coalesce(b.return_usd_amt,0)) end) as numeric(20,2)) return_usd_amt_svcgrp_split
  ,case when b.PRIVATE_STYLING_SERVICE is not null then 1 else 0 end svc_private_styling
  ,case when b.STYLE_BOARD_SERVICE is not null then 1 else 0 end svc_style_board
  ,case when b.STYLE_LINK_SERVICE is not null then 1 else 0 end svc_style_link
  ,case when b.TRUNK_SERVICE is not null then 1 else 0 end svc_trunk
  ,case when b.N2U_SERVICE is not null then 1 else 0 end svc_N2U
  ,case when b.N2U_CLOSET_SERVICE is not null then 1 else 0 end svc_N2U_closet
  ,case when b.InStore_Styling_SERVICE is not null then 1 else 0 end svc_inStore_styling
  --removing digital stylist
  --,case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='A06' then 1 else 0 end svc_digital_stylist
  --Eventually will add A07 thru A12 (new Remote-Selling Services)
  ,case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='B01' then 1 else 0 end svc_independent_stylist
  ,case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='B02' then 1 else 0 end svc_beauty_stylist
  ,case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='B03' then 1 else 0 end svc_emerging_stylist
  ,case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='B04' then 1 else 0 end svc_personal_stylist
  ,case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='C01' then 1 else 0 end svc_nordstrom_alterations
  ,case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='C02' then 1 else 0 end svc_nonnord_alterations
  ,case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='C03' then 1 else 0 end svc_rushed_alterations
  ,case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='C04' then 1 else 0 end svc_personalized_alterations
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D01' then 1 else 0 end svc_free_exp_delivery
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D02' then 1 else 0 end svc_free_exp_ship_to_store
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3) in ('D01','D02') then 1 else 0 end svc_any_free_exp
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D04' then 1 else 0 end svc_same_day_delivery
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D05' then 1 else 0 end svc_paid_exp_delivery
  ,case when b.CURBSIDE_SERVICE is not null then 1 else 0 end svc_curbside_pickup
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='E02' then 1 else 0 end svc_ship_to_store
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='E03' then 1 else 0 end svc_same_day_BOPUS
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='E04' then 1 else 0 end svc_next_day_BOPUS
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='E05' then 1 else 0 end svc_next_day_ship_to_store
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3) in ('E04','E05') then 1 else 0 end svc_any_next_day_pickup
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3) in ('D02','E02','E03','E04','E05') then 1 else 0 end svc_any_order_pickup
  --,case when substring(coalesce(b.STORE_RETURN_SERVICE,'XYZ'),1,3)='E08' then 1 else 0 end svc_store_returns
  ,case when substring(coalesce(b.BEAUTY_SERVICE,'XYZ'),1,3)='F01' then 1 else 0 end svc_spa
  ,case when substring(coalesce(b.BEAUTY_SERVICE,'XYZ'),1,3)='F02' then 1 else 0 end svc_nail_bar
  ,case when substring(coalesce(b.BEAUTY_SERVICE,'XYZ'),1,3)='F03' then 1 else 0 end svc_eyebrows
  ,case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,3)='F04' then 1 else 0 end svc_shoe_shine
  ,case when substring(coalesce(b.RESTAURANT_SERVICE,'XYZ'),1,3)='G01' then 1 else 0 end svc_food
  ,case when substring(coalesce(b.RESTAURANT_SERVICE,'XYZ'),1,3)='G02' then 1 else 0 end svc_coffee
  ,case when substring(coalesce(b.RESTAURANT_SERVICE,'XYZ'),1,3)='G03' then 1 else 0 end svc_bar

  ,case when (b.PRIVATE_STYLING_SERVICE is not null
              or b.STYLE_BOARD_SERVICE is not null
          --or b.PUBLIC_LOOKS_TRANS is not null
          --or b.SALESPERSON_VIDEOS_TRAN is not null 
              or b.STYLE_LINK_SERVICE is not null
              or b.TRUNK_SERVICE is not null
              or b.N2U_SERVICE is not null
              or b.N2U_CLOSET_SERVICE is not null
              or b.InStore_Styling_SERVICE is not null) 
          then 1 else 0 end SvcGroup_Styling
  ,case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,1)='B'
          then 1 else 0  end SvcGroup_Selling_Rel
  ,case when substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,1)='C'
          then 1 else 0  end SvcGroup_Alterations  
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,1)='D'
          then 1 else 0  end SvcGroup_Exp_Delivery
  ,case when (substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,1)='E' 
          or b.CURBSIDE_SERVICE is not null
          or substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3)='D02')
          then 1 else 0  end SvcGroup_Pickup
  ,case when (substring(coalesce(b.BEAUTY_SERVICE,'XYZ'),1,1)='F'
              or substring(coalesce(b.FEE_CODE_SERVICES,'XYZ'),1,1)='F') 
          then 1 else 0  end SvcGrp_In_Store   
  ,case when substring(coalesce(b.RESTAURANT_SERVICE,'XYZ'),1,1)='G'
          then 1 else 0  end SvcGrp_Restaurant
  ,case when b.STORE_RETURN_SERVICE is not null then 1 else 0 end SvcGrp_Store_Returns

  ,b.DESTINATION_ZIP_CODE
  --,case when substring(coalesce(b.SELLING_SERVICE,'XYZ'),1,3)='N2' then 1 else 0 end local_market_stylist
  --,case when b.CHAT_TRAN is not null then 1 else 0 end chat_tran_ind
  ,b.PRIVATE_STYLING_SERVICE
  ,b.STYLE_BOARD_SERVICE
  ,b.STYLE_LINK_SERVICE
  ,b.TRUNK_SERVICE
  ,b.BEAUTY_SERVICE
  ,b.RESTAURANT_SERVICE
  ,b.FEE_CODE_SERVICES
  ,b.N2U_SERVICE
  ,b.N2U_CLOSET_SERVICE
  ,b.InStore_Styling_SERVICE
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3) in ('D01','D02') then 'D03) Any Free Expedited' else null end ANY_FREE_EXP_SERVICE
  ,b.FULFILLMENT_SERVICE
  ,case when substring(coalesce(b.FULFILLMENT_SERVICE,'XYZ'),1,3) in ('E04','E05') then 'E06) Any Next-Day Pickup' else null end ANY_NEXT_DAY_PICKUP_SERVICE
  ,b.STORE_RETURN_SERVICE
  ,b.CURBSIDE_SERVICE 
  ,b.SELLING_SERVICE
  --,b.SALESPERSON_VIDEOS_TRAN 
  --,b.PUBLIC_LOOKS_TRANS
  --,b.CHAT_TRAN

from sls_olf_combined b
where b.purch_dt between (SELECT earliest_dt FROM date_range) AND (SELECT latest_mo_dt FROM date_range)
   or b.return_date between (SELECT earliest_dt FROM date_range) AND (SELECT latest_mo_dt FROM date_range)
) with data primary index(global_tran_id,line_item_seq_num) on commit preserve rows;


/*----------------------------------------------------------------------*/
/*----------------------------------------------------------------------
--A) Grab all SERVICE Purchases for any time-period in past 35 mo
----------------------------------------------------------------------*/
/*----------------------------------------------------------------------*/

create multiset volatile table acp_id_service_month_temp (
  acp_id varchar(50)
  ,service_mo decimal(6)
  ,service varchar(45)
  ,customer_qualifier integer
  ,nseam_only_ind integer 
  ,gross_usd_amt_whole decimal(20,2)
  ,net_usd_amt_whole decimal(20,2)
  ,gross_usd_amt_split decimal(20,2)
  ,net_usd_amt_split decimal(20,2)
) primary index (acp_id,service_mo,service) on commit preserve rows;

--Purchases for ANY Full-Price Activity
insert into acp_id_service_month_temp
select acp_id
  ,purch_mo service_mo
  ,'Z_) Nordstrom' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt) gross_usd_amt_split
  ,sum(gross_usd_amt) net_usd_amt_split
from service_line_item_detail
where purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

--Purchases for Private Styling Activity
insert into acp_id_service_month_temp
select acp_id
  ,purch_mo service_mo
  ,PRIVATE_STYLING_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where PRIVATE_STYLING_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

--Purchases for Style-Board Activity
insert into acp_id_service_month_temp
select acp_id
  ,purch_mo service_mo
  ,STYLE_BOARD_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where STYLE_BOARD_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

--Purchases for Style-Link Activity
insert into acp_id_service_month_temp
select acp_id
  ,purch_mo service_mo
  ,STYLE_LINK_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where STYLE_LINK_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

--Purchases for N2U Activity
insert into acp_id_service_month_temp
select acp_id
  ,purch_mo service_mo
  ,N2U_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where N2U_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

--Purchases for N2U CLOSET Activity
insert into acp_id_service_month_temp
select acp_id
  ,purch_mo service_mo
  ,N2U_CLOSET_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where N2U_CLOSET_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Nail, Spa, & Eyebrows
select acp_id
  ,purch_mo service_mo
  ,BEAUTY_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where BEAUTY_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Restaurant Activity
select acp_id
  ,purch_mo service_mo
  ,RESTAURANT_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where RESTAURANT_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Fee-Code Activity (Paid Alterations + Shoe-Shine)
select acp_id
  ,purch_mo service_mo
  ,FEE_CODE_SERVICES service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where FEE_CODE_SERVICES is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Fulfillment Activity
select acp_id
  ,purch_mo service_mo
  ,FULFILLMENT_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where FULFILLMENT_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Curbside Pickup
select acp_id
  ,purch_mo service_mo
  ,CURBSIDE_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where CURBSIDE_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Selling Relationships
select acp_id
  ,purch_mo service_mo
  ,SELLING_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where SELLING_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Trunks
select acp_id
  ,purch_mo service_mo
  ,TRUNK_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where TRUNK_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for In-Store Styling
select acp_id
  ,purch_mo service_mo
  ,InStore_Styling_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where InStore_Styling_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;


/*----------------------------------------------------------------------*/
/*----------------------------------------------------------------------
--B) Grab all SERVICE Returns for any time-period in past 35 mo
----------------------------------------------------------------------*/
/*----------------------------------------------------------------------*/

--Returns for ANY Full-Price Activity
insert into acp_id_service_month_temp 
--Returns for ANY Full-Price Activity
select acp_id
  ,return_mo service_mo
  ,'Z_) Nordstrom' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for private styling
select acp_id
  ,return_mo service_mo
  ,PRIVATE_STYLING_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and PRIVATE_STYLING_SERVICE is not null
group by 1,2,3,4,5,6,8;


insert into acp_id_service_month_temp 
--Returns for Style Board Activity
select acp_id
  ,return_mo service_mo
  ,STYLE_BOARD_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and STYLE_BOARD_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Style Link Activity
select acp_id
  ,return_mo service_mo
  ,STYLE_LINK_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and STYLE_LINK_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for N2U Activity
select acp_id
  ,return_mo service_mo
  ,N2U_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and N2U_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for N2U CLOSET Activity
select acp_id
  ,return_mo service_mo
  ,N2U_CLOSET_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and N2U_CLOSET_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Nail, Spa, & Eyebrows
select acp_id
  ,return_mo service_mo
  ,BEAUTY_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and BEAUTY_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Restaurant
select acp_id
  ,return_mo service_mo
  ,RESTAURANT_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and RESTAURANT_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Fee-Code activity (Paid Alterations + Shoe-Shine)
select acp_id
  ,return_mo service_mo
  ,FEE_CODE_SERVICES service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and FEE_CODE_SERVICES is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Fulfillment Activity
select acp_id
  ,return_mo service_mo
  ,FULFILLMENT_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and FULFILLMENT_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Curbside Pickup
select acp_id
  ,return_mo service_mo
  ,CURBSIDE_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and CURBSIDE_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Selling Relationships
select acp_id
  ,return_mo service_mo
  ,SELLING_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and SELLING_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Trunks
select acp_id
  ,return_mo service_mo
  ,TRUNK_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and TRUNK_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for In-Store Styling
select acp_id
  ,return_mo service_mo
  ,InStore_Styling_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and InStore_Styling_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Store Returns
select acp_id
  ,return_mo service_mo
  ,STORE_RETURN_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt) net_usd_amt_split  --Needs to remain whole
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and STORE_RETURN_SERVICE is not null
group by 1,2,3,4,5,6,8;


/*----------------------------------------------------------------------*/
/*----------------------------------------------------------------------
--C) Grab all SERVICE GROUP Purchases for any time-period in past 35 mo
----------------------------------------------------------------------*/
/*----------------------------------------------------------------------*/

insert into acp_id_service_month_temp
--Purchases for Styling Group (NEEDS REVISING AS CLICKSTREAM SERVICES BECOME AVAILABLE)
select acp_id
  ,purch_mo service_mo
  ,'A_) Styling GROUP' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
        --removing digital stylist
        (svc_private_styling+svc_style_board+svc_style_link+svc_trunk+svc_N2U+svc_N2U_closet+svc_inStore_styling
)) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
        --removing digital stylist
        (svc_private_styling+svc_style_board+svc_style_link+svc_trunk+svc_N2U+svc_N2U_closet+svc_inStore_styling
)) net_usd_amt_split
from service_line_item_detail
where SvcGroup_Styling=1
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Selling Relationship Group
select acp_id
  ,purch_mo service_mo
  ,'B_) Selling Relationship GROUP' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
        (svc_independent_stylist+svc_beauty_stylist+svc_emerging_stylist+svc_personal_stylist)) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
        (svc_independent_stylist+svc_beauty_stylist+svc_emerging_stylist+svc_personal_stylist)) net_usd_amt_split
from service_line_item_detail
where SvcGroup_Selling_Rel=1
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for (POS-Sourced) Alterations Group
select acp_id
  ,purch_mo service_mo
  ,'C_) Alterations GROUP' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
        (svc_nordstrom_alterations+svc_nonnord_alterations+svc_rushed_alterations+svc_personalized_alterations)) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
        (svc_nordstrom_alterations+svc_nonnord_alterations+svc_rushed_alterations+svc_personalized_alterations)) net_usd_amt_split
from service_line_item_detail
where SvcGroup_Alterations=1
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Expedited Delivery Group
select acp_id
  ,purch_mo service_mo
  ,'D_) Expedited Delivery GROUP' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
          (svc_free_exp_delivery
          +svc_free_exp_ship_to_store
          +svc_same_day_delivery
          +svc_paid_exp_delivery)
      ) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
          (svc_free_exp_delivery
          +svc_free_exp_ship_to_store
          +svc_same_day_delivery
          +svc_paid_exp_delivery)
      ) net_usd_amt_split
from service_line_item_detail
where SvcGroup_Exp_Delivery=1
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Order Pickup Group
select acp_id
  ,purch_mo service_mo
  ,'E_) Order Pickup GROUP' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
          (svc_free_exp_ship_to_store
          +svc_curbside_pickup
          +svc_ship_to_store
          +svc_same_day_BOPUS
          +svc_next_day_BOPUS
          +svc_next_day_ship_to_store)
      ) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
          (svc_free_exp_ship_to_store
          +svc_curbside_pickup
          +svc_ship_to_store
          +svc_same_day_BOPUS
          +svc_next_day_BOPUS
          +svc_next_day_ship_to_store)
      ) net_usd_amt_split
from service_line_item_detail
where SvcGroup_Pickup=1 
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for In-Store Services Group
select acp_id
  ,purch_mo service_mo
  ,'F_) In-Store Services GROUP' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
          (svc_spa
          +svc_nail_bar
          +svc_eyebrows
          +svc_shoe_shine)) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
          (svc_spa
          +svc_nail_bar
          +svc_eyebrows
          +svc_shoe_shine)) net_usd_amt_split
from service_line_item_detail
where SvcGrp_In_Store=1
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Restaurant Group
select acp_id
  ,purch_mo service_mo
  ,'G_) Restaurant GROUP' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
          (svc_food
          +svc_coffee
          +svc_bar)) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
          (svc_food
          +svc_coffee
          +svc_bar)) net_usd_amt_split
from service_line_item_detail
where SvcGrp_Restaurant=1
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Any Free Expedited Service (SUBGROUP)
select acp_id
  ,purch_mo service_mo
  ,ANY_FREE_EXP_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
          (svc_free_exp_delivery
          +svc_free_exp_ship_to_store)
      ) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
          (svc_free_exp_delivery
          +svc_free_exp_ship_to_store)
      ) net_usd_amt_split
from service_line_item_detail
where ANY_FREE_EXP_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;

insert into acp_id_service_month_temp
--Purchases for Any Next-Day Pickup Services (SUBGROUP)
select acp_id
  ,purch_mo service_mo
  ,ANY_NEXT_DAY_PICKUP_SERVICE service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt_svc_split*
          (svc_next_day_BOPUS
          +svc_next_day_ship_to_store)
      ) gross_usd_amt_split
  ,sum(gross_usd_amt_svc_split*
          (svc_next_day_BOPUS
          +svc_next_day_ship_to_store)
      ) net_usd_amt_split
from service_line_item_detail
where ANY_NEXT_DAY_PICKUP_SERVICE is not null
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;


insert into acp_id_service_month_temp
--Purchases for ANY SERVICE Group
select acp_id
  ,purch_mo service_mo
  ,'H_) Any Service Engagement' service
  ,1 customer_qualifier
  ,0 nseam_only_ind
  ,sum(gross_usd_amt) gross_usd_amt_whole
  ,sum(gross_usd_amt) net_usd_amt_whole
  ,sum(gross_usd_amt) gross_usd_amt_split
  ,sum(gross_usd_amt) net_usd_amt_split
from service_line_item_detail
where service_count>0
  and purch_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
group by 1,2,3,4,5;


/*----------------------------------------------------------------------*/
/*----------------------------------------------------------------------
--D) Grab all SERVICE GROUP Returns for any time-period in past 35 mo
----------------------------------------------------------------------*/
/*----------------------------------------------------------------------*/

insert into acp_id_service_month_temp 
--Returns for Styling Group (Clickstream will be added later)
select acp_id
  ,return_mo service_mo
  ,'A_) Styling GROUP' service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
        --removing digital stylist
        (svc_private_styling+svc_style_board+svc_style_link+svc_trunk+svc_N2U+svc_N2U_closet+svc_inStore_styling
)) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and SvcGroup_Styling=1
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Selling Relationship Group
select acp_id
  ,return_mo service_mo
  ,'B_) Selling Relationship GROUP' service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
        (svc_independent_stylist+svc_beauty_stylist+svc_emerging_stylist+svc_personal_stylist)) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and SvcGroup_Selling_Rel=1
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for (POS-sourced) Alterations Group
select acp_id
  ,return_mo service_mo
  ,'C_) Alterations GROUP' service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
        (svc_nordstrom_alterations+svc_nonnord_alterations+svc_rushed_alterations+svc_personalized_alterations)) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and SvcGroup_Alterations=1
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Expedited Delivery Group
select acp_id
  ,return_mo service_mo
  ,'D_) Expedited Delivery GROUP' service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
          (svc_free_exp_delivery
          +svc_free_exp_ship_to_store
          +svc_same_day_delivery
          +svc_paid_exp_delivery)
      ) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and SvcGroup_Exp_Delivery=1
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Order PIckup
select acp_id
  ,return_mo service_mo
  ,'E_) Order Pickup GROUP' service
  ,0 customer_qualifier
  --,case when svc_store_returns=1 then 1 else 0 end customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
          (svc_free_exp_ship_to_store
          +svc_curbside_pickup
          +svc_ship_to_store
          +svc_same_day_BOPUS
          +svc_next_day_BOPUS
          +svc_next_day_ship_to_store)
      ) net_usd_amt_split  --Needs to remain whole
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and SvcGroup_Pickup=1
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for In-Store Services Group
select acp_id
  ,return_mo service_mo
  ,'F_) In-Store Services GROUP' service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
          (svc_spa
          +svc_nail_bar
          +svc_eyebrows
          +svc_shoe_shine)
       ) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and SvcGrp_In_Store=1
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Restaurant Group
select acp_id
  ,return_mo service_mo
  ,'G_) Restaurant GROUP' service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
          (svc_food
          +svc_coffee
          +svc_bar)
      ) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and SvcGrp_Restaurant=1
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Free Expedited Delivery SUBGroup
select acp_id
  ,return_mo service_mo
  ,ANY_FREE_EXP_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
          (svc_free_exp_delivery
          +svc_free_exp_ship_to_store)
      ) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and ANY_FREE_EXP_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for Any Next-Day PIckup Subgroup
select acp_id
  ,return_mo service_mo
  ,ANY_NEXT_DAY_PICKUP_SERVICE service
  ,0 customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt_svc_split*
          (svc_next_day_BOPUS
          +svc_next_day_ship_to_store)
      ) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and ANY_NEXT_DAY_PICKUP_SERVICE is not null
group by 1,2,3,4,5,6,8;

insert into acp_id_service_month_temp 
--Returns for ANY SERVICE Group
select acp_id
  ,return_mo service_mo
  ,'H_) Any Service Engagement' service
  ,0 customer_qualifier
  --,case when svc_store_returns=1 then 1 else 0 end customer_qualifier
  ,0 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,sum(return_usd_amt) net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,sum(return_usd_amt) net_usd_amt_split
from service_line_item_detail
where coalesce(return_mo,999999) between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
  and service_count>0
group by 1,2,3,4,5,6,8;


/*----------------------------------------------------------------------*/
/*----------------------------------------------------------------------
--E) Grab all Non-Transactional Services (NSEAM, Videos, Chat, Public Looks)
----------------------------------------------------------------------*/
/*----------------------------------------------------------------------*/

/*----------------------------------------------------------------------
--E-1) NSEAM (to capture free Alterations not rung up at POS)
----------------------------------------------------------------------*/

--Create NSEAM lookup table
--drop table nseam_wide;
create multiset volatile table nseam_wide as (
select xr.acp_id
  ,a.ticket_id
  ,b.month_num
  ,max(ntg.Nord_ind) Nord_ind
  ,max(ntg.NonNord_ind) NonNord_ind
  ,max(ntg.Rushed_ind) Rushed_ind
  ,max(ntg.Pers_ind) Pers_ind  
from prd_nap_usr_vws.ALTERATION_TICKET_FACT a 
  join 
      (select ticket_id
         ,max(case when upper(garment_origin) like '%COG%' or upper(garment_origin) in ('OUTSIDE MERCHANDISE','UNDEFINED') then 1 else 0 end) NonNord_ind
         ,max(case when upper(garment_origin) not like '%COG%' and upper(garment_origin) not in ('OUTSIDE MERCHANDISE','UNDEFINED') then 1 else 0 end) Nord_ind
         ,max(case when coalesce(expedite_fee,0)>0 then 1 else 0 end) Rushed_ind
         ,max(case when upper(substring(garment_type,1,8))='MONOGRAM' then 1 else 0 end) Pers_ind
       from prd_nap_usr_vws.ALTERATION_TICKET_GARMENT_FACT
       where lower(garment_status) not like '%delete%'
       group by 1
       ) ntg on a.ticket_id=ntg.ticket_id
  join realigned_fiscal_calendar b on a.ticket_date=b.day_date
  join prd_nap_usr_vws.STORE_DIM st on a.store_origin=st.store_num 
  jOIN prd_nap_usr_vws.ACP_ANALYTICAL_CUST_XREF xr on a.customer_id=xr.cust_id
where st.store_type_code in ('FL','VS','NL','FC')
  and a.ticket_date between (SELECT earliest_act_dt FROM date_range) AND (SELECT latest_mo_dt FROM date_range)  
  and a.store_origin is not null
  and xr.acp_id is not null
  and lower(a.ticket_status) not like '%delete%'
group by 1,2,3
) with data primary index(ticket_id,month_num) on commit preserve rows;

COLLECT STATISTICS COLUMN (PERS_IND) ON nseam_wide;
COLLECT STATISTICS COLUMN (RUSHED_IND) ON nseam_wide;
COLLECT STATISTICS COLUMN (NONNORD_IND) ON nseam_wide;
COLLECT STATISTICS COLUMN (ACP_ID ,MONTH_NUM) ON     nseam_wide;
COLLECT STATISTICS COLUMN (NORD_IND) ON nseam_wide;


--Transpose the NSEAM data
--DROP table nseam_long;
create multiset volatile table nseam_long (
  acp_id varchar(50)
  ,service_mo decimal(6)
  ,service varchar(45)
) primary index (acp_id,service_mo,service) on commit preserve rows;

--drop table nseam_long;
insert into nseam_long
select distinct acp_id
  ,month_num service_mo
  ,'C01) Nordstrom Alterations' service
from nseam_wide a 
where a.Nord_ind=1;

insert into nseam_long
select distinct acp_id
  ,month_num service_mo
  ,'C02) Non-Nordstrom Alterations' service
from nseam_wide a 
where a.NonNord_ind=1;

insert into nseam_long
select distinct acp_id
  ,month_num service_mo
  ,'C03) Rushed Alterations' service
from nseam_wide a 
where a.Rushed_ind=1;

insert into nseam_long
select distinct acp_id
  ,month_num service_mo
  ,'C04) Personalized Alterations' service
from nseam_wide a 
where a.Pers_ind=1;

--Insert the transposed the NSEAM data into our table

insert into acp_id_service_month_temp 
--NSEAM activity
select distinct acp_id
  ,service_mo
  ,service
  ,1 customer_qualifier
  ,1 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,0 net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,0 net_usd_amt_split
from nseam_long
where service_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
UNION ALL
select distinct acp_id
  ,service_mo
  ,'C_) Alterations GROUP' service
  ,1 customer_qualifier
  ,1 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,0 net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,0 net_usd_amt_split
from nseam_long
where service_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
UNION ALL
select distinct acp_id
  ,service_mo
  ,'H_) Any Service Engagement' service
  ,1 customer_qualifier
  ,1 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,0 net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,0 net_usd_amt_split
from nseam_long
where service_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
UNION ALL
select distinct acp_id
  ,service_mo
  ,'Z_) Nordstrom' service
  ,1 customer_qualifier
  ,1 nseam_only_ind
  ,0 gross_usd_amt_whole
  ,0 net_usd_amt_whole
  ,0 gross_usd_amt_split
  ,0 net_usd_amt_split
from nseam_long
where service_mo between (SELECT earliest_act_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months);

/*----------------------------------------------------------------------*/
/*----------------------------------------------------------------------
--F) Create Final Customer/Service/Time-period-level tables
----------------------------------------------------------------------*/
/*----------------------------------------------------------------------*/

/*----------------------------------------------------------------------
--F-1) Summarize the result of our "Insert Olympics" at a customer/mo/service level
----------------------------------------------------------------------*/

COLLECT STATISTICS COLUMN (ACP_ID ,SERVICE_MO ,SERVICE) ON acp_id_service_month_temp;

--drop table acp_id_service_month_smash;
create multiset volatile table acp_id_service_month_smash (
  acp_id varchar(50)
  ,service_mo decimal(6)
  ,service varchar(45)
  ,customer_qualifier integer
  ,gross_usd_amt_whole decimal(20,2)
  ,net_usd_amt_whole decimal(20,2)
  ,gross_usd_amt_split decimal(20,2)
  ,net_usd_amt_split decimal(20,2)
  ,private_style integer
) primary index (acp_id,service_mo,service) on commit preserve rows;

insert into acp_id_service_month_smash
select acp_id
  ,service_mo
  ,service
  ,max(customer_qualifier) customer_qualifier
  ,sum(gross_usd_amt_whole) gross_usd_amt_whole
  ,sum(net_usd_amt_whole) net_usd_amt_whole
  ,sum(gross_usd_amt_split) gross_usd_amt_split
  ,sum(net_usd_amt_split) net_usd_amt_split
  --did they engage in one of the 3 private styling services
  ,max(case when service like any('A01)%','A02)%','A03)%') then 1 else 0 end) as private_style
from acp_id_service_month_temp
group by 1,2,3;

COLLECT STATISTICS COLUMN (ACP_ID ,SERVICE_MO) ON acp_id_service_month_smash;

/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
delete from {service_eng_t2_schema}.cust_service_month;

insert into {service_eng_t2_schema}.cust_service_month
select distinct a.acp_id
  ,a.service_mo month_num
  ,a.service service_name
  ,a.customer_qualifier
  ,a.gross_usd_amt_whole
  ,a.net_usd_amt_whole
  ,a.gross_usd_amt_split
  ,a.net_usd_amt_split
  ,a.private_style
  ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from  acp_id_service_month_smash a 
  join 
     (select distinct acp_id
      ,service_mo
      from acp_id_service_month_smash
      where service='Z_) Nordstrom') b on a.acp_id=b.acp_id and a.service_mo=b.service_mo;

COLLECT STATISTICS COLUMN(acp_id),
                   COLUMN(month_num),
                   COLUMN(service_name)
ON {service_eng_t2_schema}.cust_service_month;



SET QUERY_BAND = NONE FOR SESSION;