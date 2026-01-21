/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=sales_cust_fact_11521_ACE_ENG;
     Task_Name=sales_cust_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.sales_cust_fact
Prior Table Layer: T2DL_DAS_USL.sales_fact
Team/Owner: Customer Analytics
Date Created/Modified: Mar 25 2024

Note:
-- Purpose of the table: Table to directly get transaction related metrics, and some customer attributes
-- Update Cadence: Daily

*/
    
/*
 * I will be adding several metrics to the sales_fact table. The customer metrics being added are:
 * 	region
 * 	dma
 * 	engagement_cohort
 * 	predicted_segment
 * 	loyalty_level
 * 	cardmember_flag
 * 	new to jwn
 * 	age
 * 	loyalty_type
 * 
 * Otherwise, the majority of sales_fact table will be pulled to create an additional layer of information.
 */

/*set date range as full or incremental
based on day of week (which is variable to allow
manual full loads any day of the week)*/

create multiset volatile table sf_start_date as (
	select
		case
			when td_day_of_week(CURRENT_DATE) = {backfill_day_of_week}
			then date'2021-01-31'
			else current_date() - {incremental_look_back}
		end as start_date,
		case
			when td_day_of_week(CURRENT_DATE) = {backfill_day_of_week}
			then 'backfill'
			else 'incremental'
		end as delete_range
) with data primary index(start_date) on commit preserve rows;

    
-- DATE FILTER
CREATE VOLATILE MULTISET TABLE date_lookup AS (
	SELECT 
		DC.week_num
		, DC.month_num
		, DC.quarter_num
		, DC.year_num
		, MIN(day_date) AS ty_start_dt
		, MAX(day_date) AS ty_end_dt
	FROM prd_nap_usr_vws.day_cal AS DC
	WHERE 1 = 1
		AND day_date between (select start_date from sf_start_date) and current_date() -1
		-- AND day_date between date'2013-03-19' and current_date() -1
	GROUP BY 1,2,3,4
) WITH DATA PRIMARY INDEX(week_num) ON COMMIT PRESERVE ROWS;

-- CORE DATA REFERENCE TABLE
 /* 
  * 
  * 
  * Setting up the core table for which I will reference for needed filters. Based on sales_fact table.
  * 
  * 
  */ 

-- OUR ORIGINAL VERSION
CREATE MULTISET VOLATILE TABLE retail_info AS ( 
	SELECT
		SF.sale_date
		, D.week_num
		, D.month_num
		, D.quarter_num
		, D.year_num
		, SF.global_tran_id
		, SF.line_item_seq_num
		, SF.store_number
		, SF.acp_id
		, SF.sku_id
		, SF.upc_num
		, SF.trip_id
		, SF.employee_discount_flag
		, SF.transaction_type_id
		, SF.device_id
		, SF.ship_method_id
		, SF.price_type_id
		, SF.line_net_usd_amt
		, SF.giftcard_flag
		, SF.sales_returned
		, SF.units_returned
		, SF.non_gc_line_net_usd_amt
		, st.business_unit_desc
		, SF.line_item_quantity AS items 
		,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') and trim(nts.aare_chnl_code) = 'FLS' then 1
              when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') and trim(nts.aare_chnl_code) = 'NCOM' then 1
              when st.business_unit_desc in ('RACK', 'RACK CANADA') and trim(nts.aare_chnl_code) = 'RACK' then 1
              when st.business_unit_desc in ('OFFPRICE ONLINE') and trim(nts.aare_chnl_code) = 'NRHL' then 1
              else 0 end as new_to_jwn
        ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') THEN '1) Nordstrom Stores'
              when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') THEN '2) Nordstrom.com'
              when st.business_unit_desc in ('RACK', 'RACK CANADA') THEN '3) Rack Stores'
              when st.business_unit_desc in ('OFFPRICE ONLINE') THEN '4) Rack.com'
              END AS channel
        ,case when channel in ('1) Nordstrom Stores', '2) Nordstrom.com') THEN '1) Nordstrom Banner'
              when channel in ('3) Rack Stores', '4) Rack.com') THEN '2) Rack Banner'
              END AS banner
	FROM {usl_t2_schema}.sales_fact AS SF
	INNER JOIN prd_nap_usr_vws.store_dim as st
    	on SF.store_number = st.store_num
	JOIN prd_nap_usr_vws.day_cal AS D
		ON D.day_date = SF.sale_date
	LEFT JOIN prd_nap_usr_vws.CUSTOMER_NTN_STATUS_FACT nts 
		ON SF.global_tran_id = nts.aare_global_tran_id
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND SF.acp_id IS NOT NULL
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;    
    
/* 
 * 
 * Calculate the core target segment for the last 2 FY for each customer 
 * 
 */

CREATE MULTISET VOLATILE TABLE core_target_segment AS (
	SELECT
		CAS.acp_id
		, predicted_ct_segment AS predicted_segment
	FROM T2DL_DAS_CUSTOMBER_MODEL_ATTRIBUTE_PRODUCTIONALIZATION.customer_prediction_core_target_segment AS CAS
	GROUP BY 1,2
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

/* 
 * 
 * Acquire a single value DMA and Region for each Customer!
 * 
 */

CREATE MULTISET VOLATILE TABLE location_info AS (
--
	SELECT
		AC.acp_id
		, CASE
            WHEN SUBSTRING(UPPER(COALESCE(ca_dma_desc,us_dma_desc,'DMA_UNKNOWN')),1,5) = 'OTHER' THEN 'DMA_UNKNOWN'
	        ELSE UPPER(COALESCE(ca_dma_desc,us_dma_desc,'DMA_UNKNOWN'))
	    END dma
		, CASE
            WHEN UPPER(SUBSTRING(dma,1,9)) IN ('LOS ANGEL','BAKERSFIE','SANTA BAR','SAN DIEGO','PALM SPRI','YUMA AZ-E') THEN 'SCAL'
            WHEN UPPER(SUBSTRING(dma,1,7)) IN ('RICHMON','ROANOKE','NORFOLK','HARRISO') THEN 'NORTHEAST'
            WHEN UPPER(SUBSTRING(dma,1,11)) = 'CHARLOTTESV' THEN 'NORTHEAST'
            WHEN SUBSTRING(oreplace(dma,')',''),length(oreplace(dma,')',''))-1,2) 
	        IN ('AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT') THEN 'CANADA'
	            WHEN SUBSTRING(oreplace(dma,')',''),length(oreplace(dma,')',''))-1,2) 
	        IN ('IA','IL','IN','KS','KY','MI','MN','MO','ND','NE','OH','SD','WI') THEN 'MIDWEST'
	            WHEN SUBSTRING(oreplace(dma,')',''),length(oreplace(dma,')',''))-1,2) 
	        IN ('CT','DC','DE','MA','MD','ME','NH','NJ','NY','PA','RI','VT','WV') THEN 'NORTHEAST'
	            WHEN SUBSTRING(oreplace(dma,')',''),length(oreplace(dma,')',''))-1,2) 
	        IN ('AK','CA','ID','MT','NV','OR','WA','WY') THEN 'NORTHWEST'
	            WHEN SUBSTRING(oreplace(dma,')',''),length(oreplace(dma,')',''))-1,2) = 'HI' THEN 'SCAL'
	            WHEN SUBSTRING(oreplace(dma,')',''),length(oreplace(dma,')',''))-1,2) 
	        IN ('AL','FL','GA','MS','NC','PR','SC','TN','VA') THEN 'SOUTHEAST'
	            WHEN SUBSTRING(oreplace(dma,')',''),length(oreplace(dma,')',''))-1,2) 
	        IN ('AR','AZ','CO','LA','NM','OK','TX','UT') THEN 'SOUTHWEST'
	        ELSE 'UNKNOWN'
	    END region
	FROM prd_nap_usr_vws.ANALYTICAL_CUSTOMER AS AC 
	WHERE 1 = 1
		AND AC.acp_id IN (SELECT DISTINCT acp_id FROM retail_info)
--
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;


/*
 * 
 * Logic for calculating the AEC of each customer
 * 
 */

CREATE MULTISET VOLATILE TABLE aec AS (
	SELECT DISTINCT
		AEC_T.acp_id
		, AEC_T.execution_qtr
		, ROUND(AEC_T.execution_qtr/10,0) AS engagement_year 
		, AEC_T.engagement_cohort
	FROM T2DL_DAS_AEC.audience_engagement_cohorts AS AEC_T
	WHERE 1 = 1
		AND AEC_T.execution_qtr IN (SELECT DISTINCT quarter_num FROM retail_info)
		-- AND AEC_T.engagement_cohort <> 'Acquired Mid-Qtr'
) WITH DATA PRIMARY INDEX(acp_id, execution_qtr) ON COMMIT PRESERVE ROWS; 

/*
 * 
 * Logic of calculating the loyalty of each of the customers.
 * 
 */

create multiset volatile table cardmember_fix_table as (
SELECT x.LOYALTY_ID
  ,y.acp_id
  ,x.ACCT_OPEN_DT cardmember_enroll_date
  ,CASE WHEN x.ACCT_CLOSE_DT='20500101' THEN NULL ELSE cast(x.ACCT_CLOSE_DT  as date format 'yyyymmdd') END AS max_close_dt
FROM 
  (
  SELECT LOYALTY_ID
    ,MIN(cast(cast(ACCT_OPEN_DT as varchar(8)) as date format 'yyyymmdd') ) AS ACCT_OPEN_DT
    ,MAX(CASE WHEN ACCT_CLOSE_DT IS NULL THEN '20500101' ELSE cast(ACCT_CLOSE_DT  as varchar(8)) END ) AS ACCT_CLOSE_DT
  FROM T3DL_PAYMNT_LOYALTY.creditloyaltysync
  WHERE LOYALTY_ID is not null
  GROUP BY 1
  ) x
  join prd_nap_usr_vws.loyalty_member_dim_vw y on x.loyalty_id=y.loyalty_id
) WITH DATA PRIMARY INDEX(loyalty_id) on commit preserve rows;

--drop table cust_level_loyalty_cardmember;
create multiset volatile table cust_level_loyalty_cardmember as (
select lmd.acp_id
   ,1 as flg_cardmember
   ,max(case when rwd.rewards_level in ('MEMBER') then 1
             when rwd.rewards_level in ('INSIDER','INFLUENCER') then 3
             when rwd.rewards_level in ('AMBASSADOR') then 4
             when rwd.rewards_level in ('ICON') then 5
             else 0
         end) as cardmember_level
   ,min(lmd.cardmember_enroll_date) cardmember_enroll_date
from cardmember_fix_table as lmd
  left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw as rwd
      on lmd.loyalty_id = rwd.loyalty_id
     and rwd.start_day_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
     and rwd.end_day_date > (SELECT MAX(ty_end_dt) FROM date_lookup)
  where coalesce(lmd.cardmember_enroll_date, date'2099-12-31') <  coalesce(lmd.max_close_dt, date'2099-12-31')
    and coalesce(lmd.cardmember_enroll_date, date'2099-12-31') <= (SELECT MAX(ty_end_dt) FROM date_lookup)
    and coalesce(lmd.max_close_dt, date'2099-12-31') > (SELECT MAX(ty_end_dt) FROM date_lookup)
    and lmd.acp_id is not null
	AND lmd.acp_id IN (SELECT DISTINCT acp_id FROM retail_info)
group by 1,2
)with data primary index(acp_id) on commit preserve rows;

--drop table cust_level_loyalty_member;
create multiset volatile table cust_level_loyalty_member as (
select lmd.acp_id
   ,1 as flg_member
   ,max(case when rwd.rewards_level in ('MEMBER') then 1
             when rwd.rewards_level in ('INSIDER','INFLUENCER') then 3
             when rwd.rewards_level in ('AMBASSADOR') then 4
             when rwd.rewards_level in ('ICON') then 5
             else 0
         end) as member_level
   ,min(lmd.member_enroll_date) member_enroll_date
from prd_nap_usr_vws.loyalty_member_dim_vw as lmd
  left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw as rwd
      on lmd.loyalty_id = rwd.loyalty_id
     and rwd.start_day_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
     and rwd.end_day_date > (SELECT MAX(ty_end_dt) FROM date_lookup)
  where coalesce(lmd.member_enroll_date, date'2099-12-31') <  coalesce(lmd.member_close_date, date'2099-12-31')
    and coalesce(lmd.member_enroll_date, date'2099-12-31') <= (SELECT MAX(ty_end_dt) FROM date_lookup)
    and coalesce(lmd.member_close_date, date'2099-12-31') > (SELECT MAX(ty_end_dt) FROM date_lookup)
    and lmd.acp_id is not null
	AND lmd.acp_id IN (SELECT DISTINCT acp_id FROM retail_info)
group by 1,2
)with data primary index(acp_id) on commit preserve rows;


/************************************************************************************/
/********* II-D: Combine both types of Loyalty customers into 1 lookup table
 *    (prioritize "Cardmember" type & level if present, use "Member" otherwise) *****/
/************************************************************************************/

create multiset volatile table loyalty as
(
select DISTINCT a.acp_id
  ,case when b.acp_id is not null then 'a) Cardmember'
        when c.acp_id is not null then 'b) Member'
        else 'c) Non-Loyalty' end as loyalty_type
  ,case when b.acp_id is not null or c.acp_id is not null then 1 else 0 end loyalty_ind
  ,case --when
        when a.employee_discount_flag=1 and b.acp_id is not null and c.acp_id is not null then '1) MEMBER'
        when b.acp_id is not null and b.cardmember_level <= 3 then '2) INFLUENCER'
        when b.acp_id is not null and b.cardmember_level  = 4 then '3) AMBASSADOR'
        when b.acp_id is not null and b.cardmember_level  = 5 then '4) ICON'
        when c.member_level <= 1 then '1) MEMBER'
        when c.member_level = 3 then '2) INFLUENCER'
        when c.member_level >= 4 then '3) AMBASSADOR'
        else null end loyalty_level
  ,c.member_enroll_date loyalty_member_start_dt
  ,b.cardmember_enroll_date loyalty_cardmember_start_dt
from retail_info a
  left join cust_level_loyalty_cardmember b  on a.acp_id=b.acp_id
  left join cust_level_loyalty_member c on a.acp_id=c.acp_id
)with data primary index(acp_id) on commit preserve rows;



/* 
 * 
 * Combining all the different pieces together for one table to output
 * 
 */

CREATE MULTISET VOLATILE TABLE usl_sales_cust_fact AS (
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -6) 
		AND sale_date <= current_date()
) WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, sku_num) ON COMMIT PRESERVE ROWS
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -12) 
		AND sale_date < ADD_MONTHS(current_date(), -6)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -18) 
		AND sale_date < ADD_MONTHS(current_date(), -12)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -24) 
		AND sale_date < ADD_MONTHS(current_date(), -18)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -30) 
		AND sale_date < ADD_MONTHS(current_date(), -24)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -36) 
		AND sale_date < ADD_MONTHS(current_date(), -30)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -42) 
		AND sale_date < ADD_MONTHS(current_date(), -36)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -48) 
		AND sale_date < ADD_MONTHS(current_date(), -42)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -54) 
		AND sale_date < ADD_MONTHS(current_date(), -48)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -60) 
		AND sale_date < ADD_MONTHS(current_date(), -54)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -66) 
		AND sale_date < ADD_MONTHS(current_date(), -60)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -72) 
		AND sale_date < ADD_MONTHS(current_date(), -66)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -78) 
		AND sale_date < ADD_MONTHS(current_date(), -72)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -84) 
		AND sale_date < ADD_MONTHS(current_date(), -78)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -90) 
		AND sale_date < ADD_MONTHS(current_date(), -84)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -96) 
		AND sale_date < ADD_MONTHS(current_date(), -90)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -102) 
		AND sale_date < ADD_MONTHS(current_date(), -96)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -108) 
		AND sale_date < ADD_MONTHS(current_date(), -102)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -114) 
		AND sale_date < ADD_MONTHS(current_date(), -108)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -120) 
		AND sale_date < ADD_MONTHS(current_date(), -114)
;

insert into usl_sales_cust_fact
	SELECT
		RI.sale_date
		, RI.week_num
		, RI.month_num
		, RI.quarter_num
		, RI.year_num
		, RI.global_tran_id
		, RI.line_item_seq_num
		, RI.store_number AS store_num
		, RI.acp_id
		, RI.sku_id AS sku_num
		, RI.upc_num
		, RI.trip_id
		, RI.employee_discount_flag
		, RI.transaction_type_id
		, RI.device_id
		, RI.ship_method_id
		, RI.price_type_id
		, RI.line_net_usd_amt
		, RI.giftcard_flag
		, RI.items
		, RI.sales_returned AS returned_sales
		, RI.units_returned AS returned_items
		, RI.non_gc_line_net_usd_amt AS non_gc_amt
		, S.region
		, S.dma
		, E.engagement_cohort
		, C.predicted_segment
		, COALESCE(L.loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(L.loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, RI.new_to_jwn
		, RI.channel
		, RI.banner
		, RI.business_unit_desc
		-- Setting to a null value till Experian gets adjusted
	FROM retail_info AS RI
	LEFT JOIN core_target_segment AS C 
		ON RI.acp_id = C.acp_id
	LEFT JOIN location_info AS S 
		ON RI.acp_id = S.acp_id
	LEFT JOIN aec AS E 
		ON RI.acp_id = E.acp_id 
			AND RI.quarter_num = E.execution_qtr
	LEFT JOIN loyalty AS L 
		ON L.acp_id = RI.acp_id 
	WHERE 1 = 1
		AND sale_date >= ADD_MONTHS(current_date(), -126) 
		AND sale_date < ADD_MONTHS(current_date(), -120)
;

/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------

*/

DELETE 
FROM {usl_t2_schema}.sales_cust_fact
where 1=1 
and sale_date >= (select start_date from sf_start_date)
and exists
(select 1 from sf_start_date where delete_range = 'incremental'); 

DELETE 
FROM {usl_t2_schema}.sales_cust_fact
where exists
(select 1 from sf_start_date where delete_range = 'backfill');

-- DELETE FROM {usl_t2_schema}.sales_cust_fact ALL; -- Remove all historic data to prevent overlaps

INSERT INTO {usl_t2_schema}.sales_cust_fact  -- Insert new data
	SELECT
		sale_date
		, week_num
		, month_num
		, quarter_num
		, year_num
		, global_tran_id
		, line_item_seq_num
		, store_num
		, acp_id
		, sku_num
		, upc_num
		, trip_id
		, employee_discount_flag
		, transaction_type_id
		, device_id
		, ship_method_id
		, price_type_id
		, line_net_usd_amt
		, giftcard_flag
		, items
		, returned_sales
		, returned_items
		, non_gc_amt
		, region
		, dma
		, COALESCE(engagement_cohort, 'UNDEFINED') AS engagement_cohort
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		, channel
		, banner
		, business_unit_desc
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM usl_sales_cust_fact
;

COLLECT STATISTICS 
	COLUMN(global_tran_id)
	, COLUMN (line_item_seq_num)
    , COLUMN (acp_id)
    , COLUMN (sale_date)
    , COLUMN (store_num)
    , COLUMN (sku_num)
	, COLUMN (upc_num)
    , COLUMN (transaction_type_id)
    , COLUMN (device_id)
    , COLUMN (ship_method_id)
    , COLUMN (price_type_id)
    , COLUMN (week_num)
on {usl_t2_schema}.sales_cust_fact;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;