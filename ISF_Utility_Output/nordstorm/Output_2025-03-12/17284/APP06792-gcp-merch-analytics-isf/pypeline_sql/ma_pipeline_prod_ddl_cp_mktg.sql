/* 
Contribution Margin: Marketing Cost DDL's
Author: KP Tryon
Original Create Date: 2021.12.14

Table Creations:

View Creations:
    T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_COST_BY_DAY_VW

*/

--------------------------------- VIEW DDL Creations
    -- View creations should only be conducted if there is a change that needs to occur

-- 2021.12.14 - Original date of cost by day view in T2 Datalab
    -- 2022.03.22 - Altered view to switch from 7DLC transaction source build to MTA source build
	-- 2022.03.23 - Altered view to incorporate IP Health shared costs between email/push
	-- 2022.03.24 - Altered view to incorporate email specific costs (Digital Additive and Bluecore)
    -- This dataset dynamically associates the cost by day for each Marketing channel completed.
REPLACE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_COST_BY_DAY_VW
AS
LOCK ROW FOR ACCESS
select category
	,subcategory_01
	,subcategory_02
	,business_day_date
	,cnt_unique_tran
	,cnt_adjusted_unique_tran
	,trans_cost_mktg
from 
	(
	select category 
			,cast(a.subcategory_01 as varchar(20)) subcategory_01 
			,subcategory_02 
			,b.business_day_date 
			,a.ttl_order_number as cnt_unique_tran
			,d.ttl_order_number as cnt_adjusted_unique_tran
			,b.trans_cost_mktg + coalesce(c.trans_cost_mktg,0) as  trans_cost_mktg 
		from
			(select cast('MARKETING' as varchar(20)) AS CATEGORY
				,subcategory_01
				,cast(NULL as varchar(50)) AS SUBCATEGORY_02
				,order_date_pacific
				,sum(order_number) ttl_order_number
			from 
				(select order_date_pacific
					,case
						when upper(finance_detail) in ('EMAIL_MARKETING','EMAIL_TRANSACT') then 'EMAIL'
						when upper(finance_detail) = 'APP_PUSH' then 'PUSH'
						else 'OTHER'
					 end subcategory_01
					,count(distinct order_number) order_number
				from t2dl_das_contribution_margin.CP_MKTG_DIGITAL_MTA_VW 
				where upper(finance_rollup) = 'EMAIL'
				group by 1,2) a
			group by 1,2,3,4) a left join
		-- 2022.03.23 - Incorporate any shared email/push costs
			(SELECT distinct b.business_date as business_day_date
			-- 2022.03.23 - Sum up total shared email/push costs
				,sum(cast(b.cost_value as decimal(38,15))) over (partition by business_day_date)/count(distinct a.order_number) trans_cost_mktg
			from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_MTA_VW a join
				 T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST_VW b 
					on a.order_date_pacific = b.business_date
			-- 2022.03.23 - Incorporate any cost that is shared between Email/Push
			where upper(b.platform) in ('SALESFORCE','IP_HEALTH')
			-- 2022.03.22 - MTA records the financial rollup of Email to both Email and Push
				and upper(a.finance_rollup) = 'EMAIL'
			group by 1, b.cost_value) b
				on a.order_date_pacific = b.business_day_date left JOIN 
		-- 2022.03.23 - Incorporate any email specific costs
			(SELECT distinct b.business_date as business_day_date
				,'EMAIL' as subcategory_01
				,sum(cast(b.cost_value as decimal(38,15))) over (partition by business_day_date)/count(distinct a.order_number) trans_cost_mktg
			from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_MTA_VW a join
				 T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST_VW b 
					on a.order_date_pacific = b.business_date
			where upper(b.platform) in ('BLUECORE','DIGITAL_ADDITIVE')
				and upper(a.finance_rollup) = 'EMAIL'
				and upper(a.finance_detail) in ('EMAIL_TRANSACT','EMAIL_MARKETING')
			group by 1, b.cost_value) c
				on a.order_date_pacific = c.business_day_date
					and a.subcategory_01 = c.subcategory_01 left join
		-- 2022.03.24 - Incorporating the adjusted email/push counts
			-- This is to depict why the transactions/day look they way they are after adjusting for duplicate transactions between the programs for shared costs
			(select order_date_pacific
				,case
					when upper(finance_detail) in ('EMAIL_MARKETING','EMAIL_TRANSACT') then 'EMAIL'
					when upper(finance_detail) = 'APP_PUSH' then 'PUSH'
					else 'OTHER'
				 end subcategory_01
				 ,count(distinct order_number) ttl_order_number
			from 
				(select order_date_pacific
					,max(finance_detail) finance_detail
					,order_number
				from t2dl_das_contribution_margin.CP_MKTG_DIGITAL_MTA_VW 
				where upper(finance_rollup) = 'EMAIL'
				group by 1,3) a
			group by 1,2) d 
				on a.order_date_pacific = d.order_date_pacific
					and a.subcategory_01 = d.subcategory_01
		union all
		SELECT b.category
			,b.subcategory_01
			,b.subcategory_02
			,b.business_date as business_day_date
			,count(distinct a.order_number) cnt_unique_tran
			,NULL as cnt_adjusted_unique_tran
			,cast(b.cost_value as decimal(38,15))/count(distinct a.order_number) trans_cost_mktg
		from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_MTA_VW a join
			(select category
				,subcategory_01
				,subcategory_02
				,business_date
				,sum(cost_value) cost_value
			 from T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_DIGITAL_COST_VW  
			 group by 1,2,3,4) b 
				on a.order_date_pacific = b.business_date
					and a.finance_detail = b.subcategory_02
		group by 1,2,3,4,b.cost_value
		) a;

-- 2021.12.14 - Original creation of the Cost by day by transaction in T2 Datalab
	-- 2022.03.24 - Entire re-build of the cost/trans/day view
    -- This dataset dynamically associates the cost by day by transaction for each Marketing channel completed.
REPLACE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_COST_BY_TRANS_VW
AS LOCK ROW FOR ACCESS
-- 2022.03.24 - This final rollup is to sum up the total line net's across a singular order_number where applicable
	-- This is specifically to target OMS order's ending with multiple global_tran_id's
select distinct global_tran_id 
	,order_number 
	,business_day_date
	,case 
		when rn = 1 then sum(ttl_line_net_usd_amt) over (partition by order_number, business_day_date order by 1) 
		else NULL 
	 end ttl_order_line_net_usd_amt
	,ttl_line_net_usd_amt
	,case 
		when rn = 1 then trans_cost_mktg_email
		else null
	 end trans_cost_mktg_email
	-- 2022.03.24 - For push, if a cost is found in trans_cost_mktg_email, then the cost allocation has alraeady occurred and thus, will not be present for push to ensure we don't double dip
	,case 
		when rn = 1 and trans_cost_mktg_email is null then trans_cost_mktg_push
		else null
	 end trans_cost_mktg_push
	,case 
		when rn = 1 then trans_cost_mktg_digital_affiliates
		else null
	 end trans_cost_mktg_digital_affiliates
	,case 
		when rn = 1 then trans_cost_mktg_digital_display
		else null
	 end trans_cost_mktg_digital_display
	,case 
		when rn = 1 then trans_cost_mktg_digital_paid_search_branded
		else null
	 end trans_cost_mktg_digital_paid_search_branded
	,case 
		when rn = 1 then trans_cost_mktg_digital_paid_search_unbranded
		else null
	 end trans_cost_mktg_digital_paid_search_unbranded
	,case 
		when rn = 1 then trans_cost_mktg_digital_shopping
		else null
	 end trans_cost_mktg_digital_shopping
	,case 
		when rn = 1 then trans_cost_mktg_digital_social_paid
		else null
	 end trans_cost_mktg_digital_social_paid
	,case 
		when rn = 1 then trans_cost_mktg_digital_video
		else null
	 end trans_cost_mktg_digital_video
	,rn as order_number_order
from 
	(
	select global_tran_id
		,order_number
		,business_day_date
		,row_number() over (partition by order_number order by 1) rn
		,max(ttl_line_net_usd_amt) ttl_line_net_usd_amt
		,max(trans_cost_mktg_email) as trans_cost_mktg_email
		,max(trans_cost_mktg_push) as trans_cost_mktg_push
		,max(trans_cost_mktg_digital_affiliates) trans_cost_mktg_digital_affiliates
		,max(trans_cost_mktg_digital_display) trans_cost_mktg_digital_display
		,max(trans_cost_mktg_digital_paid_search_branded) trans_cost_mktg_digital_paid_search_branded
		,max(trans_cost_mktg_digital_paid_search_unbranded) trans_cost_mktg_digital_paid_search_unbranded
		,max(trans_cost_mktg_digital_shopping) trans_cost_mktg_digital_shopping
		,max(trans_cost_mktg_digital_social_paid) trans_cost_mktg_digital_social_paid
		,max(trans_cost_mktg_digital_video) trans_cost_mktg_digital_video
	from 
		(
	-- Cost allocation for each transaction by marketing channel
		select a.order_number
			,b.business_day_date
			,c.global_tran_id
			,c.ttl_line_net_usd_amt
			,CASE
				when b.subcategory_01 = 'EMAIL' then b.trans_cost_mktg
				else null
			 end trans_cost_mktg_email
			,CASE
				when b.subcategory_01 = 'PUSH' then b.trans_cost_mktg
				else null
			 end trans_cost_mktg_push
			,CASE 
				when b.subcategory_02 = 'AFFILIATES' then b.trans_cost_mktg 
				else null
			 end trans_cost_mktg_digital_affiliates
			,CASE 
				when b.subcategory_02 = 'DISPLAY' then b.trans_cost_mktg 
				else null
			 end trans_cost_mktg_digital_display
			,CASE 
				when b.subcategory_02 = 'PAID_SEARCH_BRANDED' then b.trans_cost_mktg 
				else null
			 end trans_cost_mktg_digital_paid_search_branded
			 ,CASE 
				when b.subcategory_02 = 'PAID_SEARCH_UNBRANDED' then b.trans_cost_mktg 
				else null
			 end trans_cost_mktg_digital_paid_search_unbranded
			,CASE 
				when b.subcategory_02 = 'SHOPPING' then b.trans_cost_mktg 
				else null
			 end trans_cost_mktg_digital_shopping
			,CASE 
				when b.subcategory_02 = 'SOCIAL_PAID' then b.trans_cost_mktg 
				else null
			 end trans_cost_mktg_digital_social_paid
			,CASE 
				when b.subcategory_02 = 'VIDEO' then b.trans_cost_mktg 
				else null
			 end trans_cost_mktg_digital_video
	--		,b.subcategory_01
	--		,b.subcategory_02
	--		,b.trans_cost_mktg
		from
			(
			select distinct order_number
				,order_date_pacific
				,case 
					when finance_detail in ('EMAIL_TRANSACT','EMAIL_MARKETING') then 'EMAIL'
					when finance_detail = 'APP_PUSH' then 'PUSH'
					else NULL
				 end subcategory_01
				,finance_detail as subcategory_02
			from t2dl_das_contribution_margin.CP_MKTG_DIGITAL_MTA_VW 
			) a join
			(
			select distinct business_day_date
				,subcategory_01
				,subcategory_02
				,trans_cost_mktg
			from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_COST_BY_DAY_VW
			) b
				on a.order_date_pacific = b.business_day_date
					and (a.subcategory_01 = b.subcategory_01
						or a.subcategory_02 = b.subcategory_02) JOIN 
			(
			select distinct order_number
				,business_day_date 
				,ttl_line_net_usd_amt
				,global_tran_id 
			from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_VW
			) c 
				on a.order_number = c.order_number
			-- There is a date differential when it come to MTA and ERTM/OMS at times. As such, to remain consistent with MTA, we will only go based on MTA for now.
			--	and a.order_date_pacific <> c.business_day_date
		) a
	group by 1,2,3
	) a;

-------------- The following are one time 1:1 views of final output datasets. This is to ensure there aren't locking issues at table or row for ETL pipelines

-- 2022.03.01 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_MTA_VW
CREATE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_MTA_VW
AS 
LOCK ROW FOR ACCESS
SELECT *
FROM T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_MTA;

-- 2022.03.01 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_VW
CREATE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_VW
AS 
LOCK ROW FOR ACCESS
SELECT *
FROM T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL;

-- 2022.03.01 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL_VW
CREATE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL_VW
AS 
LOCK ROW FOR ACCESS
SELECT *
FROM T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL;

-- 2022.03.01 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST_VW
CREATE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST_VW
AS 
LOCK ROW FOR ACCESS 
SELECT *
FROM T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST;

-- 2022.03.01 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_DIGITAL_COST_VW
CREATE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_DIGITAL_COST_VW
AS 
LOCK ROW FOR ACCESS 
SELECT *
FROM T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_DIGITAL_COST;


--------------------------------- Table DDL Creations
   -- Table creations should only be conducted if there is a change that needs to occur

-- 2021.12.14 - T2DL_DAS_CONTRIBUTION_MARGIN.COST_MKTG_EMAIL
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.cost_mktg_email ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      global_tran_id BIGINT,
      business_day_date DATE FORMAT 'YYYY-MM-DD',
      tran_type_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      deploy_date DATE FORMAT 'YYYY-MM-DD',
      program_type VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      intent_store_num INTEGER,
      order_num VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      update_timestamp TIMESTAMP(6) WITH TIME ZONE,
      process_timestamp TIMESTAMP(6) WITH TIME ZONE)
PRIMARY INDEX ( global_tran_id )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );

-- 2021.12.14 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      global_tran_id BIGINT,
      business_day_date DATE FORMAT 'YY/MM/DD',
      order_channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      order_channelcountry VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      order_number VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tran_type_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ttl_line_net_usd_amt DECIMAL(38,2),
      ttl_line_net_amt DECIMAL(38,2),
      line_item_net_amt_currency_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      line_net_usd_amt_final DECIMAL(38,2),
      line_net_amt_final DECIMAL(38,2),
      rn BYTEINT,
      match_type BYTEINT,
      update_date DATE FORMAT 'YY/MM/DD',
      process_date DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( order_number )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );

-- 2021.12.14 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_MTA
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_MTA ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      order_number VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      order_channelcountry VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      marketing_type VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      order_date_pacific DATE FORMAT 'YY/MM/DD',
      update_date DATE FORMAT 'YY/MM/DD',
      process_date DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( order_number )
PARTITION BY RANGE_N(order_date_pacific  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );

-- 2021.12.14 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      global_tran_id BIGINT,
      business_day_date DATE FORMAT 'YY/MM/DD',
      tran_type_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      deploy_date DATE FORMAT 'YY/MM/DD',
      program_type VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      intent_store_num INTEGER,
      order_num VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ttl_line_net_usd_amt DECIMAL(38,2),
      ttl_line_item_quantity DECIMAL(38,0),
      update_date DATE FORMAT 'YY/MM/DD',
      process_date DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( global_tran_id )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );

-- 2021.12.14 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST
	-- 2022.03.23 Updates: 
		-- % breakout for extra cost measurement in future to allow separation of costs based on fullprice vs offprice
		-- decimal increase adjustment to cost_value to allow further granularity to cost breakdown by day
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      business_date DATE FORMAT 'YY/MM/DD',
      channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_01 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_02 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cost_value DECIMAL(25,7),
      contract_start DATE FORMAT 'YY/MM/DD',
      contract_end DATE FORMAT 'YY/MM/DD',
      bu_both DECIMAL(5,2),
      bu_fullprice DECIMAL(5,2),
      bu_offprice DECIMAL(5,2),
      update_dt DATE FORMAT 'YY/MM/DD',
      process_dt DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( platform )
PARTITION BY RANGE_N(business_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );

-- 2021.12.14 - T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_DIGITAL_COST
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.cp_ref_digital_cost ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      category VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subcategory_01 VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subcategory_02 VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      business_date DATE FORMAT 'YY/MM/DD',
      country VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cost_value DECIMAL(38,2),
      sourced_from VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      process_date DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( business_date )
PARTITION BY RANGE_N(business_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );