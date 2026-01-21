/*
Contribution Profit - Marketing
Authors: KP Tryon, Sara Riker, Sara Scott
Datalab: t2dl_das_contribution_margin

Updates:
2022.03.07 - Initial ETL for Marketing Digital Costs Build Created
2022.03.07 - Marketing Costs (Digital initiated)

*/

-- ### (Teradata) - S3 to Teradata Import to Staging

--------------- S3 to Teradata

-- Nordstrom Teradata - NAP Semantic Account
-- 2021.12.16 - LOAD DATA FROM S3 TO TERADATA
	-- Currently, as it's set, the same file will be overwritten to S3 on a daily basis. This file will be consumed in this import unless we wanted to keep a historical reference in S3 of which we would want to switch this to include date
CALL SYS_MGMT.S3_TPT_LOAD ('T2DL_DAS_CONTRIBUTION_MARGIN','CP_MKTG_DIGITAL_TRANS_STG','us-west-2','nordace-isf-ma-prod','s3ToTeradata/','mktg_digital.csv','2C',OUT_MESSAGE);

COLLECT STATS 
	PRIMARY INDEX ( ORDER_NUMBER )
	,COLUMN ( ORDER_NUMBER )
		ON T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_TRANS_STG;

-- ### (Teradata) - Teradata to Teradata -- Digital Cost Build

--------------- Teradata To Teradata

	----------- Digital Cost Build

/** Not doing as we don't have the permissions across sources currently but should be the long term solution vs. the physical data creation
-- 2021.12.29 - In an ideal state, we would want to utilize this as a view, however, due to the permissions issues between datalabs, we'd either need the owner's of the source datalab to give us grant select permissions or we'll be forced to create the physical data as we are going to do with the MVP
replace view T2DL_DAS_CONTRIBUTION_MARGIN.cp_ref_digital_cost
as
lock row for access
select 'MARKETING' as category
	,'DIGITAL' as subcategory_01
	,channel as subcategory_02
	,business_date
	,country
	,cost_value
from (
	select day_date as business_date
		,'FULL PRICE' as bu
		,channel
		,country
	--	,funnel
		,sum(cast(cost as decimal(25,2))) cost_value
	from t3dl_ace_mim.fp_cost_table 
	group by 1,2,3,4
	union all
	select day_date as business_date
		,'OFF PRICE' as bu
		,channel
		,country
		,sum(cast(cost as decimal(25,2))) cost 
	from t3dl_ace_mim.nrhl_cost_table 
	group by 1,2,3,4
	) a;
*/
	
-- 2021.12.29 - Creation of the cost reference data by day/channel for Digital
	-- For the purposes of this MVP, I'm going to just drop/recreate each time, however, it'd be better to build out a delta process for this.
DROP TABLE T2DL_DAS_CONTRIBUTION_MARGIN.cp_ref_digital_cost;
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.cp_ref_digital_cost
AS (
select 'MARKETING' as category
	,'DIGITAL' as subcategory_01
	,channel as subcategory_02
	,business_date
	,country
	,cost_value
	,sourced_from
	,current_date as process_date
from (
	select day_date as business_date
		,'FULL PRICE' as bu
		,channel
		,country
	--	,funnel
		,sum(cast(cost as decimal(25,2))) cost_value
		,cast('fp_cost_table' as varchar(20)) as sourced_from
	from t3dl_ace_mim.fp_cost_table 
	group by 1,2,3,4,6
	union all
	select day_date as business_date
		,'OFF PRICE' as bu
		,CASE 
			when upper(channel) = 'AFFILIATE' then 'AFFILIATES'
			when upper(channel) = 'PAID SEARCH BRANDED' then 'PAID_SEARCH_BRANDED'
			when upper(channel) = 'PAID SEARCH NON-BRANDED' then 'PAID_SEARCH_UNBRANDED'
		 end as channel
		,country
		,sum(cast(cost as decimal(25,2))) cost 
		,cast('nrhl_cost_table' as varchar(20)) as sourced_from
	from t3dl_ace_mim.nrhl_cost_table 
	group by 1,2,3,4,6) a
)
WITH DATA
PRIMARY INDEX ( BUSINESS_DATE )
PARTITION BY RANGE_N(BUSINESS_DATE  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );

collect stats
	primary index ( business_date )
	,column ( business_date )
		on t3dl_ace_mch.cp_ref_digital_cost;

-- ### (Teradata) - Teradata to Teradata -- MTA Transaction/Digital Channel Attributed Association

	----------- MTA Transaction/Digital Channel Association Data
	
/* One time Creation
-- 2021.12.31 - DDL Creation for the MTA associated transactions to Digital
	-- For production purposes, this ddl should be optimized, i.e. character structure and compression inclusions
-- DROP TABLE T3DL_ACE_MCH.CP_MKTG_DIGITAL_MTA;
CREATE MULTISET TABLE T3DL_ACE_MCH.CP_MKTG_DIGITAL_MTA ,FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1,
     LOG
     (
      order_number VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      order_channelcountry VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      marketing_type VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      order_date_pacific DATE FORMAT 'YY/MM/DD',
      update_date DATE FORMAT 'YY/MM/DD',
      process_date DATE FORMAT 'YY/MM/DD'
     )
PRIMARY INDEX ( order_number )
PARTITION BY RANGE_N(order_date_pacific  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );
*/

-- 2021.12.31 - Prep data for MERGE 
	-- If any further adjustments need to be made prior to pushing to final dataset, do it here
--drop table cp_mktg_digital_mta_stg;
create multiset volatile table cp_mktg_digital_mta_stg
as (
	select order_number
		,order_channelcountry
		,finance_rollup
		,finance_detail
		,coalesce(marketing_type,mktg_type) marketing_type
		,cast(order_date_pacific as date) order_date_pacific
		,current_date as update_date
		,current_date as process_date
	from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_TRANS_STG
	group by 1,2,3,4,5,6,7,8
)
with data 
primary index ( order_number )
PARTITION BY RANGE_N(order_date_pacific  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY )
on commit preserve rows;

collect stats 
	primary index ( order_number )
	,column ( order_number )
	,column ( order_date_pacific )
		on cp_mktg_digital_mta_stg;
	
-- 2021.12.31 - Upsert records for transaction/channel association
merge into 
T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital_mta as a 
using 
cp_mktg_digital_mta_stg as b
on
	a.order_number = b.order_number
	and a.order_date_pacific = b.order_date_pacific
	and coalesce(a.finance_detail,a.marketing_type,'BASE') = coalesce(b.finance_detail,b.marketing_type,'BASE')
when matched
then UPDATE 
	set 
		order_channelcountry = b.order_channelcountry
		,finance_rollup = b.finance_rollup
		,finance_detail = b.finance_detail
		,marketing_type = b.marketing_type
		,update_date = b.update_date
when not MATCHED 
then insert
	VALUES 
	(
	b.order_number
	,b.order_channelcountry
	,b.finance_rollup
	,b.finance_detail
	,b.marketing_type
	,b.order_date_pacific
	,b.update_date
--	,'2020-01-01'
	,b.process_date
	);

collect stats
	primary index ( order_number )
	,column ( order_number )
	,column ( order_date_pacific )
		on T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital_mta;

-- ### (Teradata) - Teradata to Teradata -- Digital Transactions Allocation Build and Validation

	----------- Digital Transactions Allocated Build

-- 2021.12.20 - Identification of transactions MTA identified
	-- The ORDER_NUMBER value in the MTA data isn't just Order Numbers, it's a mix of ORDER_NUMBER, UNIQUE_SOURCE_ID, GLOBAL_TRAN_ID
		-- This is the initial identification at unique_source_id.
	-- Match Type 1 is being created to identify which method this record was tracked under
--drop table mktg_digital_01;
create multiset volatile table mktg_digital_01
as (
	select a.order_number
		,a.order_channel
		,a.order_channelcountry
--		,a.mktg_type
--		,a.finance_rollup
--		,a.finance_detail
--		,a.marketing_type
--		,a.order_date_pacific
		,b.business_day_date
		,b.global_tran_id
		,b.tran_type_code
--		,b.line_item_activity_type_code
--		,b.unique_source_id
		,sum(b.line_net_usd_amt) ttl_line_net_usd_amt
		,SUM(b.line_net_amt) ttl_line_net_amt
		,line_item_net_amt_currency_code
		,1 as match_type
	from 
		(select order_number
			,order_channel
			,order_channelcountry
		 from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_TRANS_STG
		 group by 1,2,3) a join
		(select global_tran_id 
			,min(business_day_date) business_day_date
			,tran_type_code
			,line_item_activity_type_code
			,unique_source_id
			,sum(line_net_usd_amt) line_net_usd_amt 
			,sum(line_net_amt) line_net_amt 
			,line_item_net_amt_currency_code		
		 from prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW 
		 where business_day_date between '2021-10-01' and current_date
		 	and tran_type_code = 'SALE'
		 	and line_item_activity_type_code = 'S'
		 	and line_item_merch_nonmerch_ind = 'MERCH'
		 group by 1,3,4,5,8) b
			on a.order_number = b.unique_source_id
	group by 1,2,3,4,5,6,9,10
)
with DATA 
primary index ( order_number )
on commit preserve rows;

collect stats 
	primary index ( order_number )
	,column ( order_number )
		on mktg_digital_01;
	
-- 2021.12.20 - Identification of transactions MTA identified
	-- The ORDER_NUMBER value in the MTA data isn't just Order Numbers, it's a mix of ORDER_NUMBER, UNIQUE_SOURCE_ID, GLOBAL_TRAN_ID
		-- This is the initial identification at order_number.
	-- Match Type 2 is being created to identify which method this record was tracked under
		-- 2 = matched at order_number 
	-- Since web order's can correlate to more than 1 global tran id, we are tracking the aggregate at order number so we ensure we are attributing the correct order counts to cost equivalent to a customer's trip
--drop table mktg_digital_02;
create multiset volatile table mktg_digital_02
as (
	select a.order_number
		,a.order_channel
		,a.order_channelcountry
--		,a.mktg_type
--		,a.finance_rollup
--		,a.finance_detail
--		,a.marketing_type
--		,a.order_date_pacific
		,coalesce(b.business_day_date,a.order_date_pacific) business_day_date
		,b.global_tran_id
		,b.tran_type_code
--		,b.line_item_activity_type_code
--		,b.unique_source_id
		,sum(b.line_net_usd_amt) ttl_line_net_usd_amt
		,SUM(b.line_net_amt) ttl_line_net_amt
		,line_item_net_amt_currency_code
		,b.line_net_usd_amt_final
		,b.line_net_amt_final
		,coalesce(b.rn,1) rn
		,2 as match_type
	from 
		(select a.*
		 from 
		 	(select order_number
		 		,order_channel
		 		,order_channelcountry
		 		,cast(order_date_pacific as date) order_date_pacific
		 	 from T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital_trans_stg
		 	 group by 1,2,3,4) a left join
			(select order_number
			 from mktg_digital_01 
			 group by 1) b
			 	on a.order_number = b.order_number
		 where b.order_number is NULL) a join
		(select global_tran_id 
			,min(business_day_date) business_day_date
			,tran_type_code
			,line_item_activity_type_code
--			,unique_source_id
			,order_num
			,sum(line_net_usd_amt) line_net_usd_amt 
			,sum(line_net_amt) line_net_amt 
			,line_item_net_amt_currency_code	
			,row_number() over (partition by order_num order by order_num) rn
			,sum(line_net_usd_amt) over (partition by order_num order by 1) line_net_usd_amt_final
			,sum(line_net_amt) over (partition by order_num order by 1) line_net_amt_final
		 from prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW 
		 where business_day_date between '2021-10-01' and current_date
		 	and order_num is not null
		 	and tran_type_code = 'SALE'
		 	and line_item_activity_type_code = 'S'
		 	and line_item_merch_nonmerch_ind = 'MERCH'
		 group by 1,3,4,5,8, line_net_usd_amt, line_net_amt) b
			on a.order_number = b.order_num
	group by 1,2,3,4,5,6,9,10,11,12,13
)
with DATA 
primary index ( order_number )
on commit preserve rows;

collect stats 
	primary index( order_number )
	,column ( order_number )
		on mktg_digital_02;

-- 2022.03.21 - Identification of transactions MTA identified
	-- The ORDER_NUMBER value in the MTA data isn't just Order Numbers, it's a mix of ORDER_NUMBER, UNIQUE_SOURCE_ID, GLOBAL_TRAN_ID
		-- This also includes order's not found in ERTM but rather just in OMS
	-- Match Type 3
		-- 3 = matched at global_tran_id
	-- Since web order's can correlate to more than 1 global tran id, we are tracking the aggregate at order number so we ensure we are attributing the correct order counts to cost equivalent to a customer's trip
--drop table mktg_digital_03;
create multiset volatile table mktg_digital_03
as (
	select a.order_number
		,a.order_channel
		,a.order_channelcountry
--		,a.mktg_type
--		,a.finance_rollup
--		,a.finance_detail
--		,a.marketing_type
--		,a.order_date_pacific
		,coalesce(b.business_day_date,a.order_date_pacific) business_day_date
		,b.global_tran_id
		,b.tran_type_code
--		,b.line_item_activity_type_code
--		,b.unique_source_id
		,sum(b.line_net_usd_amt) ttl_line_net_usd_amt
		,SUM(b.line_net_amt) ttl_line_net_amt
		,line_item_net_amt_currency_code
		,b.line_net_usd_amt_final
		,b.line_net_amt_final
		,coalesce(b.rn,1) rn
		,3 as match_type
	from 
		(select a.*
		 from 
		 	(select order_number
		 		,order_channel
		 		,order_channelcountry
		 		,cast(order_date_pacific as date) order_date_pacific
		 	 from T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital_trans_stg
		 	 group by 1,2,3,4) a left join
			(select order_number
			 from 
			 	(select order_number
				 from mktg_digital_01 
				 group by 1
				 UNION
				 select order_number
				 from mktg_digital_02
				 group by 1) a
			) b
			 	on a.order_number = b.order_number
		 where b.order_number is NULL) a join
		(select global_tran_id 
			,min(business_day_date) business_day_date
			,tran_type_code
			,line_item_activity_type_code
--			,unique_source_id
--			,order_num
			,sum(line_net_usd_amt) line_net_usd_amt 
			,sum(line_net_amt) line_net_amt 
			,line_item_net_amt_currency_code	
			,row_number() over (partition by global_tran_id order by global_tran_id) rn
			,sum(line_net_usd_amt) over (partition by global_tran_id order by 1) line_net_usd_amt_final
			,sum(line_net_amt) over (partition by global_tran_id order by 1) line_net_amt_final
		 from prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW 
		 where business_day_date between '2021-10-01' and current_date
--		 	and order_num is not null
-- 2022.03.21 - For this one, as most of the MTA transactions at thi poit are exchanges, we are eliminating the tran_type_code to catch these typesof transactions
--		 	and tran_type_code = 'EXCH'
		 	and line_item_activity_type_code = 'S'
		 	and line_item_merch_nonmerch_ind = 'MERCH'
		 group by 1,3,4,7, line_net_usd_amt, line_net_amt) b
			on cast(a.order_number as bigint) = cast(b.global_tran_id as bigint)
	group by 1,2,3,4,5,6,9,10,11,12,13
)
with DATA 
primary index ( order_number )
on commit preserve rows;

collect stats 
	primary index( order_number )
	,column ( order_number )
		on mktg_digital_03;

-- 2022.03.21 - Identification of transactions MTA identified
	-- The ORDER_NUMBER value in the MTA data isn't just Order Numbers, it's a mix of ORDER_NUMBER, UNIQUE_SOURCE_ID, GLOBAL_TRAN_ID
		-- It is also order's never fulfilled on, i.e. cancelled, etc.
	-- This is the last identification at order_number.
	-- Match Type 4 & 5 are being created to identify which method this record was tracked under
		-- 4 = matched at OMS Order not found in ERTM
		-- 5 = wasn't a match through any of our methods of matching. Unknown what value this is
	-- Since web order's can correlate to more than 1 global tran id, we are tracking the aggregate at order number so we ensure we are attributing the correct order counts to cost equivalent to a customer's trip
--drop table mktg_digital_04;	
create multiset volatile table mktg_digital_04
as (
	select a.order_number
		,a.order_channel
		,a.order_channelcountry
--		,a.mktg_type
--		,a.finance_rollup
--		,a.finance_detail
--		,a.marketing_type
--		,a.order_date_pacific
		,coalesce(b.order_date_pacific,a.order_date_pacific) business_day_date
		,null as global_tran_id
		,b.order_type_code as tran_type_code
--		,b.line_item_activity_type_code
--		,b.unique_source_id
		,sum(b.order_line_total_amount_usd) as ttl_line_net_usd_amt
		,SUM(b.order_line_total_amount) as ttl_line_net_amt
		,order_line_currency_code as line_item_net_amt_currency_code
		,b.order_line_total_amount_usd_final as line_net_usd_amt_final
		,b.order_line_total_amount_final as line_net_amt_final
		,1 as rn
		,case 
			when b.order_date_pacific is not null then 4
			else 5
		 end as match_type
	from 
		(select a.*
		 from 
		 	(select order_number
		 		,order_channel
		 		,order_channelcountry
		 		,cast(order_date_pacific as date) order_date_pacific
		 	 from T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital_trans_stg
		 	 group by 1,2,3,4) a left join
			(select order_number
			 from 
			 	(select order_number
				 from mktg_digital_01 
				 group by 1
				 UNION
				 select order_number
				 from mktg_digital_02
				 group by 1
				 UNION
				 select order_number
				 from mktg_digital_03
				 group by 1) a
			) b
			 	on a.order_number = b.order_number
		 where b.order_number is NULL) a left join
		(select order_num
			,order_date_pacific 
			,order_type_code
			,sum(order_line_total_amount_usd) order_line_total_amount_usd 
			,sum(order_line_total_amount) order_line_total_amount 
			,order_line_currency_code
			,sum(order_line_total_amount_usd) over (partition by order_num order by 1) order_line_total_amount_usd_final 
			,sum(order_line_total_amount) over (partition by order_num order by 1) order_line_total_amount_final
		 from prd_nap_usr_vws.ORDER_LINE_DETAIL_FACT
		 where order_date_pacific between '2021-10-01' and current_date
		 group by 1,2,3,6,order_line_total_amount_usd, order_line_total_amount) b
			on a.order_number = b.order_num
	group by 1,2,3,4,5,6,9,10,11,12,13
)
with DATA 
primary index ( order_number )
on commit preserve rows;

collect stats 
	primary index( order_number )
	,column ( order_number )
		on mktg_digital_04;
	
-- 2021.12.30 - Final build of the transaction data before upsert to final destination
	-- 2022.03.21 - Incorporated mktg_digital 03 (ERTM Exchanges) and 04 (cancelled OMS orders)
-- drop table mktg_digital_final;
create multiset volatile table mktg_digital_final
as (
	select global_tran_id
		,business_day_date
		,order_channel
		,order_channelcountry
		,order_number
		,tran_type_code
		,ttl_line_net_usd_amt
		,ttl_line_net_amt
		,line_item_net_amt_currency_code
		,line_net_usd_amt_final
		,line_net_amt_final
		,rn
		,match_type
		,update_date
		,process_date
	from (
		select global_tran_id 
			,business_day_date
			,order_channel
			,order_channelcountry
			,cast(global_tran_id as varchar(50)) as order_number
			,tran_type_code
			,ttl_line_net_usd_amt
			,ttl_line_net_amt
			,line_item_net_amt_currency_code
			,ttl_line_net_usd_amt as line_net_usd_amt_final
			,ttl_line_net_amt as line_net_amt_final
			,1 as rn
			,match_type
			,cast(current_date as date format 'YYYY-MM-DD') update_date
			,cast(current_date as date format 'YYYY-MM-DD') process_date
		from mktg_digital_01
		union
		select global_tran_id
			,business_day_date
			,order_channel
			,order_channelcountry
			,order_number
			,tran_type_code
			,ttl_line_net_usd_amt
			,ttl_line_net_amt
			,line_item_net_amt_currency_code
			,line_net_usd_amt_final
			,line_net_amt_final
			,rn
			,match_type
			,cast(current_date as date format 'YYYY-MM-DD') update_date
			,cast(current_date as date format 'YYYY-MM-DD') process_date
		from mktg_digital_02
		union 
		select global_tran_id
			,business_day_date
			,order_channel
			,order_channelcountry
			,order_number
			,tran_type_code
			,ttl_line_net_usd_amt
			,ttl_line_net_amt
			,line_item_net_amt_currency_code
			,line_net_usd_amt_final
			,line_net_amt_final
			,rn
			,match_type
			,cast(current_date as date format 'YYYY-MM-DD') update_date
			,cast(current_date as date format 'YYYY-MM-DD') process_date
		from mktg_digital_03
		union 
		select global_tran_id
			,business_day_date
			,order_channel
			,order_channelcountry
			,order_number
			,tran_type_code
			,ttl_line_net_usd_amt
			,ttl_line_net_amt
			,line_item_net_amt_currency_code
			,line_net_usd_amt_final
			,line_net_amt_final
			,rn
			,match_type
			,cast(current_date as date format 'YYYY-MM-DD') update_date
			,cast(current_date as date format 'YYYY-MM-DD') process_date
		from mktg_digital_04
		) a
)
with data
primary index ( order_number )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
on commit preserve rows;

collect stats
	primary index ( order_number )
	,column ( order_number) 
	,column ( business_day_date ) 
		on mktg_digital_final;


/* One time creation of DDL to support transaction tracking
-- 2021.12.30 - DDL for Digital Transaction Tracking
-- DROP TABLE T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL;
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL ,FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1,
     LOG
     (
	  global_tran_id BIGINT
	  ,business_day_date DATE FORMAT 'YY/MM/DD'
	  ,order_channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	  ,order_channelcountry VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC
      ,order_number VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	  ,tran_type_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
	  ,ttl_line_net_usd_amt DECIMAL(38,2)
	  ,ttl_line_net_amt DECIMAL(38,2)
	  ,line_item_net_amt_currency_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
	  ,line_net_usd_amt_final DECIMAL(38,2)
	  ,line_net_amt_final DECIMAL(38,2)
	  ,rn BYTEINT
	  ,match_type BYTEINT
	  ,update_date DATE FORMAT 'YY/MM/DD'
	  ,process_date DATE FORMAT 'YY/MM/DD'
	 )
PRIMARY INDEX ( order_number )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );
*/	

-- 2021.12.30 - Identify any new records/updates to push to final
	-- This push is going to be a one way trip meaning that any global tran id's identified in the past that for whatever reason can't be identified in the future will still remain and as such, will always be a component in the cost calculation
		-- If this needs to change, change the code here to support that
merge into 
T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital as a 
using 
mktg_digital_final as b
on
	a.order_number = b.order_number
	and a.business_day_date = b.business_day_date
	AND a.rn = b.rn
when matched
then UPDATE 
	set 
		global_tran_id = b.global_tran_id
		,order_channel = b.order_channel
		,order_channelcountry = b.order_channelcountry
		,tran_type_code = b.tran_type_code
		,ttl_line_net_usd_amt = b.ttl_line_net_usd_amt
		,ttl_line_net_amt = b.ttl_line_net_amt
		,line_item_net_amt_currency_code = b.line_item_net_amt_currency_code
		,line_net_usd_amt_final = b.line_net_usd_amt_final
		,line_net_amt_final = b.line_net_amt_final
		,rn = b.rn
		,match_type = b.match_type
		,update_date = b.update_date
when not MATCHED 
then insert
	VALUES 
	(
	b.global_tran_id
	,b.business_day_date
	,b.order_channel
	,b.order_channelcountry
	,b.order_number
	,b.tran_type_code
	,b.ttl_line_net_usd_amt
	,b.ttl_line_net_amt
	,b.line_item_net_amt_currency_code
	,b.line_net_usd_amt_final
	,b.line_net_amt_final
	,b.rn
	,b.match_type
	,b.update_date
	,b.process_date
	);

collect stats
	primary index ( order_number )
	,column ( order_number )
	,column ( business_day_date )
	,column ( global_tran_id )
	,column ( match_type )
		on T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital;

DELETE FROM T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_DIGITAL_TRANS_STG;

-- ### (Teradata) - Teradata to Teradata -- One Time View Creations - Revised

	----------- View Builds
	-- NOTE: Do not use the view build in the mktg_email.md file anymore as that is no longer relevant.
		-- We are going to keep it for tracking purposes and remove once all complete.
	
/*** Only do this if you want to alter the view going forward
-- 2021.12.31 - One time view ddl creation of the cost by day for marketing
REPLACE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_COST_BY_DAY
AS
LOCK ROW FOR ACCESS
select category
	,subcategory_01
	,subcategory_02
	,business_day_date
	,cnt_unique_tran
	,trans_cost_mktg
from 
	(
	select cast('MARKETING' as varchar(20)) AS CATEGORY 
		,cast('EMAIL' as varchar(20)) AS SUBCATEGORY_01
		,cast(NULL as varchar(50)) AS SUBCATEGORY_02
		,business_day_date
		,count(distinct global_tran_id) cnt_unique_tran
	--	,count(*) cnt_ttl
		,cost_value/count(distinct global_tran_id) trans_cost_mktg
	from T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_email a join
		T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST b 
			on a.business_day_date = b.business_date
	where b.platform = 'SALESFORCE'
	group by 1,2,3,4,cost_value 
	union all
	SELECT b.category
		,b.subcategory_01
		,b.subcategory_02
		,b.business_date as business_day_date
		,count(distinct a.order_number) cnt_unique_tran
		,b.cost_value/count(distinct a.order_number) trans_cost_mktg
	from T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital_mta a join
		(select category
			,subcategory_01
			,subcategory_02
			,business_date
			,sum(cost_value) cost_value
		 from T2DL_DAS_CONTRIBUTION_MARGIN.cp_ref_digital_cost 
		 group by 1,2,3,4) b 
			on a.order_date_pacific = b.business_date
				and a.finance_detail = b.subcategory_02
	group by 1,2,3,4,b.cost_value
	) a;
*/

/*** Only do this if you want to alter the view going forward
-- 2021.12.31 - One time view ddl creation of the transaction cost by day per marketing program
REPLACE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_COST_BY_TRANS
AS LOCK ROW FOR ACCESS
select a.global_tran_id
	,coalesce(b.business_day_date, c.business_day_date) business_day_date
	,coalesce(b.ttl_line_net_usd_amt, c.ttl_line_net_usd_amt) ttl_line_net_usd_amt
	,b.trans_cost_mktg_email
	,c.trans_cost_mktg_digital_affiliates
	,c.trans_cost_mktg_digital_display
	,c.trans_cost_mktg_digital_paid_search_branded
	,c.trans_cost_mktg_digital_paid_search_unbranded
	,c.trans_cost_mktg_digital_shopping
	,c.trans_cost_mktg_digital_social_paid
	,c.trans_cost_mktg_digital_video
from 
-- Unique identification of all global tran id's as base
	(select global_tran_id
	from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL
	group by 1
	union
	select global_tran_id
	from T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital
	-- Alter this if you want to include the orphan transactions
	where match_type in (1,2)
	group by 1) a left join
-- Email Cost to transaction
	(select a.global_tran_id
		,a.business_day_date
		,a.ttl_line_net_usd_amt
		,b.trans_cost_mktg as trans_cost_mktg_email
	from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL a join
		T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_cost_by_day b 	
			on a.business_day_date = b.business_day_date
	where b.category = 'MARKETING'
		and b.subcategory_01 = 'EMAIL') b
		on a.global_tran_id = b.global_tran_id left join
-- Digital Cost to transaction
	(select a.global_tran_id
		,a.business_day_date
		,a.ttl_line_net_usd_amt
		,CASE 
			when c.subcategory_02 = 'AFFILIATES' then c.trans_cost_mktg 
			else null
		 end trans_cost_mktg_digital_affiliates
		,CASE 
			when c.subcategory_02 = 'DISPLAY' then c.trans_cost_mktg 
			else null
		 end trans_cost_mktg_digital_display
		,CASE 
			when c.subcategory_02 = 'PAID_SEARCH_BRANDED' then c.trans_cost_mktg 
			else null
		 end trans_cost_mktg_digital_paid_search_branded
		 ,CASE 
			when c.subcategory_02 = 'PAID_SEARCH_UNBRANDED' then c.trans_cost_mktg 
			else null
		 end trans_cost_mktg_digital_paid_search_unbranded
		,CASE 
			when c.subcategory_02 = 'SHOPPING' then c.trans_cost_mktg 
			else null
		 end trans_cost_mktg_digital_shopping
		,CASE 
			when c.subcategory_02 = 'SOCIAL_PAID' then c.trans_cost_mktg 
			else null
		 end trans_cost_mktg_digital_social_paid
		,CASE 
			when c.subcategory_02 = 'VIDEO' then c.trans_cost_mktg 
			else null
		 end trans_cost_mktg_digital_video
	from T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital a join
		T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_digital_mta b 
			on a.order_number = b.order_number JOIN 
		T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_cost_by_day c 
			on b.order_date_pacific = c.business_day_date
				and b.finance_detail = c.subcategory_02
	where a.match_type in (1,2)
		and a.rn = 1) c
		on a.global_tran_id = c.global_tran_id
where coalesce(b.business_day_date,c.business_day_date) is not null;
*/


