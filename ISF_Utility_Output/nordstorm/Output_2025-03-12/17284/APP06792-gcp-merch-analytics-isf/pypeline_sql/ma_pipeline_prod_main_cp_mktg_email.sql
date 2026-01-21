/*
Contribution Profit - Marketing
Authors: KP Tryon, Sara Riker, Sara Scott
Datalab: t2dl_das_contribution_margin

Updates:
2022.03.03 - Initial ETL for Marketing Costs build created
2022.03.03 - Marketing Costs (Email) initiated

*/

-- 2021.12.09 - Identification of e-mails sent through email marketing
	-- Variable would be the time period looked at.
-- DROP TABLE MKTG_EMAIL_01;
create volatile table mktg_email_01
as 
(
	select 
	--	enterprise_id||'_'||campaign_name as row_id
		enterprise_id
		,program_type
	--	,program
	--	,campaign_name
-- 2022.03.02 - Sometime in early 2022, deploy_date and deploy_time was switched by source teams to last_updated_date and last_updated_time
		,last_updated_date as deploy_date
	--	,deploy_date
	--	,deploy_time
	from PRD_NAP_USR_VWS.CONTACT_EVENT_SENDLOG_dim cesd 
	where deploy_date between current_date - 7 and current_date
		and program_type = 'EMAIL'
	group by 1,2,3
)
with data
primary index ( enterprise_id )
on commit preserve rows;

collect stats
	primary index ( enterprise_id )
	,column ( enterprise_id )
		on mktg_email_01;

-- 2021.12.09 - Identification of the email's sent for Marketing to Customers
  -- Strip out email at this point as it is no longer needed after this point
--drop table mktg_email_02;
create volatile table mktg_email_02
as 
(
select b.acp_id
--	,a.enterprise_id
	,a.program_type
--	,a.campaign_name
	,a.deploy_date
--	,a.deploy_time
	,b.country
--	,b.preference_ind
from mktg_email_01 a join
	prd_nap_usr_vws.CUSTOMER_EMAIL_PREFERENCE_FACT b 
		on a.enterprise_id = b.enterprise_id
group by 1,2,3,4
)
with DATA 
primary index ( deploy_date, acp_id )
on commit preserve rows;

collect stats 
	primary index ( deploy_date, acp_id )
--	,column ( deploy_date, acp_id )
	,column ( acp_id )
	,column ( deploy_date )
		on mktg_email_02;

-- 2021.12.09 - Identification of transactions tied to a customer receiving a touchpoint from Email Marketing within timeframe specified
	-- We only care about the latest Marketing Touchpoint within a 7 day rolling period of each transaction
	-- The transaction must have a marketing touchpoint within a 7 day rolling period
	-- This dataset will contain all marketing sends in current syntax state. This is to allow us to see how many don't convert
	-- Only merch transactions are tracked
	-- Only sale transactions are tracked. We exclude any others
--drop table mktg_email_03;
create volatile table mktg_email_03
as 
(
	select a.acp_id
--		,a.enterprise_id
		,a.program_type
--		,a.campaign_name
		,max(a.deploy_date) deploy_date
--		,a.deploy_time
--		,a.country
--		,a.preference_ind
		,b.global_tran_id
		,b.tran_type_code
		,b.business_day_date
		,b.intent_store_num
		,b.order_num
		,sum(b.line_net_usd_amt) ttl_line_net_usd_amt
		,sum(b.line_item_quantity) ttl_line_item_quantity
		,current_date as update_date
		,current_date as process_date
	from mktg_email_02 a left join
		prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW b 
			on a.deploy_date between b.business_day_date - 7 and b.business_day_date
				and a.acp_id = b.acp_id
	where b.business_day_date between current_date - 30 and current_date
		and b.tran_type_code = 'SALE'
		and b.line_item_activity_type_code = 'S'
		and line_item_merch_nonmerch_ind = 'MERCH'
	group by 1,2,4,5,6,7,8,11,12
)
with DATA 
primary index ( acp_id )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
on commit preserve rows;

collect stats 
	primary index ( acp_id )
	,column ( acp_id )
	,column ( business_day_date )
		on mktg_email_03;	

/** One time DDL Creation of CP_MKTG_EMAIL for transaction tracking
	-- These are transactions that are found to have a marketing touchpoint of email within the last 7 days of the transaction business date
--drop table T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL ;
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
	  ttl_line_net_usd_amt decimal(38,2),
	  ttl_line_item_quantity decimal(38,0),
      update_date DATE FORMAT 'YY/MM/DD',
      process_date DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( global_tran_id )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );	
**/

-- 2021.12.10 - Push data in to the datalab for historical reference
merge into
t2dl_das_contribution_margin.cp_mktg_email as a 
using
mktg_email_03 as b
on
	a.global_tran_id = b.global_tran_id
	and a.business_day_date = b.business_day_date
when MATCHED 
then UPDATE 
	set
		tran_type_code = b.tran_type_code
		,acp_id = b.acp_id
		,deploy_date = b.deploy_date
		,program_type = b.program_type
		,intent_store_num = b.intent_store_num
		,order_num = b.order_num
		,ttl_line_net_usd_amt = b.ttl_line_net_usd_amt
		,ttl_line_item_quantity = b.ttl_line_item_quantity
		,update_date = b.update_date
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.global_tran_id
	,b.business_day_date
	,b.tran_type_code
	,b.acp_id
	,b.deploy_date
	,b.program_type
	,b.intent_store_num
	,b.order_num
	,b.ttl_line_net_usd_amt
	,b.ttl_line_item_quantity
	,b.update_date
	,b.process_date
	);

collect stats
	primary index ( global_tran_id )
	,column ( global_tran_id )
	,column ( business_day_date )
		on t3dl_ace_mch.CP_MKTG_EMAIL ;

/************* THIS IS THE DDL CREATION FOR SALESFORCE CONTRACT COST
-- 2021.12.10 - DDL Creation for the CRM Cost over time
CREATE MULTISET TABLE T3DL_ACE_MCH.CP_REF_CRM_COST ,FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1,
     LOG
     (
	 platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 business_date DATE FORMAT 'YY/MM/DD',
	 channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 channel_01 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 channel_02 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 cost_value DECIMAL(20,2),
	 contract_start DATE FORMAT 'YY/MM/DD',
	 contract_end DATE FORMAT 'YY/MM/DD',
	 update_dt DATE FORMAT 'YY/MM/DD',
	 process_dt DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( platform )
PARTITION BY RANGE_N(business_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );	
*/

-- ****** The following section only needs to be completed if the SalesForce contract changes, i.e. renewed or a new contract from some other platform has been initiated upon

-- 2021.12.10 - DDL Creation for the CRM Cost staging dataset
--DROP TABLE T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST_STG ;
/*
CREATE MULTISET TABLE T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST_STG ,FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1,
     LOG
     (
	 platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 business_date VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 channel_01 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 channel_02 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 cost_value VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 contract_start VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 contract_end VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 update_dt VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	 process_dt VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC)
PRIMARY INDEX ( platform );	
*/

-- 2021.12.14 - Import the CRM Platform cost data to staging ddl
--CALL SYS_MGMT.S3_TPT_LOAD ('T2DL_DAS_CONTRIBUTION_MARGIN','CP_REF_CRM_COST_STG','us-west-2','nordace-isf-ma-prod','s3ToTeradata/','crm_cost.csv','2C',OUT_MESSAGE);

-- 2021.12.14 - Clean data for final data load
	-- This is only if the csv being imported includes a header. If it doesn't, this piece can remain but it just won't find anything
--DELETE FROM T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST_STG 
--WHERE UPPER(PLATFORM) = 'PLATFORM';

-- 2021.12.14 - Push data to final data output
/*
INSERT INTO T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST 
SELECT PLATFORM
	,BUSINESS_DATE
	,CHANNEL
	,CHANNEL_01
	,CHANNEL_02
	,COST_VALUE
	,CONTRACT_START
	,CONTRACT_END
	,UPDATE_DT
	,PROCESS_DT
FROM T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST_STG;

collect stats
	primary index ( platform )
	,column ( platform )
	,column ( business_date )
		on t3dlT2DL_DAS_CONTRIBUTION_MARGIN_ace_mch.CP_REF_CRM_COST;
*/

-- 2021.12.14 - Drop of the staging table for crm cost
--drop table T2DL_DAS_CONTRIBUTION_MARGIN.cp_ref_crm_cost_stg;

-- ***** This section only needs to be re-created if a new component about marketing has been added.

-- 2021.12.14 - One time view ddl creation of the cost by day for marketing
	-- 2021.12.09 - Identification of what the cost per transaction on a given business date would be
/*
REPLACE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_COST_BY_DAY
AS
LOCK ROW FOR ACCESS
select 'MARKETING' AS CATEGORY 
	,'EMAIL' AS SUBCATEGORY_01
	,business_day_date
	,count(distinct global_tran_id) cnt_unique_tran
--	,count(*) cnt_ttl
	,cost_value/count(distinct global_tran_id) trans_cost_mktg_email
from T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_email a join
	T2DL_DAS_CONTRIBUTION_MARGIN.CP_REF_CRM_COST b 
		on a.business_day_date = b.business_date
where b.platform = 'SALESFORCE'
group by 1,2,3,cost_value;
*/

-- 2021.12.14 - One time view ddl of transaction cost
/*
REPLACE VIEW T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_COST_BY_TRANS
AS 
LOCK ROW FOR ACCESS
select a.global_tran_id
	,a.business_day_date
	,a.ttl_line_net_usd_amt
	,b.trans_cost_mktg_email
from T2DL_DAS_CONTRIBUTION_MARGIN.CP_MKTG_EMAIL a join
	T2DL_DAS_CONTRIBUTION_MARGIN.cp_mktg_cost_by_day b 	
		on a.business_day_date = b.business_day_date
where b.category = 'MARKETING'
	and b.subcategory_01 = 'EMAIL';
*/