--main script that when it runs, either everyday or weekly to update the table 

/*
 * 
Name: Planned history report
Project:  Planned History Report
Purpose: Query that builds the backend data for Planned History Report in Cost
Variable(s):    {{environment_schema}} T2DL_DAS_OPEN_TO_BUY
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
ast_pipeline_prod_main_planned_history_report.sql
Author: Paria Avij
Date Created: 02/03/23
Date Last Updated: 07/27/23

Datalab: t2dl_das_open_to_buy

*/

-- creating a week to month mapping for fiscal_year_num>=2022

--drop table hist_date;
CREATE MULTISET VOLATILE TABLE hist_date AS(
SELECT
      
	 WEEK_IDNT
	,MONTH_IDNT
	,MONTH_LABEL
	,QUARTER_IDNT 
	,QUARTER_LABEL 
	,'FY'||substr(cast(fiscal_year_num as varchar(5)),3,2)||' '||quarter_abrv as QUARTER
	,FISCAL_YEAR_NUM 
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE fiscal_year_num >= 2022
GROUP BY 1,2,3,4,5,6,7)

WITH DATA
   PRIMARY INDEX(WEEK_IDNT)
   ON COMMIT PRESERVE ROWS;
  

COLLECT STATS
     PRIMARY INDEX (WEEK_IDNT)
     ,COLUMN(WEEK_IDNT)
     ,COLUMN(MONTH_IDNT)
     ,COLUMN(QUARTER_IDNT)
     ,COLUMN(FISCAL_YEAR_NUM)
     ON hist_date;
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

--here we map channel info to banner
--drop table Hist_loc_base;
CREATE MULTISET VOLATILE TABLE Hist_loc_base AS(
	SELECT
	 a.CHANNEL_NUM
	,a.CHANNEL_DESC
	,a.CHANNEL_BRAND
	,a.CHANNEL_COUNTRY
	,a.CHANNEL_NUM||','||a.CHANNEL_DESC as CHANNEL
	,CASE
		WHEN a.CHANNEL_BRAND = 'Nordstrom' AND a.CHANNEL_COUNTRY = 'US' THEN 'US Nordstrom'
		WHEN a.CHANNEL_BRAND = 'Nordstrom' AND a.CHANNEL_COUNTRY = 'CA' THEN 'CA Nordstrom'
		WHEN a.CHANNEL_BRAND = 'Nordstrom_Rack' AND a.CHANNEL_COUNTRY = 'US' THEN 'US Rack'
		WHEN a.CHANNEL_BRAND = 'Nordstrom_Rack' AND a.CHANNEL_COUNTRY = 'CA' THEN 'CA Rack'
	END AS BANNER
	FROM (
		SELECT
			 CHANNEL_NUM
			,CHANNEL_DESC
			,CHANNEL_BRAND
			,CHANNEL_COUNTRY
		FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
		GROUP BY 1,2,3,4
		WHERE CHANNEL_BRAND in ('Nordstrom','Nordstrom_Rack')
		AND CHANNEL_NUM IN ('110', '120', '140', '210', '220', '240', '250', '260', '310', '930', '940', '990', '111', '121', '211', '221', '261', '311')		
)a
GROUP BY 1,2,3,4,5)

WITH DATA
   PRIMARY INDEX(CHANNEL_NUM)
   ON COMMIT PRESERVE ROWS;
  
 COLLECT STATS
     PRIMARY INDEX (CHANNEL_NUM)
     ,COLUMN(CHANNEL_NUM)
     ON Hist_loc_base;


-------------------------------------------------------------------------------------------------------------------
 --Metric: SP_RCPTS_LESS_RESERVE_C
-------------------------------------------------------------------------------------------------------------------
 
-- bringing in SP plan data and rolling weeks to months for SP_Rcpts_Less_Reserve_C metric for a.FULFILL_TYPE_NUM  = '3' owned based inventory

  
--drop table hist_receipt;
CREATE MULTISET VOLATILE TABLE hist_receipt AS(
SELECT
	 c.Month_Plans
	,c.DEPT_ID
	,c.BANNER
	,c.COUNTRY
	,c.MONTH_IDNT
	,c.MONTH_LABEL
	,c.QUARTER_IDNT  
	,c.FISCAL_YEAR_NUM 
	,SUM(c.SP_RECEIPTS_RESERVE_COST_AMT) AS SP_RCPTS_RESERVE_C
	,SUM(c.SP_RECEIPTS_ACTIVE_COST_AMT) AS  SP_RCPTS_ACTIVE_C
	,SUM(c.SP_RECEIPTS_INACTIVE_COST_AMT) AS SP_RCPTS_INACTIVE_C
	,SUM(c.OP_RECEIPTS_ACTIVE_COST_AMT) AS OP_RCPTS_ACTIVE_C
	,SUM(c.OP_RECEIPTS_INACTIVE_COST_AMT) AS OP_RCPTS_INACTIVE_C
	,SUM(c.OP_RECEIPTS_RESERVE_COST_AMT) AS OP_RCPTS_RESERVE_C
	,(SUM(c.SP_RECEIPTS_ACTIVE_COST_AMT)-SUM(c.SP_RECEIPTS_RESERVE_COST_AMT)) AS SP_RCPTS_LESS_RESERVE_C
	,(SUM(c.OP_RECEIPTS_ACTIVE_COST_AMT)-SUM(c.OP_RECEIPTS_RESERVE_COST_AMT)) AS OP_RCPTS_LESS_RESERVE_C
	,SUM(c.op_beginning_of_period_active_cost_amt) AS OP_BOP_Active_C -- for WFC
	,SUM(c.sp_beginning_of_period_active_cost_amt) AS SP_BOP_Active_C --for WFC
FROM(
	SELECT
	    a.snapshot_plan_month_idnt as Month_Plans,
		a.DEPT_NUM as DEPT_ID
		,a.WEEK_NUM as WEEK_ID
		,b.MONTH_IDNT
		,b.MONTH_LABEL
		,b.QUARTER_IDNT 
		,b.FISCAL_YEAR_NUM  
		,CASE
			WHEN a.BANNER_COUNTRY_NUM  = '1' THEN 'US Nordstrom'
			WHEN a.BANNER_COUNTRY_NUM = '2' THEN 'CA Nordstrom'
			WHEN a.BANNER_COUNTRY_NUM = '3' THEN 'US Rack'
			WHEN a.BANNER_COUNTRY_NUM = '4' THEN 'CA Rack'
	     ELSE 0
	     END AS BANNER
		,CASE
			 WHEN a.BANNER_COUNTRY_NUM in ('1','3') THEN 'US'
			 WHEN a.BANNER_COUNTRY_NUM in ('2','4') THEN 'CA'
		 ELSE 0
		END AS COUNTRY
		,a.SP_RECEIPTS_RESERVE_COST_AMT
		,a.SP_RECEIPTS_ACTIVE_COST_AMT
		,a.SP_RECEIPTS_INACTIVE_COST_AMT
		,a.OP_RECEIPTS_ACTIVE_COST_AMT
		,a.OP_RECEIPTS_RESERVE_COST_AMT
		,a.OP_RECEIPTS_INACTIVE_COST_AMT
		,a.op_beginning_of_period_active_cost_amt 
		,a.sp_beginning_of_period_active_cost_amt 
	FROM t2dl_das_open_to_buy.mfp_cost_plan_actual_banner_country_history a 
	JOIN hist_date b
	ON a.WEEK_NUM = b.week_idnt
	WHERE a.FULFILL_TYPE_NUM  = '3'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17)c
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
   PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_Plans)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON hist_receipt;


-------------------------------------------------------------------------------------------------------------------
 --Metric: SP_RCPTS_LESS_RESERVE_C, with current month
-------------------------------------------------------------------------------------------------------------------


--add Month_Plans to MFP cost plan actual banner country fact
create multiset volatile table month_plan_add AS
(select a.* , (select month_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM
         where CURRENT_DATE()=day_date) as Month_Plans
         from PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_BANNER_COUNTRY_FACT a)
         with data on commit preserve rows;
        

--calculate metrics for the current month       
CREATE MULTISET VOLATILE TABLE metric_cur_month AS(
SELECT
	 c.Month_Plans
	,c.DEPT_ID
	,c.BANNER
	,c.COUNTRY
	,c.MONTH_IDNT
	,c.MONTH_LABEL
	,c.QUARTER_IDNT  
	,c.FISCAL_YEAR_NUM 
	,SUM(c.SP_RECEIPTS_RESERVE_COST_AMT) AS SP_RCPTS_RESERVE_C
	,SUM(c.SP_RECEIPTS_ACTIVE_COST_AMT) AS  SP_RCPTS_ACTIVE_C
	,SUM(c.SP_RECEIPTS_INACTIVE_COST_AMT) AS SP_RCPTS_INACTIVE_C
	,SUM(c.OP_RECEIPTS_ACTIVE_COST_AMT) AS OP_RCPTS_ACTIVE_C
	,SUM(c.OP_RECEIPTS_INACTIVE_COST_AMT) AS OP_RCPTS_INACTIVE_C
	,SUM(c.OP_RECEIPTS_RESERVE_COST_AMT) AS OP_RCPTS_RESERVE_C
	,(SUM(c.SP_RECEIPTS_ACTIVE_COST_AMT)-SUM(c.SP_RECEIPTS_RESERVE_COST_AMT)) AS SP_RCPTS_LESS_RESERVE_C
	,(SUM(c.OP_RECEIPTS_ACTIVE_COST_AMT)-SUM(c.OP_RECEIPTS_RESERVE_COST_AMT)) AS OP_RCPTS_LESS_RESERVE_C
	,SUM(c.op_beginning_of_period_active_cost_amt) AS OP_BOP_Active_C -- for WFC
	,SUM(c.sp_beginning_of_period_active_cost_amt) AS SP_BOP_Active_C --for WFC
FROM(
	  SELECT
	     a.Month_Plans
		,a.DEPT_NUM as DEPT_ID
		,a.WEEK_NUM as WEEK_ID
		,b.MONTH_IDNT
		,b.MONTH_LABEL
		,b.QUARTER_IDNT 
		,b.FISCAL_YEAR_NUM  
		,CASE
			WHEN a.BANNER_COUNTRY_NUM  = '1' THEN 'US Nordstrom'
			WHEN a.BANNER_COUNTRY_NUM = '2' THEN 'CA Nordstrom'
			WHEN a.BANNER_COUNTRY_NUM = '3' THEN 'US Rack'
			WHEN a.BANNER_COUNTRY_NUM = '4' THEN 'CA Rack'
	     ELSE 0
	     END AS BANNER
		,CASE
			 WHEN a.BANNER_COUNTRY_NUM in ('1','3') THEN 'US'
			 WHEN a.BANNER_COUNTRY_NUM in ('2','4') THEN 'CA'
		 ELSE 0
		END AS COUNTRY
		,a.SP_RECEIPTS_RESERVE_COST_AMT
		,a.SP_RECEIPTS_ACTIVE_COST_AMT
		,a.SP_RECEIPTS_INACTIVE_COST_AMT
		,a.OP_RECEIPTS_ACTIVE_COST_AMT
		,a.OP_RECEIPTS_RESERVE_COST_AMT
		,a.OP_RECEIPTS_INACTIVE_COST_AMT
		,a.op_beginning_of_period_active_cost_amt 
		,a.sp_beginning_of_period_active_cost_amt 
	FROM month_plan_add a 
	JOIN hist_date b
	ON a.WEEK_NUM = b.week_idnt
	WHERE a.FULFILL_TYPE_NUM  = '3'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17)c
GROUP BY 1,2,3,4,5,6,7,8
)
WITH DATA
   PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
   ON COMMIT PRESERVE ROWS; 
  

-- combine data from history and current tables
 create multiset volatile table hist_cur_comb AS(
 select * -- get the current month only
 from metric_cur_month 
 where Month_Plans=month_idnt
 UNION ALL 
 select *  -- get the rest of the data
 from hist_receipt  
 where 
 Month_plans not in (select month_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM
where CURRENT_DATE()=day_date) or (Month_plans <> month_idnt)) 
WITH DATA PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
ON COMMIT PRESERVE ROWS; 
  

-------------------------------------------------------------------------------------------------------------------
--Metric: SP_net_Sales_C, LY_net_Sales_C
-------------------------------------------------------------------------------------------------------------------
    
--drop table hist_sale;
CREATE MULTISET VOLATILE TABLE hist_sale AS(
SELECT	
     d.Month_Plans
	,d.DEPT_ID
	,d.BANNER
	,d.COUNTRY
	,d.MONTH_IDNT
	,d.MONTH_LABEL
	,d.QUARTER_IDNT 
	,d.FISCAL_YEAR_NUM 
	,SUM(d.SP_NET_SALES_COST_AMT) AS SP_net_Sales_C
	,SUM(d.LY_NET_SALES_COST_AMT) AS LY_net_Sales_C
	,SUM(d.OP_NET_SALES_COST_AMT) AS OP_net_Sales_C --for OP WFC 
	,SUM(d.TY_NET_SALES_COST_AMT) AS TY_net_Sales_C
 FROM 
  (SELECT 
    a.snapshot_plan_month_idnt AS Month_Plans
   ,a.Dept_num AS DEPT_ID
   ,a.WEEK_NUM AS WEEK_ID
   ,b.MONTH_IDNT
   ,b.MONTH_LABEL
   ,b.QUARTER_IDNT 
   ,b.FISCAL_YEAR_NUM
   ,c.BANNER
   ,c.CHANNEL_COUNTRY AS COUNTRY
   ,c.CHANNEL
   ,c.CHANNEL_NUM
   ,a.SP_NET_SALES_COST_AMT 
   ,a.LY_NET_SALES_COST_AMT
   ,a.OP_NET_SALES_COST_AMT
   ,a.TY_NET_SALES_COST_AMT
    
    FROM t2dl_das_open_to_buy.MFP_COST_PLAN_ACTUAL_CHANNEL_history a
    JOIN hist_date b
	ON a.WEEK_NUM = b.week_idnt
	JOIN Hist_loc_base c
	ON c.CHANNEL_NUM=a.CHANNEL_NUM 
	WHERE a.FULFILL_TYPE_NUM  = '3'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) d 
  group by 1,2,3,4,5,6,7,8) WITH DATA
   PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
   ON COMMIT PRESERVE ROWS;
  
  COLLECT STATS
     PRIMARY INDEX (Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_Plans)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON hist_sale;
    
--add current month to the base table    
 create multiset volatile table cur_mnth_add AS
(select a.* , (select month_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM
  where CURRENT_DATE()=day_date) as Month_Plans
  from PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_CHANNEL_FACT  a)
  with data on commit preserve rows;
 
-- Get sales metrics for the current month 
CREATE MULTISET VOLATILE TABLE cur_month_sale AS(
SELECT
	
     d.Month_Plans
	,d.DEPT_ID
	,d.BANNER
	,d.COUNTRY
	,d.MONTH_IDNT
	,d.MONTH_LABEL
	,d.QUARTER_IDNT 
	,d.FISCAL_YEAR_NUM 
	,SUM(d.SP_NET_SALES_COST_AMT) AS SP_net_Sales_C
	,SUM(d.LY_NET_SALES_COST_AMT) AS LY_net_Sales_C
	,SUM(d.OP_NET_SALES_COST_AMT) AS OP_net_Sales_C --for OP WFC 
	,SUM(d.TY_NET_SALES_COST_AMT) AS TY_net_Sales_C
 FROM 
  (SELECT 
    Month_Plans
   ,a.Dept_num AS DEPT_ID
   ,a.WEEK_NUM AS WEEK_ID
   ,b.MONTH_IDNT
   ,b.MONTH_LABEL
   ,b.QUARTER_IDNT 
   ,b.FISCAL_YEAR_NUM
   ,c.BANNER
   ,c.CHANNEL_COUNTRY AS COUNTRY
   ,c.CHANNEL
   ,c.CHANNEL_NUM
   ,a.SP_NET_SALES_COST_AMT 
   ,a.LY_NET_SALES_COST_AMT
   ,a.OP_NET_SALES_COST_AMT
   ,a.TY_NET_SALES_COST_AMT
    
    FROM cur_mnth_add a
    JOIN hist_date b
	ON a.WEEK_NUM = b.week_idnt
	JOIN Hist_loc_base c
	ON c.CHANNEL_NUM=a.CHANNEL_NUM 
	WHERE a.FULFILL_TYPE_NUM  = '3'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) d 
  group by 1,2,3,4,5,6,7,8) WITH DATA
   PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
   ON COMMIT PRESERVE ROWS;
  
  COLLECT STATS
     PRIMARY INDEX (Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_Plans)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON cur_month_sale;
 
 -- combine current and history data
 create multiset volatile table com_cur_hist_sale AS(
 select * -- get the current month only
 from cur_month_sale
 where Month_Plans=month_idnt
 UNION ALL 
 select *  -- get the rest of the data
 from hist_sale  
 where 
 Month_plans not in (select month_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM
where CURRENT_DATE()=day_date) or (Month_plans <> month_idnt)) 
WITH DATA PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
ON COMMIT PRESERVE ROWS;



    
-------------------------------------------------------------------------------------------------------------------
 --RP_ANTICIPATED_SPEND_C  
------------------------------------------------------------------------------------------------------------------- 
---from issac report for  Rp Anticipated Spend C, changed current to historical in "t2dl_das_open_to_buy.rp_anticipated_spend_current"
---I consider rcd_load_date column as the snapped day, extract the most recent record load date for each month 
    
CREATE MULTISET VOLATILE TABLE hist_rp_ant_spnd AS(
	SELECT
	
	c.DEPT_ID
	,c.MONTH_IDNT
	,c.MONTH_LABEL
	,c.QUARTER_IDNT 
	,c.FISCAL_YEAR_NUM
	,c.BANNER
	,c.COUNTRY
	,c.month_plan AS Month_Plans
	,SUM(c.RP_ANTSPND_C) AS RP_ANTICIPATED_SPEND_C
	,SUM(c.RP_ANTSPND_U) AS RP_ANTICIPATED_SPEND_U
FROM(
   SELECT
	a.WEEK_IDNT AS WEEK_ID
	,b.MONTH_IDNT
	,b.MONTH_LABEL
	,b.QUARTER_IDNT 
	,b.FISCAL_YEAR_NUM
	,a.DEPT_IDNT AS DEPT_ID
	,CASE
	WHEN a.BANNER_ID = '1' THEN 'US Nordstrom'
	WHEN a.BANNER_ID = '2' THEN 'CA Nordstrom'
	WHEN a.BANNER_ID = '3' THEN 'US Rack'
	WHEN a.BANNER_ID = '4' THEN 'CA Rack'
	ELSE 0
	END AS BANNER
	,CASE
	WHEN a.BANNER_ID in ('1','3') THEN 'US'
	WHEN a.BANNER_ID in ('2','4') THEN 'CA'
	ELSE 0
	END AS COUNTRY
	,CATEGORY
	,SUPP_IDNT
	,RP_ANTSPND_U
	,RP_ANTSPND_C
	,d.month_idnt month_plan
FROM t2dl_das_open_to_buy.rp_anticipated_spend_hist a
JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM d
ON a.rcd_load_date = d.day_date
JOIN hist_date b
ON a.WEEK_IDNT = b.WEEK_IDNT
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)c
GROUP BY 1,2,3,4,5,6,7,8)
WITH DATA PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)ON COMMIT PRESERVE ROWS;    

COLLECT STATS
     PRIMARY INDEX (Month_plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_plans)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON hist_rp_ant_spnd;  

-------------------------------------------------------------------------------------------------------------------
--committed
-------------------------------------------------------------------------------------------------------------------    
       	
--Create a month mapping table
	
CREATE MULTISET VOLATILE TABLE hist_m_date 
AS(
SELECT	
	 MONTH_IDNT
	,MONTH_LABEL
	,QUARTER_IDNT 
	,QUARTER_LABEL 
	,'FY'||substr(cast(fiscal_year_num as varchar(5)),3,2)||' '||quarter_abrv as QUARTER
	,FISCAL_YEAR_NUM 
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE fiscal_year_num >= 2022
GROUP BY 1,2,3,4,5,6)

WITH DATA
   PRIMARY INDEX(MONTH_IDNT)
   ON COMMIT PRESERVE ROWS;
  

COLLECT STATS
     PRIMARY INDEX (MONTH_IDNT)
     ,COLUMN(MONTH_IDNT)
     ,COLUMN(QUARTER_IDNT)
     ,COLUMN(FISCAL_YEAR_NUM)
     ON hist_m_date;

--Get Monthly Committed_C based on the data entered at last day of Month_Plans
    
    
CREATE MULTISET VOLATILE TABLE month_To_max_dt
 AS (
SELECT
	d.month_idnt,
	max(process_dt) load_date
from
	t2dl_das_open_to_buy.AB_CM_ORDERS_historical a
JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM d
  ON
	a.process_dt = d.day_date
GROUP BY
	1) WITH DATA ON COMMIT PRESERVE ROWS;

--drop table hist_cm_F_1;
CREATE MULTISET VOLATILE TABLE hist_cm_F_1
AS  
( SELECT
    c.Month_Plans
   ,c.dept_id
   ,c.Banner
   ,c.CHANNEL_COUNTRY AS Country
   ,c.fiscal_month_id 
   ,c.month_label 
   ,c.QUARTER
   ,c.Fiscal_year_num
   ,c.Quarter_IDNT
   ,SUM(CASE 
		WHEN c.PLAN_TYPE in ('RKPACKHOLD') AND c.BANNER in ('US NORDSTROM','US RACK') THEN c.TTL_COST_US
		WHEN c.PLAN_TYPE in ('RKPACKHOLD') AND c.BANNER in ('CA NORDSTROM','CA RACK') THEN c.TTL_COST_CA  
		ELSE 0 END) AS INACTIVE_CM_C
	,SUM(CASE 
		WHEN c.PLAN_TYPE in ('BACKUP','FASHION','NRPREORDER','REPLNSHMNT') AND c.BANNER in ('US NORDSTROM','US RACK') THEN c.TTL_COST_US
		WHEN c.PLAN_TYPE in ('BACKUP','FASHION','NRPREORDER','REPLNSHMNT') AND c.BANNER in ('CA NORDSTROM','CA RACK') THEN c.TTL_COST_CA  
		ELSE 0 END) AS ACTIVE_CM_C
	,SUM(CASE 
			WHEN c.PLAN_TYPE in ('BACKUP','FASHION','NRPREORDER') AND c.BANNER in ('US NORDSTROM','US RACK') THEN c.TTL_COST_US
			WHEN c.PLAN_TYPE in ('BACKUP','FASHION','NRPREORDER') AND c.BANNER in ('CA NORDSTROM','CA RACK') THEN c.TTL_COST_CA  
			ELSE 0 END) AS NRP_CM_C
	,SUM(CASE 
			WHEN c.PLAN_TYPE in ('REPLNSHMNT') AND c.BANNER in ('US NORDSTROM','US RACK') THEN c.TTL_COST_US
			WHEN c.PLAN_TYPE in ('REPLNSHMNT') AND c.BANNER in ('CA NORDSTROM','CA RACK') THEN c.TTL_COST_CA  
			ELSE 0 END) AS RP_CM_C
FROM
   (
	SELECT
		 a.fiscal_month_id 
		,a.dept_id
		,b.BANNER
		,b.CHANNEL_COUNTRY
		,a.class_name 
		,a.subclass_name 
		,a.supp_id 
		,a.OTB_EOW_DATE
		,a.vpn 
		,a.SUPP_COLOR
		,a.PLAN_TYPE
		,a.PO_KEY
		,a.plan_key 
		,a.NRF_COLOR_CODE
		,a.STYLE_ID
		,a.STYLE_GROUP_ID
		,a.TTL_COST_US
		,a.TTL_COST_CA
		,a.CM_DOLLARS
		,a.process_dt
		,d.month_idnt AS Month_Plans
		,e.month_label 
   		,e.QUARTER
  		,e.Fiscal_year_num
   		,e.Quarter_IDNT
	FROM t2dl_das_open_to_buy.AB_CM_ORDERS_historical a
	JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM d
	ON a.process_dt = d.day_date
	JOIN hist_loc_base b --to bring channel_country and banner 
	ON a.ORG_ID = b.CHANNEL_NUM
    JOIN hist_m_date e
    ON a.fiscal_month_id=e.month_idnt 
    WHERE a.process_dt in (select load_date from month_To_max_dt)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25) c 
    GROUP BY 1,2,3,4,5,6,7,8,9)
    WITH DATA PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
    ON COMMIT PRESERVE ROWS;
   
  COLLECT STATS
     PRIMARY INDEX (Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_Plans)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON hist_cm_F_1;
 
 -------------------------------------------------------------------------------------------------------------------
--OO
-------------------------------------------------------------------------------------------------------------------  
--fiscal month id it seems to be months planned for
  --- from issac OTB for OO 
  --lets get the  OO info AND CHANNEL INFO, this should be snapped PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW
--PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW has the most current OO data and is a truncate and load daily
  
 --lets create a store mapping for country and banner
CREATE MULTISET VOLATILE TABLE hist_loc_base_oo AS(
	SELECT
	a.STORE_NUM
	,a.CHANNEL_NUM
	,a.CHANNEL_DESC
	,a.CHANNEL_BRAND
	,a.CHANNEL_COUNTRY
	,a.CHANNEL_NUM||','||a.CHANNEL_DESC as CHANNEL
	,CASE
		WHEN a.CHANNEL_BRAND = 'Nordstrom' AND a.CHANNEL_COUNTRY = 'US' THEN 'US Nordstrom'
		WHEN a.CHANNEL_BRAND = 'Nordstrom' AND a.CHANNEL_COUNTRY = 'CA' THEN 'CA Nordstrom'
		WHEN a.CHANNEL_BRAND = 'Nordstrom_Rack' AND a.CHANNEL_COUNTRY = 'US' THEN 'US Rack'
		WHEN a.CHANNEL_BRAND = 'Nordstrom_Rack' AND a.CHANNEL_COUNTRY = 'CA' THEN 'CA Rack'
	END AS BANNER
	FROM (
		SELECT
			STORE_NUM
			,CHANNEL_NUM
			,CHANNEL_DESC
			,CHANNEL_BRAND
			,CHANNEL_COUNTRY
		FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
		GROUP BY 1,2,3,4,5
		WHERE CHANNEL_BRAND in ('Nordstrom','Nordstrom_Rack')
		AND CHANNEL_NUM IN ('110', '120', '140', '210', '220', '240', '250', '260', '310', '930', '940', '990', '111', '121', '211', '221', '261', '311'))a
GROUP BY 1,2,3,4,5,6)
WITH DATA
   PRIMARY INDEX(STORE_NUM)
   ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (STORE_NUM)
     ,COLUMN(STORE_NUM)
     ON hist_loc_base_oo;
    
    
--drop table hist_oo_base
CREATE MULTISET VOLATILE TABLE hist_oo_base AS(

	SELECT
	
		a.snapshot_plan_month_idnt as Month_Plans
		,a.PURCHASE_ORDER_NUMBER 
		,a.RMS_SKU_NUM
		,a.RMS_CASEPACK_NUM
		,b.CHANNEL_NUM
		,b.CHANNEL_DESC
		,b.CHANNEL_COUNTRY
		,b.BANNER
		,a.STORE_NUM
		,a.WEEK_NUM
		,a.ORDER_TYPE
		,a.LATEST_APPROVAL_DATE
		,a.QUANTITY_OPEN
		,a.TOTAL_ESTIMATED_LANDING_COST
	FROM  t2dl_das_open_to_buy.merch_on_order_hist_plans a
	JOIN hist_loc_base_oo b --to get banner and channel country info
	ON a.STORE_NUM = b.STORE_NUM
	WHERE  a.QUANTITY_OPEN > 0
	   AND a.FIRST_APPROVAL_DATE IS NOT NULL
	   AND ((a.STATUS = 'CLOSED' AND a.END_SHIP_DATE >= CURRENT_DATE - 45) OR a.STATUS IN ('APPROVED','WORKSHEET') )	   
	   )
WITH DATA
PRIMARY INDEX(RMS_SKU_NUM,STORE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (RMS_SKU_NUM,STORE_NUM)
     ,COLUMN(RMS_SKU_NUM)
     ,COLUMN(STORE_NUM)
     ON hist_oo_base;
    

--Lets gets associated hierarchy to skus and find dept_id
--drop table otb_oo_hierarchy
CREATE MULTISET VOLATILE TABLE hist_oo_hierarchy AS(
SELECT
	b.RMS_SKU_NUM
	,b.channel_country
	,a.DEPT_NUM
	,a.DEPT_DESC
FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW a
JOIN (SELECT DISTINCT RMS_SKU_NUM,channel_country FROM hist_oo_base) b
ON a.RMS_SKU_NUM = b.RMS_SKU_NUM
AND a.channel_country = b.channel_country
GROUP BY 1,2,3,4)
  WITH DATA
  PRIMARY INDEX(RMS_SKU_NUM)
  ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (RMS_SKU_NUM)
     ,COLUMN(RMS_SKU_NUM)
     ON hist_oo_hierarchy;
 



--drop table hist_oo_final
--lets add back in the hierarchy to the base data and sum up inactive/active rcpts as well as rp nrp
--Inactive channels 930,220 and 221
--drop table hist_oo_final;
CREATE MULTISET VOLATILE TABLE hist_oo_final AS(
SELECT 
	 e.Month_Plans
	,e.DEPT_NUM as Dept_ID
	,e.BANNER
	,e.CHANNEL_COUNTRY as COUNTRY
	,e.MONTH_IDNT as FISCAL_MONTH_ID
	,e.MONTH_LABEL
	,e.QUARTER_IDNT 
	,e.FISCAL_YEAR_NUM
	,SUM(CASE WHEN e.CHANNEL_NUM IN ('930','220','221') THEN e.TOTAL_ESTIMATED_LANDING_COST
	     ELSE 0 END) AS RCPTS_OO_INACTIVE_C
	,SUM(CASE WHEN e.CHANNEL_NUM NOT IN ('930','220','221') AND e.ORDER_TYPE IN('AUTOMATIC_REORDER','BUYER_REORDER') THEN e.TOTAL_ESTIMATED_LANDING_COST
	     ELSE 0 END) AS RCPTS_OO_ACTIVE_RP_C
	,SUM(CASE WHEN e.CHANNEL_NUM NOT IN ('930','220','221') AND e.ORDER_TYPE NOT IN('AUTOMATIC_REORDER','BUYER_REORDER') THEN e.TOTAL_ESTIMATED_LANDING_COST
	     ELSE 0 END) AS RCPTS_OO_ACTIVE_NRP_C
FROM(
    SELECT
    	a.Month_plans
		,a.PURCHASE_ORDER_NUMBER
    	,a.RMS_SKU_NUM
    	,a.RMS_CASEPACK_NUM
		,b.DEPT_NUM
		,b.DEPT_DESC
		,a.CHANNEL_NUM
		,a.CHANNEL_DESC
		,a.STORE_NUM
		,a.BANNER
		,a.CHANNEL_COUNTRY
		,a.WEEK_NUM
		,d.MONTH_IDNT
		,d.MONTH_LABEL
		,d.QUARTER_IDNT  
		,d.FISCAL_YEAR_NUM
		,a.ORDER_TYPE
		,a.LATEST_APPROVAL_DATE
		,a.QUANTITY_OPEN
		,a.TOTAL_ESTIMATED_LANDING_COST
	FROM hist_oo_base a
	JOIN hist_oo_hierarchy b
	ON a.RMS_SKU_NUM = b.RMS_SKU_NUM
    AND a.channel_country = b.channel_country
	JOIN hist_date d
	ON a.WEEK_NUM = d.WEEK_IDNT
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20) e
GROUP BY 1,2,3,4,5,6,7,8)

 WITH DATA
  PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
  ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON hist_oo_final;
    

-------------------------------------------------------------------------------------------------------------------
-- bring RP_RCPT_Plan metric
-------------------------------------------------------------------------------------------------------------------  
 
 CREATE MULTISET VOLATILE TABLE RP_RCPT_Plan AS(
 SELECT 
     e. Month_Plans
	,e.DEPT_NUM
	,e.BANNER
	,e.COUNTRY
	,e.month_idnt
	,e.month_label
	,e.QUARTER_idnt
	,e.FISCAL_YEAR_NUM
	,SUM(replenishment_receipts_less_reserve_cost_amount) AS Rp_Rcpt_Plan_C 
FROM (SELECT 
	  a.snapshot_plan_month_idnt AS Month_Plans
	 ,a.selling_country AS COUNTRY 
	 ,b.month_idnt
	 ,b.month_label
	 ,b.quarter_idnt  
	 ,b.FISCAL_YEAR_NUM
	 ,CASE
		WHEN a.selling_brand = 'Nordstrom' AND a.selling_country = 'US' THEN 'US Nordstrom'
		WHEN a.selling_brand = 'Nordstrom' AND a.selling_country = 'CA' THEN 'CA Nordstrom'
		WHEN a.selling_brand = 'Nordstrom_Rack' AND a.selling_country = 'US' THEN 'US Rack'
		WHEN a.selling_brand = 'Nordstrom_Rack' AND a.selling_country = 'CA' THEN 'CA Rack'
	END AS BANNER
	,dept_idnt AS dept_num
	,selling_brand
	,supplier_group
	,category 
	,alternate_inventory_model
	,replenishment_receipts_less_reserve_cost_amount
FROM t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_history a
JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM b
ON a.fiscal_month_idnt=b.month_idnt 
WHERE a.alternate_inventory_model='OWN' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13) e 
GROUP BY 1,2,3,4,5,6,7,8)
WITH DATA
  PRIMARY INDEX(Month_Plans,DEPT_NUM,BANNER,COUNTRY,month_label)
  ON COMMIT PRESERVE ROWS;
 
COLLECT STATS
     PRIMARY INDEX(Month_Plans,DEPT_NUM,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_Plans)
     ,COLUMN(DEPT_NUM)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON RP_RCPT_Plan;
    
-------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------  
--drop table planned_hist_report_mfp;
CREATE MULTISET VOLATILE TABLE planned_hist_report_mfp AS(
SELECT
	 a.Month_Plans
	,a.DEPT_ID 
	,a.BANNER
	,a.COUNTRY
	,a.MONTH_IDNT
	,a.MONTH_LABEL 
	,a.QUARTER_IDNT
	,a.FISCAL_YEAR_NUM 
	,SUM(a.SP_RCPTS_RESERVE_C) AS SP_RCPTS_RESERVE_C 
	,SUM(a.SP_RCPTS_ACTIVE_C) AS SP_RCPTS_ACTIVE_C   
	,SUM(a.SP_RCPTS_ACTIVE_C-SP_RCPTS_RESERVE_C) AS Sp_Rcpts_Less_Reserve_C
	,SUM(a.OP_RCPTS_RESERVE_C) AS OP_RCPTS_RESERVE_C 
	,SUM(a.OP_RCPTS_ACTIVE_C) AS OP_RCPTS_ACTIVE_C   
	,SUM(a.OP_RCPTS_ACTIVE_C-OP_RCPTS_RESERVE_C) AS Op_Rcpts_Less_Reserve_C
	,SUM(a.OP_BOP_Active_C) AS  OP_BOP_Active_C 
	,SUM(a.SP_BOP_Active_C) AS  SP_BOP_Active_C 
	,SUM(a.SP_net_Sales_C) AS  SP_net_Sales_C 
	,SUM(a.LY_net_Sales_C) AS  LY_net_Sales_C 
	,SUM(a.OP_net_Sales_C) AS  OP_net_Sales_C 
	,SUM(a.TY_net_Sales_C) AS TY_net_Sales_C
	,(SUM(a.SP_BOP_Active_C)/NULLIF(SUM(a.SP_net_Sales_C),0)) AS SP_WFC_Active_C
	,(SUM(a.OP_BOP_Active_C)/NULLIF(SUM(a.OP_net_Sales_C),0)) AS OP_WFC_Active_C
	,SUM(a.RP_ANTICIPATED_SPEND_C) AS  RP_ANTICIPATED_SPEND_C 
	,SUM(a.RP_ANTICIPATED_SPEND_U) AS  RP_ANTICIPATED_SPEND_U
	,SUM(a.INACTIVE_CM_C) AS INACTIVE_CM_C
	,SUM(a.ACTIVE_CM_C) AS ACTIVE_CM_C
	,SUM(a.NRP_CM_C) AS NRP_CM_C
	,SUM(a.RP_CM_C) AS RP_CM_C
	,SUM(a.INACTIVE_CM_C+a.ACTIVE_CM_C) AS CM_C
	,SUM(a.RCPTS_OO_INACTIVE_C) AS RCPTS_OO_INACTIVE_C
	,SUM(a.RCPTS_OO_ACTIVE_RP_C) AS RCPTS_OO_ACTIVE_RP_C
	,SUM(a.RCPTS_OO_ACTIVE_NRP_C) AS RCPTS_OO_ACTIVE_NRP_C
	,SUM(a.Rp_Rcpt_Plan_C) AS Rp_Rcpt_Plan_C 
	,SUM(a.RCPTS_OO_ACTIVE_RP_C + a.RP_CM_C) AS RP_OO_and_CM_C
	,SUM(a.RCPTS_OO_ACTIVE_NRP_C+ a.NRP_CM_C) AS NRP_OO_and_CM_C
	,CURRENT_TIMESTAMP AS rcd_update_timestamp

FROM(

	SELECT
		 CAST(Month_Plans AS varchar(10)) AS Month_Plans
		,CAST(DEPT_ID AS int) AS DEPT_ID
		,CAST(BANNER AS varchar(20)) AS BANNER
		,CAST(COUNTRY AS varchar (5)) AS COUNTRY
		,CAST(QUARTER_IDNT AS varchar(10)) AS QUARTER_IDNT
		,CAST(FISCAL_YEAR_NUM AS varchar(10)) AS FISCAL_YEAR_NUM 
		,CAST(MONTH_IDNT AS varchar(10)) AS MONTH_IDNT 
		,CAST(MONTH_LABEL AS varchar(10)) AS MONTH_LABEL
		,CAST(SP_RCPTS_RESERVE_C AS decimal (20,4)) AS SP_RCPTS_RESERVE_C
		,CAST(SP_RCPTS_ACTIVE_C AS decimal (20,4)) AS  SP_RCPTS_ACTIVE_C
		,CAST(SP_RCPTS_INACTIVE_C AS decimal (20,4)) AS SP_RCPTS_INACTIVE_C
		,CAST(OP_RCPTS_ACTIVE_C AS decimal (20,4)) AS  OP_RCPTS_ACTIVE_C
		,CAST(OP_RCPTS_INACTIVE_C AS decimal (20,4)) AS OP_RCPTS_INACTIVE_C
		,CAST(OP_RCPTS_RESERVE_C AS decimal (20,4)) AS OP_RCPTS_RESERVE_C
		,CAST(OP_BOP_Active_C AS decimal (20,4)) AS OP_BOP_Active_C
		,CAST(SP_BOP_Active_C AS decimal (20,4)) AS SP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_net_Sales_C
		,CAST(0 AS decimal (20,4)) AS LY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS OP_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS TY_net_Sales_C
		,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_U
	    ,CAST(0 AS decimal (20,4)) AS INACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS ACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS NRP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_INACTIVE_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_RP_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_NRP_C
	    ,CAST(0 AS decimal (20,4)) AS Rp_Rcpt_Plan_C

	FROM hist_cur_comb

	UNION ALL

	SELECT
		 CAST(Month_Plans AS varchar(10)) AS Month_Plans
		,CAST(DEPT_ID AS int) AS DEPT_ID
		,CAST(BANNER AS varchar(20)) AS BANNER
		,CAST(COUNTRY AS varchar (5)) AS COUNTRY
		,CAST(QUARTER_IDNT AS varchar(10)) AS QUARTER_IDNT
		,CAST(FISCAL_YEAR_NUM AS varchar(10)) AS FISCAL_YEAR_NUM 
		,CAST(MONTH_IDNT AS varchar(10)) AS MONTH_IDNT 
		,CAST(MONTH_LABEL AS varchar(10)) AS MONTH_LABEL
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS OP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_BOP_Active_C
		,CAST(SP_net_Sales_C AS decimal (20,4)) AS SP_net_Sales_C
		,CAST(LY_net_Sales_C AS decimal (20,4)) AS LY_net_Sales_C
	    ,CAST(OP_net_Sales_C AS decimal (20,4)) AS OP_net_Sales_C
	    ,CAST(TY_net_Sales_C AS decimal (20,4)) AS TY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_U
	    ,CAST(0 AS decimal (20,4)) AS INACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS ACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS NRP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_INACTIVE_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_RP_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_NRP_C
	    ,CAST(0 AS decimal (20,4)) AS Rp_Rcpt_Plan_C

	FROM com_cur_hist_sale

	UNION ALL

	SELECT
		CAST(Month_Plans AS varchar(10)) AS Month_Plans
		,CAST(DEPT_ID AS int) AS DEPT_ID
		,CAST(BANNER AS varchar(20)) AS BANNER
		,CAST(COUNTRY AS varchar (5)) AS COUNTRY
		,CAST(QUARTER_IDNT AS varchar(10)) AS QUARTER_IDNT
		,CAST(FISCAL_YEAR_NUM AS varchar(10)) AS FISCAL_YEAR_NUM 
		,CAST(MONTH_IDNT AS varchar(10)) AS MONTH_IDNT 
		,CAST(MONTH_LABEL AS varchar(10)) AS MONTH_LABEL
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS OP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_net_Sales_C
		,CAST(0 AS decimal (20,4)) AS LY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS OP_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS TY_net_Sales_C
	    ,CAST(RP_ANTICIPATED_SPEND_C AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_C
	    ,CAST(RP_ANTICIPATED_SPEND_U AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_U
	    ,CAST(0 AS decimal (20,4)) AS INACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS ACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS NRP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_INACTIVE_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_RP_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_NRP_C
	    ,CAST(0 AS decimal (20,4)) AS Rp_Rcpt_Plan_C

	FROM hist_rp_ant_spnd

	UNION ALL

	SELECT
		CAST(Month_Plans AS varchar(10)) AS Month_Plans
		,CAST(DEPT_ID AS int) AS DEPT_ID
		,CAST(BANNER AS varchar(20)) AS BANNER
		,CAST(COUNTRY AS varchar (5)) AS COUNTRY
		,CAST(QUARTER_IDNT AS varchar(10)) AS QUARTER_IDNT
		,CAST(FISCAL_YEAR_NUM AS varchar(10)) AS FISCAL_YEAR_NUM 
		,CAST(FISCAL_MONTH_ID AS varchar(10)) AS MONTH_IDNT 
		,CAST(MONTH_LABEL AS varchar(10)) AS MONTH_LABEL
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS  SP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS OP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_net_Sales_C
		,CAST(0 AS decimal (20,4)) AS LY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS OP_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS TY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_U
	    ,CAST(INACTIVE_CM_C AS decimal (20,4)) AS INACTIVE_CM_C
	    ,CAST(ACTIVE_CM_C AS decimal (20,4)) AS ACTIVE_CM_C
	    ,CAST(RP_CM_C AS decimal (20,4)) AS RP_CM_C
	    ,CAST(NRP_CM_C AS decimal (20,4)) AS NRP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_INACTIVE_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_RP_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_NRP_C
	    ,CAST(0 AS decimal (20,4)) AS Rp_Rcpt_Plan_C
	FROM  hist_cm_F_1
 

	UNION ALL

	SELECT
		CAST(Month_Plans AS varchar(10)) AS Month_Plans
		,CAST(DEPT_ID AS int) AS DEPT_ID
		,CAST(BANNER AS varchar(20)) AS BANNER
		,CAST(COUNTRY AS varchar (5)) AS COUNTRY
		,CAST(QUARTER_IDNT AS varchar(10)) AS QUARTER_IDNT
		,CAST(FISCAL_YEAR_NUM AS varchar(10)) AS FISCAL_YEAR_NUM 
		,CAST(FISCAL_MONTH_ID AS varchar(10)) AS MONTH_IDNT 
		,CAST(MONTH_LABEL AS varchar(10)) AS MONTH_LABEL
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS OP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_net_Sales_C
		,CAST(0 AS decimal (20,4)) AS LY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS OP_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS TY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_U
	    ,CAST(0 AS decimal (20,4)) AS INACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS ACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS NRP_CM_C
	    ,CAST(RCPTS_OO_INACTIVE_C AS decimal (20,4)) AS RCPTS_OO_INACTIVE_C
	    ,CAST(RCPTS_OO_ACTIVE_RP_C AS decimal (20,4)) AS RCPTS_OO_ACTIVE_RP_C
	    ,CAST(RCPTS_OO_ACTIVE_NRP_C AS decimal (20,4)) AS RCPTS_OO_ACTIVE_NRP_C
	    ,CAST(0 AS decimal (20,4)) AS Rp_Rcpt_Plan_C
		FROM  hist_oo_final
		
	
		
	UNION ALL

	SELECT
		CAST(Month_Plans AS varchar(10)) AS Month_Plans
		,CAST(DEPT_num AS int) AS DEPT_ID
		,CAST(BANNER AS varchar(20)) AS BANNER
		,CAST(COUNTRY AS varchar (5)) AS COUNTRY
		,CAST(QUARTER_idnt AS varchar(10)) AS QUARTER_IDNT
		,CAST(FISCAL_YEAR_NUM AS varchar(10)) AS FISCAL_YEAR_NUM 
		,CAST(MONTH_IDNT AS varchar(10)) AS MONTH_IDNT 
		,CAST(MONTH_LABEL AS varchar(10)) AS MONTH_LABEL
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS SP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_ACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_INACTIVE_C
		,CAST(0 AS decimal (20,4)) AS OP_RCPTS_RESERVE_C
		,CAST(0 AS decimal (20,4)) AS OP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_BOP_Active_C
		,CAST(0 AS decimal (20,4)) AS SP_net_Sales_C
		,CAST(0 AS decimal (20,4)) AS LY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS OP_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS TY_net_Sales_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_C
	    ,CAST(0 AS decimal (20,4)) AS RP_ANTICIPATED_SPEND_U
	    ,CAST(0 AS decimal (20,4)) AS INACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS ACTIVE_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS NRP_CM_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_INACTIVE_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_RP_C
	    ,CAST(0 AS decimal (20,4)) AS RCPTS_OO_ACTIVE_NRP_C
	    ,CAST(Rp_Rcpt_Plan_C AS decimal (20,4)) AS Rp_Rcpt_Plan_C 
		FROM  RP_RCPT_Plan
		
		)a
GROUP BY 1,2,3,4,5,6,7,8)

WITH DATA
  PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_Plans)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON planned_hist_report_mfp;
    
    
--bring total metric, the new total definition was added to the dashboard no need for below total

    
-- CREATE MULTISET VOLATILE TABLE planned_hist_report_mfp_1
-- AS (
--    SELECT a.*, (CASE WHEN RP_OO_and_CM_C > RP_ANTICIPATED_SPEND_C
--	     		  	 THEN RP_OO_and_CM_C+ NRP_OO_and_CM_C
--			       WHEN RP_OO_and_CM_C < RP_ANTICIPATED_SPEND_C
--	     			 THEN RP_ANTICIPATED_SPEND_C + NRP_OO_and_CM_C
--				   ELSE 0 END) AS Ttl_Non_Rp_OO_CM_Rp_Spent_C
--    
--    FROM planned_hist_report_mfp a) 
--    WITH DATA PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
--ON COMMIT PRESERVE ROWS;
--    
--COLLECT STATS
--     PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
--     ,COLUMN(Month_Plans)
--     ,COLUMN(DEPT_ID)
--     ,COLUMN(BANNER)
--     ,COLUMN(COUNTRY)
--     ,COLUMN(MONTH_LABEL)
--     ON planned_hist_report_mfp_1;    
-----------------------
 --Bringing division, subdivision, and department information as well as format of the month-label for Month_plans
----------------------- 
    
--drop table MFP_plan_hist_report_dept_h48v;
CREATE MULTISET VOLATILE TABLE MFP_plan_hist_report_dept_h48v 
AS (
	SELECT
		 TRIM(b.DIVISION_NUM||','||b.DIVISION_NAME) AS Division
		,TRIM(b.SUBDIVISION_NUM||','||b.SUBDIVISION_NAME) AS Subdivision
		,TRIM(a.DEPT_ID||','||b.DEPT_NAME) AS Department		
		,b.ACTIVE_STORE_IND AS "Active vs Inactive"
		,c.month_label AS snap_month_label  
		,a.*	
	FROM planned_hist_report_mfp a
	JOIN prd_nap_usr_vws.department_dim b
	ON a.DEPT_ID = b.DEPT_NUM
	JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM c
	ON c.month_idnt=a.Month_Plans
    WHERE 
    b.ACTIVE_STORE_IND = 'A'
    AND a.month_idnt>= a.Month_Plans
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41)
WITH DATA PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_Plans)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON MFP_plan_hist_report_dept_h48v;
    


--eliminate rows where month_idnt=month_Plans for previous months, so it can be shown only for the current month 
  CREATE MULTISET VOLATILE TABLE MFP_plan_hist_report_dept_h48v_m
  AS(
    SELECT 
    a.*
    FROM MFP_plan_hist_report_dept_h48v a
    WHERE month_idnt > Month_Plans    
   UNION ALL    
   SELECT 
    a.*
    FROM MFP_plan_hist_report_dept_h48v a
    WHERE month_idnt=Month_Plans AND Month_Plans IN (SELECT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
    WHERE CURRENT_DATE()=day_date))WITH DATA PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
     ,COLUMN(Month_Plans)
     ,COLUMN(DEPT_ID)
     ,COLUMN(BANNER)
     ,COLUMN(COUNTRY)
     ,COLUMN(MONTH_LABEL)
     ON MFP_plan_hist_report_dept_h48v_m;
-----------------------
-----------------------    
--drop table MFP_plan_hist_report_dept_h48v_1 ;
CREATE MULTISET VOLATILE TABLE MFP_plan_hist_report_dept_h48v_1 
AS (
	SELECT
		 CAST(Division AS varchar(20)) AS Division
		,CAST(Subdivision AS varchar(20)) AS Subdivision
		,CAST(Department AS varchar(20)) AS Department
		,CAST("Active vs Inactive" AS varchar (10)) AS "Active vs Inactive"
		,CAST(snap_month_label AS varchar(10)) AS snap_month_label
		,Month_Plans
		,DEPT_ID 
	    ,BANNER
		,COUNTRY
		,MONTH_IDNT
		,MONTH_LABEL 
		,QUARTER_IDNT
		,FISCAL_YEAR_NUM 
		,SP_RCPTS_RESERVE_C 
		,SP_RCPTS_ACTIVE_C   
		,Sp_Rcpts_Less_Reserve_C
		,OP_RCPTS_RESERVE_C 
		,OP_RCPTS_ACTIVE_C   
		,OP_Rcpts_Less_Reserve_C 
		,OP_BOP_Active_C 
		,SP_BOP_Active_C  
		,SP_net_Sales_C 
		,LY_net_Sales_C 
		,OP_net_Sales_C 
		,TY_net_Sales_C 
		,SP_WFC_Active_C
		,OP_WFC_Active_C
		,RP_ANTICIPATED_SPEND_C 
		,RP_ANTICIPATED_SPEND_U
		,INACTIVE_CM_C
		,ACTIVE_CM_C
		,NRP_CM_C
		,RP_CM_C
		,CM_C
		,RCPTS_OO_INACTIVE_C
		,RCPTS_OO_ACTIVE_RP_C
		,RCPTS_OO_ACTIVE_NRP_C
		,Rp_Rcpt_Plan_C 
		,RP_OO_and_CM_C
		,NRP_OO_and_CM_C
		,rcd_update_timestamp
		FROM  MFP_plan_hist_report_dept_h48v_m
		WHERE SP_RCPTS_RESERVE_C <> 0
			OR SP_RCPTS_ACTIVE_C <> 0
			OR Sp_Rcpts_Less_Reserve_C <> 0
			OR OP_RCPTS_RESERVE_C <> 0
			OR OP_RCPTS_ACTIVE_C <> 0
			OR Op_Rcpts_Less_Reserve_C <> 0
			OR OP_BOP_Active_C <> 0
			OR SP_BOP_Active_C <> 0
			OR SP_net_Sales_C <> 0
			OR LY_net_Sales_C <> 0
			OR OP_net_Sales_C <> 0
			OR TY_net_Sales_C <> 0
			OR SP_WFC_Active_C <> 0
			OR OP_WFC_Active_C <> 0
			OR RP_ANTICIPATED_SPEND_C <> 0
			OR RP_ANTICIPATED_SPEND_U <> 0
			OR INACTIVE_CM_C <> 0 
			OR ACTIVE_CM_C <> 0 
			OR RP_CM_C <> 0
			OR NRP_CM_C <> 0
			OR CM_C <> 0 
			OR RCPTS_OO_INACTIVE_C <> 0 
			OR RCPTS_OO_ACTIVE_RP_C <> 0
			OR RCPTS_OO_ACTIVE_NRP_C <> 0
			OR Rp_Rcpt_Plan_C <> 0
			OR RP_OO_and_CM_C <> 0
			OR NRP_OO_and_CM_C <> 0)

WITH DATA PRIMARY INDEX(Month_Plans,DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)
ON COMMIT PRESERVE ROWS;



DELETE FROM {environment_schema}.planned_hist_report_mfp{env_suffix};

INSERT INTO {environment_schema}.planned_hist_report_mfp{env_suffix}
SELECT * 
FROM MFP_plan_hist_report_dept_h48v_1





