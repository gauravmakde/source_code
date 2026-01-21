------------------------------------------------------------
-- HOLIDAY OPS SCORECARD --
-- Date Created: 11/30/2023
-- Last Updated: 11/30/2023
-- Author: Ankur Niranjan
------------------------------------------------------------
-- code begins here --

CREATE MULTISET VOLATILE TABLE CURR_WK AS (
SELECT 
WEEK_IDNT,
WEEK_START_DAY_DATE,
WEEK_END_DAY_DATE,
MONTH_IDNT,
MONTH_START_DAY_DATE,
MONTH_END_DAY_DATE
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE day_date = {start_date} - 7
) WITH data PRIMARY INDEX (WEEK_IDNT) ON COMMIT PRESERVE rows ;



CREATE MULTISET VOLATILE TABLE WEEKLY_OPS_STANDUP, 
FALLBACK,
NO BEFORE JOURNAL,
NO AFTER JOURNAL,
CHECKSUM = DEFAULT,
DEFAULT MERGEBLOCKRATIO,
MAP = TD_MAP1 
(
	OPS_NAME VARCHAR(50),
	BANNER VARCHAR(50),
	CHANNEL VARCHAR(50),
	METRIC_NAME VARCHAR(50),
	DAY_DATE DATE,
	FISCAL_NUM INTEGER,
	FISCAL_DESC VARCHAR(50),
	LABEL VARCHAR(50), -- Chart Plot Axis (x-axis)
	ROLLING_FISCAL_IND INTEGER, -- identifier for Rolling & Forward weeks 
	PLAN_OP FLOAT,
	PLAN_CP FLOAT,
	TY FLOAT,
	LY FLOAT -- most recent completed week 
) PRIMARY INDEX (OPS_NAME,BANNER,METRIC_NAME,DAY_DATE,LABEL) ON COMMIT PRESERVE rows ;



-- T3DL_FIN_E2E_SALES.E2E_STORE_DIM_VW source ddl for store lookup
CREATE MULTISET VOLATILE TABLE E2E_STORE_DIM_VW AS (
SELECT
st1.business_unit_desc
,st1.store_country_code
,st1.store_type_code
,st1.store_num
,st1.store_name
,st1.store_short_name
,st1.store_open_date
-- Revising close date:
,CASE WHEN st1.store_num IN (427,474) THEN DATE '2023-07-01' -- due to closures this year
WHEN st1.store_num IN (830,831,832,833,834,835) THEN DATE '2023-06-13'
WHEN st1.store_num IN (840,841,842,843,844,845,846) THEN DATE '2023-05-14' -- due to CA closures this year
ELSE st1.store_close_date END as revised_close_date
-- Comp status:
,st1.comp_status_desc as comp_status_original
,CASE WHEN st1.store_type_code IN ('CO','FC','OC','OF','VS') OR st1.store_num IN (52,813,1443) THEN 'COMP'
WHEN st1.store_open_date <= '2019-02-03' AND revised_close_date IS NULL THEN 'COMP'
ELSE 'NON-COMP' END as comp_status_vs_19
,CASE WHEN comp_status_vs_19 = 'COMP' THEN 'COMP'
WHEN revised_close_date IS NOT NULL THEN 'CLOSED'
WHEN st1.store_open_date >= '2019-02-03' THEN 'NEW' END as comp_status_dtl_vs_19
,CASE WHEN st1.store_type_code IN ('CO','FC','OC','OF','VS') OR st1.store_num IN (52,813,1443) THEN 'COMP'
WHEN st1.store_open_date <= '2021-01-31' AND revised_close_date IS NULL THEN 'COMP'
ELSE 'NON-COMP' END as comp_status_vs_21
,CASE WHEN comp_status_vs_21 = 'COMP' THEN 'COMP'
WHEN revised_close_date IS NOT NULL THEN 'CLOSED'
WHEN st1.store_open_date >= '2021-01-31' THEN 'NEW' END as comp_status_dtl_vs_21 
,CASE WHEN st1.store_type_code IN ('CO','FC','OC','OF','VS') OR st1.store_num IN (52,813,1443) THEN 'COMP'
WHEN st1.store_open_date <= '2022-01-30' AND revised_close_date IS NULL THEN 'COMP'
ELSE 'NON-COMP' END as comp_status_vs_22
,CASE WHEN comp_status_vs_22 = 'COMP' THEN 'COMP'
WHEN revised_close_date IS NOT NULL THEN 'CLOSED'
WHEN st1.store_open_date >= '2022-01-30' THEN 'NEW' END as comp_status_dtl_vs_22
,comp_status_vs_22 as comp_status
,comp_status_dtl_vs_22 as comp_status_dtl
-- Regions & markets:
,CASE WHEN st1.business_unit_desc IN ('CORPORATE','CORPORATE CANADA','OFFPRICE ONLINE') OR st1.subgroup_medium_desc = 'NORD FLS HQ' THEN NULL
WHEN st1.store_type_code = 'CC' THEN 'LAST CHANCE' --Last Chance stores in own region
WHEN st1.subgroup_desc LIKE '% CANADA' THEN 'CANADA'
WHEN st1.subgroup_medium_desc = 'SCAL FLS' THEN 'SCAL'
ELSE LEFT(st1.subgroup_desc, POSITION(' ' IN st1.subgroup_desc)) END as store_region --Drop FLS/Rack suffix from region names, use geo area only
,CASE WHEN st1.region_desc = 'MANHATTAN' THEN 'NYC' --NYC stores into own sub-region
ELSE store_region END as store_subregion
,CASE WHEN st1.store_num IN (206,214,502) THEN 501
WHEN st1.store_num = 58 THEN 819
WHEN st1.store_num IN (98,1413) THEN 803
ELSE COALESCE(udma.dma_code,cdma.ca_dma_code) END as dma_code --Topside stores where DMA is not populated
,CASE WHEN st1.store_num IN (206,214,502) THEN 'New York NY'
WHEN cdma.ca_dma_code = 505 THEN 'Ottawa ON'
WHEN cdma.ca_dma_code = 535 THEN 'Toronto ON'
WHEN cdma.ca_dma_code = 825 THEN 'Calgary AB'
WHEN cdma.ca_dma_code = 835 THEN 'Edmonton AB'
WHEN cdma.ca_dma_code = 933 THEN 'Vancouver BC'
WHEN st1.store_num = 58 THEN 'Seattle-Tacoma WA'
WHEN st1.store_num IN (98,1413) THEN 'Los Angeles CA'
ELSE COALESCE(udma.dma_desc,cdma.ca_dma_desc) END as dma_desc --Topside stores where DMA is not populated
,CASE WHEN st1.store_type_code = 'VS' THEN CAST(REGEXP_SUBSTR(st1.store_short_name,'\d+',1,1,'i') AS INTEGER)
ELSE st1.store_num END as physical_store_num --Tag VS Stores with corresponding physical store num
FROM PRD_NAP_USR_VWS.STORE_DIM as st1
LEFT JOIN PRD_NAP_USR_VWS.STORE_DIM as st2 
ON st2.store_num = CASE WHEN st1.store_type_code = 'VS' AND st1.store_short_name <> 'VS UNASSIGNED' THEN CAST(REGEXP_SUBSTR(st1.store_short_name,'\d+',1,1,'i') AS INTEGER) END
LEFT JOIN PRD_NAP_USR_VWS.ORG_DMA as udma 
ON udma.dma_code = CASE WHEN st1.store_country_code = 'US' THEN COALESCE(st1.store_dma_code, st2.store_dma_code) END
LEFT JOIN (SELECT ca_dma_code, ca_dma_desc FROM PRD_NAP_USR_VWS.ORG_CA_ZIP_DMA GROUP BY 1,2) as cdma 
ON cdma.ca_dma_code = CASE WHEN st1.store_country_code = 'CA' THEN COALESCE(st1.store_dma_code, st2.store_dma_code) END
WHERE st1.store_num IN (SELECT DISTINCT intent_store_num FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT WHERE business_day_date >= '2019-02-03') --Only stores with transactions since start of FY19
--ORDER BY comp_status_original, comp_status, comp_status_vs_19, comp_status_vs_21, comp_status_vs_22, st1.store_num;
) WITH data PRIMARY INDEX (business_unit_desc, store_num) ON COMMIT PRESERVE rows ;

-------------------------------------------------------------------------------------------------------------------------------------------------------

-- Daily aggregated MOTHERSHIP version
CREATE MULTISET VOLATILE TABLE HOLIDAY_OPS_1 AS (
SELECT
tran_date,
CASE WHEN a.business_unit_desc IN ('FULL LINE','N.COM') THEN 'NORDSTROM'
WHEN a.business_unit_desc IN ('OFFPRICE ONLINE','RACK') THEN 'NORDSTROM RACK' END as banner,
CASE WHEN a.business_unit_desc IN ('FULL LINE') THEN 'FLS'
WHEN a.business_unit_desc IN ('N.COM') THEN 'N.COM'
WHEN a.business_unit_desc IN ('RACK') THEN 'RACK STORE'
WHEN a.business_unit_desc IN ('OFFPRICE ONLINE') THEN 'R.COM' END as channel,
SUM(CASE WHEN sd.comp_status_vs_22 = 'COMP' THEN (COALESCE(demand_amt_excl_bopus,0) + COALESCE(bopus_attr_store_amt,0) + COALESCE(bopus_attr_digital_amt,0)) END) as demand_amt_act,
SUM(net_operational_sales_amt) as net_op_sales_amt_act
FROM T2DL_DAS_MOTHERSHIP.SALES_DEMAND_ORDERS_TRAFFIC_FACT a
JOIN E2E_STORE_DIM_VW sd ON a.store_num = sd.STORE_NUM
WHERE a.business_unit_desc in ('FULL LINE','N.COM','RACK','OFFPRICE ONLINE')
and tran_date between {start_date_holiday} and ({start_date} - 1)
and a.business_unit_desc = sd.business_unit_desc
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (tran_date, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;



-- Plan Numbers (Store: net op sales)
CREATE MULTISET VOLATILE TABLE HOLIDAY_OPS_2 AS (
SELECT
day_date,
CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'NORDSTROM'
WHEN business_unit_desc IN ('RACK') THEN 'NORDSTROM RACK' END as banner,
CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'FLS'
WHEN business_unit_desc IN ('RACK') THEN 'RACK STORE' END as channel,
SUM(net_op_sales) as net_op_sales_amt_plan
FROM T2DL_DAS_OSU.E2E_STORE_SALES_PLAN 
WHERE business_unit_desc in ('FULL LINE','RACK')
and day_date between {start_date_holiday} and {end_date}	-- '2023-10-01' and '2023-12-30'
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (day_date, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;



-- Plan Numbers (Digital: net sales)
insert into HOLIDAY_OPS_2
SELECT
day_date,
CASE WHEN business_unit_desc IN ('N.COM') THEN 'NORDSTROM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NORDSTROM RACK' END as banner,
CASE WHEN business_unit_desc IN ('N.COM') THEN 'N.COM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'R.COM' END as channel,
SUM(net_sales_amt) as net_op_sales_amt_plan
FROM T2DL_DAS_OSU.E2E_DIGITAL_FUNNEL_PLAN 
WHERE business_unit_desc in ('N.COM','OFFPRICE ONLINE')
and day_date between {start_date_holiday} and {end_date}	-- '2023-10-01' and '2023-12-30'
GROUP BY 1,2,3 ;


-- Plan (Digital Demand)
CREATE MULTISET VOLATILE TABLE HOLIDAY_OPS_3 AS (
SELECT
day_date,
CASE WHEN business_unit_desc IN ('N.COM') THEN 'NORDSTROM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NORDSTROM RACK' END as banner,
CASE WHEN business_unit_desc IN ('N.COM') THEN 'N.COM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'R.COM' END as channel,
SUM(demand_amt) as digital_demand_amt_plan,
SUM(net_sales_amt) as net_sales_amt_plan
FROM T2DL_DAS_OSU.E2E_DIGITAL_FUNNEL_PLAN 
WHERE business_unit_desc in ('N.COM','OFFPRICE ONLINE')
and day_date between {start_date_holiday} and {end_date}	-- '2023-10-01' and '2023-12-30'
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (day_date, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;


-- COMBINING ALL FIELDS
CREATE MULTISET VOLATILE TABLE HOLIDAY_OPS_4 AS (
SELECT 
COALESCE(A.TRAN_DATE,B.DAY_DATE,C.DAY_DATE) AS DAY_DATE,
WEEK_NUM,
WEEK_454_NUM,
MONTH_SHORT_DESC,
COALESCE(A.BANNER, B.BANNER, C.BANNER) AS BANNER,
COALESCE(A.CHANNEL, B.CHANNEL, C.CHANNEL) AS CHANNEL,
CAST(COALESCE(A.net_op_sales_amt_act,0) AS FLOAT) AS net_op_sales_amt_act,
CAST(COALESCE(B.net_op_sales_amt_plan,0) AS FLOAT) AS net_op_sales_amt_plan,
CAST(COALESCE(A.demand_amt_act,0) AS FLOAT) AS demand_amt_act,
CAST(COALESCE(C.digital_demand_amt_plan,0) AS FLOAT) AS digital_demand_amt_plan
FROM HOLIDAY_OPS_1 A
FULL JOIN HOLIDAY_OPS_2 B ON A.TRAN_DATE = B.DAY_DATE AND A.BANNER = B.BANNER AND A.CHANNEL = B.CHANNEL
FULL JOIN HOLIDAY_OPS_3 C ON A.TRAN_DATE = C.DAY_DATE AND A.BANNER = C.BANNER AND A.CHANNEL = C.CHANNEL
JOIN PRD_NAP_USR_VWS.DAY_CAL D ON COALESCE(A.TRAN_DATE,B.DAY_DATE,C.DAY_DATE) = D.DAY_DATE
) WITH DATA PRIMARY INDEX (WEEK_NUM, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;


INSERT INTO WEEKLY_OPS_STANDUP
SELECT
'HOLIDAY' AS OPS_NAME,
BANNER,
CHANNEL,
'Event Net Sales' AS METRIC_NAME,
DAY_DATE,
NULL AS FISCAL_NUM,
'DATE' AS FISCAL_DESC,
CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS LABEL,
CASE WHEN DAY_DATE <= WEEK_END_DAY_DATE THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
SUM(net_op_sales_amt_plan) AS PLAN_OP,
Null AS PLAN_CP,
SUM(net_op_sales_amt_act) AS TY,
Null AS LY
FROM HOLIDAY_OPS_4,CURR_WK
GROUP BY 1,2,3,4,5,6,7,8,9,11,13;


INSERT INTO WEEKLY_OPS_STANDUP
SELECT
'HOLIDAY' AS OPS_NAME,
BANNER,
CHANNEL,
'Event Digital Demand' AS METRIC_NAME,
DAY_DATE,
NULL AS FISCAL_NUM,
'DATE' AS FISCAL_DESC,
CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS LABEL,
CASE WHEN DAY_DATE <= WEEK_END_DAY_DATE THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
SUM(digital_demand_amt_plan) AS PLAN_OP,
Null AS PLAN_CP,
SUM(demand_amt_act) AS TY,
Null AS LY
FROM HOLIDAY_OPS_4,CURR_WK
where channel in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6,7,8,9,11,13;


----------------------------------------------------------------------------------------------------------------------------------

DELETE FROM {environment_schema}.WEEKLY_OPS_STANDUP
WHERE OPS_NAME = 'HOLIDAY' -- Adding an extra filter since all Ops Scorecard will be using the same table
and day_date between {start_date_holiday} and {end_date};


insert into {environment_schema}.WEEKLY_OPS_STANDUP
select
OPS_NAME,
BANNER,
CHANNEL,
METRIC_NAME,
FISCAL_NUM,
FISCAL_DESC,
LABEL,
ROLLING_FISCAL_IND,
PLAN_OP,
PLAN_CP,
TY,
LY,
WEEK_IDNT AS UPDATED_WEEK, CURRENT_TIMESTAMP as update_timestamp,
day_date
FROM WEEKLY_OPS_STANDUP AS A, CURR_WK ;


-----------------------------------------------------------------------------------------------------------------------------------------
