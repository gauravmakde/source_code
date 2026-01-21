SET QUERY_BAND = 'App_ID=APP08866;
     DAG_ID= holiday_ops_scorecard_11521_ACE_ENG;
     Task_Name= holiday_ops_scorecard;'
     FOR SESSION VOLATILE;

------------------------------------------------------------
-- HOLIDAY OPS SCORECARD --
-- Table Name: 
      -- Dev: PROTO_DSA_AI_BASE_VWS.WEEKLY_OPS_STANDUP_AGG
      --Prod: PRD_NAP_DSA_AI_BASE_VWS.WEEKLY_OPS_STANDUP_AGG
-- Date Created: 10/28/2024
-- Last Updated: 10/28/2024
-- Author: Shaila Karole

-- Note:
-- Purpose: To capture Holiday Daily Sales 
-- Update Cadence:  Monday, Tuesday, and Wednesday Morning at 8:00am PST 
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
FROM PRD_NAP_BASE_VWS.DAY_CAL_454_DIM
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
    ,CASE WHEN st1.store_num = 57 THEN DATE '2023-12-31' -- due to closures in fy23
     WHEN st1.store_num = 56 THEN DATE '2023-10-14' -- due to closures in fy23
     WHEN st1.store_num = 537 THEN DATE '2024-02-05' -- due to closures this year
     WHEN st1.store_num IN (427,474,804) THEN DATE '2023-07-01' -- due to closures in fy23
     WHEN st1.store_num IN (830,831,832,833,834,835) THEN DATE '2023-06-13'
     WHEN st1.store_num IN (840,841,842,843,844,845,846) THEN DATE '2023-05-14' -- due to CA closures this year
                                                       ELSE st1.store_close_date   END as revised_close_date
    -- Comp status:
    ,st1.comp_status_desc as comp_status_original
    ,CASE WHEN st1.store_type_code IN ('CO','FC','OC','OF')
            OR st1.store_num IN (52,813,1443)           THEN 'COMP'
          WHEN st1.store_open_date <= '2019-02-03'
           AND revised_close_date IS NULL             THEN 'COMP'
          WHEN st1.store_type_code = 'VS' AND revised_close_date IS NULL THEN 'COMP'
                                                        ELSE 'NON-COMP'   END as comp_status_vs_19
    ,CASE WHEN comp_status_vs_19 = 'COMP'               THEN 'COMP'
          WHEN revised_close_date IS NOT NULL         THEN 'CLOSED'
          WHEN st1.store_open_date >= '2019-02-03'        THEN 'NEW'        END as comp_status_dtl_vs_19
    ,CASE WHEN st1.store_type_code IN ('CO','FC','OC','OF')
            OR st1.store_num IN (52,813,1443)           THEN 'COMP'
          WHEN st1.store_open_date <= '2021-01-31'
           AND revised_close_date IS NULL             THEN 'COMP'
          WHEN st1.store_type_code = 'VS' AND revised_close_date IS NULL THEN 'COMP'
                                                        ELSE 'NON-COMP'   END as comp_status_vs_21
    ,CASE WHEN comp_status_vs_21 = 'COMP'               THEN 'COMP'
          WHEN revised_close_date IS NOT NULL         THEN 'CLOSED'
          WHEN st1.store_open_date >= '2021-01-31'        THEN 'NEW'        END as comp_status_dtl_vs_21
    ,CASE WHEN st1.store_type_code IN ('CO','FC','OC','OF')
            OR st1.store_num IN (52,813,1443)           THEN 'COMP'
          WHEN st1.store_open_date <= '2022-01-30'
           AND revised_close_date IS NULL             THEN 'COMP'
          WHEN st1.store_type_code = 'VS' AND revised_close_date IS NULL THEN 'COMP'
                                                        ELSE 'NON-COMP'   END as comp_status_vs_22
    ,CASE WHEN comp_status_vs_22 = 'COMP'               THEN 'COMP'
          WHEN revised_close_date IS NOT NULL         THEN 'CLOSED'
          WHEN st1.store_open_date >= '2022-01-30'        THEN 'NEW'        END as comp_status_dtl_vs_22
    ,CASE WHEN st1.store_type_code IN ('CO','FC','OC','OF')
            OR st1.store_num IN (52,813,1443)           THEN 'COMP'
          WHEN st1.store_open_date <= '2023-01-29'
           AND revised_close_date IS NULL             THEN 'COMP'
          WHEN st1.store_type_code = 'VS' AND revised_close_date IS NULL THEN 'COMP'
          WHEN st1.store_num IN (9997,9998)  THEN 'COMP'
                                                        ELSE 'NON-COMP'   END as comp_status_vs_23
    ,CASE WHEN comp_status_vs_23 = 'COMP'               THEN 'COMP'
          WHEN revised_close_date IS NOT NULL         THEN 'CLOSED'
          WHEN st1.store_open_date >= '2023-01-29'        THEN 'NEW'        END as comp_status_dtl_vs_23
    ,comp_status_vs_23                                                        as comp_status
    ,comp_status_dtl_vs_23                                                    as comp_status_dtl
    -- Regions & markets:
    ,CASE WHEN st1.business_unit_desc IN ('CORPORATE','CORPORATE CANADA','OFFPRICE ONLINE')
            OR st1.subgroup_medium_desc = 'NORD FLS HQ' THEN NULL
          WHEN st1.store_type_code = 'CC'               THEN 'LAST CHANCE'  --Last Chance stores in own region
          WHEN st1.subgroup_desc LIKE '% CANADA'        THEN 'CANADA'
          WHEN st1.subgroup_medium_desc = 'SCAL FLS'    THEN 'SCAL'
                                                        ELSE LEFT(st1.subgroup_desc, POSITION(' ' IN  st1.subgroup_desc)) END as store_region --Drop FLS/Rack suffix from region names, use geo area only
    ,CASE WHEN st1.region_desc = 'MANHATTAN'            THEN 'NYC'  --NYC stores into own sub-region
                                                        ELSE store_region                                                 END as store_subregion
    ,CASE WHEN st1.store_num IN (206,214,502)           THEN 501
          WHEN st1.store_num  in (58,485)                     THEN 819
          WHEN st1.store_num  IN (98,1413)             THEN 803
                                                       ELSE COALESCE(udma.dma_code,cdma.ca_dma_code)                     END as dma_code --Topside stores where DMA is not populated
    ,CASE WHEN st1.store_num IN (206,214,502)           THEN 'New York NY'
          WHEN cdma.ca_dma_code = 505                   THEN 'Ottawa ON'
          WHEN cdma.ca_dma_code = 535                   THEN 'Toronto ON'
          WHEN cdma.ca_dma_code = 825                   THEN 'Calgary AB'
          WHEN cdma.ca_dma_code = 835                   THEN 'Edmonton AB'
          WHEN cdma.ca_dma_code = 933                   THEN 'Vancouver BC'
          WHEN st1.store_num in (58,485)                    THEN 'Seattle-Tacoma WA'
          WHEN st1.store_num IN (98,1413)            THEN 'Los Angeles CA'
                                                        ELSE COALESCE(udma.dma_desc,cdma.ca_dma_desc)                      END as dma_desc --Topside stores where DMA is not populated
    ,CASE WHEN st1.store_type_code = 'VS'               THEN CAST(REGEXP_SUBSTR(st1.store_short_name,'\d+',1,1,'i') AS INTEGER)
                                                        ELSE st1.store_num                                                 END as physical_store_num --Tag VS Stores with corresponding physical store num
FROM        PRD_NAP_BASE_VWS.STORE_DIM  as st1
  LEFT JOIN PRD_NAP_BASE_VWS.STORE_DIM  as st2   ON st2.store_num    = CASE WHEN st1.store_type_code = 'VS' AND st1.store_short_name <> 'VS UNASSIGNED' THEN CAST(REGEXP_SUBSTR(st1.store_short_name,'\d+',1,1,'i') AS INTEGER) END
  LEFT JOIN PRD_NAP_BASE_VWS.ORG_DMA    as udma  ON udma.dma_code    = CASE WHEN st1.store_country_code = 'US' THEN COALESCE(st1.store_dma_code, st2.store_dma_code) END
  LEFT JOIN (SELECT  ca_dma_code, ca_dma_desc FROM PRD_NAP_BASE_VWS.ORG_CA_ZIP_DMA where ca_dma_code in (select store_dma_code from PRD_NAP_BASE_VWS.STORE_DIM) GROUP BY 1,2)
                                       as cdma  ON cdma.ca_dma_code = CASE WHEN st1.store_country_code = 'CA' THEN COALESCE(st1.store_dma_code, st2.store_dma_code) END
WHERE st1.store_num IN (SELECT DISTINCT intent_store_num FROM PRD_NAP_BASE_VWS.RETAIL_TRAN_DETAIL_FACT WHERE business_day_date >= '2019-02-03')
) WITH data PRIMARY INDEX (business_unit_desc, store_num) ON COMMIT PRESERVE rows ;

-------------------------------------------------------------------------------------------------------------------------------------------------------

-- Daily aggregated CLARITY version
CREATE MULTISET VOLATILE TABLE HOLIDAY_OPS_1 AS (
SELECT
tran_date,
day_short_desc,
CASE WHEN a.business_unit_desc IN ('FULL LINE','N.COM','MARKETPLACE') THEN 'NORDSTROM'
WHEN a.business_unit_desc IN ('OFFPRICE ONLINE','RACK') THEN 'NORDSTROM RACK' END as banner,
CASE WHEN a.business_unit_desc IN ('FULL LINE') THEN 'FLS'
WHEN a.business_unit_desc IN ('N.COM') THEN 'N.COM'
WHEN a.business_unit_desc IN ('RACK') THEN 'RACK STORE'
WHEN a.business_unit_desc IN ('OFFPRICE ONLINE') THEN 'R.COM' 
WHEN a.business_unit_desc IN ('MARKETPLACE') THEN 'MARKETPLACE' END as channel,
sum(CASE WHEN comp_status_vs_23 = 'COMP'
        THEN (reported_demand_usd_amt_excl_bopus + bopus_attr_digital_reported_demand_usd_amt) END) as demand_amt_act,
SUM(op_gmv_usd_amt) as net_op_sales_amt_act
FROM PRD_NAP_DSA_AI_BASE_VWS.FINANCE_SALES_DEMAND_MARGIN_TRAFFIC_FACT a
--PRD_NAP_DSA_AI_USR_VWS.FINANCE_SALES_DEMAND_MARGIN_TRAFFIC_FACT a
JOIN E2E_STORE_DIM_VW sd ON a.store_num = sd.STORE_NUM
inner join PRD_NAP_BASE_VWS.DAY_CAL as dc on a.tran_date = dc.day_date
WHERE a.business_unit_desc in ('FULL LINE','N.COM','RACK','OFFPRICE ONLINE','MARKETPLACE')
and tran_date between {start_date_holiday} and {end_date}  
and a.business_unit_desc = sd.business_unit_desc
GROUP BY 1,2,3,4
) WITH DATA PRIMARY INDEX (tran_date, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;



-- Plan Numbers (Store: net op sales)
CREATE MULTISET VOLATILE TABLE HOLIDAY_OPS_2 AS (
SELECT
b.day_date,
day_short_desc,
CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'NORDSTROM'
WHEN business_unit_desc IN ('RACK') THEN 'NORDSTROM RACK' END as banner,
CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'FLS'
WHEN business_unit_desc IN ('RACK') THEN 'RACK STORE' END as channel,
SUM(op_gmv) as net_op_sales_amt_plan
FROM T2DL_DAS_OSU.E2E_STORE_SALES_PLAN b
inner join PRD_NAP_BASE_VWS.DAY_CAL as dc on b.day_date = dc.day_date
WHERE business_unit_desc in ('FULL LINE','RACK')
AND plan_version = 'FY24 Jun Plan'
and b.day_date between {start_date_holiday} and {end_date}  
GROUP BY 1,2,3,4
) WITH DATA PRIMARY INDEX (day_date, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;



-- Plan Numbers (Digital: net sales)
insert into HOLIDAY_OPS_2
SELECT
b.day_date,
day_short_desc,
CASE WHEN business_unit_desc IN ('N.COM') THEN 'NORDSTROM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NORDSTROM RACK' END as banner,
CASE WHEN business_unit_desc IN ('N.COM') THEN 'N.COM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'R.COM' END as channel,
SUM(net_sales_amt) as net_op_sales_amt_plan
FROM T2DL_DAS_OSU.E2E_DIGITAL_FUNNEL_PLAN b
inner join PRD_NAP_BASE_VWS.DAY_CAL as dc on b.day_date = dc.day_date
WHERE business_unit_desc in ('N.COM','OFFPRICE ONLINE')
AND plan_version = 'FY24 Jun Plan'
and b.day_date between {start_date_holiday} and {end_date}  		
GROUP BY 1,2,3,4 ;


-- Plan (Digital Demand)
CREATE MULTISET VOLATILE TABLE HOLIDAY_OPS_3 AS (
SELECT
b.day_date,
day_short_desc,
CASE WHEN business_unit_desc IN ('N.COM') THEN 'NORDSTROM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NORDSTROM RACK' END as banner,
CASE WHEN business_unit_desc IN ('N.COM') THEN 'N.COM'
WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'R.COM' END as channel,
SUM(demand_amt) as digital_demand_amt_plan,
SUM(net_sales_amt) as net_sales_amt_plan
FROM T2DL_DAS_OSU.E2E_DIGITAL_FUNNEL_PLAN b
inner join PRD_NAP_BASE_VWS.DAY_CAL as dc on b.day_date = dc.day_date
WHERE business_unit_desc in ('N.COM','OFFPRICE ONLINE')
AND plan_version = 'FY24 Jun Plan'
and b.day_date between {start_date_holiday} and {end_date} 
GROUP BY 1,2,3,4
) WITH DATA PRIMARY INDEX (day_date, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;


-- COMBINING ALL FIELDS
CREATE MULTISET VOLATILE TABLE HOLIDAY_OPS_4 AS (
SELECT 
COALESCE(A.TRAN_DATE,B.DAY_DATE,C.DAY_DATE) AS DAY_DATE,
COALESCE(A.day_short_desc,B.day_short_desc,C.day_short_desc) AS day_short_desc,
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
JOIN PRD_NAP_BASE_VWS.DAY_CAL D ON COALESCE(A.TRAN_DATE,B.DAY_DATE,C.DAY_DATE) = D.DAY_DATE
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
where channel in ('N.COM','R.COM','MARKETPLACE')
GROUP BY 1,2,3,4,5,6,7,8,9,11,13;


----------------------------------------------------------------------------------------------------------------------------------

DELETE FROM {clarity_schema}.WEEKLY_OPS_STANDUP_AGG
WHERE OPS_NAME = 'HOLIDAY' 
and day_date between {start_date_holiday} and {end_date};


insert into {clarity_schema}.WEEKLY_OPS_STANDUP_AGG
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


-------------------------------------------------------------------------------------------------------------------------------------------------------
/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

