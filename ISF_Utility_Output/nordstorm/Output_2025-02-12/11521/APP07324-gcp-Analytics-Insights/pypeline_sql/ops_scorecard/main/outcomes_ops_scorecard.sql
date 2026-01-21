SET QUERY_BAND = 'App_ID=APP08866;
     DAG_ID= outcomes_ops_scorecard_11521_ACE_ENG;
     Task_Name= outcomes_ops_scorecard;'
     FOR SESSION VOLATILE;

------------------------------------------------------------
-- OUTCOMES AND FUNNEL OPS SCORECARD --
-- Table Name: 
      -- Dev: PROTO_DSA_AI_BASE_VWS.WEEKLY_OPS_STANDUP_AGG
      --Prod: PRD_NAP_DSA_AI_BASE_VWS.WEEKLY_OPS_STANDUP_AGG
-- Date Created: 7/16/2024
-- Last Updated: 7/16/2024
-- Author: Aman Kumar

-- Note:
-- Purpose: To capture Outcomes and Funnel of Store and Digital KPIs
-- Update Cadence:  Monday,Tuesday,Wednesday Morning at 10:00am PST 
------------------------------------------------------------
-- code begins here --

CREATE MULTISET VOLATILE TABLE WEEKLY_OPS_STANDUP, --copy of the permanent table
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
      FISCAL_NUM INTEGER,
      FISCAL_DESC VARCHAR(50),
      LABEL VARCHAR(50),			-- Chart Plot Axis (x-axis)
      ROLLING_FISCAL_IND INTEGER,	-- identifier for Rolling & Forward weeks 
      PLAN_OP FLOAT,
      PLAN_CP FLOAT,
      TY FLOAT,
      LY FLOAT,						-- most recent completed week	
	DAY_DATE DATE					-- Adding new column to accomodate Holiday Season data
) PRIMARY INDEX (OPS_NAME,BANNER,METRIC_NAME,FISCAL_NUM,LABEL)  ON COMMIT PRESERVE rows ;


-- WK Identifier for automating weekly plots
CREATE MULTISET VOLATILE TABLE CURR_WK AS (
	SELECT WEEK_IDNT
		, WEEK_START_DAY_DATE
		, WEEK_END_DAY_DATE
		, MONTH_IDNT
		, MONTH_START_DAY_DATE
		, MONTH_END_DAY_DATE
		, QUARTER_IDNT 
		, FISCAL_QUARTER_NUM
		, QUARTER_START_DAY_DATE
		, QUARTER_START_WEEK_IDNT
		, QUARTER_END_WEEK_IDNT
	FROM PRD_NAP_BASE_VWS.DAY_CAL_454_DIM
	WHERE day_date = {start_date} - 7 	-- {start_date} - 7
) WITH data PRIMARY INDEX (WEEK_IDNT) ON COMMIT PRESERVE rows ;
 

-- CAL_LKUP;
CREATE MULTISET VOLATILE TABLE CAL_LKUP AS (
SELECT DISTINCT
	a.WEEK_NUM
	, a.WEEK_454_NUM
	, a.MONTH_SHORT_DESC
	, b.WEEK_START_DAY_DATE
	, b.WEEK_END_DAY_DATE
	, a.MONTH_NUM
	, b.MONTH_START_DAY_DATE
	, b.MONTH_END_DAY_DATE
	, a.QUARTER_NUM
	, a.QUARTER_454_NUM
	, a.YEAR_NUM
FROM PRD_NAP_BASE_VWS.DAY_CAL a
LEFT JOIN PRD_NAP_BASE_VWS.DAY_CAL_454_DIM b
	on a.WEEK_NUM = b.WEEK_IDNT
		and a.MONTH_NUM = b.MONTH_IDNT
WHERE b.day_date BETWEEN ({start_date}-120) AND ({start_date}+120) -- {start_date} and {end_date}	-- CURRENT_DATE-220 AND CURRENT_DATE+120
) WITH data PRIMARY INDEX (WEEK_NUM) ON COMMIT PRESERVE rows ;


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

-- Funnel and Topline weekly aggregated CLARITY version
CREATE MULTISET VOLATILE TABLE FT_OPS_1 AS (
SELECT
WEEK_NUM,
CASE WHEN business_unit_desc IN ('FULL LINE','N.COM','MARKETPLACE') THEN 'NORDSTROM'
     WHEN business_unit_desc IN ('OFFPRICE ONLINE','RACK') THEN 'NORDSTROM RACK' END as banner
,CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'FLS'
     WHEN business_unit_desc IN ('N.COM','MARKETPLACE') THEN 'N.COM'
     WHEN business_unit_desc IN ('RACK') THEN 'RACK STORE'
     WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'R.COM' END as channel
,sum(CASE WHEN
        (year_num = 2024 AND comp_status_vs_23 = 'COMP') OR
        (year_num = 2023 AND comp_status_vs_22 = 'COMP')
        THEN store_traffic END) as store_traffic_act
,sum(CASE WHEN
        (year_num = 2024 AND comp_status_vs_23 = 'COMP') OR
        (year_num = 2023 AND comp_status_vs_22 = 'COMP')
        THEN store_purchase_trips END) as purchase_trips_act
,sum(CASE WHEN
        (year_num = 2024 AND comp_status_vs_23 = 'COMP') OR
        (year_num = 2023 AND comp_status_vs_22 = 'COMP')
        THEN reported_demand_usd_amt_bopus_in_digital END) as demand_amt_act
,sum(op_gmv_usd_amt) as net_op_sales_amt_act
,sum(CASE WHEN business_unit_desc in ('N.COM', 'OFFPRICE ONLINE') 
		AND NOT (business_unit_desc = 'OFFPRICE ONLINE' AND day_date BETWEEN '2020-11-21' AND '2021-03-05') 			
     	THEN orders_count END) as digital_orders_act
,sum(visitors) as digital_traffic_act
,sum(sessions) as sessions                      -------Added Sessions metric
FROM
(select
dc.year_num
,dc.month_454_num
,dc.week_of_fyr
,dc.week_num
,tran_date AS day_date
,dc.day_date + 364*(2024 - year_num) as ty_date
,CASE WHEN fsdmtf.business_unit_desc IN ('FULL LINE','FULL LINE CANADA','RACK','RACK CANADA') THEN 'STORE'
WHEN fsdmtf.business_unit_desc IN ('N.COM','N.CA','OFFPRICE ONLINE','MARKETPLACE') THEN 'DIGITAL' END as bu_type
,fsdmtf.business_unit_desc
,st.comp_status_vs_21
,st.comp_status_vs_22
,st.comp_status_vs_23
,sum(case when visitors is null then store_traffic else visitors end) as traffic
,sum(store_traffic) as store_traffic
,sum(visitors) as visitors
--,sum(viewing_visitors) as viewing_visitors
--,sum(adding_visitors) as adding_visitors
--,sum(ordering_visitors) as ordering_visitors
,sum(case when platform_code = 'Direct to Customer (DTC)' then 0 else orders_count end) as orders_count
,sum(store_purchase_trips) as store_purchase_trips
,sum(case when orders_count is null then store_purchase_trips
	      when platform_code = 'Direct to Customer (DTC)' then 0 else orders_count end) as orders_or_trips
,sum(reported_demand_usd_amt_excl_bopus + bopus_attr_digital_reported_demand_usd_amt) as reported_demand_usd_amt_bopus_in_digital
,sum(sessions) as sessions
,sum(op_gmv_usd_amt) as op_gmv_usd_amt
from PRD_NAP_DSA_AI_BASE_VWS.FINANCE_SALES_DEMAND_MARGIN_TRAFFIC_FACT as fsdmtf --PRD_NAP_DSA_AI_USR_VWS.FINANCE_SALES_DEMAND_MARGIN_TRAFFIC_FACT 
inner join PRD_NAP_BASE_VWS.DAY_CAL as dc on fsdmtf.tran_date = dc.day_date
inner join E2E_STORE_DIM_VW as st on fsdmtf.store_num = st.store_num
where 1=1
and tran_date between '2024-02-04' and {start_date}-1
group by 1,2,3,4,5,6,7,8,9,10,11
)a
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (WEEK_NUM, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;


-- Plan Numbers (Store: Traffic, Gross Sales and Trips)
CREATE MULTISET VOLATILE TABLE FT_OPS_2 AS (
SELECT
      WEEK_NUM,
      CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'NORDSTROM'
            WHEN business_unit_desc IN ('RACK') THEN 'NORDSTROM RACK' END as banner,
      CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'FLS'
            WHEN business_unit_desc IN ('RACK') THEN 'RACK STORE' END as channel,
      SUM(traffic) as store_traffic_plan,
      SUM(purchase_trips) as purchase_trips_plan,
      SUM(demand_amt) as store_demand_amt_plan
FROM T2DL_DAS_OSU.E2E_STORE_FUNNEL_PLAN  
WHERE comp_status = 'COMP'
AND business_unit_desc in ('FULL LINE','RACK')
and WEEK_NUM IN (SELECT distinct WEEK_NUM FROM CAL_LKUP)
and plan_version='FY24 Jun Plan'
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (WEEK_NUM, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;


-- Plan Numbers (Store: net op sales)
CREATE MULTISET VOLATILE TABLE FT_OPS_3 AS (
SELECT
      WEEK_NUM,
      CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'NORDSTROM'
            WHEN business_unit_desc IN ('RACK') THEN 'NORDSTROM RACK' END as banner,
      CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'FLS'
            WHEN business_unit_desc IN ('RACK') THEN 'RACK STORE' END as channel,
      SUM(op_gmv) as op_gmv
FROM T2DL_DAS_OSU.E2E_STORE_SALES_PLAN a
JOIN PRD_NAP_BASE_VWS.DAY_CAL b ON a.day_date = b.day_date
WHERE business_unit_desc in ('FULL LINE','RACK')
and WEEK_NUM IN (SELECT distinct WEEK_NUM FROM CAL_LKUP)
and plan_version='FY24 Jun Plan'
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (WEEK_NUM, BANNER, CHANNEL) ON COMMIT PRESERVE rows;


-- Plan (Digital)
CREATE MULTISET VOLATILE TABLE FT_OPS_4 AS (
SELECT
      WEEK_NUM,
      CASE WHEN business_unit_desc IN ('N.COM') THEN 'NORDSTROM'
            WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NORDSTROM RACK' END as banner,
      CASE WHEN business_unit_desc IN ('N.COM') THEN 'N.COM'
            WHEN business_unit_desc IN ('OFFPRICE ONLINE') THEN 'R.COM' END as channel,
      SUM(total_visitors) as digital_traffic_plan,
      SUM(order_count) as digital_orders_plan,
      SUM(demand_amt) as digital_demand_amt_plan,
      SUM(net_sales_amt) as net_sales_amt_plan
FROM T2DL_DAS_OSU.E2E_DIGITAL_FUNNEL_PLAN a
JOIN PRD_NAP_BASE_VWS.DAY_CAL b  ON a.day_date = b.day_date
WHERE business_unit_desc in ('N.COM','OFFPRICE ONLINE')
and WEEK_NUM IN (SELECT distinct WEEK_NUM FROM CAL_LKUP)
and plan_version='FY24 Jun Plan'
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (WEEK_NUM, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;


-- COMBINING ALL FIELDS
CREATE MULTISET VOLATILE TABLE FT_OPS_5 AS (
SELECT 
      COALESCE(A.WEEK_NUM,B.WEEK_NUM,C.WEEK_NUM,D.WEEK_NUM) AS WEEK_NUM,
      WEEK_454_NUM,
      MONTH_SHORT_DESC,
      WEEK_START_DAY_DATE,
      WEEK_END_DAY_DATE,
      COALESCE(A.BANNER, B.BANNER, C.BANNER, D.BANNER) AS BANNER,
      COALESCE(A.CHANNEL, B.CHANNEL, C.CHANNEL, D.CHANNEL) AS CHANNEL,
      CAST(COALESCE(A.store_traffic_act,0) AS FLOAT) AS store_traffic_act,
	  CAST(COALESCE(A.sessions,0) AS FLOAT) AS sessions,
      CAST(COALESCE(B.store_traffic_plan,0) AS FLOAT) AS store_traffic_plan,
      CAST(COALESCE(A.digital_traffic_act,0) AS FLOAT) AS digital_traffic_act,
      CAST(COALESCE(D.digital_traffic_plan,0) AS FLOAT) AS digital_traffic_plan,
      CAST(COALESCE(A.purchase_trips_act,0) AS FLOAT) AS purchase_trips_act,
      CAST(COALESCE(B.purchase_trips_plan,0) AS FLOAT) AS purchase_trips_plan,
      CAST(COALESCE(A.net_op_sales_amt_act,0) AS FLOAT) AS net_op_sales_amt_act,
      CAST(COALESCE(C.op_gmv,0) AS FLOAT) AS op_gmv,
      CAST(COALESCE(D.net_sales_amt_plan,0) AS FLOAT) AS net_sales_amt_plan,
      CAST(COALESCE(A.digital_orders_act,0) AS FLOAT) AS digital_orders_act,
      CAST(COALESCE(D.digital_orders_plan,0) AS FLOAT) AS digital_orders_plan,
      CAST(COALESCE(A.demand_amt_act,0) AS FLOAT) AS demand_amt_act,
      CAST(COALESCE(B.store_demand_amt_plan,0) AS FLOAT) AS store_demand_amt_plan,
      CAST(COALESCE(D.digital_demand_amt_plan,0) AS FLOAT) AS digital_demand_amt_plan
FROM FT_OPS_1 A
FULL JOIN FT_OPS_2 B ON a.WEEK_NUM = b.WEEK_NUM AND a.BANNER = b.BANNER AND a.CHANNEL = b.CHANNEL
FULL JOIN FT_OPS_3 C ON a.WEEK_NUM = c.WEEK_NUM AND a.BANNER = c.BANNER AND a.CHANNEL = c.CHANNEL
FULL JOIN FT_OPS_4 D ON a.WEEK_NUM = d.WEEK_NUM AND a.BANNER = d.BANNER	AND a.CHANNEL = d.CHANNEL
JOIN CAL_LKUP E ON COALESCE(A.WEEK_NUM,B.WEEK_NUM,C.WEEK_NUM,D.WEEK_NUM) = E.WEEK_NUM
) WITH DATA PRIMARY INDEX (WEEK_NUM, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;

----------------------------------------------------------------------------------------------------------------------------------------------------
---- Dashboard table insertion, metric wise ----

-- NORDSTROM OUTCOME METRICS | STORE --

-- ** STORE OP GMV ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'OUTCOMES' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'OP GMV' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS FISCAL_DESC,
      NULL  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      SUM(op_gmv) AS PLAN_OP,
      Null AS PLAN_CP,
      SUM(net_op_sales_amt_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('FLS','RACK STORE')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;


-- ** STORE OP GMV QTD ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'OUTCOMES' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'OP GMV QTD' AS METRIC_NAME,
      quarter_num AS FISCAL_NUM,
      CONCAT('Q',CAST(quarter_454_num AS VARCHAR(12))) AS FISCAL_DESC,
	  'QTD'  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      SUM(op_gmv) AS PLAN_OP,
      Null AS PLAN_CP,
      SUM(net_op_sales_amt_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5 A
JOIN (SELECT DISTINCT WEEK_NUM,quarter_num,quarter_454_num FROM CAL_LKUP WHERE WEEK_NUM BETWEEN (SELECT quarter_start_week_idnt FROM CURR_WK) AND (SELECT WEEK_IDNT FROM CURR_WK)) B
ON A.WEEK_NUM = B.WEEK_NUM
WHERE CHANNEL in ('FLS','RACK STORE')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;



-- ** DIGITAL OP GMV ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'OUTCOMES' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'OP GMV' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS FISCAL_DESC,
      NULL  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      SUM(net_sales_amt_plan) AS PLAN_OP,   
      Null AS PLAN_CP,
      SUM(net_op_sales_amt_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;


-- ** DIGITAL OP GMV QTD** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'OUTCOMES' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'OP GMV QTD' AS METRIC_NAME,
      quarter_num AS FISCAL_NUM,
      CONCAT('Q',CAST(quarter_454_num AS VARCHAR(12))) AS FISCAL_DESC,
	  'QTD'  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      SUM(net_sales_amt_plan) AS PLAN_OP,
      Null AS PLAN_CP,
      SUM(net_op_sales_amt_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5 A
JOIN (SELECT DISTINCT WEEK_NUM,quarter_num,quarter_454_num FROM CAL_LKUP WHERE WEEK_NUM BETWEEN (SELECT quarter_start_week_idnt FROM CURR_WK) AND (SELECT WEEK_IDNT FROM CURR_WK)) B
ON A.WEEK_NUM = B.WEEK_NUM
WHERE CHANNEL in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;


-- ** STORE-Traffic ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT 
      'FUNNEL' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Traffic' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS FISCAL_DESC,
      NULL  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      sum(store_traffic_plan) AS PLAN_OP,
      Null AS PLAN_CP,
      sum(store_traffic_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('FLS','RACK STORE')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;

-- ** STORE-ATV ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'ATV' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS FISCAL_DESC,
      NULL  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      CASE WHEN purchase_trips_plan = 0 THEN 0 ELSE (CAST (store_demand_amt_plan AS FLOAT)/CAST (purchase_trips_plan AS FLOAT)) END AS PLAN_OP,
      Null AS PLAN_CP,
      CASE WHEN purchase_trips_act = 0 THEN 0 ELSE (CAST (demand_amt_act AS FLOAT)/CAST (purchase_trips_act AS FLOAT)) END AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM (
      SELECT 
      WEEK_NUM,
      WEEK_START_DAY_DATE,
      WEEK_454_NUM,
      MONTH_SHORT_DESC,
      BANNER,
      CHANNEL,
      SUM(purchase_trips_plan) AS purchase_trips_plan,
      SUM(purchase_trips_act) AS purchase_trips_act,
      SUM(store_demand_amt_plan) AS store_demand_amt_plan,
      SUM(demand_amt_act) AS demand_amt_act
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('FLS','RACK STORE')
group by 1,2,3,4,5,6) A ;



-- ** STORE-Conversion ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT 
      'FUNNEL' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Conversion' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS FISCAL_DESC,
	  NULL AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      CASE WHEN store_traffic_plan = 0 THEN 0 ELSE CAST(purchase_trips_plan as FLOAT)/CAST(store_traffic_plan as FLOAT) END AS PLAN_OP,
      Null AS PLAN_CP,
      CASE WHEN store_traffic_act = 0 THEN 0 ELSE CAST(purchase_trips_act as FLOAT)/CAST(store_traffic_act as FLOAT) END AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM (
SELECT 
      WEEK_NUM,
      WEEK_START_DAY_DATE,
      WEEK_454_NUM,
      MONTH_SHORT_DESC,
      BANNER,
      CHANNEL,
      SUM(purchase_trips_plan) AS purchase_trips_plan,
      SUM(purchase_trips_act) AS purchase_trips_act,
      SUM(store_traffic_plan) AS store_traffic_plan,
      SUM(store_traffic_act) AS store_traffic_act
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('FLS','RACK STORE')
GROUP BY 1,2,3,4,5,6) A ;



-- ** DIGITAL-Sessions ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Sessions' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS FISCAL_DESC,
      NULL  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      SUM(digital_traffic_plan) AS PLAN_OP,
      Null AS PLAN_CP,
      SUM(sessions) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;


-- ** DIGITAL-AOV ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'AOV' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS FISCAL_DESC,
      NULL  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      CASE WHEN digital_orders_plan = 0 THEN 0 ELSE CAST (digital_demand_amt_plan as FLOAT)/CAST (digital_orders_plan as FLOAT) END AS PLAN_OP,
      Null AS PLAN_CP,
      CASE WHEN digital_orders_act = 0 THEN 0 ELSE CAST (demand_amt_act as FLOAT)/CAST (digital_orders_act as FLOAT) END AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM (
SELECT 
      WEEK_NUM,
      WEEK_START_DAY_DATE,
      WEEK_454_NUM,
      MONTH_SHORT_DESC,
      BANNER,
      CHANNEL,
      SUM(digital_demand_amt_plan) AS digital_demand_amt_plan,
      SUM(demand_amt_act) AS demand_amt_act,
      SUM(digital_orders_plan) AS digital_orders_plan,
      SUM(digital_orders_act) AS digital_orders_act
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6) A ;


-- ** DIGITAL-Conversion ** --
INSERT INTO WEEKLY_OPS_STANDUP
      SELECT 
      'FUNNEL' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Conversion' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12)))  AS FISCAL_DESC,
      NULL  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= ({start_date} -7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      CASE WHEN digital_traffic_plan = 0 THEN 0 ELSE CAST(digital_orders_plan as FLOAT)/CAST(digital_traffic_plan as FLOAT) END AS PLAN_OP,
      Null AS PLAN_CP,
      CASE WHEN sessions = 0 THEN 0 ELSE CAST(digital_orders_act as FLOAT)/CAST(sessions as FLOAT) END AS TY, 
      Null AS LY,
      NULL AS DAY_DATE
FROM (
SELECT 
      WEEK_NUM,
      WEEK_START_DAY_DATE,
      WEEK_454_NUM,
      MONTH_SHORT_DESC,
      BANNER,
      CHANNEL,
      SUM(digital_orders_plan) AS digital_orders_plan,
      SUM(digital_orders_act) as digital_orders_act,
      SUM(digital_traffic_plan) AS digital_traffic_plan,
      SUM(sessions) AS sessions
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6) A ;


----------PURCHASE TRIPS---------------------

CREATE VOLATILE MULTISET TABLE RETAIL_TRAN_DETAIL_FACT_VW_TEMP AS (
SELECT
  DTL.global_tran_id
, DTL.business_day_date
, DTL.tran_type_code
, DTL.line_item_seq_num
, DTL.line_item_activity_type_code
, DTL.line_item_activity_type_desc
, DTL.unique_source_id
, HDR.acp_id
, HDR.marketing_profile_type_ind
, HDR.deterministic_acp_id
, DTL.ringing_store_num
, HDR.fulfilling_store_num
, DTL.intent_store_num
, DTL.claims_destination_store_num
, DTL.register_num
, DTL.tran_num
, DTL.tran_version_num
, HDR.transaction_identifier
, HDR.transaction_identifier_source
, HDR.sa_tran_status_code
, HDR.original_global_tran_id
, HDR.reversal_flag
, HDR.data_source_code
, HDR.pre_sale_type_code
, HDR.order_num
, HDR.order_date
, HDR.followup_slsprsn_num
, HDR.pbfollowup_slsprsn_num
, DTL.tran_date
, DTL.tran_time
, DTL.sku_num
, DTL.upc_num
, DTL.line_item_merch_nonmerch_ind
, DTL.merch_dept_num
, DTL.nonmerch_fee_code
, DTL.line_item_promo_id
, DTL.line_net_usd_amt
, DTL.line_net_amt
, DTL.line_item_net_amt_currency_code
, DTL.line_item_quantity
, DTL.line_item_fulfillment_type
, DTL.line_item_order_type
, DTL.commission_slsprsn_num
, DTL.employee_discount_flag
, DTL.employee_discount_num
, DTL.employee_discount_usd_amt
, DTL.employee_discount_amt
, DTL.employee_discount_currency_code
, DTL.line_item_promo_usd_amt
, DTL.line_item_promo_amt
, DTL.line_item_promo_amt_currency_code
, DTL.merch_unique_item_id
, DTL.merch_price_adjust_reason
, DTL.line_item_capture_system
, DTL.original_business_date
, DTL.original_ringing_store_num
, DTL.original_register_num
, DTL.original_tran_num
, DTL.original_transaction_identifier
, DTL.original_line_item_usd_amt
, DTL.original_line_item_amt
, DTL.original_line_item_amt_currency_code
, DTL.line_item_regular_price
, DTL.line_item_regular_price_currency_code
, DTL.line_item_split_tax_seq_num
, DTL.line_item_split_tax_pct
, DTL.line_item_split_tax_type
, DTL.line_item_split_tax_amt
, DTL.dw_batch_date
, DTL.dw_sys_load_tmstp
, DTL.dw_sys_updt_tmstp
, DTL.error_flag
, DTL.tran_latest_version_ind
, DTL.item_source
, DTL.line_item_tax_amt
, DTL.line_item_tax_currency_code
, DTL.line_item_tax_usd_amt
, DTL.line_item_tax_exempt_flag
, DTL.line_item_tax_pct
, DTL.price_adj_code
, DTL.tran_line_id
, HDR.deterministic_profile_id
, DTL.financial_retail_tran_record_id
, DTL.financial_retail_tran_line_id
, DTL.original_financial_retail_tran_record_id
, DTL.ownership_model 
, DTL.partner_relationship_num
, DTL.partner_relationship_type_code
,(case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB') then 'NORDSTROM'
          when st.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then 'RACK'
          else null end as banner)
,(case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'N_STORE'
          when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'N_COM'
          when st.business_unit_desc in ('RACK', 'RACK CANADA') then 'R_STORE'
          when st.business_unit_desc in ('OFFPRICE ONLINE') then 'R_COM'
          else null end as channel)
,(case when dtl.line_net_usd_amt <  0 then dtl.line_item_quantity *(-1)
         when dtl.line_net_usd_amt = 0 then 0
         else dtl.line_item_quantity end as net_items)
,(case when coalesce(dtl.nonmerch_fee_code, '-999') = '6666' then 1 else 0 end as gc_flag)
,(case
        when DTL.line_item_order_type = 'CustInitWebOrder'
        and DTL.line_item_fulfillment_type = 'StorePickUp'
        and DTL.line_item_net_amt_currency_code = 'USD'
        then 808
        when DTL.line_item_order_type = 'CustInitWebOrder'
        and DTL.line_item_fulfillment_type = 'StorePickUp'
        and DTL.line_item_net_amt_currency_code = 'CAD'
        then 867
        else DTL.intent_store_num end as store_num)
,(case when DTL.line_net_usd_amt > 0 then coalesce(HDR.order_date, DTL.tran_date) else DTL.tran_date end as customer_shopped_date)
,(case when DTL.line_net_usd_amt > 0   then acp_id||store_num||customer_shopped_date
        else null end as trip_id)
FROM PRD_NAP_BASE_VWS.RETAIL_TRAN_DETAIL_FACT_VW DTL
JOIN PRD_NAP_BASE_VWS.RETAIL_TRAN_HDR_FACT HDR
ON DTL.global_tran_id = HDR.global_tran_id AND DTL.business_day_date = HDR.business_day_date
join PRD_NAP_BASE_VWS.store_dim as st
    on (case when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp'
                                 and dtl.line_item_net_amt_currency_code = 'CAD' then 867
             when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp'
                                 and dtl.line_item_net_amt_currency_code = 'USD' then 808
             else dtl.intent_store_num
         end) = st.store_num
WHERE DTL.business_day_date BETWEEN {start_date} - 90 AND {start_date}  
) WITH DATA PRIMARY INDEX (acp_id,business_day_date) ON COMMIT PRESERVE ROWS;


-- TRANSACTIONS (ERTM)
CREATE VOLATILE MULTISET TABLE TRANSACTIONS AS (
SELECT 
WEEK_IDNT,
WEEK_START_DAY_DATE,
WEEK_END_DAY_DATE,
CASE WHEN US_DMA_CODE = -1 THEN 'UNKNOWN'
	 WHEN US_DMA_CODE IS NOT NULL THEN 'US'
     WHEN CA_DMA_CODE IS NOT NULL THEN 'CA'
     ELSE 'UNKNOWN' END AS COUNTRY,
CASE WHEN STR.BUSINESS_UNIT_DESC IN ('FULL LINE','N.COM','FULL LINE CANADA','N.CA','TRUNK CLUB') THEN 'NORDSTROM'
	 WHEN STR.BUSINESS_UNIT_DESC IN ('RACK','OFFPRICE ONLINE', 'RACK CANADA') THEN 'NORDSTROM RACK'
	 END AS BANNER,
CASE WHEN STR.BUSINESS_UNIT_DESC IN ('FULL LINE') THEN 'FLS'
     WHEN STR.BUSINESS_UNIT_DESC IN ('N.COM') THEN 'N.COM'
     WHEN STR.BUSINESS_UNIT_DESC IN ('RACK') THEN 'RACK STORE'
     WHEN STR.BUSINESS_UNIT_DESC IN ('OFFPRICE ONLINE') THEN 'R.COM' END as CHANNEL,	 
COUNT(DISTINCT DTL.ACP_ID) AS CUST_COUNT,
SUM(LINE_NET_USD_AMT) AS NET_SPEND,
SUM(CASE WHEN DTL.LINE_NET_USD_AMT > 0 THEN LINE_NET_USD_AMT ELSE 0 END) AS GROSS_SPEND,
COUNT(DISTINCT CASE WHEN DTL.LINE_NET_USD_AMT > 0 THEN DTL.ACP_ID||STR.STORE_NUM||COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) END) AS TRIPS
FROM RETAIL_TRAN_DETAIL_FACT_VW_TEMP AS DTL
JOIN PRD_NAP_BASE_VWS.STORE_DIM AS STR
ON CASE WHEN LINE_ITEM_ORDER_TYPE LIKE 'CustInit%' AND LINE_ITEM_FULFILLMENT_TYPE = 'StorePickUp' AND DATA_SOURCE_CODE = 'COM'
		THEN RINGING_STORE_NUM ELSE INTENT_STORE_NUM END = STR.STORE_NUM
JOIN PRD_NAP_BASE_VWS.DAY_CAL_454_DIM B ON CASE WHEN LINE_NET_USD_AMT > 0 THEN COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) ELSE DTL.TRAN_DATE END = B.DAY_DATE
LEFT JOIN PRD_NAP_BASE_VWS.ANALYTICAL_CUSTOMER AC
ON DTL.ACP_ID = AC.ACP_ID
WHERE CASE WHEN LINE_NET_USD_AMT > 0 THEN COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) ELSE DTL.TRAN_DATE END BETWEEN ({start_date}-28) AND (SELECT WEEK_END_DAY_DATE FROM CURR_WK)
AND BUSINESS_UNIT_DESC IN ('FULL LINE', 'FULL LINE CANADA', 'N.CA', 'N.COM', 'OFFPRICE ONLINE', 'RACK', 'RACK CANADA', 'TRUNK CLUB')
AND NOT DTL.ACP_ID IS NULL
GROUP BY 1,2,3,4,5,6
) WITH DATA PRIMARY INDEX (WEEK_IDNT,BANNER) ON COMMIT PRESERVE ROWS;



-- PURCHASE TRIPS
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
'OUTCOMES' AS OPS_NAME,
BANNER,
CHANNEL,
'PURCHASE TRIPS' AS METRIC_NAME,
A.WEEK_NUM AS FISCAL_NUM,
CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS FISCAL_DESC,
NULL AS LABEL,
CASE WHEN WEEK_START_DAY_DATE <= ({start_date}-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
TRIPS_PLAN AS PLAN_OP,
NULL AS PLAN_CP,
TRIPS AS TY,
NULL AS LY,
NULL AS DAY_DATE
FROM 
(SELECT DISTINCT
COALESCE(A.BANNER, B.BANNER) AS BANNER,
COALESCE(A.CHANNEL, B.CHANNEL) AS CHANNEL,
COALESCE(A.WEEK_IDNT, B.WEEK_NUM) AS WEEK_NUM,
TRIPS_PLAN,
TRIPS
FROM 
(SELECT WEEK_IDNT, WEEK_START_DAY_DATE, WEEK_END_DAY_DATE, BANNER,CHANNEL,SUM(TRIPS) AS TRIPS
FROM TRANSACTIONS 
WHERE COUNTRY IN  ('US','UNKNOWN')
GROUP BY 1,2,3,4,5) A
FULL JOIN  T2DL_DAS_OSU.WEEKLY_OPS_CUSTOMER_CHANNEL_PLAN B 
ON A.WEEK_IDNT = B.WEEK_NUM
AND A.BANNER = B.BANNER
AND A.CHANNEL=B.CHANNEL) A
JOIN CAL_LKUP B
ON A.WEEK_NUM = B.WEEK_NUM
WHERE B.WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK);


-- Final table insertion

DELETE FROM {clarity_schema}.WEEKLY_OPS_STANDUP_AGG
WHERE OPS_NAME IN ('OUTCOMES','FUNNEL') -- Adding an extra filter since all Ops Scorecard will be using the same table
AND UPDATED_WEEK = (SELECT WEEK_IDNT FROM CURR_WK) ;


insert into {clarity_schema}.WEEKLY_OPS_STANDUP_AGG
select
ops_name               
    ,banner            
    ,channel               
    ,metric_name            
    ,fiscal_num            
    ,fiscal_desc            
    ,label                  
    ,rolling_fiscal_ind     
    ,plan_op                
    ,plan_cp               
    ,ty                    
    ,ly                     
    ,WEEK_IDNT AS updated_week            
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp      
    ,day_date 
FROM WEEKLY_OPS_STANDUP AS A, CURR_WK ;


-------------------------------------------------------------------------------------------------------------------------------------------------------------

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
