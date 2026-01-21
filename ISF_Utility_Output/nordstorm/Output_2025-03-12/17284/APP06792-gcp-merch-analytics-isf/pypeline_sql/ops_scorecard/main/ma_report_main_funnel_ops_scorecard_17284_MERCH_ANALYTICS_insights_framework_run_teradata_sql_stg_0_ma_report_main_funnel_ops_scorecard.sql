------------------------------------------------------------
-- FUNNEL & TOPLINE OPS SCORECARD --
-- Date Created: 09/26/2023
-- Last Updated: 04/18/2024 -- By Shaila Karole
-- Author: Soutrik Saha
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
SELECT
      WEEK_IDNT,
      WEEK_START_DAY_DATE,
      WEEK_END_DAY_DATE,
      MONTH_IDNT,
      MONTH_START_DAY_DATE,
      MONTH_END_DAY_DATE
FROM PRD_NAP_BASE_VWS.DAY_CAL_454_DIM
WHERE day_date = CURRENT_DATE - 7
) WITH data PRIMARY INDEX (WEEK_IDNT) ON COMMIT PRESERVE rows ;


CREATE MULTISET VOLATILE TABLE CAL_LKUP AS (
SELECT DISTINCT
      a.WEEK_NUM,
      a.WEEK_454_NUM,
      a.MONTH_SHORT_DESC,
      b.WEEK_START_DAY_DATE,
      b.WEEK_END_DAY_DATE,
      a.MONTH_NUM,
      b.MONTH_START_DAY_DATE,
      b.MONTH_END_DAY_DATE,
      a.YEAR_NUM
FROM PRD_NAP_BASE_VWS.DAY_CAL a
LEFT JOIN PRD_NAP_BASE_VWS.DAY_CAL_454_DIM b
on a.WEEK_NUM = b.WEEK_IDNT and a.MONTH_NUM = b.MONTH_IDNT
WHERE b.day_date BETWEEN (CURRENT_DATE-30) AND (CURRENT_DATE+30)
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
CASE WHEN business_unit_desc IN ('FULL LINE','N.COM') THEN 'NORDSTROM'
     WHEN business_unit_desc IN ('OFFPRICE ONLINE','RACK') THEN 'NORDSTROM RACK' END as banner
,CASE WHEN business_unit_desc IN ('FULL LINE') THEN 'FLS'
     WHEN business_unit_desc IN ('N.COM') THEN 'N.COM'
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
FROM
(select
dc.year_num
,dc.month_454_num
,dc.week_of_fyr
,dc.week_num
,tran_date AS day_date
,dc.day_date + 364*(2024 - year_num) as ty_date
,CASE WHEN fsdmtf.business_unit_desc IN ('FULL LINE','FULL LINE CANADA','RACK','RACK CANADA') THEN 'STORE'
WHEN fsdmtf.business_unit_desc IN ('N.COM','N.CA','OFFPRICE ONLINE') THEN 'DIGITAL' END as bu_type
,fsdmtf.business_unit_desc
,st.comp_status_vs_21
,st.comp_status_vs_22
,st.comp_status_vs_23
,sum(case when visitors is null then store_traffic else visitors end) as traffic
,sum(store_traffic) as store_traffic
,sum(visitors) as visitors
,sum(viewing_visitors) as viewing_visitors
,sum(adding_visitors) as adding_visitors
,sum(ordering_visitors) as ordering_visitors
,sum(case when platform_code = 'Direct to Customer (DTC)' then 0 else orders_count end) as orders_count
,sum(store_purchase_trips) as store_purchase_trips
,sum(case when orders_count is null then store_purchase_trips
when platform_code = 'Direct to Customer (DTC)' then 0 else orders_count end) as orders_or_trips
,sum(reported_demand_usd_amt_excl_bopus + bopus_attr_digital_reported_demand_usd_amt) as reported_demand_usd_amt_bopus_in_digital
,sum(reported_demand_units_excl_bopus + bopus_attr_digital_reported_demand_units) as reported_demand_units_bopus_in_digital
,sum(bopus_attr_digital_gross_demand_usd_amt) as bopus_attr_digital_gross_demand
,sum(bopus_attr_digital_gross_demand_units) as bopus_attr_digital_gross_demand_units
,sum(reported_demand_usd_amt_excl_bopus) as reported_demand_usd_amt_excl_bopus
,sum(reported_demand_units_excl_bopus) as reported_demand_units_excl_bopus
,sum(canceled_gross_demand_usd_amt_excl_bopus) as canceled_gross_demand_usd_amt_excl_bopus
,sum(canceled_gross_demand_units_excl_bopus) as canceled_gross_demand_units_excl_bopus
,sum(fulfilled_demand_usd_amt) as fulfilled_demand_usd_amt
,sum(fulfilled_demand_units) as fulfilled_demand_units
,sum(abs(actual_product_returns_usd_amt)) as actual_product_returns_usd_amt
,sum(abs(actual_product_returns_units)) as actual_product_returns_units
,sum(merch_net_sales_usd_amt) as merch_net_sales_usd_amt
,sum(merch_net_sales_units) as merch_net_sales_units
,sum(op_gmv_usd_amt) as op_gmv_usd_amt
from PRD_NAP_DSA_AI_BASE_VWS.FINANCE_SALES_DEMAND_MARGIN_TRAFFIC_FACT as fsdmtf
inner join PRD_NAP_BASE_VWS.DAY_CAL as dc on fsdmtf.tran_date = dc.day_date
inner join E2E_STORE_DIM_VW as st on fsdmtf.store_num = st.store_num
where 1=1
and tran_date between '2024-02-04' and current_date-1
group by 1,2,3,4,5,6,7,8,9,10,11
--ORDER BY 1,2,3,4,5,6,7,8,9,10
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
      SUM(net_op_sales) as net_op_sales_amt_plan
FROM T2DL_DAS_OSU.E2E_STORE_SALES_PLAN a
JOIN PRD_NAP_BASE_VWS.DAY_CAL b ON a.day_date = b.day_date
WHERE business_unit_desc in ('FULL LINE','RACK')
and WEEK_NUM IN (SELECT distinct WEEK_NUM FROM CAL_LKUP)
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (WEEK_NUM, BANNER, CHANNEL) ON COMMIT PRESERVE rows ;


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
      SUM(demand_amt) as digital_demand_amt_plan
FROM T2DL_DAS_OSU.E2E_DIGITAL_FUNNEL_PLAN a
JOIN PRD_NAP_BASE_VWS.DAY_CAL b  ON a.day_date = b.day_date
WHERE business_unit_desc in ('N.COM','OFFPRICE ONLINE')
and WEEK_NUM IN (SELECT distinct WEEK_NUM FROM CAL_LKUP)
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
      CAST(COALESCE(B.store_traffic_plan,0) AS FLOAT) AS store_traffic_plan,
      CAST(COALESCE(A.digital_traffic_act,0) AS FLOAT) AS digital_traffic_act,
      CAST(COALESCE(D.digital_traffic_plan,0) AS FLOAT) AS digital_traffic_plan,
      CAST(COALESCE(A.purchase_trips_act,0) AS FLOAT) AS purchase_trips_act,
      CAST(COALESCE(B.purchase_trips_plan,0) AS FLOAT) AS purchase_trips_plan,
      CAST(COALESCE(A.net_op_sales_amt_act,0) AS FLOAT) AS net_op_sales_amt_act,
      CAST(COALESCE(C.net_op_sales_amt_plan,0) AS FLOAT) AS net_op_sales_amt_plan,
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

-- NORDSTROM FUNNEL & TOPLINE | STORE --

-- ** STORE-Traffic ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL_&_TOPLINE' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Traffic' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      'WEEK' AS FISCAL_DESC,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12)))  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= (CURRENT_DATE-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      sum(store_traffic_plan) AS PLAN_OP,
      Null AS PLAN_CP,
      sum(store_traffic_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('FLS','RACK STORE')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;

-- ** STORE-Conversion ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL_&_TOPLINE' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Conversion' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      'WEEK' AS FISCAL_DESC,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12))) AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= (CURRENT_DATE-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
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


-- ** STORE-ATV ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL_&_TOPLINE' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'ATV' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      'WEEK' AS FISCAL_DESC,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12)))  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= (CURRENT_DATE-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
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


-- ** STORE-Ops GMV ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL_&_TOPLINE' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Ops GMV' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      'WEEK' AS FISCAL_DESC,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12)))  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= (CURRENT_DATE-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      SUM(net_op_sales_amt_plan) AS PLAN_OP,
      Null AS PLAN_CP,
      SUM(net_op_sales_amt_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('FLS','RACK STORE')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;

-- ** DIGITAL-Traffic ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL_&_TOPLINE' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Traffic' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      'WEEK' AS FISCAL_DESC,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12)))  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= (CURRENT_DATE-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      SUM(digital_traffic_plan) AS PLAN_OP,
      Null AS PLAN_CP,
      SUM(digital_traffic_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;

-- ** DIGITAL-Conversion ** --
INSERT INTO WEEKLY_OPS_STANDUP
      SELECT
      'FUNNEL_&_TOPLINE' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Conversion' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      'WEEK' AS FISCAL_DESC,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12)))  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= (CURRENT_DATE-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      CASE WHEN digital_traffic_plan = 0 THEN 0 ELSE CAST(digital_orders_plan as FLOAT)/CAST(digital_traffic_plan as FLOAT) END AS PLAN_OP,
      Null AS PLAN_CP,
      CASE WHEN digital_traffic_act = 0 THEN 0 ELSE CAST(digital_orders_act as FLOAT)/CAST(digital_traffic_act as FLOAT) END AS TY,
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
      SUM(digital_traffic_act) AS digital_traffic_act
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6) A ;


-- ** DIGITAL-AOV ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL_&_TOPLINE' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'AOV' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      'WEEK' AS FISCAL_DESC,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12)))  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= (CURRENT_DATE-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
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


-- ** DIGITAL-Demand ** --
INSERT INTO WEEKLY_OPS_STANDUP
SELECT
      'FUNNEL_&_TOPLINE' AS OPS_NAME,
      BANNER,
      CHANNEL,
      'Demand' AS METRIC_NAME,
      WEEK_NUM AS FISCAL_NUM,
      'WEEK' AS FISCAL_DESC,
      CONCAT(MONTH_SHORT_DESC,' ','WK ',CAST(WEEK_454_NUM AS VARCHAR(12)))  AS LABEL,
      CASE WHEN WEEK_START_DAY_DATE <= (CURRENT_DATE-7) THEN 1 ELSE 0 END AS ROLLING_FISCAL_IND,
      SUM(digital_demand_amt_plan) AS PLAN_OP,
      Null AS PLAN_CP,
      SUM(demand_amt_act) AS TY,
      Null AS LY,
      NULL AS DAY_DATE
FROM FT_OPS_5
WHERE WEEK_START_DAY_DATE BETWEEN (SELECT (WEEK_START_DAY_DATE -14) FROM CURR_WK) AND (SELECT (WEEK_START_DAY_DATE +27) FROM CURR_WK)
AND CHANNEL in ('N.COM','R.COM')
GROUP BY 1,2,3,4,5,6,7,8,10,12,13;

----------------------------------------------------------------------------------------------------------------------------------------------------

-- Final table insertion

DELETE FROM PRD_NAP_DSA_AI_BASE_VWS.WEEKLY_OPS_STANDUP_AGG
WHERE OPS_NAME = 'FUNNEL_&_TOPLINE' -- Adding an extra filter since all Ops Scorecard will be using the same table
AND UPDATED_WEEK = (SELECT WEEK_IDNT FROM CURR_WK);

INSERT INTO PRD_NAP_DSA_AI_BASE_VWS.WEEKLY_OPS_STANDUP_AGG
SELECT
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
	WEEK_IDNT AS UPDATED_WEEK,
	CURRENT_TIMESTAMP as update_timestamp,
	DAY_DATE
FROM WEEKLY_OPS_STANDUP AS A, CURR_WK;

----------------------------------------------------------------------------------------------------------------------------------------------------

