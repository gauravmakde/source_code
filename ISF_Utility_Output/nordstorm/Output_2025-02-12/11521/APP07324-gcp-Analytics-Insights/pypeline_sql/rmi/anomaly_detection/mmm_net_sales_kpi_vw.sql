SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_net_sales_kpi_vw_11521_ACE_ENG;
     Task_Name=mmm_net_sales_kpi_vw;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.mmm_net_sales_kpi_vw
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-10-28
--Note:
-- This view supports the Monthly Insights Dashboard.


REPLACE VIEW {kpi_scorecard_t2_schema}.mmm_net_sales_kpi_vw
AS
LOCK ROW FOR ACCESS 
select 
week_idnt,
business_unit_desc,
store_num,
bill_zip_code,
price_type,
order_platform_type,
loyalty_status,
engagement_cohort,
cust_status,
division_name,
subdivision_name,
sum(jwn_reported_net_sales_amt) as jwn_reported_net_sales_amt,
sum(LW_jwn_reported_net_sales_amt) as LW_jwn_reported_net_sales_amt,
sum(LY_jwn_reported_net_sales_amt) as LY_jwn_reported_net_sales_amt
from (
select 
 b.week_idnt,
business_unit_desc,
store_num,
bill_zip_code,
price_type,
order_platform_type,
loyalty_status,
engagement_cohort,
cust_status,
division_name,
subdivision_name,
sum(jwn_reported_net_sales_amt) as jwn_reported_net_sales_amt,
CAST(0 AS FLOAT) as LW_jwn_reported_net_sales_amt,
CAST(0 AS FLOAT) as LY_jwn_reported_net_sales_amt
FROM {kpi_scorecard_t2_schema}.MMM_NET_SALES_KPI a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.business_date = b.day_date
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11
	   UNION ALL
SELECT
CASE WHEN MOD(b.week_idnt,100) > 51 THEN b.week_idnt +49
ELSE b.week_idnt +1
END AS week_idnt,
business_unit_desc,
store_num,
bill_zip_code,
price_type,
order_platform_type,
loyalty_status,
engagement_cohort,
cust_status,
division_name,
subdivision_name,
CAST(0 AS FLOAT) as jwn_reported_net_sales_amt,
SUM(jwn_reported_net_sales_amt) as LW_jwn_reported_net_sales_amt,
CAST(0 AS FLOAT) as LY_jwn_reported_net_sales_amt
FROM {kpi_scorecard_t2_schema}.MMM_NET_SALES_KPI a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.business_date = b.day_date
		 GROUP BY 1,2,3,4,5,6,7,8,9,10,11
		 UNION ALL
SELECT 
CASE WHEN MOD(b.week_idnt,100) > 52 THEN b.week_idnt +51
ELSE b.week_idnt +99
END AS week_idnt,
business_unit_desc,
store_num,
bill_zip_code,
price_type,
order_platform_type,
loyalty_status,
engagement_cohort,
cust_status,
division_name,
subdivision_name,
CAST(0 AS FLOAT) as jwn_reported_net_sales_amt,
CAST(0 AS FLOAT) as LW_jwn_reported_net_sales_amt,
SUM(jwn_reported_net_sales_amt) as LY_jwn_reported_net_sales_amt
FROM {kpi_scorecard_t2_schema}.MMM_NET_SALES_KPI a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.business_date = b.day_date
		 GROUP BY 1,2,3,4,5,6,7,8,9,10,11)c
        WHERE week_idnt between (SELECT MIN(week_idnt)
                     FROM {kpi_scorecard_t2_schema}.MMM_NET_SALES_KPI a
                       LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.business_date = b.day_date)
					   and (SELECT MAX(week_idnt)
                     FROM {kpi_scorecard_t2_schema}.MMM_NET_SALES_KPI a
                       LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.business_date = b.day_date)
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11;
 
SET QUERY_BAND = NONE FOR SESSION;