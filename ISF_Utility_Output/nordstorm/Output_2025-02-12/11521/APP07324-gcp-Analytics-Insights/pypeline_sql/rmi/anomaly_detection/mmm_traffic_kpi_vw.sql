SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_traffic_kpi_vw_11521_ACE_ENG;
     Task_Name=mmm_traffic_kpi_vw;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.mmm_traffic_kpi_vw
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-10-28
--Note:
-- This view supports the Monthly Insights Dashboard.


REPLACE VIEW {kpi_scorecard_t2_schema}.mmm_traffic_kpi_vw
AS
LOCK ROW FOR ACCESS 
 select 
week_idnt,
store_num,
business_unit_desc,
platform_type,
cust_status,
engagement_cohort,
loyalty_status,
sum(total_traffic) as total_traffic,
sum(LW_total_traffic) as LW_total_traffic,
sum(LY_total_traffic) as LY_total_traffic,
sum(trips) as trips,
sum(LW_trips) as LW_trips,
sum(LY_trips) as LY_trips
from (
select 
 b.week_idnt,
store_num,
business_unit_desc,
platform_type,
cust_status,
engagement_cohort,
loyalty_status,
sum(total_traffic) as total_traffic,
CAST(0 AS FLOAT) as LW_total_traffic,
CAST(0 AS FLOAT) as LY_total_traffic,
sum(trips) as trips,
CAST(0 AS FLOAT) as LW_trips,
CAST(0 AS FLOAT) as LY_trips
FROM {kpi_scorecard_t2_schema}.mmm_traffic_kpi a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.activity_date_pacific = b.day_date
       GROUP BY 1,2,3,4,5,6,7
	   UNION ALL
SELECT
CASE WHEN MOD(b.week_idnt,100) > 51 THEN b.week_idnt +49
ELSE b.week_idnt +1
END AS week_idnt,
store_num,
business_unit_desc,
platform_type,
cust_status,
engagement_cohort,
loyalty_status,
CAST(0 AS FLOAT) as total_traffic,
sum(total_traffic) as LW_total_traffic,
CAST(0 AS FLOAT) as LY_total_traffic,
CAST(0 AS FLOAT) as trips,
sum(trips) as LW_trips,
CAST(0 AS FLOAT) as LY_trips
FROM {kpi_scorecard_t2_schema}.mmm_traffic_kpi a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.activity_date_pacific = b.day_date
		 GROUP BY 1,2,3,4,5,6,7
		 UNION ALL
SELECT 
CASE WHEN MOD(b.week_idnt,100) > 52 THEN b.week_idnt +51
ELSE b.week_idnt +99
END AS week_idnt,
store_num,
business_unit_desc,
platform_type,
cust_status,
engagement_cohort,
loyalty_status,
CAST(0 AS FLOAT) as total_traffic,
CAST(0 AS FLOAT) as LW_total_traffic,
sum(total_traffic) as LY_total_traffic,
CAST(0 AS FLOAT) as trips,
CAST(0 AS FLOAT) as LW_trips,
sum(trips) as LY_trips
FROM {kpi_scorecard_t2_schema}.mmm_traffic_kpi a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.activity_date_pacific = b.day_date
		 GROUP BY 1,2,3,4,5,6,7)c
        WHERE week_idnt between (SELECT MIN(week_idnt)
                     FROM {kpi_scorecard_t2_schema}.mmm_traffic_kpi a
                       LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.activity_date_pacific = b.day_date)
					   and (SELECT MAX(week_idnt)
                     FROM {kpi_scorecard_t2_schema}.mmm_traffic_kpi a
                       LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.activity_date_pacific = b.day_date)
 GROUP BY 1,2,3,4,5,6,7;
 
 SET QUERY_BAND = NONE FOR SESSION;


