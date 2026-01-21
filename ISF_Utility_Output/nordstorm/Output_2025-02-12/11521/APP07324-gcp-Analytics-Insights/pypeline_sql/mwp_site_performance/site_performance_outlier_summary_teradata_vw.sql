SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=site_performance_outlier_summary_teradata_vw_11521_ACE_ENG;
     Task_Name=site_performance_outlier_summary_teradata_vw;'
     FOR SESSION VOLATILE;

/*

T2/View Name: T2DL_DAS_DIGENG.site_performance_outlier_summary_vw
Team/Owner: Digital Product Analytics
Date Modified: 04/06/2023

Note:
-- Calculate 100 day rolling average and dynamically store data in the view.

*/

REPLACE VIEW {mwp_t2_schema}.site_performance_outlier_summary_vw 
AS 
LOCK ROW FOR ACCESS
SELECT
    td.eventtime  
    , td.navigationtype 
    , td.pagetype 
    , td.experience 
    , td.brand_country 
    , td.avg_page_interactive 
    , AVG(yd.avg_page_interactive) as pageinteractive_100_day_avg
    , STDDEV_SAMP(yd.avg_page_interactive) as pageinteractive_100_day_std_dev
    FROM {mwp_t2_schema}.site_performance_outlier_summary td
	 	LEFT JOIN {mwp_t2_schema}.site_performance_outlier_summary yd
    	ON (td.eventtime - yd.eventtime) BETWEEN 0 AND 101
   	    AND td.experience = yd.experience
        AND td.navigationtype = yd.navigationtype
        AND td.pagetype = yd.pagetype
        AND td.brand_country = yd.brand_country
        GROUP BY 1, 2, 3, 4, 5, 6 
        HAVING COUNT(DISTINCT yd.eventtime) = 100
;

SET QUERY_BAND = NONE FOR SESSION;