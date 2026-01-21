SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=site_performance_monthly_detail_11521_ACE_ENG;
     Task_Name=site_performance_monthly_detail;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_DIGENG.site_performance_monthly_detail
Team/Owner: Digital Product Analytics
Date Modified: 04/06/2023

Note:
-- Data transfer from T2_landing_table to T2_final_table
-- Update Cadence - Weekly

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

create Multiset VOLATILE table MBR_date_table 
as (  
select	
	distinct month_idnt, month_start_day_date , month_end_day_date 
	from PRD_NAP_USR_VWS.DAY_CAL_454_DIM	
	where day_date between {start_date} and {end_date}
	)
WITH data PRIMARY INDEX (month_idnt) ON COMMIT PRESERVE ROWS  
;

delete 
from    {mwp_t2_schema}.site_performance_monthly_detail
where   month_idnt in (select month_idnt from MBR_date_table)
;

insert into {mwp_t2_schema}.site_performance_monthly_detail
select navigationtype   
	    , pagetype 
	    , experience 	
	    , brand_country  
	    , p25_pageinteractive 
	    , p50_pageinteractive 
	    , p75_pageinteractive 
	    , p90_pageinteractive 
	    , p95_pageinteractive 
	    , p99_pageinteractive 
	    , Soft_Navs 
	    , page_views 
	    , month_idnt 
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {mwp_t2_schema}.site_performance_monthly_detail_ldg
where   month_idnt in (select month_idnt from MBR_date_table)  
;

collect statistics column (month_idnt)
on {mwp_t2_schema}.site_performance_monthly_detail
;  

-- drop staging table
drop table {mwp_t2_schema}.site_performance_monthly_detail_ldg
;

SET QUERY_BAND = NONE FOR SESSION;