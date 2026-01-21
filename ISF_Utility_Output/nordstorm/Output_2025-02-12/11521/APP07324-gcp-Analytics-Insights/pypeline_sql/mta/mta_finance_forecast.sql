SET QUERY_BAND = 'App_ID=APP08743;
     DAG_ID=mta_finance_forecast_11521_ACE_ENG;
     Task_Name=mta_finance_forecast;'
     FOR SESSION VOLATILE;

/*


T2/Table Name: T2DL_DAS_MTA.FINANCE_FORECAST
Team/Owner: MOA/Nhan Le
Date Created/Modified: 2023-03-22

Note:
-- What is the the purpose of the table: CONTAINS MARKETING FINANCE FORECAST VALUES FOR REPORTING
-- What is the update cadence/lookback window: UPLOADED QUARTERLY

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from    {mta_t2_schema}.finance_forecast;

insert into {mta_t2_schema}.finance_forecast
select  "date"               
        ,"box"              
        ,marketing_type     
        ,cost               
        ,traffic_udv        
        ,orders            
        ,gross_sales        
        ,net_sales          
        ,"sessions"           
        ,session_orders  
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {mta_t2_schema}.finance_forecast_ldg
;

collect statistics column ("date")
                   ,column ("box")
                   ,column (marketing_type)
on {mta_t2_schema}.finance_forecast
;

-- drop staging table
drop table {mta_t2_schema}.finance_forecast_ldg
;


SET QUERY_BAND = NONE FOR SESSION;