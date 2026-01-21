SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=dashboard_catalog_11521_ACE_ENG;
     Task_Name=dashboard_catalog;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_BIE_DEV.dashboard_catalog
Owner: Analytics Engineering
Modified: 2023-03-08

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

delete
from   {t2_schema}.dashboard_catalog
;

insert into {t2_schema}.dashboard_catalog

select
    is_featured
    , has_rms
    , analyst
    , asset_type
    , business_area
    , subject_area
    , dashboard_name
    , "description"
    , "data_source"
    , "database"
    , update_frequency
    , dashboard_url
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

from  {t2_schema}.dashboard_catalog_ldg 

where 1=1
;

-- drop staging table
DROP TABLE {t2_schema}.dashboard_catalog_ldg; 

SET QUERY_BAND = NONE FOR SESSION;