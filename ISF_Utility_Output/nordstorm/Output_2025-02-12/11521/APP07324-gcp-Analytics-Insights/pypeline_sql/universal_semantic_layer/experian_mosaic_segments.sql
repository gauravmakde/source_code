/*

T2/Table Name: T2DL_DAS_USL.experian_mosaic_segments
Team/Owner: Customer Analytics/ Brian McGrane
Date Created/Modified: 05/31/2024

*/



SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=usl_experian_mosaic_segments_11521_ACE_ENG;
     Task_Name=usl_experian_mosaic_segments;'
     FOR SESSION VOLATILE;



delete 
from    {usl_t2_schema}.experian_mosaic_segments 
;

insert into {usl_t2_schema}.experian_mosaic_segments

select
     mosaic_segment_id
     ,mosaic_segment
     ,mosaic_segment_household_pct
     ,mosaic_segment_pop_pct
     ,mosaic_segment_pop
     ,mosaic_group_id
     ,mosaic_group
     ,mosaic_group_summary
     ,mosaic_segment_features
     ,mosaic_segment_summary
     ,mosaic_segment_description
     ,mosaic_group_howtomarket
     ,mosaic_segment_link
     ,mosaic_segment_field_name
     ,mosaic_group_field_name
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {usl_t2_schema}.experian_mosaic_segments_ldg
;

collect statistics column (mosaic_segment_id)
on {usl_t2_schema}.experian_mosaic_segments
;

-- drop staging table
drop table {usl_t2_schema}.experian_mosaic_segments_ldg
;

SET QUERY_BAND = NONE FOR SESSION;
