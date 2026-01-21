-- This file uses Spark sql to pull data from S3 or Hive and move it to a new s3 location in a format needed for TPT.

create or replace temporary view experian_mosaic_segments_csv
         (
          mosaic_segment_id string
          ,mosaic_segment string
          ,mosaic_segment_household_pct decimal(5,4)
          ,mosaic_segment_pop_pct decimal(5,4)
          ,mosaic_segment_pop int
          ,mosaic_group_id string
          ,mosaic_group string
          ,mosaic_group_summary string
          ,mosaic_segment_features string
          ,mosaic_segment_summary string
          ,mosaic_segment_description string
          ,mosaic_group_howtomarket string
          ,mosaic_segment_link string
          ,mosaic_segment_field_name string
          ,mosaic_group_field_name string
         )
 USING CSV
 OPTIONS (path "s3://analytics-insights-triggers/cco_strategy/experian_mosaic_segments.csv",
         sep ",",
         header "true");


insert overwrite table experian_mosaic_segments_ldg_output
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
from experian_mosaic_segments_csv
;
