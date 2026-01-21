SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_ct_segment_agg_11521_ACE_ENG;
     Task_Name=ct_segment_agg;'
     FOR SESSION VOLATILE;

/*
T2DL_DAS_MOTHERSHIP.CT_SEGMENT_AGG
Description: get segmentation model results but only keep rows where segment changes. Fewer rows = faster join later
this is also the slowest step in the mothership pipeline, so if we run it ahead of time then the morning pipeline is faster
Contacts: Matthew Bond, Analytics
*/

--customer_prediction_core_target_segment table updates ~monthly

DELETE FROM {mothership_t2_schema}.CT_SEGMENT_AGG WHERE scored_date > '2018-01-01';

INSERT INTO {mothership_t2_schema}.CT_SEGMENT_AGG
with combo as (
select acp_id, scored_date, predicted_ct_segment 
from T2DL_DAS_CUSTOMBER_MODEL_ATTRIBUTE_PRODUCTIONALIZATION.customer_prediction_core_target_segment_hist
-- only for backfills, ask Julie Liu to get data from s3
--    UNION ALL
--select acp_id, scored_date, predicted_ct_segment
--from T2DL_DAS_CUSTOMBER_MODEL_ATTRIBUTE_PRODUCTIONALIZATION.customer_prediction_core_target_segment_hist_old
)
SELECT
acp_id
, scored_date
, predicted_ct_segment as predicted_segment
, LAG (predicted_ct_segment, 1) OVER (PARTITION BY acp_id ORDER BY scored_date) AS seg_lag
, LEAD (predicted_ct_segment, 1) OVER (PARTITION BY acp_id ORDER BY scored_date) AS seg_lead
, CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM combo
QUALIFY predicted_segment <> seg_lag OR predicted_segment <> seg_lead
;

COLLECT STATISTICS COLUMN (acp_id) ON {mothership_t2_schema}.CT_SEGMENT_AGG;

SET QUERY_BAND = NONE FOR SESSION;