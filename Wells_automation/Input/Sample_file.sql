INSERT INTO {{ params.stg_dataset_nm}}.CURATION_STYLING_INTERACTION_FACT
(customer_id, customer_id_type, curation_curator_id, eventtime_pst, activity_date, styling_event, curation_id, channel, platform)
SELECT customer_id, customer_id_type, curation_curator_id, eventtime_pst, CAST(eventtime_pst AS DATE) as activity_date, styling_event, curation_id, channel, platform
FROM abc.CURATION_STYLING_INTERACTION_STG;

SET QUERY_BAND = NONE FOR SESSION;