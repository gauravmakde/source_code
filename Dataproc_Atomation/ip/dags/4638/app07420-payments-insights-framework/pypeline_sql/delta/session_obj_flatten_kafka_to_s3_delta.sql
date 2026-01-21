--creating persistant table to use for other event joins
-- noqa: disable=all
CREATE TABLE IF NOT EXISTS {delta_schema_name}.session_analytical_object_flatten_delta
(
    sessionId string,
    sessionLastUpdatedTime timestamp,
    sessionAlgorithmId string,
    sessionStartTime timestamp,
    sessionEndTime timestamp,
    sessionExperience string,
    sessionChannel struct<channelCountry:string,channelBrand:string,sellingChannel:string>,
    session_event_name string,
    session_event_id string,
    activity_date date,
    year int,
    month int,
    day int,
    batch_id int,
    batch_date date,
    rcd_load_tmstp timestamp,
    rcd_update_tmstp timestamp
)
USING DELTA
LOCATION 's3://{delta_s3_bucket}/session_event_delta/session_analytical_object_flatten_delta/';
-- noqa: enable=all

--picking up the latest version of the record from kafka
--CREATE TEMPORARY VIEW session_analytical_object_temp AS
--SELECT
--    *,
--    ROW_NUMBER() OVER (PARTITION BY id ORDER BY lastUpdatedTime DESC) as rownumber
--FROM kafka_customer_session_analytical_avro_value;

--picking up the latest version of the record
CREATE TEMPORARY VIEW session_analytical_object_parquet_dedupe AS
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY value.id ORDER BY value.lastUpdatedTime DESC) as rownumber
FROM {event_parquet_schema_name}.customer_sessions_parquet
WHERE YEAR = 2023 AND MONTH = 06 AND DAY BETWEEN 01 AND 10;

CREATE TEMPORARY VIEW session_analytical_object_parquet_latest AS
SELECT *
FROM session_analytical_object_parquet_dedupe
WHERE rownumber = 1;

--Delete all data for next batch
DELETE FROM {delta_schema_name}.session_analytical_object_flatten_delta;

--inserting data into the above persistant temp table
WITH CTE AS (
    SELECT
        batch_id,
        batch_date
    FROM {delta_schema_name}.delta_elt_control_tbl
    WHERE
        dag_name = 'session_event_delta'
        AND subject_area_name = 'session_event_metadata'
        AND is_latest = 'Y'
        AND active_load_ind = 'Y'
)

INSERT INTO TABLE {delta_schema_name}.session_analytical_object_flatten_delta
SELECT DISTINCT
    value.id as sessionId,
    value.lastUpdatedTime as sessionLastUpdatedTime,
    value.algorithmId as sessionAlgorithmId,
    value.startTime as sessionStartTime,
    value.endTime as sessionEndTime,
    value.experience as sessionExperience,
    value.channel as sessionChannel,
    customerSessionEvents.name as session_event_name,
    customerSessionEvents.id as session_event_id,
    CAST(value.startTime as date) as activity_date,
    year,
    month,
    day,
    (SELECT batch_id FROM CTE) as batch_id,
    (SELECT batch_date FROM CTE) as batch_date,
    CURRENT_TIMESTAMP() as rcd_load_tmstp,
    CURRENT_TIMESTAMP() as rcd_update_tmstp
FROM
    session_analytical_object_parquet_latest
        lateral view EXPLODE(value.customerSessionEvents) exploded_customerSessionEvents as customerSessionEvents;

VACUUM {delta_schema_name}.session_analytical_object_flatten_delta;

-- noqa: disable=all
OPTIMIZE {delta_schema_name}.session_analytical_object_flatten_delta ZORDER BY (sessionid,session_event_id);
-- noqa: enable=all
