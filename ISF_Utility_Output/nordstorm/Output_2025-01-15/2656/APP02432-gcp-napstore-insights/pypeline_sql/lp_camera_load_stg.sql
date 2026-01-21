SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=lp_camera_load_2656_napstore_insights;
Task_Name=lpcamera_events_to_csv_load_0_stg;'
FOR SESSION VOLATILE;

--Source
--Reading from s3 bucket
CREATE TEMPORARY VIEW lp_camera_source USING AVRO
OPTIONS (path "s3://{lp_camera_input_bucket}/lp-camera/integrations/lp-camera-aggregated-events/ongoing_data/");

CREATE TEMPORARY VIEW lp_camera_source_daily as
    (select * from lp_camera_source where datestamp = current_date());

-- Writing Avro S3 to Semantic Layer staging table:
insert
    overwrite table lp_camera_stg
select distinct
        source                      AS SOURCE_TYPE,
        measurementtime             AS MEASUREMENT_TIME,
        start                       AS START_TIME,
        end                         AS END_TIME,
        location.type               AS MEASUREMENT_LOCATION_TYPE,
        location.id                 AS MEASUREMENT_LOCATION_ID,
        location.name               AS MEASUREMENT_LOCATION_NAME,
        location.parentid           AS MEASUREMENT_PARENT_ID,
        location.storeid            AS LOCATION_STORE_NUM,
        location.time_zone          AS LOCATION_TIME_ZONE,
        storeid                     AS STORE_NUM,
        type                        AS TRAFFIC_TYPE,
        value                       AS TRAFFIC_COUNT,
        validity                    AS VALIDITY
from lp_camera_source_daily;
