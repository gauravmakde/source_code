-- noqa: disable=all
CREATE TABLE IF NOT EXISTS {delta_schema_name}.customer_activity_authenticated_session_delta
(
  sessionId string,
  sessionLastUpdatedTime timestamp,
  sessionAlgorithmId string,
  sessionStartTime timestamp,
  sessionEndTime timestamp,
  sessionExperience string,
  sessionChannel struct<channelCountry:string,channelBrand:string,sellingChannel:string>,
  sessionEventName string,
  sessionEventId string,
  eventId string,
  eventHeaderEventTime timestamp,
  eventSystemTime timestamp,
  eventHeaders map<string,binary>,
  source struct<channelcountry:string,channel:string,platform:string,feature:string,servicename:string,store:string,register:string>,
  customerbefore struct<idtype:string,id:string>,
  customer struct<idtype:string,id:string>,
  credentials struct<iconid:string,shopperid:string,tokenizingcreditcardhvt:struct<value:string,authority:string,strategy:string,dataclassification:string>,creditcardhvt:struct<value:string,authority:string,dataclassification:string>,fraudtoken:string,useragent:string,tokenizingipaddress:struct<value:string,authority:string,strategy:string,dataclassification:string>,ipaddress:struct<value:string,authority:string,dataclassification:string>,experimentid:string,adtrackingstatus:string,advertisingid:string,applevendorid:string,firebaseinstanceid:string>,
  persistentsigninmode string,
  clientInstanceId string,
  activity_date date,
  batch_id int,
  batch_date date,
  rcd_load_tmstp timestamp,
  rcd_update_tmstp timestamp
)
USING DELTA
PARTITIONED BY (activity_date)
LOCATION 's3://{delta_s3_bucket}/session_event_delta/customer_activity_authenticated_session_delta/';
-- noqa: enable=all

CREATE TEMPORARY VIEW customer_activity_authenticated_dedupe AS
WITH session_dates AS (
    select
        min(cast(sessionEndTime as date)) - 2 AS min_date,
        max(cast(sessionEndTime as date)) + 1 AS max_date
    from {delta_schema_name}.session_analytical_object_flatten_delta
)

SELECT
    cast(element_at(headers, 'Id') as string) as eventId,
    timestamp_millis(cast(cast(element_at(headers, 'EventTime') as string) as bigint)) as eventHeaderEventTime,
    timestamp_millis(cast(cast(element_at(headers, 'SystemTime') as string) as bigint)) as eventSystemTime,
    headers as eventHeaders,
    value.source as source,
    value.customerBefore as customerBefore,
    value.customer as customer,
    value.credentials as credentials,
    value.persistentSignInMode as persistentSignInMode,
    value.clientInstanceId as clientInstanceId,
    row_number() over (
        partition by cast(element_at(headers, 'Id') as string)
        order by timestamp_millis(cast(cast(element_at(headers, 'EventTime') as string) as bigint)) desc
    )
    as rownumber
FROM {event_parquet_schema_name}.customer_activity_authenticated_parquet
WHERE cast(concat_ws('-', year, month, day) as date) between (select cast(min_date as date) from session_dates) and (select cast(max_date as date) from session_dates);

MERGE INTO {delta_schema_name}.customer_activity_authenticated_session_delta AS TGT
USING (
    SELECT DISTINCT sessionId
    FROM {delta_schema_name}.session_analytical_object_flatten_delta
) AS SRC
    ON TGT.sessionId = SRC.sessionId
WHEN MATCHED THEN DELETE;

CREATE TEMPORARY VIEW customer_activity_authenticated_session_delta_temp AS
WITH authenticated_session AS (
    select *
    from {delta_schema_name}.session_analytical_object_flatten_delta
    where session_event_name = 'com.nordstrom.customer.Authenticated'
),

authenticated_event_dedupe AS (
    select *
    from customer_activity_authenticated_dedupe
    where rownumber = 1
)

SELECT
    a.sessionId,
    a.sessionLastUpdatedTime,
    a.sessionAlgorithmId,
    a.sessionStartTime,
    a.sessionEndTime,
    a.sessionExperience,
    a.sessionChannel,
    a.session_event_name as sessionEventName,
    a.session_event_id as sessionEventId,
    b.eventId,
    b.eventHeaderEventTime,
    b.eventSystemTime,
    b.eventHeaders,
    b.source,
    b.customerBefore,
    b.customer,
    b.credentials,
    b.persistentSignInMode,
    b.clientInstanceId,
    a.activity_date,
    a.batch_id,
    a.batch_date,
    current_timestamp() as rcd_load_tmstp,
    current_timestamp() as rcd_update_tmstp
from authenticated_session AS a
left join authenticated_event_dedupe AS b
    on
        a.session_event_id = b.eventId;

INSERT INTO TABLE {delta_schema_name}.customer_activity_authenticated_session_delta PARTITION (activity_date)
SELECT
    sessionId,
    sessionLastUpdatedTime,
    sessionAlgorithmId,
    sessionStartTime,
    sessionEndTime,
    sessionExperience,
    sessionChannel,
    sessionEventName,
    sessionEventId,
    eventId,
    eventHeaderEventTime,
    eventSystemTime,
    eventHeaders,
    source,
    customerBefore,
    customer,
    credentials,
    persistentSignInMode,
    clientInstanceId,
    activity_date,
    batch_id,
    batch_date,
    rcd_load_tmstp,
    rcd_update_tmstp
from customer_activity_authenticated_session_delta_temp;

VACUUM {delta_schema_name}.customer_activity_authenticated_session_delta;

-- noqa: disable=all
OPTIMIZE {delta_schema_name}.customer_activity_authenticated_session_delta ZORDER BY (sessionid);
-- noqa: enable=all
