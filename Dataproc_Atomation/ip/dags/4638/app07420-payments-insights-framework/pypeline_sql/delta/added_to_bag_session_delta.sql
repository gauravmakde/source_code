-- noqa: disable=all
CREATE TABLE IF NOT EXISTS {delta_schema_name}.customer_activity_added_to_bag_session_delta
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
  eventSource struct<channelcountry:string,channel:string,platform:string,feature:string,servicename:string,store:string,register:string>,
  bagtype string,
  bagactiontype string,
  contextid string,
  customer struct<idtype:string,id:string>,
  credentials struct<iconid:string,shopperid:string,tokenizingcreditcardhvt:struct<value:string,authority:string,strategy:string,dataclassification:string>,creditcardhvt:struct<value:string,authority:string,dataclassification:string>,fraudtoken:string,useragent:string,tokenizingipaddress:struct<value:string,authority:string,strategy:string,dataclassification:string>,ipaddress:struct<value:string,authority:string,dataclassification:string>,experimentid:string,adtrackingstatus:string,advertisingid:string,applevendorid:string,firebaseinstanceid:string>,
  byemployee struct<idtype:string,id:string>,
  product struct<idtype:string,id:string>,
  productstyle struct<idtype:string,id:string>,
  quantity int,
  finalquantity int,
  bagsize int,
  price struct<current:struct<currencycode:string,units:bigint,nanos:int>,ownership:struct<currencycode:string,units:bigint,nanos:int>,regular:struct<currencycode:string,units:bigint,nanos:int>,percentoff:double,discount:struct<currencycode:string,units:bigint,nanos:int>,taxdetail:struct<taxbreakdown:array<struct<amount:struct<currencycode:string,units:bigint,nanos:int>,category:string>>,tax:struct<currencycode:string,units:bigint,nanos:int>>,adjustmentdetail:struct<discount:struct<currencycode:string,units:bigint,nanos:int>,adjustmentreason:string,pricematchurl:string>,total:struct<currencycode:string,units:bigint,nanos:int>>,
  promotions array<struct<promotionid:string,changetype:string,reason:string,discount:struct<currencycode:string,units:bigint,nanos:int>,percentoff:double,activationcode:string,employee:struct<idtype:string,id:string>>>,
  clientInstanceId string,
  activity_date date,
  batch_id int,
  batch_date date,
  rcd_load_tmstp timestamp,
  rcd_update_tmstp timestamp
)
USING DELTA
PARTITIONED BY (activity_date)
LOCATION 's3://{delta_s3_bucket}/session_event_delta/customer_activity_added_to_bag_session_delta/';
-- noqa: enable=all

CREATE TEMPORARY VIEW customer_activity_added_to_bag_dedupe AS
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
    value.source as eventSource,
    value.bagType as bagtype,
    value.bagActionType as bagactiontype,
    value.contextId as contextid,
    value.customer as customer,
    value.credentials as credentials,
    value.byEmployee as byEmployee,
    value.product as product,
    value.productStyle as productStyle,
    value.quantity as quantity,
    value.finalQuantity as finalQuantity,
    value.bagSize as bagSize,
    value.price as price,
    value.promotions as promotions,
    value.clientInstanceId as clientInstanceId,
    row_number() over (
        partition by cast(element_at(headers, 'Id') as string)
        order by timestamp_millis(cast(cast(element_at(headers, 'EventTime') as string) as bigint)) desc
    )
    as rownumber
FROM {event_parquet_schema_name}.customer_activity_added_to_bag_parquet
WHERE cast(concat_ws('-', year, month, day) as date) between (select cast(min_date as date) from session_dates) and (select cast(max_date as date) from session_dates);

MERGE INTO {delta_schema_name}.customer_activity_added_to_bag_session_delta AS TGT
USING (
    SELECT DISTINCT sessionId
    FROM {delta_schema_name}.session_analytical_object_flatten_delta
) AS SRC
    ON TGT.sessionId = SRC.sessionId
WHEN MATCHED THEN DELETE;

CREATE TEMPORARY VIEW customer_activity_added_to_bag_session_delta_temp AS
WITH added_to_bag_session AS (
    select *
    from {delta_schema_name}.session_analytical_object_flatten_delta
    where session_event_name = 'com.nordstrom.customer.AddedToBag'
),

added_to_bag_event_dedupe AS (
    select *
    from customer_activity_added_to_bag_dedupe
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
    b.eventSource,
    b.bagtype,
    b.bagactiontype,
    b.contextid,
    b.customer,
    b.credentials,
    b.byEmployee,
    b.product,
    b.productStyle,
    b.quantity,
    b.finalQuantity,
    b.bagSize,
    b.price,
    b.promotions,
    b.clientInstanceId,
    a.activity_date,
    a.batch_id,
    a.batch_date,
    current_timestamp() as rcd_load_tmstp,
    current_timestamp() as rcd_update_tmstp
from added_to_bag_session AS a
left join added_to_bag_event_dedupe AS b
    on
        a.session_event_id = b.eventId;

INSERT INTO TABLE {delta_schema_name}.customer_activity_added_to_bag_session_delta PARTITION (activity_date)
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
    eventSource,
    bagtype,
    bagactiontype,
    contextid,
    customer,
    credentials,
    byEmployee,
    product,
    productStyle,
    quantity,
    finalQuantity,
    bagSize,
    price,
    promotions,
    clientInstanceId,
    activity_date,
    batch_id,
    batch_date,
    rcd_load_tmstp,
    rcd_update_tmstp
from customer_activity_added_to_bag_session_delta_temp;

VACUUM {delta_schema_name}.customer_activity_added_to_bag_session_delta;

---- noqa: disable=all
OPTIMIZE {delta_schema_name}.customer_activity_added_to_bag_session_delta ZORDER BY (sessionid);
-- noqa: enable=all
