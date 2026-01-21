SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=booking_load_2656_napstore_insights;
Task_Name=booking_load_load_1_fct_table;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table that has the latest, unique data:
CREATE
VOLATILE MULTISET TABLE BOOKING_OBJECT_MODEL_FACT_UNIQUE_TEMP AS (
    SELECT DISTINCT
    cast(substr(event_time,1,19) || '+00:00' as timestamp(0)) as event_time_t,
    B.booking_id as booking_id,
    case when source_country_code='' then NULL else source_country_code end as source_country_code,
    case when source_channel='' then NULL else source_channel end as source_channel,
    case when source_platform='' then NULL else source_platform end as source_platform,
    case when source_store='' then NULL else source_store end as source_store,
    case when source_service_name='' then NULL else source_service_name end as source_service_name,
    case when selling_channel='' then NULL else selling_channel end as selling_channel,
    case when shopper_id='' then NULL else shopper_id end as shopper_id,
    cast(substr(schedule_start_time,1,19) || '+00:00' as timestamp(0)) as schedule_start_time_t,
    cast(substr(schedule_end_time,1,19) || '+00:00' as timestamp(0)) as schedule_end_time_t,
    case when booking_made_by='' then NULL else booking_made_by end as booking_made_by,
    case when booking_service_id='' then NULL else booking_service_id end as booking_service_id,
    case when booking_service_name='' then NULL else booking_service_name end as booking_service_name,
    case when booking_service_description='' then NULL else booking_service_description end as booking_service_description,
    case when service_location_code='' then NULL else service_location_code end as service_location_code,
    case when service_location_number='' then NULL else service_location_number end as service_location_number,
    case when auto_assigned='' then NULL else auto_assigned end as auto_assigned,
    case when customer_contact_email='' then NULL else customer_contact_email end as customer_contact_email,
    case when customer_contact_phone='' then NULL else customer_contact_phone end as customer_contact_phone,
    case when customer_contact_first_name='' then NULL else customer_contact_first_name end as customer_contact_first_name,
    case when customer_contact_last_name='' then NULL else customer_contact_last_name end as customer_contact_last_name,
    case when booking_staff_id_type='' then NULL else booking_staff_id_type end as booking_staff_id_type,
    case when booking_staff_id='' then NULL else booking_staff_id end as booking_staff_id,
    case when booking_status='' then NULL else booking_status end as booking_status,
    case when ingress_point='' then NULL else ingress_point end as ingress_point,
    case when cancellation_reason='' then NULL else cancellation_reason end as cancellation_reason,
    CASE
       WHEN client_arrival_time <> '' THEN cast(substr(client_arrival_time,1,19) || '+00:00' as timestamp(6))
    ELSE NULL
       END as client_arrival_time_t,
    CASE
       WHEN completed_time <> '' THEN cast(substr(completed_time,1,19) || '+00:00' as timestamp(6))
    ELSE NULL
       END as completed_time_t,
    current_date as dw_batch_date,
    current_timestamp(0) as dw_sys_load_tmstp,
    current_timestamp(0) as dw_sys_updt_tmstp
    FROM (
     SELECT b.*
    ,row_number() over (partition by b.booking_id, b.booking_service_id ORDER BY event_time DESC) as row_num
    FROM
    {db_env}_NAP_BASE_VWS.BOOKING_OBJECT_MODEL_STG b
    qualify row_num = 1
    )B
) WITH DATA PRIMARY INDEX(booking_id, booking_service_id) ON COMMIT PRESERVE ROWS;
ET;


MERGE INTO {db_env}_NAP_FCT.BOOKING_OBJECT_MODEL_FACT tgt
    USING BOOKING_OBJECT_MODEL_FACT_UNIQUE_TEMP src
    ON (src.booking_id = tgt.booking_id and src.booking_service_id = tgt.booking_service_id)
    WHEN MATCHED THEN
UPDATE
    SET
        event_time = src.event_time_t,
    source_country_code = src.source_country_code,
    source_channel = src.source_channel,
    source_platform = src.source_platform,
    source_store = src.source_store,
    source_service_name = src.source_service_name,
    selling_channel = src.selling_channel,
    shopper_id = src.shopper_id,
    schedule_start_time = src.schedule_start_time_t,
    schedule_end_time = src.schedule_end_time_t,
    booking_made_by = src.booking_made_by,
    booking_service_id = src.booking_service_id,
    booking_service_name = src.booking_service_name,
    booking_service_description = src.booking_service_description,
    service_location_code = src.service_location_code,
    service_location_number = src.service_location_number,
    auto_assigned = src.auto_assigned,
    customer_contact_email = src.customer_contact_email,
    customer_contact_phone = src.customer_contact_phone,
    customer_contact_first_name = src.customer_contact_first_name,
    customer_contact_last_name = src.customer_contact_last_name,
    booking_staff_id_type = src.booking_staff_id_type,
    booking_staff_id = src.booking_staff_id,
    booking_status = src.booking_status,
    ingress_point = src.ingress_point,
    cancellation_reason = src.cancellation_reason,
    client_arrival_time = src.client_arrival_time_t,
    completed_time = src.completed_time_t,
    dw_sys_updt_tmstp = current_timestamp(0)
    WHEN NOT MATCHED THEN
INSERT (
event_time,
booking_id,
source_country_code,
source_channel,
source_platform,
source_store,
source_service_name,
selling_channel,
shopper_id,
schedule_start_time,
schedule_end_time,
booking_made_by,
booking_service_id,
booking_service_name,
booking_service_description,
service_location_code,
service_location_number,
auto_assigned,
customer_contact_email,
customer_contact_phone,
customer_contact_first_name,
customer_contact_last_name,
booking_staff_id_type,
booking_staff_id,
booking_status,
ingress_point,
cancellation_reason,
client_arrival_time,
completed_time,
dw_batch_date,
dw_sys_load_tmstp,
dw_sys_updt_tmstp)
VALUES (src.event_time_t,
    src.booking_id,
    src.source_country_code,
    src.source_channel,
    src.source_platform,
    src.source_store,
    src.source_service_name,
    src.selling_channel,
    src.shopper_id,
    src.schedule_start_time_t,
    src.schedule_end_time_t,
    src.booking_made_by,
    src.booking_service_id,
    src.booking_service_name,
    src.booking_service_description,
    src.service_location_code,
    src.service_location_number,
    src.auto_assigned,
    src.customer_contact_email,
    src.customer_contact_phone,
    src.customer_contact_first_name,
    src.customer_contact_last_name,
    src.booking_staff_id_type,
    src.booking_staff_id,
    src.booking_status,
    src.ingress_point,
    src.cancellation_reason,
    src.client_arrival_time_t,
    src.completed_time_t,
    src.dw_batch_date,
    src.dw_sys_load_tmstp,
    src.dw_sys_updt_tmstp);
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
