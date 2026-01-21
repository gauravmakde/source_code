SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=appointment_cart_load_2656_napstore_insights;
Task_Name=appointment_cart_v1_load_td_stg_to_dims;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table that has engagement_type = 'APPOINTMENT_CONFIRMED.
CREATE VOLATILE MULTISET TABLE APPOINTMENT_CART_ENGAGEMENT_TEMP AS (
    SELECT * FROM {db_env}_NAP_STG.APPOINTMENT_CART_LDG
    WHERE engagement_type <> 'APPOINTMENT_CONFIRMED' and engagement_type <> 'APPOINTMENT_STARTED'
) WITH DATA PRIMARY INDEX(cart_id) ON COMMIT PRESERVE ROWS;

ET;
-- Only inserts if the same combination of cart_id,event_time and engagement_type doesn't exist to prevent duplicates .
MERGE INTO {db_env}_NAP_DIM.APPOINTMENT_CART_ENGAGEMENT_DIM tgt
USING  APPOINTMENT_CART_ENGAGEMENT_TEMP src
	ON (src.cart_id = tgt.cart_id ) and (src.last_updated = tgt.event_time) and (src.engagement_type = tgt.engagement_type) 
WHEN MATCHED THEN
UPDATE
SET
    start_time = src.date_start_time,
    end_time = src.date_end_time,
    channel_country = src.channel_country,
    channel_brand = src.channel_brand,
    selling_channel = src.selling_channel,
    experience = src.experience,
    store_number = src.store_number,
    service_id = src.service_id, 
    service_name = src.service_name, 
    service_category = src.service_category,
    staff_id = src.staff_id,
    staff_id_type = src.staff_id_type,
    is_staff_auto_assigned = src.is_staff_auto_assigned,
    dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    engagement_type,
    event_time,
    cart_id,
    start_time,
    end_time,
    channel_country,
    channel_brand,
    selling_channel,
    experience,
    store_number,
    service_id, 
    service_name, 
    service_category,
    staff_id,
    staff_id_type,
    is_staff_auto_assigned,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.engagement_type,
    src.last_updated,
    src.cart_id,
    src.date_start_time,
    src.date_end_time,
    src.channel_country,
    src.channel_brand,
    src.selling_channel,
    src.experience,
    src.store_number,
    src.service_id, 
    src.service_name, 
    src.service_category,
    src.staff_id,
    src.staff_id_type,
    src.is_staff_auto_assigned,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
);

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
