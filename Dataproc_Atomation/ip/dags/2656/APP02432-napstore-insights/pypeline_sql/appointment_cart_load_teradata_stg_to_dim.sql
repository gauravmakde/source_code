SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=appointment_cart_load_2656_napstore_insights;
Task_Name=appointment_cart_v1_load_td_stg_to_dims;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table that has engagement type 'APPOINTMENT_STARTED'.
CREATE VOLATILE MULTISET TABLE APPOINTMENT_CART_STARTED_TEMP AS (
    SELECT DISTINCT * FROM {db_env}_NAP_STG.APPOINTMENT_CART_LDG
    WHERE engagement_type = 'APPOINTMENT_STARTED'
    qualify row_number() over (partition BY cart_id ORDER BY last_updated DESC) = 1
) WITH DATA PRIMARY INDEX(cart_id) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if cart_id exists and engagement_type is 'APPOINTMENT_STARTED'.
MERGE INTO {db_env}_NAP_DIM.APPOINTMENT_CART_DIM tgt
USING  APPOINTMENT_CART_STARTED_TEMP src
	ON (src.cart_id = tgt.cart_id )
WHEN MATCHED THEN
UPDATE
SET
last_updated = src.last_updated,
booking_started_at = src.last_updated,
channel_country = src.channel_country,
channel_brand = src.channel_brand,
selling_channel =  src.selling_channel,
experience = src.experience,
ingress_point = src.ingress_point,
customer_id = src.customer_id,
customer_id_type = src.customer_id_type,
referral_service = src.referral_service,
referral_store_number = src.referral_store_number,
dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    last_updated,
    cart_id,
    booking_started_at,
    channel_country,
    channel_brand,
    selling_channel,
    experience,
    ingress_point,
    customer_id,
    customer_id_type,
    referral_service,
    referral_store_number,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.last_updated,
    src.cart_id,
    src.last_updated,
    src.channel_country,
    src.channel_brand,
    src.selling_channel,
    src.experience,
    src.ingress_point,
    src.customer_id,
    src.customer_id_type,
    src.referral_service,
    src.referral_store_number,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
);
ET;

-- Create temporary table that has engagement_type = 'APPOINTMENT_CONFIRMED.
CREATE VOLATILE MULTISET TABLE APPOINTMENT_CART_CONFIRMED_TEMP AS (
    SELECT DISTINCT * FROM {db_env}_NAP_STG.APPOINTMENT_CART_LDG
    WHERE engagement_type = 'APPOINTMENT_CONFIRMED'
    qualify row_number() over (partition BY cart_id ORDER BY last_updated DESC) = 1
) WITH DATA PRIMARY INDEX(cart_id) ON COMMIT PRESERVE ROWS;

ET;
-- Merge and update if cart_id exists and engagement_type is 'APPOINTMENT_CONFIRMED.
MERGE INTO {db_env}_NAP_DIM.APPOINTMENT_CART_DIM tgt
USING  APPOINTMENT_CART_CONFIRMED_TEMP src
	ON (src.cart_id = tgt.cart_id )
WHEN MATCHED THEN
UPDATE
SET
appointment_id = src.appointment_id,
last_updated = src.last_updated,
booking_confirmed_at = src.last_updated,
channel_country = src.channel_country,
channel_brand = src.channel_brand,
selling_channel =  src.selling_channel,
dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    last_updated,
    cart_id,
    appointment_id,
    booking_confirmed_at,
    channel_country,
    channel_brand,
    selling_channel,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.last_updated,
    src.cart_id,
    src.appointment_id,
    src.last_updated,
    src.channel_country,
    src.channel_brand,
    src.selling_channel,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
);

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
