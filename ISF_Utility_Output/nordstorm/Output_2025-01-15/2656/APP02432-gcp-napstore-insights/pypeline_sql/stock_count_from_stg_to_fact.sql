CREATE
VOLATILE MULTISET TABLE STOCK_COUNT_TEMP AS (
    SELECT DISTINCT * FROM {db_env}_NAP_STG.STOCK_COUNT_STG
    qualify row_number() over (partition BY stock_count_id ORDER BY last_updated_at DESC, kafka_timestamp DESC) = 1
) WITH DATA PRIMARY INDEX(stock_count_id) ON COMMIT PRESERVE ROWS;
ET;
-- Merge and update:
MERGE INTO {db_env}_NAP_FCT.STOCK_COUNT_FACT tgt
    USING STOCK_COUNT_TEMP src
    ON (src.stock_count_id = tgt.stock_count_id)
    WHEN MATCHED THEN UPDATE SET

	created_at = src.created_at,
	last_updated_at = src.last_updated_at,
	created_transaction_id = src.created_transaction_id,
	stock_count_name = src.stock_count_name,
	stock_count_type = src.stock_count_type,
	location_id = src.location_id,
	source_system = src.source_system,
	external_stock_count_id = src.external_stock_count_id,
	external_system = src.external_system,
	stock_count_scheduled_time = src.stock_count_scheduled_time,
	created_by_user = src.created_by_user,
	submit_manifested_at = src.submit_manifested_at,
	submit_manifested_transaction_id = src.submit_manifested_transaction_id,
	stock_count_started_time = src.stock_count_started_time,
	stock_count_submitted_time = src.stock_count_submitted_time,
	submitted_transaction_ids = src.submitted_transaction_ids,
	canceled_at = src.canceled_at,
	canceled_transaction_id = src.canceled_transaction_id,
	canceled_by_user = src.canceled_by_user,
	expired_at = src.expired_at,
	expired_transaction_id = src.expired_transaction_id,
	dw_batch_date = CURRENT_DATE,
	dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)

WHEN NOT MATCHED THEN INSERT (
    stock_count_id,
    created_at,
    last_updated_at,
    created_transaction_id,
    stock_count_name,
    stock_count_type,
    location_id,
    source_system,
    external_stock_count_id,
    external_system,
    stock_count_scheduled_time,
    created_by_user,
    submit_manifested_at,
    submit_manifested_transaction_id,
    stock_count_started_time,
    stock_count_submitted_time,
    submitted_transaction_ids,
    canceled_at,
    canceled_transaction_id,
    canceled_by_user,
    expired_at,
    expired_transaction_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.stock_count_id,
    src.created_at,
    src.last_updated_at,
    src.created_transaction_id,
    src.stock_count_name,
    src.stock_count_type,
    src.location_id,
    src.source_system,
    src.external_stock_count_id,
    src.external_system,
    src.stock_count_scheduled_time,
    src.created_by_user,
    src.submit_manifested_at,
    src.submit_manifested_transaction_id,
    src.stock_count_started_time,
    src.stock_count_submitted_time,
    src.submitted_transaction_ids,
    src.canceled_at,
    src.canceled_transaction_id,
    src.canceled_by_user,
    src.expired_at,
    src.expired_transaction_id,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
    );
ET;



CREATE
VOLATILE MULTISET TABLE STOCK_COUNT_CRITERIA_TEMP AS (
    SELECT DISTINCT * FROM {db_env}_NAP_STG.STOCK_COUNT_CRITERIA_STG
    qualify row_number() over (partition BY stock_count_id, criterion_type, criterion_id,
    department_number, class_number, subclass_number, item_id_type, item_id
    ORDER BY kafka_timestamp DESC) = 1
) WITH DATA PRIMARY INDEX(stock_count_id) ON COMMIT PRESERVE ROWS;
ET;
-- Merge and update:
MERGE INTO {db_env}_NAP_FCT.STOCK_COUNT_CRITERIA_FACT tgt
    USING STOCK_COUNT_CRITERIA_TEMP src
    ON (src.stock_count_id = tgt.stock_count_id
        AND src.criterion_type = tgt.criterion_type
        AND src.criterion_id = tgt.criterion_id
        AND src.department_number = tgt.department_number
        AND src.class_number = tgt.class_number
        AND src.subclass_number = tgt.subclass_number
        AND src.item_id_type = tgt.item_id_type
        AND src.item_id = tgt.item_id
    )
    WHEN MATCHED THEN
UPDATE
SET
	criterion_type = src.criterion_type,
	criterion_id = src.criterion_id,
	department_number = src.department_number,
	class_number = src.class_number,
	subclass_number = src.subclass_number,
	item_id_type = src.item_id_type,
	item_id = src.item_id,
	dw_batch_date = CURRENT_DATE,
	dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    stock_count_id,
    criterion_type,
    criterion_id,
    department_number,
    class_number,
    subclass_number,
    item_id_type,
    item_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.stock_count_id,
    src.criterion_type,
    src.criterion_id,
    src.department_number,
    src.class_number,
    src.subclass_number,
    src.item_id_type,
    src.item_id,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
    );




CREATE
VOLATILE MULTISET TABLE STOCK_COUNT_SUBMITTED_DETAILS_TEMP AS (
    SELECT DISTINCT * FROM {db_env}_NAP_STG.STOCK_COUNT_SUBMITTED_DETAILS_STG
    qualify row_number() over (partition BY submitted_transaction_id ORDER BY kafka_timestamp DESC) = 1
) WITH DATA PRIMARY INDEX(submitted_transaction_id) ON COMMIT PRESERVE ROWS;
ET;
-- Merge and update:
MERGE INTO {db_env}_NAP_FCT.STOCK_COUNT_SUBMITTED_DETAILS_FACT tgt
    USING STOCK_COUNT_SUBMITTED_DETAILS_TEMP src
    ON (src.submitted_transaction_id = tgt.submitted_transaction_id)
    WHEN MATCHED THEN
UPDATE
SET
    stock_count_id = src.stock_count_id,
	submitted_at = src.submitted_at,
	source_system = src.source_system,
	dw_batch_date = CURRENT_DATE,
	dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    stock_count_id,
    submitted_at,
    submitted_transaction_id,
    source_system,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.stock_count_id,
    src.submitted_at,
    src.submitted_transaction_id,
    src.source_system,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
    );



CREATE
VOLATILE MULTISET TABLE STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_TEMP AS (
    SELECT DISTINCT * FROM {db_env}_NAP_STG.STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_STG
    qualify row_number() over (partition BY id ORDER BY kafka_timestamp DESC) = 1
) WITH DATA PRIMARY INDEX(id) ON COMMIT PRESERVE ROWS;
ET;
-- Merge and update:
MERGE INTO {db_env}_NAP_FCT.STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_FACT tgt
    USING STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_TEMP src
    ON (src.id = tgt.id)
    WHEN MATCHED THEN
UPDATE
SET
    stock_count_id = src.stock_count_id,
	submitted_details_transaction_id = src.submitted_details_transaction_id,
	expected_item_id = src.expected_item_id,
	expected_item_id_type = src.expected_item_id_type,
	expected_item_quantity = src.expected_item_quantity,
	expected_item_epcs = src.expected_item_epcs,
	is_in_rfid_program = src.is_in_rfid_program,
	user_id = src.user_id,
	user_id_type = src.user_id_type,
	dw_batch_date = CURRENT_DATE,
	dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    id,
    stock_count_id,
    submitted_details_transaction_id,
    expected_item_id,
    expected_item_id_type,
    expected_item_quantity,
    expected_item_epcs,
    is_in_rfid_program,
    user_id,
    user_id_type,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.id,
    src.stock_count_id,
    src.submitted_details_transaction_id,
    src.expected_item_id,
    src.expected_item_id_type,
    src.expected_item_quantity,
    src.expected_item_epcs,
    src.is_in_rfid_program,
    src.user_id,
    src.user_id_type,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
    );




CREATE
VOLATILE MULTISET TABLE STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_COUNTED_ITEM_QUANTITY_TEMP AS (
    SELECT DISTINCT * FROM {db_env}_NAP_STG.STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_COUNTED_ITEM_QUANTITY_STG
    qualify row_number() over (partition BY id ORDER BY kafka_timestamp DESC) = 1
) WITH DATA PRIMARY INDEX(id) ON COMMIT PRESERVE ROWS;
ET;
-- Merge and update:
MERGE INTO {db_env}_NAP_FCT.STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_COUNTED_ITEM_QUANTITY_FACT tgt
    USING STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_COUNTED_ITEM_QUANTITY_TEMP src
    ON (src.id = tgt.id)
    WHEN MATCHED THEN
UPDATE
SET
    stock_count_id = src.stock_count_id,
	stock_count_product_result_id = src.stock_count_product_result_id,
	counted_item_id = src.counted_item_id,
	counted_item_id_type = src.counted_item_id_type,
	counted_item_quantity = src.counted_item_quantity,
	counted_item_epcs = src.counted_item_epcs,
	dw_batch_date = CURRENT_DATE,
	dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    id,
    stock_count_id,
    stock_count_product_result_id,
    counted_item_id,
    counted_item_id_type,
    counted_item_quantity,
    counted_item_epcs,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.id,
    src.stock_count_id,
    src.stock_count_product_result_id,
    src.counted_item_id,
    src.counted_item_id_type,
    src.counted_item_quantity,
    src.counted_item_epcs,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
    );