-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=hive_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Hive Table
create temporary view temp_settlement_intent as
select distinct value.transactionidentifier.id as transaction_identifier_id
from
    payments_object_model.payment_settlement_intent_parquet
where
    base64(headers['TriggerEvent']) = 'Y29tLm5vcmRzdHJvbS5jdXN0b21lci5ldmVudC5TZXR0bGVtZW50U3VjY2VlZGVkVjI='
    and value.failurereason is not null;;

--Writing Data to Target Teradata Table
insert into payment_settlement_intent_data_fix_ldg
select transaction_identifier_id
from temp_settlement_intent;
