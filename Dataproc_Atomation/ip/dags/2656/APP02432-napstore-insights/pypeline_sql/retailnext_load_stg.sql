-- Query Band part without required DAG_ID
SET QUERY_BAND = '
App_ID=app02432;
Task_Name=retailnext_load_stg;'
FOR SESSION VOLATILE;

-- Reading from kafka
-- SOURCE TOPIC NAME: store_traffic_measurement_avro
create
temporary view retailnext_input AS
select *
from kafka_retailnext_input;


-- Writing Kafka to Semantic Layer:
insert
overwrite table retailnext_stg_table
select
    source as source_type,
    measurementTime as measurement_time,
    start as start_time,
    end as end_time,
    location.type as measurement_location_type,
    location.id as measurement_location_id,
    location.name as measurement_location_name,
    location.parentid as measurement_parent_id,
    location.storeid as location_store_num,
    location.time_zone as location_time_zone,
    storeid as store_num,
    type as traffic_type,
    value as traffic_count,
    validity as validity
from retailnext_input;


