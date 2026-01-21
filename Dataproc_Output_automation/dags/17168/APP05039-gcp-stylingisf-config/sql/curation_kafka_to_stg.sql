--Source
--Reading Data from  Source Kafka Topic Name=curation_object_model_avro using SQL API CODE
create temporary view  temp_kafka_curation_object_model AS select * from kafka_curation_analytical_model_avro;


--Transform Logic
--Extracting Required Column from Kafka (Cache in previous SQL)
-- create employee commission exploded
create temporary view employeeCommission_exploded AS
 select
  temp_kafka_curation_object_model.curationId as curation_id,
  temp_kafka_curation_object_model.channel.channelCountry as channel_country,
  temp_kafka_curation_object_model.channel.channelBrand as channel_brand,
  temp_kafka_curation_object_model.channel.sellingChannel as selling_channel,
  temp_kafka_curation_object_model.lookId as look_id,
  temp_kafka_curation_object_model.requestedEmployee.id as requested_employee_id,
  temp_kafka_curation_object_model.requestedEmployee.idType as requested_employee_id_type,
  temp_kafka_curation_object_model.curatingEmployee.id as curating_employee_id,
  temp_kafka_curation_object_model.curatingEmployee.idType as curating_employee_id_type,
  temp_kafka_curation_object_model.customer.id as customer_id,
  temp_kafka_curation_object_model.customer.idType as customer_id_type,
  temp_kafka_curation_object_model.curationRequestedTime as curation_requested_time,
  temp_kafka_curation_object_model.curationStartedTime as curation_started_time,
  temp_kafka_curation_object_model.curationCompletedTime as curation_completed_time,
  temp_kafka_curation_object_model.curationBoardType as curation_board_type,
  temp_kafka_curation_object_model.requestedExperience as curation_requested_experience,
  temp_kafka_curation_object_model.numberOfItems as number_of_items,
  temp_kafka_curation_object_model.lastUpdatedTime as activity_tmstp,
  cast(decode(temp_kafka_curation_object_model.headers.SystemTime,'UTF-8') as bigint) as systemtime,
  explode_outer(temp_kafka_curation_object_model.employeeCommission) as employee_commission
from temp_kafka_curation_object_model;

create temporary view kafka_curation_object_model_avro_extract_columns as
   select curation_id as curation_id,
    channel_brand as channel_brand,
    channel_country as channel_country,
    selling_channel as selling_channel,
    look_id as look_id,
    requested_employee_id as requested_employee_id,
    requested_employee_id_type as requested_employee_id_type,
    curating_employee_id as curating_employee_id,
    curating_employee_id_type as curating_employee_id_type,
    customer_id as customer_id,
    customer_id_type as customer_id_type,
    curation_requested_time as curation_requested_time,
    curation_started_time as curation_started_time,
    curation_completed_time as curation_completed_time,
    curation_board_type as curation_board_type,
    curation_requested_experience as curation_requested_experience,
    number_of_items as number_of_items,
    activity_tmstp as  activity_tmstp,
    to_date(activity_tmstp) as activity_date,
    systemtime as systemtime,
    employee_commission.orderNumber as order_number,
    employee_commission.orderLineId as order_line_id,
    employee_commission.eventTime  as employee_commission_eventtime,
    employee_commission.source.platform as source_platform
    from employeeCommission_exploded;
--Sink
---Writing Kafka Data to Teradata using SQL API CODE
insert overwrite table write_teradata_curation_object_tbl select distinct *  from kafka_curation_object_model_avro_extract_columns ;
