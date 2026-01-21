SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=appointment_cart_load_2656_napstore_insights;
Task_Name=appointment_cart_v1_load_orc_td_stg;'
FOR SESSION VOLATILE;


--Reading Data from  Source Kafka Topic Name=customer-appointment-cart-analytical-avro
create temporary view temp_object_model AS select * from kafka_customer_appointment_cart_analytical_avro;

-- Writing Kafka Data to S3 Path
insert into table appointment_cart_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from temp_object_model;

-- Writing Kafka Data to Semantic Layer 
insert overwrite table appointment_cart_ldg
select
lastUpdated as last_updated,
'-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as lastupdated_tz,
cartId as cart_id,
appointmentId as appointment_id,
engagementType as engagement_type,
channel.channelCountry as channel_country,
channel.channelBrand as channel_brand,
channel.sellingChannel as selling_channel,
experience as experience,
customer.id as customer_id,
customer.idType as customer_id_type,
referralService as referral_service,
ingressPoint as ingress_point,
referralStoreNumber as referral_store_number,
dateStartTime as date_start_time,
'-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as date_start_time_tz,
dateEndTime as date_end_time,
'-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as date_end_time_tz,
storeNumber as store_number,
serviceId as service_id,
serviceName as service_name,
serviceCategory as service_category,
staff.id as staff_id,
staff.idType as staff_id_type,
cast(isStaffAutoAssigned as string) as is_staff_auto_assigned
from temp_object_model;
