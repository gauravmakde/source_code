SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=booking_load_2656_napstore_insights;
Task_Name=booking_load_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view booking_input AS
select *
from kafka_booking_input;

create
temporary view temp_booking_input as
select eventTime,
       bookingId,
       channel.channelCountry,
       channel.channelBrand,
       experience,
       storeNumber,
       source.serviceName,
       channel.sellingChannel,
       customer.id                     as customerId,
       scheduleStartTime,
       scheduleEndTime,
       bookingMadeBy,
       explode_outer(services)         as service,
       serviceLocation.typeCode        as serviceLocationTypeCode,
       storeNumber                     as serviceLocationNumber,
       customerContact.email.value     as customerContactEmail,
       customerContact.phone.value     as customerContactPhone,
       customerContact.firstName.value as customerContactFirstName,
       customerContact.lastName.value  as customerContactLastName,
       bookingStatus,
       ingressPoint,
       isCreatedByCustomer,
       isRescheduledByCustomer,
       isCancelledByCustomer,
       cancellationReason,
       clientArrivalTime,
       appointmentCompletedTime
from booking_input;

-- Writing Kafka to Semantic Layer:

insert
overwrite table booking_stg_table
select cast(eventTime AS string)                  as event_time,
       bookingId                                  as booking_id,
       channelCountry                             as source_country_code,
       channelBrand                               as source_channel,
       experience                                 as source_platform,
       storeNumber                                as source_store,
       serviceName                                as source_service_name,
       sellingChannel                             as selling_channel,
       customerId                                 as shopper_id,
       cast(scheduleStartTime as string)          as schedule_start_time,
       cast(scheduleEndTime as string)            as schedule_end_time,
       bookingMadeBy                              as booking_made_by,
       service.id                                 as booking_service_id,
       service.name                               as booking_service_name,
       service.serviceCategory                    as booking_service_description,
       serviceLocationTypeCode                    as service_location_code,
       serviceLocationNumber                      as service_location_number,
       CAST(service.autoAssigned as string)       as auto_assigned,
       customerContactEmail                       as customer_contact_email,
       customerContactPhone                       as customer_contact_phone,
       customerContactFirstName                   as customer_contact_first_name,
       customerContactLastName                    as customer_contact_last_name,
       service.staff.idType                       as booking_staff_id_type,
       service.staff.id                           as booking_staff_id,
       bookingStatus                              as booking_status,
       ingressPoint                               as ingress_point,
       cast(isCreatedByCustomer as string)        as is_created_by_customer,
       cast(isRescheduledByCustomer as string)    as is_rescheduled_by_customer,
       cast(isCancelledByCustomer as string)      as is_cancelled_by_customer,
       cancellationReason                         as cancellation_reason,
       cast(clientArrivalTime as string)          as client_arrival_time,
       cast(appointmentCompletedTime as string)   as completed_time
from temp_booking_input;
