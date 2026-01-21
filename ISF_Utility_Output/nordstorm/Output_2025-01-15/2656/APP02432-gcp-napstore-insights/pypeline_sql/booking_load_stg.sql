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
select eventTime                                  as EVENT_TIME,
       bookingId                                  as BOOKING_ID,
       channelCountry                             as SOURCE_COUNTRY_CODE,
       channelBrand                               as SOURCE_CHANNEL,
       experience                                 as SOURCE_PLATFORM,
       storeNumber                                as SOURCE_STORE,
       serviceName                                as SOURCE_SERVICE_NAME,
       sellingChannel                             as SELLING_CHANNEL,
       customerId                                 as SHOPPER_ID,
       scheduleStartTime                          as SCHEDULE_START_TIME,
       scheduleEndTime                            as SCHEDULE_END_TIME,
       bookingMadeBy                              as BOOKING_MADE_BY,
       service.id                                 as BOOKING_SERVICE_ID,
       service.name                               as BOOKING_SERVICE_NAME,
       service.serviceCategory                    as BOOKING_SERVICE_DESCRIPTION,
       serviceLocationTypeCode                    as SERVICE_LOCATION_CODE,
       serviceLocationNumber                      as SERVICE_LOCATION_NUMBER,
       CAST(service.autoAssigned as VARCHAR(100)) as AUTO_ASSIGNED,
       customerContactEmail                       as CUSTOMER_CONTACT_EMAIL,
       customerContactPhone                       as CUSTOMER_CONTACT_PHONE,
       customerContactFirstName                   as CUSTOMER_CONTACT_FIRST_NAME,
       customerContactLastName                    as CUSTOMER_CONTACT_LAST_NAME,
       service.staff.idType                       as BOOKING_STAFF_ID_TYPE,
       service.staff.id                           as BOOKING_STAFF_ID,
       bookingStatus                              as BOOKING_STATUS,
       ingressPoint                               as INGRESS_POINT,
       isCreatedByCustomer                        as IS_CREATED_BY_CUSTOMER,
       isRescheduledByCustomer                    as IS_RESCHEDULED_BY_CUSTOMER,
       isCancelledByCustomer                      as IS_CANCELLED_BY_CUSTOMER,
       cancellationReason                         as CANCELLATION_REASON,
       clientArrivalTime                          as CLIENT_ARRIVAL_TIME,
       appointmentCompletedTime                   as COMPLETED_TIME
from temp_booking_input;
