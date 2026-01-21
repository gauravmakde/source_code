SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_location_events_load_2656_napstore_insights;
Task_Name=hr_loc_events_load_0_stg_tables;'
FOR SESSION VOLATILE;

-- Reading from kafka: humanresources-location-changed-avro:
create temporary view hr_location_input AS select * from kafka_hr_location_input;

-- Writing Kafka to Semantic Layer:
insert overwrite td_location_ldg_table
select 
addressLine1,
areaPhoneCode,
countryCode,
countryRegion,
countryRegionDescription,
defaultCurrency,
CAST(isInactive as STRING),
CAST((lastUpdated / 1000) AS TIMESTAMP) AS lastUpdated,
'-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as lastupdated_tz,
locationId,
locationName,
locationNumber,
locationType,
municipality,
phoneNumnber as phonenumber,
postalCode,
timeZone
from hr_location_input;