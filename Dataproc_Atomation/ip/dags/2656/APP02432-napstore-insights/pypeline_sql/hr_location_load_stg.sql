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
isInactive,
CAST((lastUpdated / 1000) AS TIMESTAMP) AS lastUpdated,
locationId,
locationName,
locationNumber,
locationType,
municipality,
phoneNumnber as phoneNumber,
postalCode,
timeZone
from hr_location_input;

