SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_location_events_load_2656_napstore_insights;
Task_Name=hr_loc_events_load_1_dim_tables;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table that has the latest, unique data:
CREATE VOLATILE MULTISET TABLE HR_WORKER_LOCATION_DIM_UNIQUE_TEMP AS (
    SELECT DISTINCT
    locationId,
	case when addressLine1='' then NULL else addressLine1 end as addressLine1,
	case when areaPhoneCode='' then NULL else areaPhoneCode end as areaPhoneCode,
	case when countryCode='' then NULL else countryCode end as countryCode,
	case when countryRegion='' then NULL else countryRegion end as countryRegion,
	case when countryRegionDescription='' then NULL else countryRegionDescription end as countryRegionDescription,
	case when defaultCurrency='' then NULL else defaultCurrency end as defaultCurrency,
	case when isInactive='' then NULL else isInactive end as isInactive,
	case when locationName='' then NULL else locationName end as locationName,
	case when locationNumber='' then NULL else locationNumber end as locationNumber,
	case when locationType='' then NULL else locationType end as locationType,
	case when municipality='' then NULL else municipality end as municipality,
	case when phoneNumber='' then NULL else phoneNumber end as phoneNumber,
	case when postalCode='' then NULL else postalCode end as postalCode,
	case when timeZone='' then NULL else timeZone end as timeZone,
	lastUpdated
    FROM {db_env}_NAP_HR_STG.HR_WORKER_LOCATION_LDG
    qualify rank() over (partition BY locationId ORDER BY lastUpdated DESC) = 1
) WITH DATA PRIMARY INDEX(locationId) ON COMMIT PRESERVE ROWS;
ET;

MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_WORKER_LOCATION_DIM tgt
USING HR_WORKER_LOCATION_DIM_UNIQUE_TEMP src
	ON (src.locationId = tgt.location_id)
WHEN MATCHED THEN 
UPDATE
SET
	address_line1 = src.addressLine1,
	area_phone_code = src.areaPhoneCode,
	country_code = src.countryCode,
	country_region = src.countryRegion,
	country_region_description = src.countryRegionDescription,
	default_currency = src.defaultCurrency,
	is_inactive = src.isInactive,
	location_name = src.locationName,
	location_number = src.locationNumber,
	location_type = src.locationType,
	municipality = src.municipality,
	phone_number = src.phoneNumber,
	postal_code = src.postalCode,
	time_zone = src.timeZone,
	last_updated = src.lastUpdated,
	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN 
INSERT (
	location_id,
	address_line1,
	area_phone_code,
	country_code,
	country_region,
	country_region_description,
	default_currency,
	is_inactive,
	location_name,
	location_number,
	location_type,
	municipality,
	phone_number,
	postal_code,
	time_zone,
	last_updated, 
	dw_batch_date, 
	dw_sys_load_tmstp
)
VALUES (
	src.locationId,
	src.addressLine1,
	src.areaPhoneCode,
	src.countryCode,
	src.countryRegion,
	src.countryRegionDescription,
	src.defaultCurrency,
	src.isInactive,
	src.locationName,
	src.locationNumber,
	src.locationType,
	src.municipality,
	src.phoneNumber,
	src.postalCode,
	src.timeZone,
	src.lastUpdated, 
	CURRENT_DATE, 
	CURRENT_TIMESTAMP(0)
);

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
