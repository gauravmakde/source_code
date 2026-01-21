CREATE TEMPORARY TABLE IF NOT EXISTS hr_worker_location_dim_unique_temp as
SELECT DISTINCT locationid,
  CASE
  WHEN LOWER(addressline1) = LOWER('')
  THEN NULL
  ELSE addressline1
  END AS addressline1,
  CASE
  WHEN LOWER(areaphonecode) = LOWER('')
  THEN NULL
  ELSE areaphonecode
  END AS areaphonecode,
  CASE
  WHEN LOWER(countrycode) = LOWER('')
  THEN NULL
  ELSE countrycode
  END AS countrycode,
  CASE
  WHEN LOWER(countryregion) = LOWER('')
  THEN NULL
  ELSE countryregion
  END AS countryregion,
  CASE
  WHEN LOWER(countryregiondescription) = LOWER('')
  THEN NULL
  ELSE countryregiondescription
  END AS countryregiondescription,
  CASE
  WHEN LOWER(defaultcurrency) = LOWER('')
  THEN NULL
  ELSE defaultcurrency
  END AS defaultcurrency,
  CASE
  WHEN LOWER(isinactive) = LOWER('')
  THEN NULL
  ELSE isinactive
  END AS isinactive,
  CASE
  WHEN LOWER(locationname) = LOWER('')
  THEN NULL
  ELSE locationname
  END AS locationname,
  CASE
  WHEN LOWER(locationnumber) = LOWER('')
  THEN NULL
  ELSE locationnumber
  END AS locationnumber,
  CASE
  WHEN LOWER(locationtype) = LOWER('')
  THEN NULL
  ELSE locationtype
  END AS locationtype,
  CASE
  WHEN LOWER(municipality) = LOWER('')
  THEN NULL
  ELSE municipality
  END AS municipality,
  CASE
  WHEN LOWER(phonenumber) = LOWER('')
  THEN NULL
  ELSE phonenumber
  END AS phonenumber,
  CASE
  WHEN LOWER(postalcode) = LOWER('')
  THEN NULL
  ELSE postalcode
  END AS postalcode,
  CASE
  WHEN LOWER(timezone) = LOWER('')
  THEN NULL
  ELSE timezone
  END AS timezone,
 lastupdated,
 lastupdated_tz 
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_hr_stg.hr_worker_location_ldg
QUALIFY (RANK() OVER (PARTITION BY locationid ORDER BY lastupdated DESC)) = 1;


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_HR_DIM.HR_WORKER_LOCATION_DIM AS tgt
USING hr_worker_location_dim_unique_temp AS src
ON LOWER(src.locationid) = LOWER(tgt.location_id)
WHEN MATCHED THEN UPDATE SET
    address_line1 = src.addressline1, 
    area_phone_code = src.areaphonecode, 
    country_code = src.countrycode, 
    country_region = src.countryregion, 
    country_region_description = src.countryregiondescription, 
    default_currency = src.defaultcurrency, 
    is_inactive = src.isinactive, 
    location_name = src.locationname, 
    location_number = src.locationnumber, 
    location_type = src.locationtype, 
    municipality = src.municipality, 
    phone_number = src.phonenumber, 
    postal_code = src.postalcode, 
    time_zone = src.timezone, 
    last_updated = src.lastupdated,
    last_updated_tz = src.lastupdated_tz,	
    dw_batch_date = current_date('PST8PDT'), 
     dw_sys_load_tmstp = current_datetime('PST8PDT')
   WHEN NOT MATCHED BY TARGET THEN
    INSERT (location_id, address_line1, area_phone_code, country_code, country_region, country_region_description, default_currency, is_inactive, location_name, location_number, location_type, municipality, phone_number, postal_code, time_zone, last_updated, dw_batch_date, dw_sys_load_tmstp,last_updated_tz)
    VALUES (src.locationid, src.addressline1, src.areaphonecode, src.countrycode, src.countryregion, src.countryregiondescription, src.defaultcurrency, src.isinactive, src.locationname, src.locationnumber, src.locationtype, src.municipality, src.phonenumber, src.postalcode, src.timezone, src.lastupdated, current_date('PST8PDT'),
	current_datetime('PST8PDT'),
	lastupdated_tz);

