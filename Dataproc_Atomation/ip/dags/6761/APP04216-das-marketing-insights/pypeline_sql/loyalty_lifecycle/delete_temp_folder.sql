CREATE TABLE IF NOT EXISTS {schema_name_for_emptytable}.empty_set_lmo(
     lastupdatedtime timestamp,
     loyaltynoteid string,
     loyaltyid string,
     notenumber struct<value:string,authority:string,dataClassification:string>,
     notestate string,
     notetype string,
     expirydate date,
     modeofnotedelivery string,
     notedeliverymodereason string,
     issuedate timestamp,
     objectmodelcreationtime timestamp,
     record_load_timestamp timestamp
);

INSERT OVERWRITE s3_merged_data SELECT * FROM {schema_name_for_emptytable}.empty_set_lmo;