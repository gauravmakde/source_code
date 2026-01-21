-- Read from kafka source Cpp Preference type
CREATE TEMPORARY VIEW cpp_preferences AS
SELECT
    preference.id enterprise_id,
    preference.type preference_type,
    preference.value.value tokenized_value,
    preference.country country,
    preference.lastUpdateCountry as last_update_country,
    preference.regulationDomain regulation_domain,
    preference.contactFrequencyType.frequencyType contactFrequencyType,
    preference.doNotUseReason as do_not_use_reason,
    preference.existingBusinessRelations.hasExistingBusinessRelationship as existing_business_relations,
    preference.receivedDetails.firstReceivedDate firstReceivedDate,
    preference.receivedDetails.customerDataSourceDetails.customerDataSource as first_received_datasource ,
    preference.receivedDetails.customerDataSourceDetails.customerDataSubSource as first_received_datasubsource,
    row_number() over (partition by preference.id order by headers.SystemTime DESC) as rowNumber,
    preference.contactPreferences contactPreferenceList,
    preference.contentPreferences contentPreferenceList,
    decode(headers.SystemTime,'UTF-8') as system_time
FROM kafka_source;


CREATE TEMPORARY VIEW CPPPreferenceObjectStage AS
SELECT  enterprise_id,
        preference_type,
        tokenized_value,
        country,
        last_update_country,
        regulation_domain,
        contactFrequencyType,
        do_not_use_reason,
        existing_business_relations,
        firstReceivedDate,
        first_received_datasource ,
        first_received_datasubsource,
        contactPreferenceList,
        contentPreferenceList,
        system_time
FROM cpp_preferences
where rowNumber=1 ;

/*
****************************************
                                            PREFERENCE  BLOCK
****************************************
*/
CREATE TEMPORARY VIEW CPPPreferenceObject AS
SELECT
    enterprise_id,
    preference_type,
    tokenized_value,
    country,
    last_update_country,
    regulation_domain,
    contactFrequencyType,
    do_not_use_reason,
    existing_business_relations,
    firstReceivedDate,
    first_received_datasource ,
    first_received_datasubsource,
    system_time
FROM CPPPreferenceObjectStage;


-- write result table
INSERT OVERWRITE cpp_preference_data_extract
SELECT * FROM CPPPreferenceObject;

-- Audit table
INSERT OVERWRITE cpp_preference_final_count
SELECT COUNT(*) FROM CPPPreferenceObject;



/*
****************************************
                                            CONTACT BLOCK
****************************************
*/

CREATE TEMPORARY VIEW CPPContactPreferenceExploded AS
SELECT
    enterprise_id,
    explode(transform(contactPreferenceList, (x, xi) -> named_struct('index', xi, 'name', x.name,  'preferenceValue', x.preferenceValue, 'sourceUpdateDate', x.sourceUpdateDate, 'expressCaptured', x.expressCaptured))) contactPreferences,
    system_time
FROM CPPPreferenceObjectStage;

CREATE TEMPORARY VIEW CPPContactPreferenceStg AS
SELECT
    enterprise_id,
    contactPreferences.name as preference_name,
    contactPreferences.preferenceValue as preference_value,
    contactPreferences.sourceUpdateDate as source_update_date,
    contactPreferences.expressCaptured as express_captured,
    system_time,
    row_number() over(partition by enterprise_id, contactPreferences.name order by contactPreferences.sourceUpdateDate desc, contactPreferences.index) as contactPreferenceRowNumber
FROM CPPContactPreferenceExploded;

CREATE TEMPORARY VIEW CPPContactPreferenceFinal AS
SELECT
    enterprise_id,
    preference_name,
    preference_value,
    source_update_date,
    express_captured,
    system_time
FROM CPPContactPreferenceStg
WHERE contactPreferenceRowNumber=1 ;


-- write result table
INSERT OVERWRITE cpp_preference_contact_data_extract
SELECT * FROM CPPContactPreferenceFinal;

-- Audit table
INSERT OVERWRITE cpp_preference_contact_final_count
SELECT COUNT(*) FROM CPPContactPreferenceFinal;


/*
****************************************
                                            CONTENT BLOCK
****************************************
*/

CREATE TEMPORARY VIEW CPPContentPreferenceExploded AS
SELECT
    enterprise_id,
    explode(transform(contentPreferenceList, (x, xi) -> named_struct('index', xi, 'name', x.name, 'preferenceValue', x.preferenceValue, 'sourceUpdateDate', x.sourceUpdateDate, 'expressCaptured', x.expressCaptured))) contentPreferences,
    system_time
FROM CPPPreferenceObjectStage;

CREATE TEMPORARY VIEW CPPContentPreferenceStg AS
SELECT
    enterprise_id,
    contentPreferences.name as preference_name,
    contentPreferences.preferenceValue as preference_value,
    contentPreferences.sourceUpdateDate as source_update_date,
    contentPreferences.expressCaptured as express_captured,
    system_time,
    row_number() over(partition by enterprise_id, contentPreferences.name order by contentPreferences.sourceUpdateDate desc, contentPreferences.index) as contentPreferenceRowNumber
FROM CPPContentPreferenceExploded;

CREATE TEMPORARY VIEW CPPContentPreferenceFinal AS
SELECT
    enterprise_id,
    preference_name,
    preference_value,
    source_update_date,
    express_captured,
    system_time
FROM CPPContentPreferenceStg
where contentPreferenceRowNumber=1;

-- write result table
INSERT OVERWRITE cpp_preference_content_data_extract
SELECT * FROM CPPContentPreferenceFinal;

-- Audit table
INSERT OVERWRITE cpp_preference_content_final_count
SELECT COUNT(*) FROM CPPContentPreferenceFinal;

/*
****************************************
                                            ChannelBrand BLOCK
****************************************
*/
CREATE TEMPORARY VIEW CPPChannelBrandPreferenceExploded AS
SELECT
    enterprise_id,
    explode(array_union(flatten(transform(contactPreferenceList, (x, xi) -> transform(x.channelBrandPreferences, (y, yi) -> named_struct('granularPreferenceIndex', xi, 'granularPreferenceName', x.name, 'granularPreferenceSourceUpdateDate', x.sourceUpdateDate, 'brandPreferenceIndex', yi, 'brandPreferenceName', y.name, 'brandPreferenceValue', y.PreferenceValue, 'brandPreferenceSourceUpdateDate', y.sourceUpdateDate, 'granularPreferenceType', 'CONTACT')))), flatten(transform(contentPreferenceList, (x, xi) -> transform(x.channelBrandPreferences, (y, yi) -> named_struct('granularPreferenceIndex', xi, 'granularPreferenceName', x.name, 'granularPreferenceSourceUpdateDate', x.sourceUpdateDate, 'brandPreferenceIndex', yi, 'brandPreferenceName', y.name, 'brandPreferenceValue', y.PreferenceValue, 'brandPreferenceSourceUpdateDate', y.sourceUpdateDate, 'granularPreferenceType', 'CONTENT')))))) preferences,
    system_time
FROM CPPPreferenceObjectStage;

CREATE TEMPORARY VIEW CPPChannelBrandPreferenceStg AS
SELECT
    enterprise_id,
    preferences.granularPreferenceName as granular_preference_name,
    preferences.brandPreferenceName as brand_preference_name,
    preferences.brandPreferenceValue as brand_preference_value,
    preferences.brandPreferenceSourceUpdateDate as brand_preference_source_update_date,
    preferences.granularPreferenceType as granular_preference_type,
    system_time,
    rank() over(partition by enterprise_id, preferences.granularPreferenceType, preferences.granularPreferenceName order by preferences.granularPreferenceSourceUpdateDate desc, preferences.granularPreferenceIndex) as granularPreferenceRank,
    row_number() over(partition by enterprise_id, preferences.granularPreferenceType, preferences.granularPreferenceName, preferences.brandPreferenceName order by preferences.granularPreferenceSourceUpdateDate desc, preferences.granularPreferenceIndex, preferences.brandPreferenceSourceUpdateDate desc, preferences.brandPreferenceIndex) as brandPreferenceRowNumber
FROM CPPChannelBrandPreferenceExploded;

CREATE TEMPORARY VIEW CPPChannelBrandPreferenceFinal AS
SELECT
    enterprise_id,
    granular_preference_name,
    brand_preference_name,
    brand_preference_value,
    brand_preference_source_update_date,
    granular_preference_type,
    system_time
FROM CPPChannelBrandPreferenceStg
where granularPreferenceRank=1 and brandPreferenceRowNumber=1;

-- write result table
INSERT OVERWRITE cpp_preference_channel_brand_data_extract
SELECT * FROM CPPChannelBrandPreferenceFinal;

-- Audit table
INSERT OVERWRITE cpp_preference_channel_brand_final_count
SELECT COUNT(*) FROM CPPChannelBrandPreferenceFinal;
