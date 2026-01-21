CREATE TEMPORARY VIEW kafka_avro_tbl AS
SELECT
    preference.id AS id,
    preference.type AS type,
    preference.value AS value,
    preference.country AS country,
    preference.lastUpdateCountry AS lastUpdateCountry,
    preference.contactFrequencyType AS contactFrequencyType,
    preference.doNotUseReason AS doNotUseReason,
    preference.regulationDomain AS regulationDomain,
    preference.contactPreferences AS contactPreferences,
    preference.contentPreferences AS contentPreferences,
    preference.existingBusinessRelations AS existingBusinessRelations,
    preference.receivedDetails AS receivedDetails,
    preference.applicationWrites AS applicationWrites,
    CAST(CAST(headers.SystemTime AS BIGINT)/1000 AS TIMESTAMP) AS objectModelCreationTime,
    headers.usid AS uniquesourceid
FROM kafka_tbl;

INSERT overwrite s3_merged_data SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY id ORDER BY objectModelCreationTime DESC) AS rn FROM ( 
        SELECT * FROM kafka_avro_tbl
            UNION ALL
            SELECT 
                id, 
                type, 
                value, 
                country, 
                lastUpdateCountry, 
                contactFrequencyType, 
                doNotUseReason, 
                regulationDomain, 
                contactPreferences,
                contentPreferences,
                existingBusinessRelations,
                receivedDetails,
                applicationWrites,
                objectModelCreationTime,
                uniquesourceid
            FROM {schema_name_cpp}.{resulting_cpp_table_name})
        AS uninon) 
    AS tbl
WHERE tbl.rn=1 and 1=1;

