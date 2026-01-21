CREATE TEMPORARY VIEW kafka_avro_tbl AS
SELECT
    lastUpdatedTime AS lastupdatedtime,
    loyaltynoteid AS loyaltynoteid,
    loyaltyid AS loyaltyid,
    notenumber AS notenumber,
    notestate AS notestate,
    notetype AS notetype,
    expirydate AS expirydate,
    modeofnotedelivery AS modeofnotedelivery,
    notedeliverymodereason AS notedeliverymodereason,
    issuedate AS issuedate,
    CAST(CAST(headers.SystemTime AS BIGINT)/1000 AS TIMESTAMP) AS objectmodelcreationtime,
    current_timestamp() as record_load_timestamp
FROM kafka_tbl;

INSERT overwrite s3_merged_data SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY loyaltynoteid ORDER BY objectmodelcreationtime DESC) AS rn FROM (
        SELECT * FROM kafka_avro_tbl
            UNION ALL
            SELECT
                lastupdatedtime,
                loyaltynoteid,
                loyaltyid,
                notenumber,
                notestate,
                notetype,
                expirydate,
                modeofnotedelivery,
                notedeliverymodereason,
                issuedate,
                objectmodelcreationtime,
                nvl(record_load_timestamp,current_timestamp())
            FROM {schema_name}.{resulting_lmo_table_name})
        AS uninon) 
    AS tbl
WHERE tbl.rn=1 and 1=1;

