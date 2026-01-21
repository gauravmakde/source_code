CREATE TEMPORARY VIEW kafka_avro_tbl AS
SELECT
    loyaltyId                           AS loyaltyid,
    loyaltyActivityId                   AS loyaltyactivityid,
    loyaltyTransactionKey               AS loyaltytransactionkey,
    transactionNumber                   AS transactionnumber,
    activityType                        AS activitytype,
    actionType                          AS actiontype,
    transactionType                     AS transactiontype,
    loyaltyEffectiveTransactionDateTime AS loyaltyeffectivetransactiondatetime,
    intentStoreNumber                   AS intentstorenumber,
    countryOfTransaction                AS countryoftransaction,
    isSystemGenerated                   AS issystemgenerated,
    isEmployeeAssociated                as isemployeeassociated,
    creditTransaction                   as credittransaction,
    originalTransaction                 as originaltransaction,
    agentID                             as agentid,
    interactionReason                   as interactionreason,
    accumulatorInfo                     as accumulatorinfo,
    activityAttributes                  as activityattributes,
    transactionTenders                  as transactiontenders,
    businessDayDate                     as businessdaydate,
    CAST(CAST(headers.SystemTime AS BIGINT)/1000 AS TIMESTAMP) AS objectmodelcreationtime,
    headers.usid                        as uniquesourceid,
    current_timestamp()                 as record_load_timestamp
FROM kafka_tbl;

INSERT overwrite s3_merged_data SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY loyaltyactivityid ORDER BY objectmodelcreationtime DESC) AS rn FROM (
        SELECT * FROM kafka_avro_tbl
        -- since we set out of event id default to 0, fliter out these events to prevent loading into semantic layer
        where loyaltyid != '0'
            UNION ALL
            SELECT
                loyaltyId,
                loyaltyactivityid,
                loyaltytransactionkey,
                transactionnumber,
                activitytype,
                actiontype,
                transactiontype,
                loyaltyeffectivetransactiondatetime,
                intentstorenumber,
                countryoftransaction,
                issystemgenerated,
                isemployeeassociated,
                credittransaction,
                originaltransaction,
                agentid,
                interactionreason,
                accumulatorinfo,
                activityattributes,
                transactiontenders,
                businessdaydate,
                objectmodelcreationtime,
                uniquesourceid,
                record_load_timestamp
            FROM {schema_name}.{resulting_lao_table_name})
        AS uninon) 
    AS tbl
WHERE tbl.rn=1 and 1=1;

