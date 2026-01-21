-- Read kafka source data with exploded subsumedLoyaltyId
CREATE TEMPORARY VIEW loyalty_transaction_s01 AS
SELECT
    loyaltyActivityId loyaltyActivityId,
    loyaltyId loyaltyId,
    financialRetailTransactionRecordId financialRetailTransactionRecordId,
    loyaltyTransactionKey loyaltyTransactionKey,
    transactionType transactionType,
    transactionNumber transactionNumber,
    loyaltyEffectiveTransactionDateTime loyaltyEffectiveTransactionDateTime,
    activityType activityType,
    intentStoreNumber intentStoreNumber,
    originalTransaction.store as orig_store,
    originalTransaction.transactionNumber as orig_transactionNumber,
    originalTransaction.register as orig_register,
    originalTransaction.businessDayDate as orig_businessDayDate,
    actionType actionType,
    creditTransaction.merchantCategoryCode as credit_merch_category_code,
    creditTransaction.channel as credit_channel_code,
    isEmployeeAssociated isEmployeeAssociated,
    isSystemGenerated isSystemGenerated,
    accumulatorInfo.totalNetSpend as totalNetSpend,
    accumulatorInfo.netTenderSpend netTenderSpend,
    accumulatorInfo.netNonTenderSpend netNonTenderSpend,
    accumulatorInfo.evolutionTierSpend evolutionTierSpend,
    accumulatorInfo.iconTierSpend iconTierSpend,
    accumulatorInfo.netOutsideSpend netOutsideSpend,
    accumulatorInfo.basePoints basePoints,
    accumulatorInfo.bonusPoints bonusPoints,
    accumulatorInfo.totalPoints totalPoints,
    explode_outer(transactionTenders) transactionTenders,
    activityAttributes activityAttributes,
    countryOfTransaction countryOfTransaction,
    agentID agentID,
    interactionReason interactionReason,
    decode(headers.SystemTime,'UTF-8') as SystemTime,
    businessdaydate
from kafka_source
where activityType='TRANSACTION' and loyaltyid != '0';

-- prepare final table
CREATE TEMPORARY VIEW loyalty_transaction_final_extract as
SELECT
    DISTINCT loyaltyActivityId,
             loyaltyId,
             financialRetailTransactionRecordId,
              loyaltyTransactionKey,
              transactionType,
              transactionNumber,
              loyaltyEffectiveTransactionDateTime,
              activityType,
              intentStoreNumber,
              orig_store,
              orig_transactionNumber,
              orig_register,
              orig_businessDayDate,
              actionType,
              credit_merch_category_code,
              credit_channel_code,
              isEmployeeAssociated,
              isSystemGenerated,
              totalNetSpend,
              netTenderSpend,
              netNonTenderSpend,
              evolutionTierSpend,
              iconTierSpend,
              netOutsideSpend,
              basePoints,
              bonusPoints,
              totalPoints,
              transactionTenders.tenderItemSeqNumber,
              transactionTenders.tenderType,
              transactionTenders.cardTypeCode,
              transactionTenders.cardSubTypeCode,
              transactionTenders.productType,
              transactionTenders.amount.units,
              transactionTenders.amount.nanos,
              transactionTenders.amount.currencyCode,
              countryOfTransaction,
              agentID,
              interactionReason,
              SystemTime,
              businessdaydate
FROM loyalty_transaction_s01;

CREATE TEMPORARY VIEW loyalty_promo_s01 as
select
        loyaltyActivityId,
        activityType,
        explode_outer(activityAttributes) activityAttributes,
        SystemTime,businessdaydate
        from loyalty_transaction_s01;

CREATE TEMPORARY VIEW loyalty_promo_final_extract as
select
    DISTINCT loyaltyActivityId,
    activityType,
    activityAttributes.promotionId as promotionId,
    activityAttributes.promotionName as promotionName,
    activityAttributes.attribute as attribute,
    activityAttributes.attributeValue as attributeValue,
    SystemTime,businessdaydate
from loyalty_promo_s01;

-- TRANSACTION

-- write result table
INSERT OVERWRITE loyalty_transaction_data_extract
SELECT * FROM loyalty_transaction_final_extract;

-- Audit table
INSERT OVERWRITE loyalty_transaction_final_count
SELECT COUNT(*) FROM loyalty_transaction_final_extract;



-- PROMO

-- write result table
INSERT OVERWRITE loyalty_promo_data_extract
    SELECT * FROM loyalty_promo_final_extract;

-- Audit table
INSERT OVERWRITE loyalty_promo_final_count
    SELECT COUNT(*) FROM loyalty_promo_final_extract;


    
