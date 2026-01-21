-- Read kafka source data with exploded subsumedLoyaltyId
CREATE TEMPORARY VIEW exploded_kafka_source AS
SELECT
    loyaltyId AS primaryLoyaltyid,
    customerId,
    mergedTo,
    explode_outer(subsumedLoyaltyId) AS subsumedLoyaltyId,
    membershipStatus,
    cardHolderStatus,
    enrollmentInfo,
    nordyLevel,
    decode(headers.SystemTime,'UTF-8') as SystemTime,
    decode(headers.CreationTime,'UTF-8') AS CreationTime,
    loyaltyPointsBalance
from kafka_source;

-- prepare final table
CREATE TEMPORARY VIEW prepared_final_result_table as
SELECT
    primaryLoyaltyid,
    customerId,
    mergedTo,
    subsumedLoyaltyId.loyaltyId AS subsumedLoyaltyId_loyaltyId,
    subsumedLoyaltyId.enrollmentDateTime AS subsumedLoyaltyId_enrollmentDateTime,
    subsumedLoyaltyId.mergeDateTimeAndOffset.value AS subsumedLoyaltyId_mergeDateTimeAndOffset_value,
    subsumedLoyaltyId.mergeDateTimeAndOffset.offset AS subsumedLoyaltyId_mergeDateTimeAndOffset_offset,
    membershipStatus.membershipStatus,
    membershipStatus.effectiveDateTimeAndOffset.value AS membershipStatus_effectiveDateTimeAndOffset_value,
    membershipStatus.effectiveDateTimeAndOffset.offset AS membershipStatus_effectiveDateTimeAndOffset_offset,
    membershipStatus.changeDateTimeAndOffset.value AS membershipStatus_changeDateTimeAndOffset_value,
    membershipStatus.changeDateTimeAndOffset.offset AS membershipStatus_changeDateTimeAndOffset_offset,
    cardHolderStatus.isCardholder,
    cardHolderStatus.effectiveDateTimeAndOffset.value AS cardHolderStatus_effectiveDateTimeAndOffset_value,
    cardHolderStatus.effectiveDateTimeAndOffset.offset AS cardHolderStatus_effectiveDateTimeAndOffset_offset,
    cardHolderStatus.changeDateTimeAndOffset.value AS cardHolderStatus_changeDateTimeAndOffset_value,
    cardHolderStatus.changeDateTimeAndOffset.offset AS cardHolderStatus_changeDateTimeAndOffset_offset,
    enrollmentInfo.enrollCountry,
    enrollmentInfo.enrollEmployee,
    enrollmentInfo.enrollEmpDept,
    enrollmentInfo.enrollStore,
    enrollmentInfo.enrollChannel,
    enrollmentInfo.enrollDateTimeAndOffset.value AS enrollmentInfo_enrollDateTimeAndOffset_value,
    enrollmentInfo.enrollDateTimeAndOffset.offset AS enrollmentInfo_enrollDateTimeAndOffset_offset,
    enrollmentInfo.migrationDate,
    nordyLevel.nordyLevel,
    nordyLevel.startDateTime,
    nordyLevel.startDateOffset,
    nordyLevel.endDateTime,
    nordyLevel.endDateOffset,
    nordyLevel.changeDateTimeAndOffset.value AS nordyLevel_changeDateTimeAndOffset_value,
    nordyLevel.changeDateTimeAndOffset.offset AS nordyLevel_changeDateTimeAndOffset_offset,
    SystemTime,
    CreationTime,
    loyaltyPointsBalance
FROM exploded_kafka_source;

-- write result table
INSERT OVERWRITE loyalty_member_data_extract
    SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE loyalty_member_final_count
    SELECT COUNT(*) FROM prepared_final_result_table;
