CREATE TEMPORARY VIEW exploded_kafka_source AS
SELECT
    loyaltyId,
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

CREATE TEMPORARY VIEW loyalty_member_ldg AS
SELECT
	loyaltyId,
    customerId,
    mergedTo,
    subsumedLoyaltyId.loyaltyId AS subsumedLoyaltyId_loyaltyId,
    subsumedLoyaltyId.enrollmentDateTime AS subsumedLoyaltyId_enrollmentDateTime,
    subsumedLoyaltyId.mergeDateTimeAndOffset.value AS subsumedLoyaltyId_mergeDateTimeAndOffset_value,
    subsumedLoyaltyId.mergeDateTimeAndOffset.offset AS subsumedLoyaltyId_mergeDateTimeAndOffset_offset,
    membershipStatus.membershipStatus AS membershipStatus_membershipStatus,
    membershipStatus.effectiveDateTimeAndOffset.value AS membershipStatus_effectiveDateTimeAndOffset_value,
    membershipStatus.effectiveDateTimeAndOffset.offset AS membershipStatus_effectiveDateTimeAndOffset_offset,
    membershipStatus.changeDateTimeAndOffset.value AS membershipStatus_changeDateTimeAndOffset_value,
    membershipStatus.changeDateTimeAndOffset.offset AS membershipStatus_changeDateTimeAndOffset_offset,
    cardHolderStatus.isCardholder AS cardHolderStatus_isCardholder,
    cardHolderStatus.effectiveDateTimeAndOffset.value AS cardHolderStatus_effectiveDateTimeAndOffset_value,
    cardHolderStatus.effectiveDateTimeAndOffset.offset AS cardHolderStatus_effectiveDateTimeAndOffset_offset,
    cardHolderStatus.changeDateTimeAndOffset.value AS cardHolderStatus_changeDateTimeAndOffset_value,
    cardHolderStatus.changeDateTimeAndOffset.offset AS cardHolderStatus_changeDateTimeAndOffset_offset,
    enrollmentInfo.enrollCountry AS enrollmentInfo_enrollCountry,
    enrollmentInfo.enrollEmployee,
    enrollmentInfo.enrollEmpDept,
    enrollmentInfo.enrollStore AS enrollmentInfo_enrollStore,
    enrollmentInfo.enrollChannel AS enrollmentInfo_enrollChannel,
    enrollmentInfo.enrollDateTimeAndOffset.value AS enrollmentInfo_enrollDateTimeAndOffset_value,
    enrollmentInfo.enrollDateTimeAndOffset.offset AS enrollmentInfo_enrollDateTimeAndOffset_offset,
    enrollmentInfo.migrationDate,
    nordyLevel.nordyLevel AS nordyLevel_nordyLevel,
    nordyLevel.startDateTime AS nordyLevel_startDateTime,
    nordyLevel.startDateOffset,
    nordyLevel.endDateTime AS nordyLevel_endDateTime,
    nordyLevel.endDateOffset,
    nordyLevel.changeDateTimeAndOffset.value AS nordyLevel_changeDateTimeAndOffset_value,
    nordyLevel.changeDateTimeAndOffset.offset AS nordyLevel_changeDateTimeAndOffset_offset,
    systemTime,
    CreationTime,
    loyaltyPointsBalance
FROM exploded_kafka_source;


DELETE FROM acp_sandbox.loyalty_member_wrk ALL;


CREATE OR REPLACE TEMPORARY VIEW mbmr_ldg AS
SELECT * from 
    (SELECT *, row_number() over (
    partition by loyaltyid  order by systemtime desc
    ) rank from loyalty_member_ldg ) tmp where tmp.rank = 1;

-- ALTER TABLE acp_sandbox.loyaltymember_dim_delta SET TBLPROPERTIES (
--    'delta.columnMapping.mode' = 'name',
--    'delta.minReaderVersion' = '2',
--    'delta.minWriterVersion' = '5');

-- ALTER TABLE acp_sandbox.loyaltymember_dim_delta RENAME COLUMN loyaltyId TO loyalty_id;

-- ALTER TABLE acp_sandbox.loyaltymember_dim_delta RENAME COLUMN customerId TO acp_id;

INSERT INTO acp_sandbox.loyalty_member_wrk
SELECT
    mbmr_ldg.loyaltyId AS loyaltyId,
    dim.customerId as customerId,
    mbmr_ldg.mergedTo as merged_to_loyalty_id,
    CASE
        WHEN mbmr_ldg.mergedTo IS NOT NULL
            THEN 'N'
        ELSE 'Y'
    END loyalty_id_active_ind,
    CASE
        WHEN (cardmember_enroll_date IS NULL OR cardmember_ind='N') AND mbmr_ldg.membershipStatus_membershipStatus NOT IN ('CLOSED','PENDING_CLOSED')
            THEN 'Y'
        ELSE 'N'
    END AS member_ind,
    mbmr_ldg.membershipStatus_membershipStatus AS member_status_desc,
    to_date(mbmr_ldg.membershipStatus_effectiveDateTimeAndOffset_value) AS member_status_date,
    to_date(mbmr_ldg.enrollmentInfo_enrollDateTimeAndOffset_value) AS member_enroll_date,
    mbmr_ldg.enrollmentInfo_enrollStore as member_enroll_store_num,
    CASE WHEN str.currentData.adminHier.buDesc is NULL or str.storenumber=828
            THEN mbmr_ldg.enrollmentInfo_enrollChannel
        ELSE str.currentData.adminHier.buDesc
    END member_enroll_channel_desc,
    str.currentData.adminHier.regionDesc  AS member_enroll_region_desc,
    mbmr_ldg.enrollmentInfo_enrollCountry AS member_enroll_country_code,
    CASE WHEN mbmr_ldg.cardHolderStatus_isCardholder ='TRUE'
            THEN 'Y'
        WHEN mbmr_ldg.cardHolderStatus_isCardholder ='FALSE'
            THEN 'N'
        ELSE 'N'
    END cardmember_ind,
    to_date(mbmr_ldg.cardHolderStatus_effectiveDateTimeAndOffset_value) AS cardmember_status_date,
    CASE WHEN member_enroll_date < '2016-05-18'
            THEN member_enroll_date
        WHEN member_enroll_date >='2016-05-18' AND  mbmr_ldg.cardHolderStatus_isCardholder='TRUE'
            THEN cardmember_status_date
        ELSE NULL
    END cardmember_enroll_date,
    CASE WHEN datediff(member_enroll_date,cardmember_enroll_date)> 7 then 'Y'
        ELSE 'N'
    END AS member_migration_ind,
    mbmr_ldg.nordyLevel_nordyLevel AS rewards_level,
    to_date(mbmr_ldg.nordyLevel_startDateTime)  AS rewards_level_start_date,
    to_date(mbmr_ldg.nordyLevel_endDateTime)  AS rewards_level_end_date,
    mbmr_ldg.systemTime as sys_time,
    dim.dw_sys_load_tmstp as dw_sys_load_tmstp,
    mbmr_ldg.loyaltyPointsBalance AS loyalty_points_balance
    FROM mbmr_ldg
        LEFT JOIN nsp.org_store_dim_delta AS str 
            ON mbmr_ldg.enrollmentInfo_enrollStore = str.storenumber
        LEFT JOIN acp_sandbox.loyaltymember_dim_delta AS dim
            ON mbmr_ldg.loyaltyId = dim.loyaltyId 
            WHERE dim.loyaltyId is NULL 
            OR mbmr_ldg.systemtime > dim.sys_time;
