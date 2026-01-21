-- Loyalty Member Alteration
CREATE TEMPORARY VIEW level1_loyalty_member_alteration_metamorph_task AS
SELECT
    loyaltyId,
    explode_outer(alterationBenefitDetail) alterationBenefitDetail,
    decode(headers.SystemTime,'UTF-8') as SystemTime
FROM kafka_source
WHERE size(alterationBenefitDetail) > 0;

CREATE TEMPORARY VIEW final_member_alteration_result_view AS
SELECT
    loyaltyId,
    alterationBenefitDetail.alterationChangeType,
    alterationBenefitDetail.alterationAmount,
    alterationBenefitDetail.actionDateTime,
    SystemTime
FROM level1_loyalty_member_alteration_metamorph_task;

SELECT COUNT(*) FROM final_member_alteration_result_view;

-- write result table
INSERT OVERWRITE loyalty_member_alteration_data_extract
SELECT * FROM final_member_alteration_result_view;

-- Audit table
INSERT OVERWRITE loyalty_member_alteration_final_count
SELECT COUNT(*) FROM final_member_alteration_result_view;

-- Loyalty Member PBPD
CREATE TEMPORARY VIEW level1_loyalty_member_pbpd_metamorph_task AS
SELECT
    loyaltyId,
    explode_outer(personalBonusPointDay) personalBonusPointDay,
    decode(headers.SystemTime,'UTF-8') as SystemTime
FROM kafka_source
WHERE size(personalBonusPointDay) > 0;

CREATE TEMPORARY VIEW final_member_pbpd_result_view AS
SELECT
    loyaltyId,
    personalBonusPointDay.pbpdType,
    personalBonusPointDay.pbpdStatus,
    personalBonusPointDay.countPbpd,
    personalBonusPointDay.actionDateTime,
    SystemTime
FROM level1_loyalty_member_pbpd_metamorph_task;

SELECT COUNT(*) FROM final_member_pbpd_result_view;

-- write result table
INSERT OVERWRITE loyalty_member_pbpd_data_extract
SELECT * FROM final_member_pbpd_result_view;

-- Audit table
INSERT OVERWRITE loyalty_member_pbpd_final_count
SELECT COUNT(*) FROM final_member_pbpd_result_view;
