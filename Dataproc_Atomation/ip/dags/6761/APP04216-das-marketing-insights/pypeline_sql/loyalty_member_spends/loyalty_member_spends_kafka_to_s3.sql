-- Read kafka source data with exploded nonTenderSpendAccumulator
CREATE TEMPORARY VIEW exploded_kafka_source AS
SELECT
    loyaltyId,
    explode_outer(nonTenderSpendAccumulator) as (nonTenderSpendAccumulator_key, nonTenderSpendAccumulator_value),
    tenderSpendAccumulator,
    evolutionTierSpendAccumulator,
    decode(headers.SystemTime,'UTF-8') as SystemTime
FROM kafka_source
WHERE
    size(nonTenderSpendAccumulator) > 0 OR
    size(tenderSpendAccumulator) > 0 OR
    size(evolutionTierSpendAccumulator) > 0;

-- Create temp view with exploded tenderSpendAccumulator
CREATE TEMPORARY VIEW exploded_tenderSpendAccumulator_view AS
SELECT
    loyaltyId,
    nonTenderSpendAccumulator_key,
    nonTenderSpendAccumulator_value,
    explode_outer(tenderSpendAccumulator) as (tenderSpendAccumulator_key, tenderSpendAccumulator_value),
    evolutionTierSpendAccumulator,
    SystemTime
FROM exploded_kafka_source;

-- Create temp view with exploded evolutionTierSpendAccumulator for final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    loyaltyId,
    nonTenderSpendAccumulator_key,
    nonTenderSpendAccumulator_value,
    tenderSpendAccumulator_key,
    tenderSpendAccumulator_value,
    explode_outer(evolutionTierSpendAccumulator) as (evolutionTierSpendAccumulator_key, evolutionTierSpendAccumulator_value),
    SystemTime
FROM exploded_tenderSpendAccumulator_view;

-- write result table
INSERT OVERWRITE loyalty_member_spends_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE loyalty_member_spends_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
