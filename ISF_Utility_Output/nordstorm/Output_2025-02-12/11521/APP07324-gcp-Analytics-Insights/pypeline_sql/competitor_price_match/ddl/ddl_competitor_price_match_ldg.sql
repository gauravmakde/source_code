SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_competitor_price_matching_11521_ACE_ENG;
     Task_Name=ddl_competitor_price_match_dump_ldg;'
     FOR SESSION VOLATILE;

/*

T2 ldg Table Name: T2DL_DAS_PRICE_MATCHING.competitor_price_match_dump_ldg
Team/Owner: Analytics Engineering
Date Created/Modified:2023-10-13

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{price_matching_t2_schema}', 'competitor_price_match_dump_ldg', OUT_RETURN_MSG);


create multiset table {price_matching_t2_schema}.competitor_price_match_dump_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    style_group_num VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
    nordstrom_direct_color_code VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
    product_size VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    product_brand_name VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    regular_price DECIMAL(12,2),
    competitor_name VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    comp_scrape_tstamp_est TIMESTAMP,
    comp_offer_price DECIMAL(12,2),
    effective_timestamp_est TIMESTAMP,
    on_promo INTEGER,
    promo_name VARCHAR(3600) CHARACTER SET UNICODE NOT CASESPECIFIC,
    promo_effective_date DATE,
    vpn VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    rms_sku_num VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
    competitor_base_price DECIMAL(12,2), 
    comp_scrape_date_est DATE
    )
    primary index(comp_scrape_date_est, style_group_num)
    PARTITION BY RANGE_N(comp_scrape_date_est BETWEEN DATE '2018-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY);

SET QUERY_BAND = NONE FOR SESSION;
