SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_competitor_price_match_dump_dev_11521_ACE_ENG;
     Task_Name=ddl_competitor_price_match_dump;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_PRICE_MATCHING.competitior_price_matching
Team/Owner: Analytics Engineering
Date Created/Modified:2023-10-13

*/
-- Comment out prior to merging to production.
-- drop table {price_matching_t2_schema}.competitior_price_matching;
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{price_matching_t2_schema}', 'competitor_price_match_dump', OUT_RETURN_MSG);


create multiset table {price_matching_t2_schema}.competitor_price_match_dump
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
    comp_scrape_date_est DATE,
    dw_sys_load_timestamp_pst timestamp
    )
    primary index(comp_scrape_date_est,style_group_num)
    PARTITION BY RANGE_N(comp_scrape_date_est BETWEEN DATE '2018-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY);
    ;

-- Table comments
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump IS 'Competitor Price Matching data, sourced from dataweave third party vendor - dataweave';

--Column Comments
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.style_group_num  IS 'Nordstrom Style Group ID';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.nordstrom_direct_color_code  IS 'Nordstrom Display Color Code (NDCC Color Code)';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.product_size  IS 'Nordstrom Size of the item';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.product_brand_name IS 'Nordstrom Product label';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.regular_price IS 'Nordstrom Selling Retail';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.competitor_name IS 'Competitor Name';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.comp_scrape_tstamp_est IS 'Date when data was last scraped in EST timestamp';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.comp_offer_price IS 'Competitor Selling Retail';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.effective_timestamp_est IS 'Same as comp_scrape_tstamp_est';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.on_promo IS 'Whether the Item is currently on promotion on the Competitor website';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.promo_name IS 'Competitor Banner/Enticement (if applicable)';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.promo_effective_date IS 'Date until the promotion is effective. This comes from the PDP page of competitor sites. No timezone captured.';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.vpn IS 'Nordstrom Vendor Product Numbers';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.rms_sku_num IS 'Nordstrom RMS SKU Id of the item';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.competitor_base_price IS 'Competitor base price of the item';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.comp_scrape_date_est IS 'date of comp_scrape_tstamp_est';
COMMENT ON {price_matching_t2_schema}.competitor_price_match_dump.dw_sys_load_timestamp_pst IS 'table load timestamp in PST';

--SQL script must end with statement to turn off QUERY_BAND 
SET QUERY_BAND = NONE FOR SESSION;
