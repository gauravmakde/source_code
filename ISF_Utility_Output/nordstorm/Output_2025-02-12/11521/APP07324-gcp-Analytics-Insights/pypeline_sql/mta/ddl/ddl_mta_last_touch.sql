SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=ddl_mta_last_touch_11521_ACE_ENG;
     Task_Name=ddl_mta_last_touch;'
     FOR SESSION VOLATILE;

CREATE MULTISET TABLE {mta_t2_schema}.mta_last_touch
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    order_number VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , mktg_type VARCHAR(32) COMPRESS CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_channel VARCHAR(400) COMPRESS CHARACTER SET UNICODE NOT CASESPECIFIC -- utm_channel VARCHAR(64) max 261
    , order_date_pacific date
    , sp_campaign VARCHAR(2000) CHARACTER SET Unicode NOT CaseSpecific --  sp_campaign VARCHAR(200) max 1448
    , utm_campaign VARCHAR(2000) CHARACTER SET Unicode NOT CaseSpecific
    , utm_content VARCHAR(4000) CHARACTER SET Unicode NOT CaseSpecific -- utm_content VARCHAR(3000) max 3378
    , utm_source VARCHAR(200) CHARACTER SET Unicode NOT CaseSpecific
    , utm_term VARCHAR(300) CHARACTER SET Unicode NOT CaseSpecific
    , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(order_number)
PARTITION BY RANGE_N(order_date_pacific BETWEEN DATE '2022-01-31' AND DATE '2025-12-31' EACH INTERVAL '1' DAY);


--Table Comments
COMMENT ON  {mta_t2_schema}.mta_last_touch IS 'Used for 30-day click model reporting for NMN';
--Column Comments
COMMENT ON  {mta_t2_schema}.mta_last_touch.order_number IS 'Reflects order_num when available, and global_tran_id otherwise';
COMMENT ON  {mta_t2_schema}.mta_last_touch.mktg_type IS '"Paid" when utm_channel is not null, and SEO_SEARCH otherwise';
COMMENT ON  {mta_t2_schema}.mta_last_touch.utm_channel IS 'The utm_channel associated with the latest arrived_timestamp';
COMMENT ON  {mta_t2_schema}.mta_last_touch.order_date_pacific IS ' Reflects order_date when available, and tran_date otherwise';
COMMENT ON  {mta_t2_schema}.mta_last_touch.sp_campaign IS 'Single-player campaign';
COMMENT ON  {mta_t2_schema}.mta_last_touch.utm_campaign IS 'Identifies a specific product promotion or strategic campaign';
COMMENT ON  {mta_t2_schema}.mta_last_touch.utm_content IS 'This is an optional field. If you have multiple links in the same campaign, like two links in the same email, you can fill in this value so you can differentiate them. For most marketers, this data is more detailed than they really need.';
COMMENT ON  {mta_t2_schema}.mta_last_touch.utm_source IS 'The individual site within that channel. For example, Facebook would be one of the sources within your Social medium for any unpaid links that you post to Facebook.';
COMMENT ON  {mta_t2_schema}.mta_last_touch.utm_term IS 'Channel or platform. Contain keywords like "IOS"';



SET QUERY_BAND = NONE FOR SESSION;