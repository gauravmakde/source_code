SET QUERY_BAND = 'App_ID=APP09267;
     DAG_ID=ddl_consented_customer_funnel_aggs_11521_ACE_ENG;
     Task_Name=ddl_consented_customer_funnel_aggs;'
     FOR SESSION VOLATILE;

--comment out before move to prod
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'consented_customer_funnel_aggs', OUT_RETURN_MSG);
/*
Table definition for T2DL_DAS_CLIENTELING.consented_customer_funnel_aggs
*/
CREATE MULTISET TABLE {cli_t2_schema}.consented_customer_funnel_aggs
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    , MAP = TD_MAP1
    (
    first_clienteling_consent_week_num				INTEGER
    , x_weeks_post_clienteling_consent				INTEGER
    
    , payroll_store                       			VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , payroll_department      			            VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC

	--AGGREGATED TO MONTH LEVEL 
	, week0_reln_cnt             					INTEGER COMPRESS
	, reln_cnt 		                    			INTEGER COMPRESS
	, reln_w_nonTermed_seller_cnt    				INTEGER COMPRESS
	, reln_opt_out                  				INTEGER COMPRESS
	, reln_w_purchase_cnt           				INTEGER COMPRESS
	
	, seller_fp_trips           					INTEGER COMPRESS
	, seller_gross_fp_spend 					    DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00

    , x_weeks_post_reln_w_purchase_cnt              INTEGER COMPRESS
    , x_weeks_post_SC_consented_fp_trips            INTEGER COMPRESS
    , x_weeks_post_SC_consented_gross_fp_spend      DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	
    , dw_sys_load_tmstp  							TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
    PRIMARY INDEX (first_clienteling_consent_week_num, x_weeks_post_clienteling_consent) 
    PARTITION BY RANGE_N(first_clienteling_consent_week_num between 202301 and 202612 each 1)
    ;


-- table comment
COMMENT ON  {cli_t2_schema}.consented_customer_funnel_aggs IS 'Consented Customer Funnel Aggregations - Feeds Dashboard';
-- Column comments
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.first_clienteling_consent_week_num IS 'first clienteling consent week number';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.x_weeks_post_clienteling_consent IS 'weeks post clienteling consent';

COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.payroll_store IS 'payroll store';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.payroll_department IS 'payroll department';

-- AGGREGATED TO MONTH LEVEL
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.week0_reln_cnt IS 'week 0 relationship count';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.reln_cnt IS 'week x relationship count';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.reln_w_nonTermed_seller_cnt IS 'week x relationship with non-terminated seller count';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.reln_opt_out IS 'week x relationship opt out';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.reln_w_purchase_cnt IS 'week x relationship with purchase count';

COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.seller_fp_trips IS 'week x consented seller seller full-priced trips';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.seller_gross_fp_spend IS 'week x seller gross full-priced spend';

COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.x_weeks_post_reln_w_purchase_cnt IS 'accumulated through week x relationship with purchase count';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.x_weeks_post_SC_consented_fp_trips IS 'accumulated through week x consented seller full-priced trips';
COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.x_weeks_post_SC_consented_gross_fp_spend IS 'accumulated through week x consented seller full-priced gross spend';

COMMENT ON {cli_t2_schema}.consented_customer_funnel_aggs.dw_sys_load_tmstp IS 'timestamp when data was last updated';

SET QUERY_BAND = NONE FOR SESSION;


