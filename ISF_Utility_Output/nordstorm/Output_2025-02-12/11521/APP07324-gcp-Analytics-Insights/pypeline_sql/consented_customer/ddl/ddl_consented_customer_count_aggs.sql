
SET QUERY_BAND = 'App_ID=APP09267;
     DAG_ID=ddl_consented_customer_count_aggs_11521_ACE_ENG;
     Task_Name=ddl_consented_customer_count_aggs;'
     FOR SESSION VOLATILE; 

--comment out before merge to prod
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'consented_customer_count_aggs', OUT_RETURN_MSG);
/*
Table definition for T2DL_DAS_CLIENTELING.consented_customer_count_aggs
*/
CREATE MULTISET TABLE {cli_t2_schema}.consented_customer_count_aggs
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    , MAP = TD_MAP1
    (
	week_num										INTEGER
    , fm_range                                		VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, fiscal_week									INTEGER COMPRESS
    , fiscal_month									INTEGER COMPRESS
    , fiscal_quarter								INTEGER COMPRESS
    , fiscal_year									INTEGER COMPRESS
	, agg_rollup									VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
	, payroll_store									VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
	, payroll_department							VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
	, loyalty_level									VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , reln_per_seller                               VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC

	--AGGREGATED
    , cnt_sellers                             		INTEGER COMPRESS
	
    , cust_optedInToday 							INTEGER COMPRESS
    , cust_optedOutToday 							INTEGER COMPRESS
    , cust_Consented_wSeller 						INTEGER COMPRESS
    , cust_has1yrNonTerminatedSeller 				INTEGER COMPRESS
    , cust_hasOnlyTerminatedSeller 					INTEGER COMPRESS
    , cust_allConnections_churned 					INTEGER COMPRESS
    , cust_fp_churned 								INTEGER COMPRESS
    , cust_new_to_clienteling 						INTEGER COMPRESS
    , cust_phoneMarketable_Y 						INTEGER COMPRESS
    , cust_phoneMarketable_N 						INTEGER COMPRESS
    , cust_emailMarketable_Y 						INTEGER COMPRESS
    , cust_emailMarketable_N 						INTEGER COMPRESS
    , cnt_optedInToday 								INTEGER COMPRESS
    , cnt_optedOutToday 							INTEGER COMPRESS
    , cnt_Consented_wSeller 						INTEGER COMPRESS
    , cnt_has1yrNonTerminatedSeller 				INTEGER COMPRESS
    , cnt_hasOnlyTerminatedSeller 					INTEGER COMPRESS
    , cnt_allConnections_churned 					INTEGER COMPRESS
    , cnt_fp_churned 								INTEGER COMPRESS
    , cnt_new_to_clienteling 						INTEGER COMPRESS
    , fls_trips                                     INTEGER COMPRESS
    , ncom_trips                                    INTEGER COMPRESS
    , sb_ncom_trips                                 INTEGER COMPRESS
    , gross_fls_spend                               DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , gross_ncom_spend                              DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , sb_gross_ncom_spend                           DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , fls_trips_wConnection                         INTEGER COMPRESS
    , ncom_trips_wConnection                        INTEGER COMPRESS
    , sb_ncom_trips_wConnection                     INTEGER COMPRESS
    , gross_fls_spend_wConnection                   DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , gross_ncom_spend_wConnection                  DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , sb_gross_ncom_spend_wConnection               DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , fls_trips_wNonConnection                      INTEGER COMPRESS
    , ncom_trips_wNonConnection                     INTEGER COMPRESS
    , sb_ncom_trips_wNonConnection                  INTEGER COMPRESS
    , gross_fls_spend_wNonConnection                DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , gross_ncom_spend_wNonConnection               DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , sb_gross_ncom_spend_wNonConnection            DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , fls_trips_woSeller                            INTEGER COMPRESS
    , ncom_trips_woSeller                           INTEGER COMPRESS
    , sb_ncom_trips_woSeller                        INTEGER COMPRESS
    , gross_fls_spend_woSeller                      DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , gross_ncom_spend_woSeller                     DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00
    , sb_gross_ncom_spend_woSeller                  DECIMAL(20, 2) DEFAULT 0.00 COMPRESS 0.00

    , dw_sys_load_tmstp  							TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
    PRIMARY INDEX (week_num, payroll_store, payroll_department) 
	PARTITION BY RANGE_N(week_num between 202301 and 202612 each 1)
    ;


-- table comment
COMMENT ON  {cli_t2_schema}.consented_customer_count_aggs IS 'Consented Customer Count Aggregations - Feeds Dashboard';
-- Column comments
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.week_num IS 'week number';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fm_range IS 'fiscal month date range';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fiscal_week IS 'fiscal week';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fiscal_month IS 'fiscal month';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fiscal_quarter IS 'fiscal quarter';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fiscal_year IS 'fiscal year';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.agg_rollup IS 'aggregation rollup';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.payroll_store IS 'payroll store';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.payroll_department IS 'payroll department';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.loyalty_level IS 'loyalty level';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.reln_per_seller IS 'count of customers in sellers book';

-- AGGREGATED
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_sellers IS 'count of sellers';

COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_optedInToday IS 'customers opted in today';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_optedOutToday IS 'customers opted out today';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_Consented_wSeller IS 'customers consented with seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_has1yrNonTerminatedSeller IS 'customers with 1 year non-terminated seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_hasOnlyTerminatedSeller IS 'customers with only terminated seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_allConnections_churned IS 'customers with all connections churned';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_fp_churned IS 'customers with first purchase churned';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_new_to_clienteling IS 'count of new to clienteling';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_phoneMarketable_Y IS 'customers phone marketable yes';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_phoneMarketable_N IS 'customers phone marketable no';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_emailMarketable_Y IS 'customers email marketable yes';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_emailMarketable_N IS 'customers email marketable no';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_optedInToday IS 'count of opted in today';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_optedOutToday IS 'count of opted out today';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_Consented_wSeller IS 'count of consented with seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_has1yrNonTerminatedSeller IS 'count of 1 year non-terminated seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_hasOnlyTerminatedSeller IS 'count of only terminated seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_allConnections_churned IS 'count of all connections churned';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cnt_fp_churned IS 'count of first purchase churned';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.cust_new_to_clienteling IS 'customers new to clienteling';

COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fls_trips IS 'fls trips';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.ncom_trips IS 'ncom trips';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.sb_ncom_trips IS 'sb ncom trips';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.gross_fls_spend IS 'gross fls spend';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.gross_ncom_spend IS 'gross ncom spend';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.sb_gross_ncom_spend IS 'sb gross ncom spend';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fls_trips_wConnection IS 'fls trips with connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.ncom_trips_wConnection IS 'ncom trips with connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.sb_ncom_trips_wConnection IS 'sb ncom trips with connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.gross_fls_spend_wConnection IS 'gross fls spend with connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.gross_ncom_spend_wConnection IS 'gross ncom spend with connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.sb_gross_ncom_spend_wConnection IS 'sb gross ncom spend with connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fls_trips_wNonConnection IS 'fls trips with non-connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.ncom_trips_wNonConnection IS 'ncom trips with non-connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.sb_ncom_trips_wNonConnection IS 'sb ncom trips with non-connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.gross_fls_spend_wNonConnection IS 'gross fls spend with non-connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.gross_ncom_spend_wNonConnection IS 'gross ncom spend with non-connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.sb_gross_ncom_spend_wNonConnection IS 'sb gross ncom spend with non-connection';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.fls_trips_woSeller IS 'fls trips without seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.ncom_trips_woSeller IS 'ncom trips without seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.sb_ncom_trips_woSeller IS 'sb ncom trips without seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.gross_fls_spend_woSeller IS 'gross fls spend without seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.gross_ncom_spend_woSeller IS 'gross ncom spend without seller';
COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.sb_gross_ncom_spend_woSeller IS 'sb gross ncom spend without seller';

COMMENT ON {cli_t2_schema}.consented_customer_count_aggs.dw_sys_load_tmstp IS 'timestamp when data was last updated'; 

SET QUERY_BAND = NONE FOR SESSION;