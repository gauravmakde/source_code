SET QUERY_BAND = 'App_ID=APP09267;
     DAG_ID=ddl_consented_relationship_count_aggs_11521_ACE_ENG; 
     Task_Name=ddl_consented_relationship_count_aggs;'
     FOR SESSION VOLATILE;

--comment out before merge to prod
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'consented_relationship_count_aggs', OUT_RETURN_MSG);
/*
Table definition for T2DL_DAS_CLIENTELING.consented_relationship_count_aggs
*/
CREATE MULTISET TABLE {cli_t2_schema}.consented_relationship_count_aggs
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

	, loyalty_level									VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC

	, worker_status									VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
	, payroll_store									VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
	, payroll_department							VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC

    , seller_tenure                              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , reln_w_seller_tenure                       VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , reln_w_Clienteling_tenure                  VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC

	--AGGREGATED
    , cnt_optedInToday 							INTEGER COMPRESS
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
COMMENT ON  {cli_t2_schema}.consented_relationship_count_aggs IS 'Consented Relationship Count Aggregations - Feeds Dashboard';
-- Column comments
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.week_num IS 'week number';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fm_range IS 'fiscal month date range';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fiscal_week IS 'fiscal week';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fiscal_month IS 'fiscal month';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fiscal_quarter IS 'fiscal quarter';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fiscal_year IS 'fiscal year';

COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.loyalty_level IS 'loyalty level';

COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.worker_status IS 'worker status';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.payroll_store IS 'payroll store';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.payroll_department IS 'payroll department';

COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.seller_tenure IS 'seller tenure';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.reln_w_seller_tenure IS 'relationship with seller tenure';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.reln_w_Clienteling_tenure IS 'relationship with clienteling tenure';

-- AGGREGATED
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.cnt_optedInToday IS 'count of opted in today';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.cnt_optedOutToday IS 'count of opted out today';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.cnt_Consented_wSeller IS 'count of consented with seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.cnt_has1yrNonTerminatedSeller IS 'count of 1 year non-terminated seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.cnt_hasOnlyTerminatedSeller IS 'count of only terminated seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.cnt_allConnections_churned IS 'count of all connections churned';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.cnt_fp_churned IS 'count of first purchase churned';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.cnt_new_to_clienteling IS 'count of new to clienteling';

COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fls_trips IS 'total FLS trips';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.ncom_trips IS 'total NCOM trips';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.sb_ncom_trips IS 'total SB NCOM trips';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.gross_fls_spend IS 'total gross FLS spend';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.gross_ncom_spend IS 'total gross NCOM spend';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.sb_gross_ncom_spend IS 'total SB gross NCOM spend';

COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fls_trips_wConnection IS 'FLS trips with Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.ncom_trips_wConnection IS 'NCOM trips with Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.sb_ncom_trips_wConnection IS 'SB NCOM trips with Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.gross_fls_spend_wConnection IS 'gross FLS spend with Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.gross_ncom_spend_wConnection IS 'gross NCOM spend with Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.sb_gross_ncom_spend_wConnection IS 'SB gross NCOM spend with Consented Seller';

COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fls_trips_wNonConnection IS 'FLS trips with Non-Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.ncom_trips_wNonConnection IS 'NCOM trips with Non-Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.sb_ncom_trips_wNonConnection IS 'SB NCOM trips with Non-Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.gross_fls_spend_wNonConnection IS 'gross FLS spend with Non-Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.gross_ncom_spend_wNonConnection IS 'gross NCOM spend with Non-Consented Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.sb_gross_ncom_spend_wNonConnection IS 'SB gross NCOM spend with Non-Consented Seller';

COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.fls_trips_woSeller IS 'FLS trips without Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.ncom_trips_woSeller IS 'NCOM trips without Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.sb_ncom_trips_woSeller IS 'SB NCOM trips without Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.gross_fls_spend_woSeller IS 'gross FLS spend without Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.gross_ncom_spend_woSeller IS 'gross NCOM spend without Seller';
COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.sb_gross_ncom_spend_woSeller IS 'SB gross NCOM spend without Seller';

COMMENT ON {cli_t2_schema}.consented_relationship_count_aggs.dw_sys_load_tmstp IS 'timestamp when data was last updated';


SET QUERY_BAND = NONE FOR SESSION;