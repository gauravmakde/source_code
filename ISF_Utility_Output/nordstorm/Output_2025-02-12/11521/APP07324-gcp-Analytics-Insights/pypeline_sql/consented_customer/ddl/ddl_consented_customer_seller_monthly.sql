
SET QUERY_BAND = 'App_ID=APP09267;
     DAG_ID=ddl_consented_customer_seller_monthly_11521_ACE_ENG;
     Task_Name=ddl_consented_customer_seller_monthly;'
     FOR SESSION VOLATILE;


--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'consented_customer_seller_monthly', OUT_RETURN_MSG);
/*
Table definition for T2DL_DAS_CLIENTELING.consented_customer_seller_monthly
*/
CREATE MULTISET TABLE {cli_t2_schema}.consented_customer_seller_monthly
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    , MAP = TD_MAP1
    (
    fm_range                                		VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , fiscal_month									INTEGER
    , fiscal_quarter								INTEGER COMPRESS
    , fiscal_year									INTEGER

    , seller_id                             		VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    --, seller_name                           		VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , worker_status                         		VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , payroll_store                       			VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , payroll_store_description           			VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , payroll_department_description      			VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC

	--AGGREGATED TO MONTH LEVEL 
	, month_tot_cnt_OptedInToday 					INTEGER COMPRESS
	, month_tot_cnt_OptedOutToday 					INTEGER COMPRESS
	, month_tot_cnt_Consented_wSeller 				INTEGER COMPRESS
	
	, month_tot_hasOnlyTerminatedSeller_ind 		INTEGER COMPRESS
	, month_tot_new_to_clienteling 					INTEGER COMPRESS
	, month_tot_has1yrNonTerminatedSeller_ind		INTEGER COMPRESS
	, month_tot_allConnections_churned_ind 			INTEGER COMPRESS
	, month_tot_FP_churned_ind 						INTEGER COMPRESS
	, month_tot_phoneMarketable_Y					INTEGER COMPRESS
	, month_tot_phoneMarketable_N					INTEGER COMPRESS
	, month_tot_emailMarketable_Y					INTEGER COMPRESS
	, month_tot_emailMarketable_N					INTEGER COMPRESS
	
	, month_tot_fls_trips 							INTEGER COMPRESS
	, month_tot_ncom_trips							INTEGER COMPRESS
	, month_tot_sb_ncom_trips 						INTEGER COMPRESS
	
	, month_tot_gross_fls_spend  					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, month_tot_gross_ncom_spend  					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, month_tot_sb_gross_ncom_spend 				DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	
	--AGGREGATED TO STORE LEVEL 
	, store_tot_cnt_OptedInToday 					INTEGER COMPRESS
	, store_tot_cnt_OptedOutToday 					INTEGER COMPRESS
	, store_tot_cnt_Consented_wSeller 				INTEGER COMPRESS

	, store_tot_hasOnlyTerminatedSeller_ind 		INTEGER COMPRESS
	, store_tot_new_to_clienteling 					INTEGER COMPRESS
	, store_tot_has1yrNonTerminatedSeller_ind		INTEGER COMPRESS
	, store_tot_allConnections_churned_ind 			INTEGER COMPRESS
	, store_tot_FP_churned_ind 						INTEGER COMPRESS
	, store_tot_phoneMarketable_Y					INTEGER COMPRESS
	, store_tot_phoneMarketable_N					INTEGER COMPRESS
	, store_tot_emailMarketable_Y					INTEGER COMPRESS
	, store_tot_emailMarketable_N					INTEGER COMPRESS
	
	, store_tot_fls_trips 							INTEGER COMPRESS
	, store_tot_ncom_trips 							INTEGER COMPRESS
	, store_tot_sb_ncom_trips 						INTEGER COMPRESS
	
	, store_tot_fls_trips_Connection 				INTEGER COMPRESS
	, store_tot_ncom_trips_Connection 				INTEGER COMPRESS
	, store_tot_sb_ncom_trips_Connection 			INTEGER COMPRESS
	
	, store_tot_fls_trips_NonConnection 			INTEGER COMPRESS
	, store_tot_ncom_trips_NonConnection 			INTEGER COMPRESS
	, store_tot_sb_ncom_trips_NonConnection 		INTEGER COMPRESS
	
	, store_tot_fls_trips_woSeller 					INTEGER COMPRESS
	, store_tot_ncom_trips_woSeller 				INTEGER COMPRESS
	, store_tot_sb_ncom_trips_woSeller 				INTEGER COMPRESS
	
	, store_tot_gross_fls_spend 					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, store_tot_gross_ncom_spend 					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, store_tot_sb_gross_ncom_spend					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
		
	--AGGREGATED TO SELLER LEVEL 
	, cnt_OptedInToday 								INTEGER COMPRESS
	, cnt_OptedOutToday 							INTEGER COMPRESS
	, cnt_Consented_wSeller 						INTEGER COMPRESS

	, cnt_has1yrNonTerminatedSeller 				INTEGER COMPRESS
	, cnt_hasOnlyTerminatedSeller 					INTEGER COMPRESS
	, cnt_allConnections_churned 					INTEGER COMPRESS
	
	, cnt_fp_churned 								INTEGER COMPRESS
	, cnt_new_to_clienteling 						INTEGER COMPRESS
	
	, cnt_phoneMarketable_Y							INTEGER COMPRESS
	, cnt_phoneMarketable_N							INTEGER COMPRESS
	, cnt_emailMarketable_Y							INTEGER COMPRESS
	, cnt_emailMarketable_N							INTEGER COMPRESS
	
	, cnt_1mo_wSeller								INTEGER COMPRESS
	, cnt_3mo_wSeller								INTEGER COMPRESS
	, cnt_6mo_wSeller								INTEGER COMPRESS
	, cnt_1yr_wSeller								INTEGER COMPRESS
	, cnt_2yr_wSeller								INTEGER COMPRESS
	
	, toConsented_fls_trips 					INTEGER COMPRESS
	, toConsented_ncom_trips 					INTEGER COMPRESS
	, toConsented_sb_ncom_trips				INTEGER COMPRESS

	, toConsented_gross_fls_spend 					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, toConsented_gross_ncom_spend 					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, toConsented_sb_gross_ncom_spend				DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	
    , dw_sys_load_tmstp  							TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
    PRIMARY INDEX (fm_range, seller_id, payroll_store) 
	PARTITION BY RANGE_N(fiscal_year*100+fiscal_month between 202301 and 202512 each 1)
    ;


-- table comment
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly IS 'Consented Customer Seller Monthly - Feeds Dashboard';
-- Column comments
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.fm_range IS 'fiscal month date range';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.fiscal_month IS 'fiscal month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.fiscal_quarter IS 'fiscal quarter';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.fiscal_year IS 'fiscal year';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.seller_id IS 'worker id of the consented seller';
--COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.seller_name IS 'name of the consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.worker_status IS 'describes ACTIVE/TERMINATED if consented seller or Non-Consented Seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.payroll_store IS 'home store employee id is tied to, but employee may not physically be located at store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.payroll_store_description IS 'home store description of employee id, but employee may not physically be located at store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.payroll_department_description IS 'home store department description of employee id';

-- Monthly Aggregates
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_cnt_OptedInToday IS 'count of customers that took action to opt in during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_cnt_OptedOutToday IS 'count of customers that took action to opt out during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_cnt_Consented_wSeller IS 'count of customers in consented relationship with a seller during given month';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_hasOnlyTerminatedSeller_ind IS 'count of consented customers during given month for whom all consented sellers have been terminated';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_new_to_clienteling IS 'count of consented customers that took action to opt into the program for the first time during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_has1yrNonTerminatedSeller_ind IS 'count of consented customers during given month for whom >=1 consented seller relationship has lasted >=1 year without termination';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_allConnections_churned_ind IS 'count of consented customers during given month for whom no FP purchase commissioned to a consented seller has occurred in >=1 year';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_FP_churned_ind IS 'count of consented customers during given month for whom no FP purchase has occurred in >=1 year';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_phoneMarketable_Y IS 'count of phone marketable consented customers during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_phoneMarketable_N IS 'count of non-phone marketable consented customers during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_emailMarketable_Y IS 'count of email marketable consented customers during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_emailMarketable_N IS 'count of non-email marketable consented customers during given month';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_fls_trips IS 'count of FLS Trips taken by consented customers during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_ncom_trips IS 'count of NCOM Trips taken by consented customers during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_sb_ncom_trips IS 'count of Styleboard NCOM Trips taken by consented customers during given month';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_gross_fls_spend IS 'total FLS gross $ spent by consented customers during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_gross_ncom_spend IS 'total NCOM gross $ spent by consented customers during given month';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.month_tot_sb_gross_ncom_spend IS 'total Styleboard NCOM gross $ spent by consented customers during given month';

-- Store Level Aggregates
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_cnt_OptedInToday IS 'count of customers that took action to opt in during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_cnt_OptedOutToday IS 'count of customers that took action to opt out during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_cnt_Consented_wSeller IS 'count of customers in consented relationship with a seller during given month and for given payroll store';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_hasOnlyTerminatedSeller_ind IS 'count of consented customers during given month and for given payroll store for whom all consented sellers have been terminated';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_new_to_clienteling IS 'count of consented customers that took action to opt into the program for the first time during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_has1yrNonTerminatedSeller_ind IS 'count of consented customers during given month and for given payroll store for whom >=1 consented seller relationship has lasted >=1 year without termination';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_allConnections_churned_ind IS 'count of consented customers during given month and for given payroll store for whom no FP purchase commissioned to a consented seller has occurred in >=1 year';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_FP_churned_ind IS 'count of consented customers during given month and for given payroll store for whom no FP purchase has occurred in >=1 year';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_phoneMarketable_Y IS 'count of phone marketable consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_phoneMarketable_N IS 'count of non-phone marketable consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_emailMarketable_Y IS 'count of email marketable consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_emailMarketable_N IS 'count of non-email marketable consented customers during given month and for given payroll store';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_fls_trips IS 'count of FLS Trips taken by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_ncom_trips IS 'count of NCOM Trips taken by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_sb_ncom_trips IS 'count of Styleboard NCOM Trips taken by consented customers during given month and for given payroll store';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_fls_trips_Connection IS 'count of consented seller commissionable FLS Trips taken by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_ncom_trips_Connection IS 'count of consented seller commissionable NCOM Trips taken by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_sb_ncom_trips_Connection IS 'count of consented seller commissionable Styleboard NCOM Trips taken by consented customers during given month and for given payroll store';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_fls_trips_NonConnection IS 'count of commissionable FLS Trips to a non-consented seller taken by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_ncom_trips_NonConnection IS 'count of commissionable NCOM Trips to a non-consented seller taken by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_sb_ncom_trips_NonConnection IS 'count of commissionable Styleboard NCOM Trips to a non-consented seller taken by consented customers during given month and for given payroll store';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_fls_trips_woSeller IS 'count of non-commissionable FLS Trips taken by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_ncom_trips_woSeller IS 'count of non-commissionable NCOM Trips taken by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_sb_ncom_trips_woSeller IS 'count of non-commissionable Styleboard NCOM Trips taken by consented customers during given month and for given payroll store';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_gross_fls_spend IS 'total FLS gross $ spent by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_gross_ncom_spend IS 'total NCOM gross $ spent by consented customers during given month and for given payroll store';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.store_tot_sb_gross_ncom_spend IS 'total Styleboard NCOM gross $ spent by consented customers during given month and for given payroll store';

-- Seller Level Aggregates  (PENDING)
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_OptedInToday IS 'count of customers that took action to opt in for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_OptedOutToday IS 'count of customers that took action to opt out for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_Consented_wSeller IS 'count of customers in consented relationship with a seller for given month/payroll store/consented seller';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_has1yrNonTerminatedSeller IS 'count of consented customers for given month/payroll store/consented seller for whom >=1 consented seller relationship has lasted >=1 year without termination';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_hasOnlyTerminatedSeller IS 'count of consented customers for given month/payroll store/consented seller for whom all consented sellers have been terminated';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_allConnections_churned IS 'count of consented customers for given month/payroll store/consented seller for whom no FP purchase commissioned to a consented seller has occurred in >=1 year';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_FP_churned IS 'count of consented customers for given month/payroll store/consented seller for whom no FP purchase has occurred in >=1 year';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_new_to_clienteling IS 'count of consented customers that took action to opt into the program for the first time for given month/payroll store/consented seller';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_phoneMarketable_Y IS 'count of phone marketable consented customers for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_phoneMarketable_N IS 'count of non-phone marketable consented customers for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_emailMarketable_Y IS 'count of email marketable consented customers for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_emailMarketable_N IS 'count of non-email marketable consented customers for given month/payroll store/consented seller';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_1mo_wSeller IS 'count of consented customers with a relationship length of >=1 month for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_3mo_wSeller IS 'count of consented customers with a relationship length of >=3 months for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_6mo_wSeller IS 'count of consented customers with a relationship length of >=6 months for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_1yr_wSeller IS 'count of consented customers with a relationship length of >=1 year for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.cnt_2yr_wSeller IS 'count of consented customers with a relationship length of >=2 years for given month/payroll store/consented seller';


COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.toConsented_fls_trips IS 'count of FLS Trips taken by consented customers for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.toConsented_ncom_trips IS 'count of NCOM Trips taken by consented customers for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.toConsented_sb_ncom_trips IS 'count of Styleboard NCOM Trips taken by consented customers for given month/payroll store/consented seller';

COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.toConsented_gross_fls_spend IS 'total FLS gross $ spent by consented customers for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.toConsented_gross_ncom_spend IS 'total NCOM gross $ spent by consented customers for given month/payroll store/consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.toConsented_sb_gross_ncom_spend IS 'total Styleboard NCOM gross $ spent by consented customers for given month/payroll store/consented seller';


COMMENT ON  {cli_t2_schema}.consented_customer_seller_monthly.dw_sys_load_tmstp IS 'timestamp when data was last updated'; 

SET QUERY_BAND = NONE FOR SESSION;