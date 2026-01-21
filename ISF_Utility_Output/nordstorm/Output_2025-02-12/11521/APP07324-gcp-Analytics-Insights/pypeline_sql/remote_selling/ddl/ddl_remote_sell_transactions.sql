SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=ddl_remote_sell_transactions_11521_ACE_ENG;
     Task_Name=ddl_remote_sell_transactions;'
     FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{remote_selling_t2_schema}', 'remote_sell_transactions', OUT_RETURN_MSG);
/*
Table definition for T2DL_DAS_REMOTE_SELLING.REMOTE_SELL_TRANSACTIONS
*/
CREATE MULTISET TABLE {remote_selling_t2_schema}.remote_sell_transactions,
    FALLBACK ,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
    business_day_date                        DATE NOT NULL,
    remote_sell_swimlane                     VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
    board_type                               VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
    global_tran_id                           VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    store_country_code                       VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
    upc_num                                  VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
    private_styling_attributed_item_ind      SMALLINT COMPRESS (0, 1),
    nordstrom_to_you_item_ind                SMALLINT COMPRESS (0, 1),
    acp_id                                   VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    order_num                                VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
    platform                                 VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
    shipped_sales                            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
    shipped_usd_sales                        DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
    shipped_qty                              DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
    return_date                              DATE,
    return_amt                               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
    return_qty                               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
    return_usd_amt                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
    days_to_return                           INTEGER COMPRESS,
    remote_sell_employee_id                  VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
    employee_name                            VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
    sale_payroll_store                       CHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
    sale_payroll_store_description           VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
    sale_payroll_department_description      VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
    employee_payroll_store_region_num        VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
    employee_payroll_region_desc             VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
    number_days_since_termination            INTEGER COMPRESS,
    order_pickup_ind                         INTEGER COMPRESS,
    ship_to_store_ind                        INTEGER COMPRESS,
    line_item_seq_num                        INTEGER COMPRESS,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
    PRIMARY INDEX (global_tran_id) --unqiue not allowed
    PARTITION BY RANGE_N(business_day_date BETWEEN DATE '2018-02-04' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN)
    ;


-- table comment
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions IS 'Remote Sell Transactions';
-- Column comments
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.business_day_date IS 'the business date of the transaction';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.remote_sell_swimlane IS 'the remote sell swimlane';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.board_type IS 'differentiates between a styleboard and a stylelink';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.global_tran_id IS 'the transaction identifier';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.store_country_code IS 'country code of the intent store';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.upc_num IS 'upc number of the item';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.private_styling_attributed_item_ind IS 'item was sent by stylist to customer';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.nordstrom_to_you_item_ind IS 'item selected by stylist and not a service fee';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.acp_id IS 'analytical customer profile identifier';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.order_num IS 'digital order number';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.platform IS 'platform from which styleboard was ordered';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.shipped_sales IS 'amount paid, less employee discount in local currency';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.shipped_usd_sales IS 'amount paid, less employee discount in USD currency';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.shipped_qty IS 'item count'; -- remote sell will count services in future
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.return_date IS 'the business date of the return';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.return_amt IS 'amount returned, less employee discount in local currency';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.return_qty IS 'returned item count';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.return_usd_amt IS 'amount returned, less employee discount in USD currency';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.days_to_return IS 'number of days between the original sale and the return';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.remote_sell_employee_id IS 'employee id of the stylist';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.sale_payroll_store IS 'home store employee id is tied to, but employee may not physically be located at store';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.sale_payroll_store_description IS 'home store description of employee id, but employee may not physically be located at store';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.sale_payroll_department_description IS 'home store department description of employee id';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.employee_payroll_store_region_num IS 'employee payroll store region number';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.employee_payroll_region_desc IS 'employee payroll store region description';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.number_days_since_termination IS 'number of days since employee is no longer active with company';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.order_pickup_ind IS 'traditional picks filled by store nodes. either filled in store or via Nordstrom overnight vans';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.ship_to_store_ind IS 'standard or expedited shipping of product to a store. includes s2s next day pickup filled from FC';
COMMENT ON  {remote_selling_t2_schema}.remote_sell_transactions.line_item_seq_num IS 'the transaction line item identifier';

SET QUERY_BAND = NONE FOR SESSION;
