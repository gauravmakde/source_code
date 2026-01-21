SET QUERY_BAND = 'App_ID=APP08193;
     DAG_ID=ddl_appointments_driven_value_11521_ACE_ENG;
     Task_Name=ddl_appointments_driven_value;'
     FOR SESSION VOLATILE;


--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'appointments_driven_value', OUT_RETURN_MSG);
/*
Table definition for T2DL_DAS_CLIENTELING.appointments_driven_value
*/
CREATE MULTISET TABLE {cli_t2_schema}.appointments_driven_value
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    , MAP = TD_MAP1
    (
    booking_id                              VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , acp_id                                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , schedule_start_date                   DATE NOT NULL
    , source_store                          VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , booking_service_description           VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , booking_made_by                       VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , store_dma_desc                        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , region_group                          VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , purch_date                            DATE
    , intent_store_num                      INTEGER
    , payroll_dept                          VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dept_name                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , commission_slsprsn_num                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sales_amt                             DECIMAL(38,2)
    , units                                 INTEGER 
    , return_amt                            DECIMAL(38,2)
    , return_qty                            INTEGER
    , consent_within_10                     VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , consent_within_10_new2seller          VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , consent_within_10_new2clienteling     VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , consent_within_10_prior               VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , consent_on_appt_dt                    VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , consent_within_10_post                VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC

    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
    PRIMARY INDEX (acp_id) 
    PARTITION BY RANGE_N(schedule_start_date BETWEEN DATE '2022-08-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN)
    ;


-- table comment
COMMENT ON  {cli_t2_schema}.appointments_driven_value IS 'Appointments Driven Value';
-- Column comments
COMMENT ON  {cli_t2_schema}.appointments_driven_value.booking_id IS 'appointment booking id';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.acp_id IS 'analytical customer profile identifier';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.schedule_start_date IS 'date on which appointment is scheduled';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.source_store IS 'store in which appointment is scheduled';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.booking_service_description IS 'appointment service description';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.booking_made_by IS 'who appointment was booked by';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.store_dma_desc IS 'DMA of store in which appointment is scheduled';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.region_group IS 'region of store in which appointment is scheduled';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.purch_date IS 'purchase made on same date and same store of appointment';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.intent_store_num IS 'store in which purchase was made';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.payroll_dept IS 'payroll department number in which purchase was made';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.dept_name IS 'payroll department name in which purchase was made';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.commission_slsprsn_num IS 'worker id of the consented seller';

COMMENT ON  {cli_t2_schema}.appointments_driven_value.sales_amt IS 'gross sales of purchase';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.units IS 'gross units in purchased';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.return_amt IS 'return sales relating to purchase (SARF)';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.return_qty IS 'return units relating to purchase (SARF)';

COMMENT ON  {cli_t2_schema}.appointments_driven_value.consent_within_10 IS 'customer consent given within 10 days pre/post appointment (Y/N)';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.consent_within_10_new2seller IS 'customer consent given within 10 days pre/post appointment (Y/N first time customer consent to seller)';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.consent_within_10_new2clienteling IS 'customer consent given within 10 days pre/post appointment (Y/N first time customer consent to clienteling)';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.consent_within_10_prior IS 'customer consent given within 10 days pre appointment (Y/N)';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.consent_on_appt_dt IS 'customer consent given on day of appointment (Y/N)';
COMMENT ON  {cli_t2_schema}.appointments_driven_value.consent_within_10_post IS 'customer consent given within 10 days post appointment (Y/N)';

COMMENT ON  {cli_t2_schema}.appointments_driven_value.dw_sys_load_tmstp IS 'timestamp when data was last updated'; 


SET QUERY_BAND = NONE FOR SESSION;