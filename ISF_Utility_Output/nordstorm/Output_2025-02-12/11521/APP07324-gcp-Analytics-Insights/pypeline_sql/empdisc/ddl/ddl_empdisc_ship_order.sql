SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=ddl_empdisc_ship_order_11521_ACE_ENG;
     Task_Name=ddl_empdisc_ship_order;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {empdisc_t2_schema}.empdisc_ship_order
Team/Owner: Data Science And Analytics - Digital and Fraud
Date Created/Modified: 1/4/2024

Note:
-- Purpose of the table: Shipping order associated to employee discount
-- Update Cadence: Daily
*/
/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{empdisc_t2_schema}', 'empdisc_ship_order', OUT_RETURN_MSG);
*/
create multiset table {empdisc_t2_schema}.empdisc_ship_order
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    emp_number BIGINT NOT NULL
    ,global_tran_id BIGINT NOT NULL
    ,order_num BIGINT NOT NULL
    ,business_day_date DATE FORMAT 'YYYY-MM-DD' NOT NULL
	,week_num INTEGER compress
	,month_num INTEGER compress
	,year_num INTEGER compress
	,order_usd_amt DECIMAL(12,2) compress
	,tran_emp_disc_amt DECIMAL(12,2) compress
	,tran_net_amt DECIMAL(12,2) compress
	,data_source_code VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC compress ('POS', 'RPOS', 'COM')
	,register_num INTEGER compress
	,tran_num INTEGER compress
	,online_shopper_id VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,acp_id VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,deterministic_profile_id VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,receipt_delivery_method_code BYTEINT compress --1,2,3,null
	,is_bopus SMALLINT COMPRESS (0, 1)
	,ringing_store_num INTEGER compress
	,fulfilling_store_num INTEGER compress
    ,ring_channel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC compress ('NL', 'FL', 'NCOM', 'RACK', 'RCOM')
    ,fullfill_channel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC compress ('NL', 'FL', 'NCOM', 'RACK', 'RCOM')
    ,ship_to_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,ship_to_city VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,ship_to_state VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,ship_to_zip VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,ship_to_address VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    ,CONSTRAINT unique_composite_key UNIQUE (emp_number, global_tran_id, order_num)
    )
primary index(emp_number, global_tran_id, order_num)
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2022-01-01' AND DATE '2033-12-31' EACH INTERVAL '1' DAY ,
 NO RANGE);
;
CREATE INDEX(emp_number)
       ,INDEX(global_tran_id)
       ,INDEX(order_num)
       ,INDEX(business_day_date, emp_number) 
on {empdisc_t2_schema}.empdisc_ship_order;

-- Table Comment (STANDARD)
COMMENT ON {empdisc_t2_schema}.empdisc_ship_order IS 'Shipping order associated to employee discount';

SET QUERY_BAND = NONE FOR SESSION;
