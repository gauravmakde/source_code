SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=ddl_empdisc_item_trans_11521_ACE_ENG;
     Task_Name=ddl_empdisc_item_trans;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {empdisc_t2_schema}.empdisc_item_trans
Team/Owner: Data Science And Analytics - Digital and Fraud
Date Created/Modified: 1/3/2023

Note:
-- Purpose of the table: item level transaction associated to employee discount
-- Update Cadence: Daily
*/
/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{empdisc_t2_schema}', 'empdisc_item_trans', OUT_RETURN_MSG);
*/
create multiset table {empdisc_t2_schema}.empdisc_item_trans
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    emp_number                             BIGINT NOT NULL 
    ,global_tran_id                        BIGINT NOT NULL 
    ,business_day_date                     date format 'yyyy-mm-dd' not null
    ,week_num                              integer compress
    ,month_num                             integer compress
    ,year_num                              integer compress
    ,tran_type_code                        char(8) character set unicode not casespecific compress
    ,line_item_seq_num                     smallint NOT NULL
    ,line_item_activity_type_desc          varchar(20) character set unicode not casespecific compress
    ,unique_source_id                      varchar(50) character set unicode not casespecific
    ,acp_id                                varchar(100) character set unicode not casespecific
    ,marketing_profile_type_ind            char character set unicode not casespecific compress
    ,deterministic_profile_id              varchar(100) character set unicode not casespecific
    ,ringing_store_num                     integer compress
    ,fulfilling_store_num                  integer compress
    ,intent_store_num                      integer compress
    ,claims_destination_store_num          integer compress
    ,register_num                          integer compress
    ,tran_num                              integer compress
    ,tran_version_num                      decimal(4)
    ,sa_tran_status_code                   char(4) character set unicode not casespecific compress
    ,original_global_tran_id               varchar(50) character set unicode not casespecific
    ,reversal_flag                         char character set unicode not casespecific compress
    ,data_source_code                      varchar(10) character set unicode not casespecific compress ('POS', 'RPOS', 'COM')
    ,pre_sale_type_code                    decimal(4)
    ,order_num                             BIGINT 
    ,order_date                            date format 'yyyy-mm-dd'
    ,followup_slsprsn_num                  varchar(16) character set unicode not casespecific
    ,pbfollowup_slsprsn_num                varchar(16) character set unicode not casespecific
    ,tran_time                             timestamp with time zone
    ,sku_num                               varchar(16) character set unicode not casespecific
    ,upc_num                               varchar(32) character set unicode not casespecific
    ,upc10_num                             varchar(32) character set unicode not casespecific
    ,line_item_merch_nonmerch_ind          varchar(8) compress --MERCH, NMERCH
    ,merch_dept_num                        varchar(8) character set unicode not casespecific
    --fee code
    ,nonmerch_fee_code                      varchar(8) character set unicode not casespecific
    ,fee_code_is_valid                      smallint compress (0, 1)
    ,fee_code_empdisc_allowed_flag          char(1) character set unicode not casespecific
    ,fee_code_price_amt                     number(8,2) compress 0
    ,fee_code_desc                          varchar(1000) character set unicode not casespecific compress 'SPECIAL ITEM OR EVENT'
    ,fee_code_text                          varchar(1000) character set unicode not casespecific compress ('SALES AUDIT','DEFAULT RECORD','UNASSIGNED FEE CODE')
    ,fee_code_effective_date                date format 'YYYY-MM-DD'
    ,fee_code_termination_date              date format 'YYYY-MM-DD'
    ,fee_code_display_at_pos_flag           char(1) character set unicode not casespecific
    --line item
    ,line_item_promo_id                    varchar(16) character set unicode not casespecific
    ,line_net_usd_amt                      decimal(12, 2)
    ,line_item_quantity                    decimal(8)
    ,line_item_fulfillment_type            varchar(32) character set unicode not casespecific
    ,line_item_order_type                  varchar(32) character set unicode not casespecific -- PROPS, POS
    ,commission_slsprsn_num                varchar(16) character set unicode not casespecific compress
    ,employee_discount_flag                smallint compress
    ,employee_discount_usd_amt             decimal(12, 2) compress
    ,line_item_promo_usd_amt               decimal(12, 2) compress
    ,merch_unique_item_id                  varchar(32) character set unicode not casespecific
    ,merch_price_adjust_reason             varchar(8) character set unicode not casespecific
    ,line_item_capture_system              varchar(32) character set unicode not casespecific
    ,original_business_date                date format 'yyyy-mm-dd'
    ,original_ringing_store_num            integer compress
    ,original_register_num                 integer compress
    ,original_tran_num                     integer compress
    ,original_line_item_usd_amt            decimal(12, 2) compress
    ,line_item_regular_price               decimal(12, 2) compress
    ,line_item_regular_price_currency_code char(5) character set unicode not casespecific compress
    ,error_flag                            char character set unicode not casespecific compress
    ,tran_latest_version_ind               char character set unicode not casespecific compress
    ,item_source                           varchar(32) character set unicode not casespecific
    ,price_adj_code                        varchar(10) character set unicode not casespecific
    ,tran_line_id                          integer compress
    --bopus
    ,is_bopus                              smallint compress (0, 1)
    --tender
    ,tender_tran_type_code varchar(8) character set unicode not casespecific compress ('EXCH','PAID','PMT','REFU','RETN','SALE','VOID')
    ,tender_item_seq_num smallint compress (1 ,2 ,-1)
    ,tender_tran_version_num decimal(4,0) compress 1.
    ,tender_tran_time timestamp(6) with time zone
    ,tender_type_code varchar(20) character set unicode not casespecific compress ('CREDIT_CARD','DEBIT_CARD','CASH','GIFT_CARD','NORDSTROM_NOTE','PAYPAL','CHECK')
    ,card_subtype_code varchar(10) character set unicode not casespecific compress ('TR','RT','GC','TV','NN','MD','ND')
    ,card_type_code varchar(10) character set unicode not casespecific compress ('VC','NC','NG','MC','NV','AE','DS')
    ,tender_item_auth_type varchar(15) character set unicode not casespecific compress ('NOT SETTLED','SETTLED')
    ,tender_item_protection_method varchar(10) character set unicode not casespecific compress 'TOKENIZED'
    ,tender_item_protection_source varchar(1) character set unicode not casespecific compress  ('V','N','F')
    ,tender_item_usd_amt decimal(12,2)
    ,tender_item_entry_method_code smallint compress (0,1,2,3,6,7,8)
    ,tender_item_activity_code varchar(10) character set unicode not casespecific compress ('0','51','8')
    ,third_party_gift_card_ind varchar(8) character set unicode not casespecific compress 'False'
    ,tender_error_flag char(1) character set unicode not casespecific compress ('N','Y')
    ,tender_item_account_number_v2 varchar(50) character set unicode not casespecific compress
    --sku
    ,web_style_num bigint
    ,style_group_num varchar(10) character set unicode not casespecific
    ,style_group_desc varchar(100) character set unicode not casespecific
    ,division varchar(200) character set unicode not casespecific
    ,subdiv varchar(200) character set unicode not casespecific
    ,department varchar(200) character set unicode not casespecific
    ,"class" varchar(80) character set unicode not casespecific
    ,subclass varchar(80) character set unicode not casespecific
    ,brand_name varchar(100) character set unicode not casespecific
    ,supplier varchar(100) character set unicode not casespecific
    ,product_type_1 varchar(40) character set unicode not casespecific
    ,style_desc varchar(800) character set unicode not casespecific
    ,color_desc varchar(60) character set unicode not casespecific
    ,nord_display_color varchar(60) character set unicode not casespecific
    ,size_1_num varchar(6) character set unicode not casespecific
    ,size_1_desc varchar(30) character set unicode not casespecific
    ,size_2_num varchar(6) character set unicode not casespecific
    ,size_2_desc varchar(30) character set unicode not casespecific
    ,supp_color varchar(80) character set unicode not casespecific
    ,supp_size varchar(80) character set unicode not casespecific
    ,drop_ship_eligible_ind char(1) character set unicode not casespecific compress ('N','Y')
    --channel
    ,ring_channel varchar(20) character set unicode not casespecific compress ('NL', 'FL', 'NCOM', 'RACK', 'RCOM')
    ,org_ring_channel varchar(20) character set unicode not casespecific compress ('NL', 'FL', 'NCOM', 'RACK', 'RCOM')
    ,fullfill_channel varchar(20) character set unicode not casespecific compress ('NL', 'FL', 'NCOM', 'RACK', 'RCOM')
    ,intent_channel varchar(20) character set unicode not casespecific compress ('NL', 'FL', 'NCOM', 'RACK', 'RCOM')
    --ring store dma
    ,ring_dma varchar(60) character set unicode not casespecific
    ,ring_region varchar(150) character set unicode not casespecific
    ,ring_city varchar(150) character set unicode not casespecific
    ,ring_state varchar(60) character set unicode not casespecific
    ,ring_county varchar(60) character set unicode not casespecific
    ,ring_zipcode varchar(10) character set unicode not casespecific
    --original store dma
    ,org_ring_dma varchar(60) character set unicode not casespecific
    ,org_ring_region varchar(150) character set unicode not casespecific
    ,org_ring_city varchar(150) character set unicode not casespecific
    ,org_ring_state varchar(60) character set unicode not casespecific
    ,org_ring_county varchar(60) character set unicode not casespecific
    ,org_ring_zipcode varchar(10) character set unicode not casespecific
    --compute
    ,retn_delta integer
    ,exch_delta integer
    --load timestamp
    ,dw_sys_load_tmstp  timestamp(6) default current_timestamp(6) not null
    ,CONSTRAINT unique_composite_key UNIQUE (emp_number, global_tran_id, line_item_seq_num)
    )
primary index(emp_number, global_tran_id, line_item_seq_num)
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2022-01-01' AND DATE '2033-12-31' EACH INTERVAL '1' DAY ,
 NO RANGE);
;
CREATE  INDEX(emp_number),
        INDEX(business_day_date, emp_number),
        INDEX(business_day_date, ring_channel),
        INDEX(business_day_date, data_source_code),
        INDEX(business_day_date, tran_type_code),
        INDEX(business_day_date, followup_slsprsn_num),
        INDEX(business_day_date, pbfollowup_slsprsn_num),
        INDEX(business_day_date, emp_number, followup_slsprsn_num),
        INDEX(business_day_date, emp_number, pbfollowup_slsprsn_num)
ON {empdisc_t2_schema}.empdisc_item_trans;

-- Table Comment (STANDARD)
COMMENT ON {empdisc_t2_schema}.empdisc_item_trans IS 'Item level transaction associated to employee discount';

SET QUERY_BAND = NONE FOR SESSION;
