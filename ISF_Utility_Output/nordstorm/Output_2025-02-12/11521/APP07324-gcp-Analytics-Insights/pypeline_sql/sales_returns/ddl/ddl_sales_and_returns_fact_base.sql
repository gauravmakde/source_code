SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=ddl_sales_and_returns_fact_base_11521_ACE_ENG;
     Task_Name=ddl_sales_and_returns_fact_base;'
     FOR SESSION VOLATILE;

--T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT_BASE
--ACCESSED BY USERS VIA VIEW T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS

create multiset table {sales_returns_t2_schema}.SALES_AND_RETURNS_FACT_BASE,
no before journal, no after  journal,
checksum =default  
(business_day_date  date format 'yyyy-mm-dd',
order_date date format 'yyyy-mm-dd' compress, 
tran_date  date format 'yyyy-mm-dd' compress,  
business_unit_desc varchar(50) character set unicode not casespecific
compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA' ) , 
acp_id varchar(50) character set unicode not casespecific compress, 
global_tran_id   bigint,
line_item_seq_num   smallint ,
order_num varchar(32) character set unicode not casespecific compress,
order_line_id VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
transaction_type varchar(10) character set unicode not casespecific compress ('retail','services'), 
upc_num varchar(32) character set unicode not casespecific compress,
sku_num varchar(32) character set unicode not casespecific compress,
shipped_qty integer compress (1),
shipped_sales decimal(12,2),
shipped_usd_sales decimal(12,2),
employee_discount_amt decimal(12,2) compress 0.00 ,
has_employee_discount integer compress (0,1), 
employee_discount_usd_amt decimal(12,2) compress 0.00 , 
line_item_order_type varchar(32) character set unicode not casespecific compress 
('StoreInitStoreTake','CustInitWebOrder','RPOS','StoreInitDTCAuto','StoreInitSameStrSend','CustInitPhoneOrder','StoreInitDTCManual'),
line_item_fulfillment_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC 
COMPRESS ('StoreTake','FulfillmentCenter','StoreShipSend','RPOS','VendorDropShip','StorePickUp'),
intent_store_num integer compress (808,828,867),
ringing_store_num integer compress (808,828,867),
price_type char(1) character set unicode not casespecific compress ('R','C','P'),
regular_price_amt decimal(12,2),
transaction_price_amt decimal(12,2),
sale_price_adj char(1) character set unicode not casespecific compress ('N','Y'),
merch_dept_num varchar(8) character set unicode not casespecific,
division_num varchar(8) character set unicode not casespecific,
nonmerch_fee_code varchar(8) character set unicode not casespecific compress ('150','6666','140'),
item_source varchar(32) character set unicode not casespecific compress 'SB_SALESEMPINIT',
commission_slsprsn_num varchar(16) character set unicode not casespecific compress ('2079333','2079334','2079335','0000'),
ps_employee_id varchar(16) character set unicode not casespecific compress,
remote_sell_employee_id varchar(16) character set unicode not casespecific compress ('2079333','2079334','2079335','0000'),
payroll_dept   char(5) character set unicode not casespecific compress,
hr_es_ind  char(1)  character set unicode not casespecific compress,
shopper_id     varchar(50)  character set unicode not casespecific compress,
source_platform_code   varchar(40)  character set unicode not casespecific compress,
promise_type_code  varchar(40)  character set unicode not casespecific compress,
requested_level_of_service_code varchar(10) character set unicode not casespecific  compress,
delivery_method_code  varchar(40)  character set unicode not casespecific compress,
destination_node_num  integer compress,
curbside_pickup_ind char(1)  character set unicode not casespecific compress,
bill_zip_code  varchar(10)  character set unicode not casespecific compress,
destination_zip_code  varchar(10)  character set unicode not casespecific compress,
return_date date format 'yyyy-mm-dd',
original_business_date date format 'yyyy-mm-dd',
return_ringing_store_num integer compress (808,828,867),
return_global_tran_id bigint ,
return_line_item_seq_num smallint ,
return_qty integer compress (1) ,
return_amt decimal(12,2) compress,
return_usd_amt decimal(12,2) compress,
return_employee_disc_amt decimal(12,2) compress 0.00,
return_employee_disc_usd_amt decimal(12,2) compress 0.00, 
xborder_retn_ind  varchar(10) character set unicode not casespecific compress ('N', 'Y'),
return_mapped_to_origin integer compress (0,1,2),
days_to_return  integer compress(1,2,3,4,5,6,7,8,9,10,11,12,13,14),   
dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
days_to_return_act INTEGER Compress(1,2,3,4,5,6,7,8,9,10,11,12,13,14),
nopr_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
not_picked_up_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
sa_source_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
promo_tran_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
source_store_num INTEGER Compress, 
marketplace_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress 
)primary index (business_day_date, global_tran_id, line_item_seq_num, return_date, return_global_tran_id, return_line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day, 
 unknown);

SET QUERY_BAND = NONE FOR SESSION;