
SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=sarf_hr_dups_fix_11521_ACE_ENG;
     Task_Name=sarf_hr_dups_fix;'
     FOR SESSION VOLATILE;

/* Create temp table with duplicate keys in T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT_BASE*/
CREATE MULTISET VOLATILE TABLE sarf_hr_dup_keys,
NO FALLBACK, NO BEFORE JOURNAL, NO AFTER  JOURNAL,
CHECKSUM =DEFAULT
(business_day_date  DATE FORMAT 'yyyy-mm-dd',
global_tran_id   BIGINT,
line_item_seq_num   SMALLINT ,
return_date DATE FORMAT 'yyyy-mm-dd' ,
return_global_tran_id BIGINT ,
return_line_item_seq_num SMALLINT  
) 
PRIMARY INDEX(business_day_date ,global_tran_id ,line_item_seq_num ,
return_date ,return_global_tran_id ,return_line_item_seq_num)
PARTITION BY Range_N(business_day_date  BETWEEN DATE '2017-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' DAY , UNKNOWN)
 ON COMMIT PRESERVE ROWS;
 
/*insert the dup keys - one record per dups*/
insert into sarf_hr_dup_keys
SELECT 
business_day_date, 
global_tran_id ,
line_item_seq_num ,
return_date , 
return_global_tran_id , 
return_line_item_seq_num  
FROM {sales_returns_t2_schema}.sales_and_returns_fact_base  sarfb
GROUP BY  
business_day_date, 
global_tran_id ,
line_item_seq_num ,
return_date , 
return_global_tran_id , 
return_line_item_seq_num  
   HAVING COUNT(*)>1;

/* Create temp table to have all columns, not just keys, for the dups*/
       
CREATE MULTISET VOLATILE TABLE sarf_hr_dups,
NO FALLBACK, NO BEFORE JOURNAL, NO AFTER  JOURNAL,
CHECKSUM =DEFAULT
(business_day_date  DATE FORMAT 'yyyy-mm-dd',
order_date DATE FORMAT 'yyyy-mm-dd' Compress,
tran_date  DATE FORMAT 'yyyy-mm-dd' Compress,
business_unit_desc VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific
Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA', 'MARKETPLACE' ) ,
acp_id VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress,
global_tran_id   BIGINT,
line_item_seq_num   SMALLINT ,
order_num VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
order_line_id VARCHAR(100) CHARACTER SET Unicode NOT CaseSpecific Compress,
transaction_type VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress ('retail','services'),
upc_num VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
sku_num  VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
shipped_qty INTEGER Compress (1),
shipped_sales DECIMAL(12,2),
shipped_usd_sales DECIMAL(12,2),
employee_discount_amt DECIMAL(12,2) Compress 0.00 ,
has_employee_discount INTEGER Compress (0,1),
employee_discount_usd_amt DECIMAL(12,2) Compress 0.00 ,
line_item_order_type VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress
('StoreInitStoreTake','CustInitWebOrder','RPOS','StoreInitDTCAuto','StoreInitSameStrSend','CustInitPhoneOrder','StoreInitDTCManual','DESKTOP_WEB'),
line_item_fulfillment_type VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific
Compress ('StoreTake','FulfillmentCenter','StoreShipSend','RPOS','VendorDropShip','StorePickUp'),
intent_store_num INTEGER Compress (808,828,867),
ringing_store_num INTEGER Compress (808,828,867),
price_type CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress ('R','C','P'),
regular_price_amt DECIMAL(12,2),
transaction_price_amt DECIMAL(12,2),
sale_price_adj CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress ('N','Y'),
merch_dept_num VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific,
division_num VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific,
nonmerch_fee_code VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific Compress ('150','6666','140'),
item_source VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress 'SB_SALESEMPINIT',
commission_slsprsn_num VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress ('2079333','2079334','2079335','0000'),
ps_employee_id VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress,
remote_sell_employee_id VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress ('2079333','2079334','2079335','0000'),
payroll_dept   CHAR(5) CHARACTER SET Unicode NOT CaseSpecific Compress,
hr_es_ind  CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
shopper_id     VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress,
source_platform_code   VARCHAR(40) CHARACTER SET Unicode NOT CaseSpecific Compress,
promise_type_code  VARCHAR(40) CHARACTER SET Unicode NOT CaseSpecific Compress,
requested_level_of_service_code VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
delivery_method_code  VARCHAR(40) Compress,
destination_node_num  INTEGER Compress,
curbside_pickup_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
bill_zip_code  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
destination_zip_code  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
return_date DATE FORMAT 'yyyy-mm-dd' ,
original_business_date DATE FORMAT 'yyyy-mm-dd' Compress,
return_ringing_store_num INTEGER Compress (808,828,867),
return_global_tran_id BIGINT ,
return_line_item_seq_num SMALLINT ,
return_qty INTEGER Compress (1) ,
return_amt DECIMAL(12,2) Compress,
return_usd_amt DECIMAL(12,2) Compress,
return_employee_disc_amt DECIMAL(12,2) Compress 0.00,
return_employee_disc_usd_amt DECIMAL(12,2) Compress 0.00,
xborder_retn_ind  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress ('N', 'Y'),
return_mapped_to_origin INTEGER Compress (0,1,2),
days_to_return  INTEGER Compress(1,2,3,4,5,6,7,8,9,10,11,12,13,14),
dw_sys_load_tmstp  TIMESTAMP(6),
days_to_return_act INTEGER Compress(1,2,3,4,5,6,7,8,9,10,11,12,13,14),
nopr_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
not_picked_up_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
sa_source_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress, 
promo_tran_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
source_store_num INTEGER Compress,
marketplace_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress) 
PRIMARY INDEX(business_day_date ,global_tran_id ,line_item_seq_num ,
return_date ,return_global_tran_id ,return_line_item_seq_num)
PARTITION BY Range_N(business_day_date  BETWEEN DATE '2017-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' DAY , UNKNOWN)
 ON COMMIT PRESERVE ROWS;

/* insert full records for the dups base on dup keys*/
 
insert into sarf_hr_dups
SELECT SARFB.*
FROM {sales_returns_t2_schema}.SALES_AND_RETURNS_FACT_BASE   SARFB
INNER JOIN
 sarf_hr_dup_keys dupk
on 1=1
and coalesce(sarfb.business_day_date, date'2027-12-31' )  
=   coalesce(dupk.business_day_date, date'2027-12-31' )  
and coalesce(sarfb.global_tran_id, 0 ) 
=   coalesce(dupk.global_tran_id, 0 )
and coalesce(sarfb.line_item_seq_num, 0 ) 
=   coalesce(dupk.line_item_seq_num, 0 )
and coalesce(sarfb.return_date, date'2027-12-31')    
=   coalesce(dupk.return_date, date'2027-12-31')  
and coalesce(sarfb.return_global_tran_id, 0 ) 
=   coalesce(dupk.return_global_tran_id, 0 )
and coalesce(sarfb.return_line_item_seq_num, 0 ) 
=   coalesce(dupk.return_line_item_seq_num, 0 ) ;

/* now dedup the subset of duplicate records with all columns to make just one full record per key*/

CREATE MULTISET VOLATILE TABLE sarf_hr_dedups,
NO FALLBACK, NO BEFORE JOURNAL, NO AFTER  JOURNAL,
CHECKSUM =DEFAULT
(business_day_date  DATE FORMAT 'yyyy-mm-dd',
order_date DATE FORMAT 'yyyy-mm-dd' Compress,
tran_date  DATE FORMAT 'yyyy-mm-dd' Compress,
business_unit_desc VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific
Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA', 'MARKETPLACE' ) ,
acp_id VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress,
global_tran_id   BIGINT,
line_item_seq_num   SMALLINT ,
order_num VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
order_line_id VARCHAR(100) CHARACTER SET Unicode NOT CaseSpecific Compress,
transaction_type VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress ('retail','services'),
upc_num VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
sku_num  VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
shipped_qty INTEGER Compress (1),
shipped_sales DECIMAL(12,2),
shipped_usd_sales DECIMAL(12,2),
employee_discount_amt DECIMAL(12,2) Compress 0.00 ,
has_employee_discount INTEGER Compress (0,1),
employee_discount_usd_amt DECIMAL(12,2) Compress 0.00 ,
line_item_order_type VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress
('StoreInitStoreTake','CustInitWebOrder','RPOS','StoreInitDTCAuto','StoreInitSameStrSend','CustInitPhoneOrder','StoreInitDTCManual','DESKTOP_WEB'),
line_item_fulfillment_type VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific
Compress ('StoreTake','FulfillmentCenter','StoreShipSend','RPOS','VendorDropShip','StorePickUp'),
intent_store_num INTEGER Compress (808,828,867),
ringing_store_num INTEGER Compress (808,828,867),
price_type CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress ('R','C','P'),
regular_price_amt DECIMAL(12,2),
transaction_price_amt DECIMAL(12,2),
sale_price_adj CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress ('N','Y'),
merch_dept_num VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific,
division_num VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific,
nonmerch_fee_code VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific Compress ('150','6666','140'),
item_source VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress 'SB_SALESEMPINIT',
commission_slsprsn_num VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress ('2079333','2079334','2079335','0000'),
ps_employee_id VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress,
remote_sell_employee_id VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress ('2079333','2079334','2079335','0000'),
payroll_dept   CHAR(5) CHARACTER SET Unicode NOT CaseSpecific Compress,
hr_es_ind  CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
shopper_id     VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress,
source_platform_code   VARCHAR(40) CHARACTER SET Unicode NOT CaseSpecific Compress,
promise_type_code  VARCHAR(40) CHARACTER SET Unicode NOT CaseSpecific Compress,
requested_level_of_service_code VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
delivery_method_code  VARCHAR(40) Compress,
destination_node_num  INTEGER Compress,
curbside_pickup_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
bill_zip_code  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
destination_zip_code  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
return_date DATE FORMAT 'yyyy-mm-dd' ,
original_business_date DATE FORMAT 'yyyy-mm-dd' Compress,
return_ringing_store_num INTEGER Compress (808,828,867),
return_global_tran_id BIGINT ,
return_line_item_seq_num SMALLINT ,
return_qty INTEGER Compress (1) ,
return_amt DECIMAL(12,2) Compress,
return_usd_amt DECIMAL(12,2) Compress,
return_employee_disc_amt DECIMAL(12,2) Compress 0.00,
return_employee_disc_usd_amt DECIMAL(12,2) Compress 0.00,
xborder_retn_ind  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress ('N', 'Y'),
return_mapped_to_origin INTEGER Compress (0,1,2),
days_to_return  INTEGER Compress(1,2,3,4,5,6,7,8,9,10,11,12,13,14),
dw_sys_load_tmstp  TIMESTAMP(6),
days_to_return_act INTEGER Compress(1,2,3,4,5,6,7,8,9,10,11,12,13,14),
nopr_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
not_picked_up_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
sa_source_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress, 
promo_tran_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
source_store_num INTEGER Compress,
marketplace_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress) 
PRIMARY INDEX(business_day_date ,global_tran_id ,line_item_seq_num ,
return_date ,return_global_tran_id ,return_line_item_seq_num)
PARTITION BY Range_N(business_day_date  BETWEEN DATE '2017-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' DAY , UNKNOWN)
 ON COMMIT PRESERVE ROWS;

/* insert subset of duplicate records to make just one per key*/

insert into sarf_hr_dedups 
SELECT dups.*  
FROM sarf_hr_dups  dups
group by
	business_day_date             
,	order_date                    
,	tran_date                     
,	business_unit_desc            
,	acp_id                        
,	global_tran_id                
,	line_item_seq_num             
,	order_num                     
,	order_line_id                 
,	transaction_type              
,	upc_num                       
,	sku_num                       
,	shipped_qty                   
,	shipped_sales                 
,	shipped_usd_sales             
,	employee_discount_amt         
,	has_employee_discount         
,	employee_discount_usd_amt     
,	line_item_order_type          
,	line_item_fulfillment_type    
,	intent_store_num              
,	ringing_store_num             
,	price_type                    
,	regular_price_amt             
,	transaction_price_amt         
,	sale_price_adj                
,	merch_dept_num                
,	division_num                  
,	nonmerch_fee_code             
,	item_source                   
,	commission_slsprsn_num        
,	ps_employee_id                
,	remote_sell_employee_id       
,	payroll_dept                  
,	hr_es_ind                     
,	shopper_id                    
,	source_platform_code          
,	promise_type_code             
,	requested_level_of_service_code
,	delivery_method_code          
,	destination_node_num          
,	curbside_pickup_ind           
,	bill_zip_code                 
,	destination_zip_code          
,	return_date                   
,	original_business_date        
,	return_ringing_store_num      
,	return_global_tran_id         
,	return_line_item_seq_num      
,	return_qty                    
,	return_amt                    
,	return_usd_amt                
,	return_employee_disc_amt      
,	return_employee_disc_usd_amt  
,	xborder_retn_ind              
,	return_mapped_to_origin       
,	days_to_return                
,	dw_sys_load_tmstp             
,	days_to_return_act            
,	nopr_ind                      
,	not_picked_up_ind             
,	sa_source_ind                 
,	promo_tran_ind                
,	source_store_num              
,	marketplace_ind 
having count(*) > 1; 

/* now delete the dups from sarf*/
delete from 
{sales_returns_t2_schema}.sales_and_returns_fact_base  sarfb
where exists 
(select 1 
from sarf_hr_dup_keys dupk
where 1=1
and coalesce(sarfb.business_day_date, date'2027-12-31' )  
=   coalesce(dupk.business_day_date, date'2027-12-31' )  
and coalesce(sarfb.global_tran_id, 0 ) 
=   coalesce(dupk.global_tran_id, 0 )
and coalesce(sarfb.line_item_seq_num, 0 ) 
=   coalesce(dupk.line_item_seq_num, 0 )
and coalesce(sarfb.return_date, date'2027-12-31')    
=   coalesce(dupk.return_date, date'2027-12-31')  
and coalesce(sarfb.return_global_tran_id, 0 ) 
=   coalesce(dupk.return_global_tran_id, 0 )
and coalesce(sarfb.return_line_item_seq_num, 0 ) 
=   coalesce(dupk.return_line_item_seq_num, 0 ));

/* insert back into t2dl_das_sales_returns.sales_and_returns_fact_base
the deduped rows with all columns; result will be a single copy of row where
initially there were dups */

insert into 
{sales_returns_t2_schema}.sales_and_returns_fact_base
select *
from
sarf_hr_dedups;

SET QUERY_BAND = NONE FOR SESSION;
 

  
 
 
 


