/*
DDL for Receipts Base table 
Author: Sara Scott

Creates Tables:
    {environment_schema}.receipt_sku_loc_week_agg_fact{env_suffix}
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'receipt_sku_loc_week_agg_fact{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.receipt_sku_loc_week_agg_fact{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
	  sku_idnt								 VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
   	, week_num 								 INTEGER
   	, week_start_day_date					 DATE FORMAT 'YYYY-MM-DD'
   	, week_end_day_date						 DATE FORMAT 'YYYY-MM-DD'
   	, mnth_idnt								 INTEGER
   	, channel_num 							 INTEGER
   	, store_num 							 INTEGER
   	, ds_ind								 CHAR(1)
   	, rp_ind								 CHAR(1)
   	, npg_ind								 CHAR(1)
   	, ss_ind								 CHAR(1)
   	, gwp_ind								 CHAR(1)
	, price_type							 CHAR(1)
   	, receipt_tot_units						 INTEGER
   	, receipt_tot_retail					 DECIMAL(33,4)
   	, receipt_tot_cost					 	 DECIMAL(33,4)
   	, receipt_po_units						 INTEGER
   	, receipt_po_retail						 DECIMAL(33,4)
   	, receipt_po_cost						 DECIMAL(33,4)
   	, receipt_ds_units						 INTEGER
   	, receipt_ds_retail						 DECIMAL(33,4)
   	, receipt_ds_cost						 DECIMAL(33,4)
   	, receipt_rsk_units						 INTEGER
   	, receipt_rsk_retail					 DECIMAL(33,4)
   	, receipt_rsk_cost						 DECIMAL(33,4)
   	, receipt_pah_units						 INTEGER
   	, receipt_pah_retail					 DECIMAL(33,4)
   	, receipt_pah_cost 						 DECIMAL(33,4)
   	, receipt_flx_units 					 INTEGER
   	, receipt_flx_retail					 DECIMAL(33,4)
   	, receipt_flx_cost						 DECIMAL(33,4)

)
PRIMARY INDEX(sku_idnt, week_num, store_num)
PARTITION BY RANGE_N(week_num BETWEEN 201901 AND 202553 EACH 1);

GRANT SELECT ON {environment_schema}.receipt_sku_loc_week_agg_fact{env_suffix} TO PUBLIC;