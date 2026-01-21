  /*
DDL for Receipts Base table from MADM data
Note: This is a static table and not a scheduled job.
Author: Sara Scott

Creates Tables:
    {environment_schema}.receipt_sku_loc_week_agg_fact_madm{env_suffix}
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'receipt_sku_loc_week_agg_fact_madm{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.receipt_sku_loc_week_agg_fact_madm{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
	  sku_idnt					VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
   	, week_num					INTEGER
   	, week_start_day_date		DATE FORMAT 'YYYY-MM-DD'
   	, week_end_day_date			DATE FORMAT 'YYYY-MM-DD'
   	, mnth_idnt					INTEGER
   	, channel_num				INTEGER
   	, store_num					INTEGER
	, price_type				CHAR(1)
	, rp_ind					BYTEINT
   	, receipt_po_units			INTEGER
   	, receipt_po_retail			DECIMAL(33,4)
   	, receipt_ds_units			INTEGER
   	, receipt_ds_retail			DECIMAL(33,4)
   	, receipt_rsk_units			INTEGER
   	, receipt_rsk_retail		DECIMAL(33,4)

)
PRIMARY INDEX(sku_idnt, week_num, store_num)
PARTITION BY RANGE_N(week_num BETWEEN 201901 AND 202553 EACH 1);

GRANT SELECT ON {environment_schema}.receipt_sku_loc_week_agg_fact_madm{env_suffix} TO PUBLIC;  
