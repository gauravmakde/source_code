
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'sku_loc_daily_se_an_on_order', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.sku_loc_daily_se_an_on_order
  ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
	(
    sku_idnt VARCHAR(20)
    , loc_idnt VARCHAR(20)
		, day_date DATE FORMAT 'YYYY/MM/DD' NOT NULL
		, an_on_order_retail DECIMAL(12,2)
		, an_on_order_cost DECIMAL(12,2)
		, an_on_order_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
		, an_total_ordered_oo_retail DECIMAL(12,2)
		, an_total_ordered_oo_cost DECIMAL(12,2)
		, an_total_ordered_oo_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
    , an_total_canceled_oo_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
		, an_in_transit_retail DECIMAL(12,2)
		, an_in_transit_cost DECIMAL(12,2)
		, an_in_transit_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
		, eoh_retail DECIMAL(12,2)
		, eoh_cost DECIMAL(12,2)
		, eoh_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
		, an_receipts_retail DECIMAL(12,2)
		, an_receipts_cost DECIMAL(12,2)
		, an_receipts_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
		, process_tmstp TIMESTAMP(6) WITH TIME ZONE
	)

PRIMARY INDEX (sku_idnt, loc_idnt, day_date);

GRANT SELECT ON {environment_schema}.sku_loc_daily_se_an_on_order TO PUBLIC;
