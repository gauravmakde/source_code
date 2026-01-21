/******************************************************************************
Name: Scaled Events - Anniversary Block Data Check
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Checks upstream PPFD/Wishlist dependency table for updates:
    - If PPFD/Wishlist not updated and current time < 8am PDT: Fails + blocks sku_clk_rms from refreshing 
    - If PPFD/Wishlist not updated and current time >= 8am PDT: Fails but allows sku_clk_rms to refresh
    - If PPFD/Wishlist is updated: Succeeds and allows sku_clk_rms to refresh
DAG: 
TABLE NAME: Multiple Views
Author(s): Alli Moore
Date Last Updated: 07-27-2023
******************************************************************************/

	
CREATE MULTISET VOLATILE TABLE source_check AS (
	SELECT 
		source_table
		, date_check
		, source_database
		, source_field
		, check_type
	FROM (
		SELECT 
			'product_price_funnel_daily' AS source_table
			, 't2dl_das_product_funnel' AS source_database
			, 'event_date_pacific' AS source_field
			, 'primary' AS check_type
			, CURRENT_DATE - 1 AS date_check
		FROM (SELECT 1 AS "dummy") "dummy"
		UNION ALL
		SELECT 
			'sku_item_added' AS source_table 
			, 'T2DL_DAS_SCALED_EVENTS' AS source_database
			, 'event_date_pacific' AS source_field
			, 'primary' AS check_type
			, CURRENT_DATE AS date_check
		FROM (SELECT 1 AS "dummy") "dummy"
		) a
) 
WITH DATA 
PRIMARY INDEX (source_table) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS 
    PRIMARY INDEX ( source_table ) 
        ON source_check;
		
	
       
       
		
CREATE MULTISET VOLATILE TABLE block_data_check AS (		
SELECT 
	CASE 
		WHEN b.date_check = a.date_updated then 'PASS'
		ELSE 'FAIL'
	 END SOURCE_STATUS
	--,b.check_type
	, b.source_database
	, b.source_table
	, b.source_field
	, a.date_updated
	, a.ttl_records
FROM (
	SELECT 
		'product_price_funnel_daily' source_table
		, event_date_pacific as date_updated
		, COUNT(*) ttl_records
	FROM T2DL_DAS_PRODUCT_FUNNEL.PRODUCT_PRICE_FUNNEL_DAILY 
	WHERE event_date_pacific = CURRENT_DATE - 1
	GROUP BY 1,2
	UNION ALL
	SELECT 
		'sku_item_added' source_table
		, CAST(MAX(dw_sys_load_tmstp) AS DATE FORMAT 'YYYY-MM-DD') as date_updated
		, COUNT(*) ttl_records
	FROM T2DL_DAS_SCALED_EVENTS.SKU_ITEM_ADDED
	WHERE event_date_pacific = CURRENT_DATE - 1
	GROUP BY 1
) a
	RIGHT JOIN source_check b 
		ON b.source_table = a.source_table
) 
WITH DATA 
PRIMARY INDEX (source_table) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS 
    PRIMARY INDEX ( source_table ) 
        ON block_data_check;	

	
SELECT 
    CASE 
	    WHEN SOURCE_STATUS = 'PASS' THEN SOURCE_STATUS
	    WHEN CAST((CURRENT_TIMESTAMP AT TIME ZONE 'GMT-7') AS TIME) >= '08:00:00' THEN SOURCE_STATUS --IF it's past 8am PDT, THEN succeed DAG
	    ELSE CAST(SOURCE_STATUS AS INTEGER)
    END DATA_CHECK
    , source_database
    , source_table
    --, date_updated 
    --, ttl_records
FROM block_data_check