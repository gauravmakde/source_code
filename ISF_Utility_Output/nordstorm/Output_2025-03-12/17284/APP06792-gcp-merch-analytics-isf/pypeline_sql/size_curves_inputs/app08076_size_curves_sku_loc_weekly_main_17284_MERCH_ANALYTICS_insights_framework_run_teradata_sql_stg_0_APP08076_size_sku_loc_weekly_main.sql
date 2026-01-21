/*
Name: Size Curves SKU Groupid
APPID-Name: APP08076 Data Driven Size Curves
Purpose: Creates a table with weekly sales and inventory at the sku-location level from 2021 to present
    - size_sku_loc_weekly
Variable(s):    {environment_schema} T2DL_DAS_SIZE
                {env_suffix} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_sku_loc_weekly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:2/21/2023
*/

-- DROP TABLE week_lkp;
CREATE MULTISET VOLATILE TABLE week_lkp AS (
SELECT
     day_date
    ,week_idnt
    ,week_start_day_date
    ,day_abrv
FROM prd_nap_usr_vws.day_cal_454_dim
WHERE day_date BETWEEN current_date - 14 AND current_date
) WITH DATA
PRIMARY INDEX(day_date)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    COLUMN (day_date)
    ON week_lkp;

-- DROP TABLE loc_lkup;
CREATE MULTISET VOLATILE TABLE loc_lkup AS (
SELECT DISTINCT
     store_num
FROM prd_nap_usr_vws.price_store_dim_vw loc
WHERE business_unit_num in ('1000', '6000', '2000', '5000')
  AND store_name NOT like 'CLSD%' -- can be found as CLSD or CLOSED
  AND store_name NOT like 'CLOSED%'
) WITH DATA
PRIMARY INDEX(store_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    PRIMARY INDEX(store_num)
    ON loc_lkup;

-- DROP TABLE daily_loc;
CREATE MULTISET VOLATILE TABLE daily_loc AS (
SELECT
     sku_idnt
    ,loc_idnt
    ,day_date
    ,w.week_idnt
    ,w.week_start_day_date
    ,CASE WHEN day_abrv = 'SUN' THEN boh_units ELSE 0 END AS boh_units
    ,CASE WHEN day_abrv = 'SUN' THEN boh_dollars ELSE 0 END AS boh_dollars
    ,CASE WHEN day_abrv = 'SAT' THEN eoh_units ELSE 0 END AS eoh_units
    ,CASE WHEN day_abrv = 'SAT' THEN eoh_dollars ELSE 0 END AS eoh_dollars
    ,COALESCE(sales_units, 0) AS sales_units
    ,COALESCE(sales_dollars, 0) AS sales_dollars
	  ,COALESCE(return_units, 0) AS return_units
	  ,COALESCE(return_dollars, 0) AS return_dollars
    ,COALESCE(receipt_po_units, 0) AS receipt_po_units
    ,COALESCE(receipt_po_dollars, 0) AS receipt_po_dollars
FROM t2dl_das_ace_mfp.sku_loc_pricetype_day_vw d
JOIN week_lkp w
  ON d.day_dt = w.day_date
JOIN loc_lkup loc
  ON d.loc_idnt = loc.store_num
) WITH DATA
PRIMARY INDEX (week_idnt, sku_idnt, loc_idnt)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX (week_idnt, sku_idnt, loc_idnt)
    ,COLUMN(week_idnt)
    ,COLUMN(loc_idnt)
    ON daily_loc;

-- DROP TABLE week_loc;
CREATE MULTISET VOLATILE TABLE week_loc AS (
SELECT
     sku_idnt
    ,loc_idnt
    ,week_idnt
    ,week_start_day_date
    ,SUM(ZEROIFNULL(boh_units)) AS boh_units
    ,SUM(ZEROIFNULL(boh_dollars)) AS boh_dollars
    ,SUM(ZEROIFNULL(eoh_units)) AS eoh_units
    ,SUM(ZEROIFNULL(eoh_dollars)) AS eoh_dollars
    ,SUM(sales_units) AS sales_units
    ,SUM(sales_dollars) AS sales_dollars
	  ,SUM(return_units) AS return_units
    ,SUM(return_dollars) AS return_dollars
    ,SUM(receipt_po_units) AS receipt_po_units
    ,SUM(receipt_po_dollars) AS receipt_po_dollars
    ,current_timestamp AS update_timestamp
FROM daily_loc
GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (week_idnt, sku_idnt, loc_idnt)
ON COMMIT PRESERVE ROWS
;

DELETE FROM t2dl_das_size.size_sku_loc_weekly WHERE week_idnt IN (SELECT DISTINCT week_idnt FROM week_lkp);
INSERT INTO t2dl_das_size.size_sku_loc_weekly
SELECT *
FROM week_loc
;
