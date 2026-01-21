/*
Name: Size Curves SKU Groupid
APPID-Name: APP08076 Data Driven Size Curves
Purpose: Creates a table with weekly sales and inventory at the sku-location level from 2021 to present
    - size_sku_loc_weekly
Variable(s):    {{environment_schema}} T2DL_DAS_SIZE
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_sku_loc_weekly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:2/21/2023
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'size_sku_loc_weekly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.size_sku_loc_weekly{env_suffix},
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         sku_idnt                VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,loc_idnt                INTEGER
        ,week_idnt               INTEGER
        ,week_start_day_date     DATE FORMAT 'YYYY-MM-DD'
        ,boh_units               INTEGER NOT NULL
        ,boh_dollars             DECIMAL(12,2) NOT NULL
        ,eoh_units               INTEGER NOT NULL
        ,eoh_dollars             DECIMAL(12,2) NOT NULL
        ,sales_units             INTEGER NOT NULL
        ,sales_dollars           DECIMAL(12,2) NOT NULL
        ,return_units            INTEGER NOT NULL
        ,return_dollars          DECIMAL(12,2) NOT NULL
        ,receipt_po_units        INTEGER NOT NULL
        ,receipt_po_dollars      DECIMAL(12,2) NOT NULL
        ,rcd_update_timestamp    TIMESTAMP(6) WITH TIME ZONE 
    )
PRIMARY INDEX (sku_idnt, loc_idnt, week_idnt)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '7' DAY , NO RANGE);
