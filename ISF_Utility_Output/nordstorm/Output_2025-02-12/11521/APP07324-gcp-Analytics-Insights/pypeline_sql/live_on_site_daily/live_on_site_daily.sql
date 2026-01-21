SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=live_on_site_daily_11521_ACE_ENG;
     Task_Name=live_on_site_daily;'
     FOR SESSION VOLATILE;

-- First create a volatile table of all LOS Skus we will be looking at
-- replaced code:  TIMESTAMP 'parathesize_end_date 01:00:00' +  INTERVAL '1' DAY) -- Keep all operations date inclusive
CREATE MULTISET VOLATILE TABLE live_on_site_skus AS (
    SELECT
        DISTINCT
        rms_sku_num sku_id,
        channel_country,
        channel_brand,
        CAST(BEGIN(eff_period) AT local AS date) AS day_date
    FROM
        prd_nap_usr_vws.product_online_purchasable_item_dim EXPAND ON
        PERIOD(eff_begin_tmstp, eff_end_tmstp) AS eff_period
        BY interval '1' day for PERIOD(TIMESTAMP '2021-06-14 01:00:00',
        CAST({end_date} AS TIMESTAMP(0)) + Interval '1' Hour + Interval '1' Day )
    WHERE
        is_online_purchasable = 'Y'
    UNION ALL
    SELECT
        rms_sku_num sku_id,
        channel_country,
        channel_brand,
        day_date
    FROM
        T2DL_DAS_SITE_MERCH.live_on_site_historical
    WHERE day_date BETWEEN {start_date} AND DATE '2021-06-13'
)
WITH DATA
PRIMARY INDEX (sku_id, channel_country, day_date)
ON COMMIT PRESERVE ROWS;

-- Price Table
CREATE MULTISET VOLATILE TABLE live_on_site_skus_prices AS (
    SELECT
        los.sku_id,
        los.channel_country,
        los.channel_brand,
        los.day_date,
        CASE
            WHEN prices.selling_retail_price_amt IS NULL THEN 'UNKNOWN'
            WHEN prices.selling_retail_price_amt = 0.00 THEN 'UNKNOWN'
            WHEN prices.selling_retail_price_amt <= 10.00 THEN '< $10'
            WHEN prices.selling_retail_price_amt <= 25.00 THEN '$10 - $25'
            WHEN prices.selling_retail_price_amt <= 50.00 THEN '$25 - $50'
            WHEN prices.selling_retail_price_amt <= 100.00 THEN '$50 - $100'
            WHEN prices.selling_retail_price_amt <= 150.00 THEN '$100 - $150'
            WHEN prices.selling_retail_price_amt <= 200.00 THEN '$150 - $200'
            WHEN prices.selling_retail_price_amt <= 300.00 THEN '$200 - $300'
            WHEN prices.selling_retail_price_amt <= 500.00 THEN '$300 - $500'
            WHEN prices.selling_retail_price_amt <= 1000.00 THEN '$500 - $1000'
            WHEN prices.selling_retail_price_amt > 1000.00 THEN '> $1000'
            ELSE 'UNKNOWN'
        END AS price_band_one,
        CASE
            WHEN prices.selling_retail_price_amt IS NULL THEN 'UNKNOWN'
            WHEN prices.selling_retail_price_amt = 0.00 THEN 'UNKNOWN'
            WHEN prices.selling_retail_price_amt <= 10.00 THEN '< $10'
            WHEN prices.selling_retail_price_amt <= 15.00 THEN '$10 - $15'
            WHEN prices.selling_retail_price_amt <= 20.00 THEN '$15 - $20'
            WHEN prices.selling_retail_price_amt <= 25.00 THEN '$20 - $25'
            WHEN prices.selling_retail_price_amt <= 30.00 THEN '$25 - $30'
            WHEN prices.selling_retail_price_amt <= 40.00 THEN '$30 - $40'
            WHEN prices.selling_retail_price_amt <= 50.00 THEN '$40 - $50'
            WHEN prices.selling_retail_price_amt <= 60.00 THEN '$50 - $60'
            WHEN prices.selling_retail_price_amt <= 80.00 THEN '$60 - $80'
            WHEN prices.selling_retail_price_amt <= 100.00 THEN '$80 - $100'
            WHEN prices.selling_retail_price_amt <= 125.00 THEN '$100 - $125'
            WHEN prices.selling_retail_price_amt <= 150.00 THEN '$125 - $150'
            WHEN prices.selling_retail_price_amt <= 175.00 THEN '$150 - $175'
            WHEN prices.selling_retail_price_amt <= 200.00 THEN '$175 - $200'
            WHEN prices.selling_retail_price_amt <= 250.00 THEN '$200 - $250'
            WHEN prices.selling_retail_price_amt <= 300.00 THEN '$250 - $300'
            WHEN prices.selling_retail_price_amt <= 400.00 THEN '$300 - $400'
            WHEN prices.selling_retail_price_amt <= 500.00 THEN '$400 - $500'
            WHEN prices.selling_retail_price_amt <= 700.00 THEN '$500 - $700'
            WHEN prices.selling_retail_price_amt <= 900.00 THEN '$700 - $900'
            WHEN prices.selling_retail_price_amt <= 1000.00 THEN '$900 - $1000'
            WHEN prices.selling_retail_price_amt <= 1200.00 THEN '$1000 - $1200'
            WHEN prices.selling_retail_price_amt <= 1500.00 THEN '$1200 - $1500'
            WHEN prices.selling_retail_price_amt <= 1800.00 THEN '$1500 - $1800'
            WHEN prices.selling_retail_price_amt <= 2000.00 THEN '$1800 - $2000'
            WHEN prices.selling_retail_price_amt <= 3000.00 THEN '$2000 - $3000'
            WHEN prices.selling_retail_price_amt <= 4000.00 THEN '$3000 - $4000'
            WHEN prices.selling_retail_price_amt <= 5000.00 THEN '$4000 - $5000'
            WHEN prices.selling_retail_price_amt > 5000.00 THEN '> $5000'
            ELSE 'UNKNOWN'
        END AS price_band_two,
        CASE
            WHEN prices.ownership_retail_price_type_code = 'CLEARANCE' THEN 'C'
            WHEN prices.selling_retail_price_type_code = 'CLEARANCE' THEN 'C'
            WHEN prices.selling_retail_price_type_code = 'REGULAR' THEN 'R'
            WHEN prices.selling_retail_price_type_code = 'PROMOTION' THEN 'P'
            ELSE 'N/A'
        END AS current_price_type,
        CASE
            WHEN pro.enticement_tags LIKE '%FLASH%' THEN 1
            ELSE 0
        END AS flash_ind
    FROM live_on_site_skus los

    LEFT JOIN prd_nap_usr_vws.PRODUCT_PRICE_TIMELINE_DIM prices
    ON los.sku_id = prices.rms_sku_num
        AND los.channel_brand = prices.channel_brand
        AND prices.selling_channel = 'ONLINE'
        AND los.channel_country = prices.channel_country
        AND cast(los.day_date as timestamp) between prices.EFF_BEGIN_TMSTP AND prices.EFF_END_TMSTP - interval '0.001' second

    LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PROMOTION_TIMELINE_DIM pro
    ON los.sku_id = pro.rms_sku_num
        AND los.channel_brand = pro.channel_brand
        AND pro.selling_channel = 'ONLINE'
        AND los.channel_country = pro.channel_country
        AND cast(los.day_date as timestamp) between pro.EFF_BEGIN_TMSTP AND pro.EFF_END_TMSTP - interval '0.001' second
    WHERE los.day_date between {start_date} AND {end_date}
    QUALIFY row_number() OVER(PARTITION BY los.day_date, los.sku_id, los.channel_country, los.channel_brand ORDER BY prices.eff_begin_tmstp DESC) = 1
)
WITH DATA
PRIMARY INDEX (sku_id, channel_country, day_date)
ON COMMIT PRESERVE ROWS;

-- RP Table
CREATE MULTISET VOLATILE TABLE rp_lkp AS (
      SELECT
          rms_sku_num
          , CASE WHEN business_unit_desc IN ('N.COM', 'N.CA') THEN 'NORDSTROM' ELSE 'NORDSTROM_RACK' END AS channel_brand
          , CASE WHEN business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE') THEN 'US' else 'CA' END AS channel_country
          , day_date
          , 'Y' AS aip_rp_fl
      FROM PRD_NAP_USR_VWS.MERCH_RP_SKU_LOC_DIM_HIST rp
      INNER JOIN (
                SELECT DISTINCT day_date
                FROM prd_nap_usr_vws.day_cal
                WHERE day_date BETWEEN CAST({start_date}  AS DATE) AND CAST({end_date} AS DATE)
            ) d
      ON d.day_date BETWEEN BEGIN(rp_period) AND END(rp_period)
      INNER JOIN prd_nap_usr_vws.store_dim st
         ON rp.location_num = st.store_num
      WHERE business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE', 'N.CA')
      GROUP BY 1,2,3,4,5
) WITH DATA PRIMARY INDEX (rms_sku_num, day_date, channel_brand, channel_country) ON COMMIT PRESERVE ROWS;

-- Refresh data
DELETE
FROM    {live_on_site_t2_schema}.live_on_site_daily
WHERE   day_date BETWEEN {start_date} AND {end_date}
;

-- Insert new data
-- Join on dates to get fiscal year/week
-- join on business units AND replenishment table
-- do these cases in this part of the query in cASe there are null values from left join
-- join for dropship information
-- No historical dropship data prior to '2021-01-01'
-- Rely on inventory_stock_quantity_BY_day_fact for 2021 onwards
INSERT INTO {live_on_site_t2_schema}.live_on_site_daily
    SELECT
        dates.year_num AS fiscal_year,
        dates.week_of_fyr AS fiscal_week,
        dates.day_of_fyr AS day_num,
        los.day_date,
        los.channel_country,
        los.channel_brand,
        los.sku_id,
        los.price_band_one,
        los.price_band_two,
        MAX(CASE WHEN rp.aip_rp_fl = 'Y' THEN 1 ELSE 0 END) rp_ind,
        MAX(CASE WHEN ds.location_type in ('DS', 'DS_OP') THEN 1 ELSE 0 END) AS ds_ind,
        MAX(CASE WHEN los.current_price_type = 'C' THEN 1 ELSE 0 END) cl_ind,
        MAX(CASE WHEN los.current_price_type = 'P' THEN 1 ELSE 0 END) pm_ind,
        MAX(CASE WHEN los.current_price_type = 'R' THEN 1 ELSE 0 END) reg_ind,
        MAX(CASE WHEN los.flash_ind = 1 THEN 1 ELSE 0 END) flash_ind,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM live_on_site_skus_prices los
        LEFT JOIN prd_nap_usr_vws.day_cal dates
        ON los.day_date = dates.day_date
        LEFT JOIN rp_lkp rp
        ON los.sku_id = rp.rms_sku_num
        AND los.day_date = rp.day_date
        AND los.channel_brand = rp.channel_brand
        AND los.channel_country = rp.channel_country
        LEFT JOIN (
      			SELECT
      			    rms_sku_id AS sku_id,
      			    snapshot_date AS day_date,
      			    location_type,
      			    CASE WHEN location_type = 'DS' THEN 'NORDSTROM' WHEN location_type = 'DS_OP' THEN 'NORDSTROM_RACK' END channel_brand
      			FROM  prd_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact
      			WHERE location_type in ('DS', 'DS_OP')
      			AND snapshot_date >= {start_date}
        ) ds
        ON los.sku_id = ds.sku_id
        AND los.day_date = ds.day_date
		AND los.channel_brand = ds.channel_brand
        AND los.channel_country = 'US'
    GROUP BY
        dates.year_num,
        dates.week_of_fyr,
        dates.day_of_fyr,
        los.day_date,
        los.channel_country,
        los.channel_brand,
        los.sku_id,
	  	  los.price_band_one,
        los.price_band_two
;

-- Collect Statistics
COLLECT STATISTICS COLUMN (SKU_ID ,CHANNEL_BRAND ,DAY_DATE) ON live_on_site_skus_prices;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,CHANNEL_BRAND) ON {live_on_site_t2_schema}.live_on_site_daily;
COLLECT STATISTICS COLUMN (DAY_DATE ,CHANNEL_COUNTRY,CHANNEL_BRAND) ON {live_on_site_t2_schema}.live_on_site_daily;
COLLECT STATISTICS COLUMN (FISCAL_YEAR ,FISCAL_WEEK ,DAY_NUM, DAY_DATE ,CHANNEL_COUNTRY ,CHANNEL_BRAND ,PRICE_BAND_ONE,PRICE_BAND_TWO) ON {live_on_site_t2_schema}.live_on_site_daily;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,SKU_ID) ON {live_on_site_t2_schema}.live_on_site_daily;
COLLECT STATISTICS COLUMN (FISCAL_WEEK) ON {live_on_site_t2_schema}.live_on_site_daily;
COLLECT STATISTICS COLUMN (DAY_DATE) ON {live_on_site_t2_schema}.live_on_site_daily;
COLLECT STATISTICS COLUMN (DAY_DATE ,CHANNEL_COUNTRY,CHANNEL_BRAND ,SKU_ID) ON {live_on_site_t2_schema}.live_on_site_daily;

SET QUERY_BAND = NONE FOR SESSION;
