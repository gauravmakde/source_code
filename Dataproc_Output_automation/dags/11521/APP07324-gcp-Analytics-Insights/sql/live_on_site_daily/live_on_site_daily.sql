CREATE TEMPORARY TABLE IF NOT EXISTS live_on_site_skus AS (
    SELECT DISTINCT
        rms_sku_num AS sku_id,
        channel_country,
        channel_brand,
        eff_date AS day_date
    FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_online_purchasable_item_dim,
        UNNEST(GENERATE_ARRAY(0, LEAST(DATE_DIFF(eff_end_tmstp, eff_begin_tmstp, DAY), 365))) AS offset
        CROSS JOIN UNNEST([eff_begin_tmstp + INTERVAL offset DAY]) AS eff_date
    WHERE
        lower(is_online_purchasable) = lower('Y')
        AND TIMESTAMP('2021-06-14 01:00:00') <= TIMESTAMP(eff_end_tmstp)
    UNION ALL
    SELECT
        rms_sku_num AS sku_id,
        channel_country,
        channel_brand,
        day_date
    FROM
         `{{params.gcp_project_id}}`.t2dl_das_site_merch.live_on_site_historical
    WHERE
        day_date BETWEEN {{params.start_date}} AND DATE '2021-06-13'
);


CREATE TEMPORARY TABLE IF NOT EXISTS live_on_site_skus_prices AS (
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
            WHEN lower(prices.ownership_retail_price_type_code) = lower('CLEARANCE') THEN 'C'
            WHEN lower(prices.selling_retail_price_type_code) = lower('CLEARANCE') THEN 'C'
            WHEN lower(prices.selling_retail_price_type_code) = lower('REGULAR') THEN 'R'
            WHEN lower(prices.selling_retail_price_type_code) = lower('PROMOTION') THEN 'P'
            ELSE 'N/A'
        END AS current_price_type,
        CASE
            WHEN pro.enticement_tags LIKE '%FLASH%' THEN 1
            ELSE 0
        END AS flash_ind
    FROM live_on_site_skus los
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim prices
        ON lower(los.sku_id) = lower(prices.rms_sku_num)
        AND lower(los.channel_brand) = lower(prices.channel_brand)
        AND lower(prices.selling_channel) = lower('ONLINE')
        AND lower(los.channel_country) = lower(prices.channel_country)
        AND TIMESTAMP(los.day_date) BETWEEN prices.eff_begin_tmstp_utc AND prices.eff_end_tmstp_utc
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_promotion_timeline_dim pro
        ON lower(los.sku_id) = lower(pro.rms_sku_num)
        AND lower(los.channel_brand) = lower(pro.channel_brand)
        AND lower(pro.selling_channel) = lower('ONLINE')
        AND lower(los.channel_country) = lower(pro.channel_country)
        AND TIMESTAMP(los.day_date) BETWEEN pro.eff_begin_tmstp_utc AND pro.eff_end_tmstp_utc
    WHERE los.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
	QUALIFY row_number() OVER(PARTITION BY los.day_date, los.sku_id, los.channel_country, los.channel_brand ORDER BY prices.eff_begin_tmstp DESC) = 1
);


CREATE TEMPORARY TABLE IF NOT EXISTS rp_lkp AS (
    SELECT
        rms_sku_num,
        CASE WHEN lower(business_unit_desc) IN (lower('N.COM'), lower('N.CA')) THEN 'NORDSTROM' ELSE 'NORDSTROM_RACK' END AS channel_brand,
        CASE WHEN lower(business_unit_desc) IN (lower('N.COM'), lower('OFFPRICE ONLINE')) THEN 'US' ELSE 'CA' END AS channel_country,
        day_date,
        'Y' AS aip_rp_fl
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
    INNER JOIN (SELECT DISTINCT day_date 
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal 
    WHERE day_date BETWEEN CAST({{params.start_date}} AS DATE) AND CAST({{params.end_date}} AS DATE)) d
        ON d.day_date BETWEEN RANGE_START(rp_period) AND RANGE_END(rp_period)
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim st
        ON rp.location_num = st.store_num
    WHERE lower(business_unit_desc) IN (lower('N.COM'), lower('OFFPRICE ONLINE'), lower('N.CA'))
	GROUP BY 1,2,3,4,5
);


DELETE FROM `{{params.gcp_project_id}}`.{{params.live_on_site_t2_schema}}.live_on_site_daily WHERE  day_date BETWEEN {{params.start_date}} AND {{params.end_date}};

INSERT INTO `{{params.gcp_project_id}}`.{{params.live_on_site_t2_schema}}.live_on_site_daily
    SELECT
        dates.year_num AS fiscal_year,
        dates.week_of_fyr AS fiscal_week,
        dates.day_of_fyr AS day_num,
        CAST(los.day_date as DATE),
        los.channel_country,
        los.channel_brand,
        los.sku_id,
        los.price_band_one,
        los.price_band_two,
        MAX(CASE WHEN rp.aip_rp_fl = 'Y' THEN 1 ELSE 0 END) rp_ind,
        MAX(CASE WHEN ds.location_type IN ('DS', 'DS_OP') THEN 1 ELSE 0 END) AS ds_ind,
        MAX(CASE WHEN los.current_price_type = 'C' THEN 1 ELSE 0 END) cl_ind,
        MAX(CASE WHEN los.current_price_type = 'P' THEN 1 ELSE 0 END) pm_ind,
        MAX(CASE WHEN los.current_price_type = 'R' THEN 1 ELSE 0 END) reg_ind,
        MAX(CASE WHEN los.flash_ind = 1 THEN 1 ELSE 0 END) flash_ind,
        CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
    FROM live_on_site_skus_prices los
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal dates
        ON los.day_date = dates.day_date
    LEFT JOIN rp_lkp rp
        ON lower(los.sku_id) = lower(rp.rms_sku_num)
        AND los.day_date = rp.day_date
        AND lower(los.channel_brand) = lower(rp.channel_brand)
        AND lower(los.channel_country) = lower(rp.channel_country)
    LEFT JOIN (
        SELECT
            rms_sku_id AS sku_id,
            snapshot_date AS day_date,
            location_type,
            CASE WHEN location_type = 'DS' THEN 'NORDSTROM' WHEN location_type = 'DS_OP' THEN 'NORDSTROM_RACK' END AS channel_brand
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact
        WHERE lower(location_type) IN (lower('DS'), lower('DS_OP'))
        AND snapshot_date >= {{params.start_date}}
    ) ds
    ON lower(los.sku_id) = lower(ds.sku_id)
    AND los.day_date = ds.day_date
    AND lower(los.channel_brand) = lower(ds.channel_brand)
    AND lower(los.channel_country) = lower('US')
    GROUP BY
        dates.year_num,
        dates.week_of_fyr,
        dates.day_of_fyr,
        los.day_date,
        los.channel_country,
        los.channel_brand,
        los.sku_id,
        los.price_band_one,
        los.price_band_two;
