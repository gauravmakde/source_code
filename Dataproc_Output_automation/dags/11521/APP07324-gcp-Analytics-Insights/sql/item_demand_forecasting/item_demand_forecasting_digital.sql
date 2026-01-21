/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
-- SET QUERY_BAND = 'App_ID=APP08073;
--      DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
--      Task_Name=item_demand_forecasting_digital;'
--      FOR SESSION VOLATILE;

/*
T2/Table Name: IDF_DIGITAL
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all digital data by EPM_CHOICE_NUM X WEEK
Includes product views, averagerating, add to bag

*/ 

 DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_digital
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};

INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_digital
WITH wk AS (
    SELECT DISTINCT
        week_idnt AS week_num,
        CAST(day_date AS DATE) AS day_date,
        CAST(week_start_day_date AS DATE) AS week_start_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim dcd
    WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND {{params.end_date}}
),
sku AS (
    SELECT DISTINCT
        epm_choice_num,
        web_style_num AS web_style_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_hist
    WHERE lower(channel_country) = lower('US') AND epm_choice_num IS NOT NULL AND web_style_num IS NOT NULL
)
SELECT
    CASE
        WHEN lower(pfd.channel) = lower('FULL_LINE') THEN 'NORDSTROM'
        WHEN lower(pfd.channel) = lower('RACK') THEN 'NORDSTROM_RACK'
        ELSE NULL
    END AS channel_brand,
    'ONLINE' AS selling_channel,
    sku.epm_choice_num,
    wk.week_num,
    wk.week_start_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATE)as last_updated_utc,  -- Adjust timezone as needed
    CAST(AVG(CASE WHEN CAST(pfd.averagerating AS STRING) = '*' OR pfd.averagerating = 0 THEN NULL ELSE pfd.averagerating END) AS BIGNUMERIC) AS averagerating,
    CAST(SUM(pfd.product_views) AS BIGNUMERIC) AS product_views,
    CAST(SUM(pfd.add_to_bag_quantity) AS BIGNUMERIC) AS add_to_bag,
    CURRENT_DATETIME('GMT') AS dw_sys_load_tmstp
FROM
    wk
JOIN `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_funnel_daily AS pfd ON pfd.event_date_pacific = wk.day_date
JOIN sku ON sku.web_style_id = pfd.style_id
WHERE lower(pfd.channelcountry) = lower('US')
GROUP BY 1, 2, 3, 4, 5, 6, 10;