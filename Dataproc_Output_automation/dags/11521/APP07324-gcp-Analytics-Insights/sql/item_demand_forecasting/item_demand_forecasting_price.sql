BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_price;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_price
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_price
WITH wk AS (
    SELECT DISTINCT
        week_idnt AS week_num, 
        week_start_day_date AS week_start_date,
        week_end_day_date AS week_end_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd 
        WHERE week_start_day_date BETWEEN {{params.start_date}} AND {{params.end_date}} 
), 
sku AS (  
    SELECT 
    	DISTINCT 
        rms_sku_num,
        epm_choice_num
    FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
    WHERE
       LOWER(channel_country) = LOWER('US')
),
all_data AS (SELECT
    price_fcst.channel_brand,
    price_fcst.selling_channel,
    sku.epm_choice_num, 
    wk.week_num,
    wk.week_start_date,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS DATE) AS last_updated_utc,
    MAX(price_fcst.regular_price_amt) AS regular_price_amt,
    MIN(price_fcst.selling_retail_price_amt) AS current_price_amt,
    CASE
	    WHEN MAX(price_fcst.regular_price_amt) = MIN(price_fcst.selling_retail_price_amt) then 'R' else 'C'
    END AS current_price_type
    FROM wk
		INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS price_fcst
    ON wk.week_start_date <= price_fcst.eff_end_tmstp 
      AND wk.week_end_date >= price_fcst.eff_begin_tmstp 
    INNER JOIN sku
    ON LOWER(price_fcst.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE
    sku.epm_choice_num IS NOT NULL
    AND LOWER(price_fcst.channel_country) = LOWER('US')
    AND LOWER(price_fcst.selling_channel) IN (LOWER('ONLINE'), LOWER('STORE'))
    AND LOWER(price_fcst.channel_brand) IN (LOWER('NORDSTROM'), LOWER('NORDSTROM_RACK'))
GROUP BY
  price_fcst.channel_brand,
  price_fcst.selling_channel,
  sku.epm_choice_num,
  wk.week_num,
  wk.week_start_date,
  last_updated_utc)

SELECT b.* 
FROM
(SELECT DISTINCT 
channel_brand,
selling_channel,
epm_choice_num,
week_start_date
FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_inv
UNION DISTINCT
SELECT DISTINCT channel_brand,
selling_channel,
epm_choice_num,
week_start_date
FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_store_inv) AS a
INNER JOIN all_data AS b
ON LOWER(a.channel_brand) = LOWER(b.channel_brand) 
AND LOWER(a.selling_channel) = LOWER(b.selling_channel)
AND a.epm_choice_num = b.epm_choice_num 
AND a.week_start_date = b.week_start_date;


--COLLECT STATISTICS  COLUMN (week_start_date), COLUMN (epm_choice_num), COLUMN (channel_brand), COLUMN (selling_channel), COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel) on t2dl_das_inv_position_forecast.IDF_PRICE;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
