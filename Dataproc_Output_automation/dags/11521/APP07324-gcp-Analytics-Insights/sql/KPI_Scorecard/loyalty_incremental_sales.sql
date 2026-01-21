BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS start_varibale
AS
SELECT MIN(day_date) AS start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = {{params.start_date}});
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS end_varibale
AS
SELECT MAX(day_date) AS end_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = {{params.end_date}});

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS _variables
AS
SELECT *
FROM start_varibale
 INNER JOIN end_varibale ON TRUE;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS delete_from_year
AS
SELECT MIN(day_cal_454_dim.fiscal_year_num) AS start_year,
 MAX(day_cal_454_dim.fiscal_year_num) AS end_year
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
 INNER JOIN `_variables` ON TRUE
WHERE day_cal_454_dim.day_date
 AND `_variables`.start_date
 AND `_variables`.end_date;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS delete_from_month
AS
SELECT DISTINCT day_cal_454_dim.month_abrv AS month_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
 INNER JOIN `_variables` ON TRUE
WHERE day_cal_454_dim.day_date
 AND `_variables`.start_date
 AND `_variables`.end_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.kpi_scorecard_t2_schema}}.loyalty_incremental_sales
WHERE EXISTS (SELECT *
    FROM delete_from_year
    WHERE loyalty_incremental_sales.report_year BETWEEN start_year AND end_year AND loyalty_incremental_sales.report_month IN (SELECT month_desc
                FROM delete_from_month));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.kpi_scorecard_t2_schema}}.loyalty_incremental_sales
(SELECT report_year,
  report_month,
  banner,
  CAST(SUM(card_sales) AS FLOAT64) AS card_sales,
  CAST(SUM(member_sales) AS FLOAT64) AS member_sales,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT dc.year_num AS report_year,
     dc.month_short_desc AS report_month,
      CASE
      WHEN a.intent_store_num IN (742, 780)
      THEN NULL
      WHEN LOWER(sd.business_unit_desc) = LOWER('FULL LINE')
      THEN 'Nordstrom'
      WHEN LOWER(sd.business_unit_desc) = LOWER('OFFPRICE ONLINE')
      THEN 'Nordstrom Rack'
      WHEN LOWER(sd.business_unit_desc) = LOWER('N.COM')
      THEN 'Nordstrom'
      WHEN LOWER(sd.business_unit_desc) = LOWER('Rack')
      THEN 'Nordstrom Rack'
      ELSE NULL
      END AS banner,
     SUM(a.net_tender_spend_usd_amt) AS card_sales,
     SUM(a.net_nontender_spend_usd_amt) AS member_sales
    FROM (SELECT loyalty_id,
       base_points,
       bonus_points,
       total_points,
       net_nontender_spend_usd_amt,
       credit_channel_code,
       net_tender_spend_usd_amt,
       net_outside_spend_usd_amt,
       intent_store_num,
       business_day_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_transaction_fact_vw AS ltfv) AS a
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON a.intent_store_num = sd.store_num
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc ON a.business_day_date = dc.day_date
     INNER JOIN `_variables` ON TRUE
    WHERE a.business_day_date BETWEEN `_variables`.start_date AND `_variables`.end_date
     AND LOWER(CASE
        WHEN a.intent_store_num IN (742, 780)
        THEN NULL
        WHEN LOWER(sd.business_unit_desc) = LOWER('FULL LINE')
        THEN 'Nordstrom'
        WHEN LOWER(sd.business_unit_desc) = LOWER('OFFPRICE ONLINE')
        THEN 'Nordstrom Rack'
        WHEN LOWER(sd.business_unit_desc) = LOWER('N.COM')
        THEN 'Nordstrom'
        WHEN LOWER(sd.business_unit_desc) = LOWER('Rack')
        THEN 'Nordstrom Rack'
        ELSE NULL
        END) IN (LOWER('Nordstrom'), LOWER('Nordstrom Rack'))
    GROUP BY report_year,
     report_month,
     banner
    UNION ALL
    SELECT dc0.year_num AS report_year,
     dc0.month_short_desc AS report_month,
      CASE
      WHEN a.intent_store_num IN (742, 780)
      THEN NULL
      WHEN LOWER(sd0.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('Rack'
         ))
      THEN 'JWN'
      ELSE NULL
      END AS banner,
     SUM(a.net_tender_spend_usd_amt) AS card_sales,
     SUM(a.net_nontender_spend_usd_amt) AS member_sales
    FROM (SELECT loyalty_id,
       base_points,
       bonus_points,
       total_points,
       net_nontender_spend_usd_amt,
       credit_channel_code,
       net_tender_spend_usd_amt,
       net_outside_spend_usd_amt,
       intent_store_num,
       business_day_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_transaction_fact_vw AS ltfv) AS a
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd0 ON a.intent_store_num = sd0.store_num
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc0 ON a.business_day_date = dc0.day_date
     INNER JOIN `_variables` AS `_variables0` ON TRUE
    WHERE a.business_day_date BETWEEN `_variables0`.start_date AND `_variables0`.end_date
     AND LOWER(CASE
        WHEN a.intent_store_num IN (742, 780)
        THEN NULL
        WHEN LOWER(sd0.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('Rack'
           ))
        THEN 'JWN'
        ELSE NULL
        END) IN (LOWER('JWN'))
    GROUP BY report_year,
     report_month,
     banner) AS a
 GROUP BY report_year,
  report_month,
  banner);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
