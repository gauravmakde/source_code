CREATE TEMPORARY TABLE IF NOT EXISTS locations
AS
SELECT DISTINCT store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(selling_channel) = LOWER('ONLINE')
 AND channel_num NOT IN (310, 920, 921, 922, 930, 940, 990);


CREATE TEMPORARY TABLE IF NOT EXISTS calendar_realigned
AS
SELECT day_date,
 week_idnt,
 month_idnt,
 week_end_day_date,
 week_start_day_date,
 month_start_day_date,
 month_end_day_date,
 month_start_week_idnt,
 month_end_week_idnt,
 fiscal_month_num AS mnth_454,
 fiscal_quarter_num AS qtr_454,
 fiscal_year_num AS yr_454
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS rdlv
WHERE day_date <= CURRENT_DATE('PST8PDT')
UNION ALL
SELECT day_date,
 week_idnt,
 month_idnt,
 week_end_day_date,
 week_start_day_date,
 month_start_day_date,
 month_end_day_date,
 month_start_week_idnt,
 month_end_week_idnt,
 fiscal_month_num AS mnth_454,
 fiscal_quarter_num AS qtr_454,
 fiscal_year_num AS yr_454
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE day_date <= CURRENT_DATE('PST8PDT')
 AND fiscal_year_num NOT IN (SELECT DISTINCT fiscal_year_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw);


CREATE TEMPORARY TABLE IF NOT EXISTS calendar
AS
SELECT cal.day_date,
 cal.week_idnt,
 cal.month_idnt,
 cal.week_end_day_date,
 cal.week_start_day_date,
 cal.month_start_day_date,
 cal.month_end_day_date,
 cal.month_start_week_idnt,
 cal.month_end_week_idnt,
 cal.mnth_454,
 cal.qtr_454,
 cal.yr_454,
 t.day_date AS day_date_true,
 t.week_idnt AS week_idnt_true,
 t.month_idnt AS month_idnt_true,
 t.month_end_day_date AS month_end_day_date_true,
 t.week_end_day_date AS week_end_day_date_true
FROM calendar_realigned AS cal
 FULL JOIN (SELECT day_date,
   day_idnt,
   week_idnt,
   month_idnt,
   month_end_day_date,
   week_end_day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
  WHERE fiscal_year_num >= 2019
   AND day_date <= CURRENT_DATE('PST8PDT')) AS t ON cal.day_date = t.day_date;


CREATE TEMPORARY TABLE IF NOT EXISTS receipts_prep AS WITH dates AS (SELECT week_idnt_true,
   week_idnt AS week_num,
   month_idnt AS mnth_idnt,
   month_start_day_date AS mnth_start_day_date,
   month_start_week_idnt,
   mnth_454,
   qtr_454,
   yr_454
  FROM calendar
  WHERE month_idnt >= 201901
  GROUP BY week_idnt_true,
   week_num,
   mnth_idnt,
   mnth_start_day_date,
   month_start_week_idnt,
   mnth_454,
   qtr_454,
   yr_454), receipts_base AS (SELECT rcpt.sku_idnt,
    d.week_num,
    d.mnth_idnt,
    d.mnth_start_day_date,
    rcpt.store_num,
    SUM(rcpt.receipt_po_units + rcpt.receipt_ds_units) AS rcpt_tot_units
   FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact AS rcpt
    INNER JOIN locations AS loc ON rcpt.store_num = loc.store_num
    INNER JOIN dates AS d ON rcpt.week_num = d.week_idnt_true
   WHERE d.mnth_idnt >= 202301
   GROUP BY rcpt.sku_idnt,
    d.week_num,
    d.mnth_idnt,
    d.mnth_start_day_date,
    rcpt.store_num
   UNION ALL
   SELECT rcpt0.sku_idnt,
    d.week_num,
    d.mnth_idnt,
    d.mnth_start_day_date,
    rcpt0.store_num,
    SUM(rcpt0.receipt_po_units + rcpt0.receipt_ds_units) AS rcpt_tot_units
   FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact_madm AS rcpt0
    INNER JOIN locations AS loc0 ON rcpt0.store_num = loc0.store_num
    INNER JOIN dates AS d ON rcpt0.week_num = d.week_idnt_true
   WHERE d.mnth_idnt < 202301
   GROUP BY rcpt0.sku_idnt,
    d.week_num,
    d.mnth_idnt,
    d.mnth_start_day_date,
    rcpt0.store_num) (SELECT *
  FROM receipts_base AS rcpt
  WHERE week_num BETWEEN (SELECT DISTINCT month_start_week_idnt
     FROM calendar
     WHERE month_idnt = (SELECT CAST(CASE
           WHEN CONCAT(SUBSTR(CAST(CASE
                WHEN mnth_454 >= 5
                THEN yr_454
                ELSE yr_454 - 1
                END AS STRING), 1, 4), LPAD(SUBSTR(CAST(CASE
                 WHEN mnth_454 >= 5
                 THEN mnth_454 - 4
                 ELSE mnth_454 + 8
                 END AS STRING), 1, 2), 2, '0')) = ''
           THEN '0'
           ELSE CONCAT(SUBSTR(CAST(CASE
               WHEN mnth_454 >= 5
               THEN yr_454
               ELSE yr_454 - 1
               END AS STRING), 1, 4), LPAD(SUBSTR(CAST(CASE
                WHEN mnth_454 >= 5
                THEN mnth_454 - 4
                ELSE mnth_454 + 8
                END AS STRING), 1, 2), 2, '0'))
           END AS INTEGER) AS mnth_454_mnth_454_yr_454_yr_454_mnth_454_mnth_454
        FROM calendar
        WHERE day_date = {{params.start_date}})) AND (SELECT month_end_week_idnt
     FROM calendar
     WHERE day_date = {{params.end_date}})
   AND rcpt_tot_units <> 0);



CREATE TEMPORARY TABLE IF NOT EXISTS receipts_monthly AS 
SELECT
    mnth_idnt,
    mnth_start_day_date,
    rp.sku_idnt,
    channel_country,
    channel_brand,
    CASE 
        WHEN selling_channel = 'ONLINE' THEN 'DIGITAL' 
        ELSE selling_channel 
    END AS channel,
    SUM(rcpt_tot_units) AS receipt_units,
    DENSE_RANK() OVER (ORDER BY mnth_idnt) AS new_cf_rank
FROM receipts_prep rp
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw st
  ON rp.store_num = st.store_num 
GROUP BY 
    mnth_idnt, 
    mnth_start_day_date, 
    rp.sku_idnt, 
    channel_country, 
    channel_brand, 
    channel;


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.cc_type_realigned{{params.env_suffix}}
WHERE mnth_idnt BETWEEN (SELECT DISTINCT month_idnt
  FROM calendar
  WHERE day_date = {{params.start_date}}) AND (SELECT DISTINCT month_idnt
  FROM calendar
  WHERE day_date = {{params.end_date}});



INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.cc_type_realigned{{params.env_suffix}}
WITH los_ccs AS (
    SELECT DISTINCT
        d.month_idnt AS mnth_idnt,
        l.channel_country,
        l.channel_brand,
        cc.customer_choice,
        1 AS los_flag
    FROM `{{params.gcp_project_id}}`.t2dl_das_site_merch.live_on_site_daily l
    JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp cc
      ON l.sku_id = cc.sku_idnt
     AND LOWER(l.channel_country) = LOWER(cc.channel_country)
    JOIN `calendar` d
      ON d.day_date = l.day_date
),
rcpts AS (
    SELECT
        rcpt.mnth_idnt,
        rcpt.mnth_start_day_date,
        rcpt.new_cf_rank,
        rcpt.channel_country,
        rcpt.channel_brand,
        rcpt.channel,
        cc.customer_choice,
        SUM(rcpt.receipt_units) AS rcpt_units,
        LAG(rcpt.new_cf_rank) OVER (
            PARTITION BY cc.customer_choice, rcpt.channel_country, rcpt.channel_brand, rcpt.channel 
            ORDER BY rcpt.new_cf_rank
        ) AS mnth_lag
    FROM `receipts_monthly` rcpt
    JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp cc
      ON cc.sku_idnt = rcpt.sku_idnt
     AND LOWER(cc.channel_country) = LOWER(rcpt.channel_country)
    GROUP BY 
        rcpt.mnth_idnt, 
        rcpt.mnth_start_day_date, 
        rcpt.new_cf_rank, 
        rcpt.channel_country, 
        rcpt.channel_brand, 
        rcpt.channel, 
        cc.customer_choice
)
SELECT
    r.mnth_idnt,
    r.mnth_start_day_date,
    r.channel_country,
    r.channel_brand,
    r.channel,
    r.customer_choice,
    COALESCE(l.los_flag, 0) AS los_flag,
    CASE 
        WHEN r.new_cf_rank - r.mnth_lag <= 4 THEN 'CF'
        ELSE 'NEW' 
    END AS cc_type,
   timestamp(current_datetime('PST8PDT')) AS update_timestamp,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(current_datetime('PST8PDT') as string)) as update_timestamp_tz
    FROM rcpts r
LEFT JOIN los_ccs l
  ON r.mnth_idnt = l.mnth_idnt
 AND r.customer_choice = l.customer_choice
 AND LOWER(r.channel_country) = LOWER(l.channel_country)
 AND LOWER(r.channel_brand) = LOWER(l.channel_brand)
WHERE r.mnth_idnt BETWEEN (
        SELECT DISTINCT month_idnt
        FROM `calendar`
        WHERE day_date = {{params.start_date}}
    ) AND (
        SELECT DISTINCT month_idnt
        FROM `calendar`
        WHERE day_date = {{params.end_date}}
    );
