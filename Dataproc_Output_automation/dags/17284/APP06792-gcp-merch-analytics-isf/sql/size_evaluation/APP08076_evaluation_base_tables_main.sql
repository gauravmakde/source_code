    /*
    Name: Size Curves Evaluation Calendar & Locations
    APPID-Name: APP08076 Data Driven Size Curves
    Purpose: 
        - views for evaluation & monitoring dashboard
    Variable(s):    `{{params.gcp_project_id}}`.{{params.environment_schema}} `{{params.gcp_project_id}}`.{{params.environment_schema}}
                    {env_suffix} '' or '_dev' tablesuffix for prod testing
    
    DAG: APP08076_size_curves_eval_vws
    Author(s): Sara Riker
    Date Created: 6/12/24
    
    Creates: 
        - size_eval_cal
        - locations
    */
    
    
    
    
    TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal;
    
    
    INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal
    (
    day_date,
    week_idnt,
    week_label,
    ly_week_num_realigned,
    ly_month_idnt_realigned,
    ly_half_idnt_realigned,
    week_end_date,
    month_idnt,
    month_label,
    month_end_date,
    month_end_week_idnt,
    quarter_idnt,
    quarter_label,
    quarter_end_date,
    quarter_end_week_idnt,
    quarter_end_month_idnt,
    half_idnt,
    half_label,
    half_end_quarter_idnt,
    fiscal_year_num,
    hist_ind,
    current_month,
    last_completed_week,
    last_completed_month,
    last_completed_3_months,
    next_3_months,
    update_timestamp,
    update_timestamp_tz
    )
    WITH fiscal_year_range AS (SELECT DISTINCT fiscal_year_num - 1 AS start_year,
      fiscal_year_num + 1 AS end_year
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE('PST8PDT'))
    ,numbered_months AS (SELECT month_idnt,
     ROW_NUMBER() OVER (ORDER BY month_idnt DESC) AS row_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE fiscal_year_num BETWEEN (SELECT start_year
       FROM fiscal_year_range) AND (SELECT end_year
       FROM fiscal_year_range)
    GROUP BY month_idnt
    HAVING CURRENT_DATE('PST8PDT') > MAX(day_date))
    SELECT cal.day_date,
     cal.week_idnt,
     cal.week_label,
     ly.week_idnt AS ly_week_num_realigned,
     ly.month_idnt AS ly_month_idnt_realigned,
     ly.fiscal_halfyear_num AS ly_half_idnt_realigned,
      CASE
      WHEN cal.day_date = cal.week_end_day_date
      THEN 1
      ELSE 0
      END AS week_end_date,
     cal.month_idnt,
     cal.month_label,
      CASE
      WHEN cal.day_date = cal.month_end_day_date
      THEN 1
      ELSE 0
      END AS month_end_date,
      CASE
      WHEN cal.week_idnt = cal.month_end_week_idnt
      THEN 1
      ELSE 0
      END AS month_end_week_idnt,
     cal.quarter_idnt,
     cal.quarter_label,
      CASE
      WHEN cal.day_date = cal.quarter_end_day_date
      THEN 1
      ELSE 0
      END AS quarter_end_date,
      CASE
      WHEN cal.week_idnt = cal.quarter_end_week_idnt
      THEN 1
      ELSE 0
      END AS quarter_end_week_idnt,
      CASE
      WHEN cal.month_idnt = cal.quarter_end_month_idnt
      THEN 1
      ELSE 0
      END AS quarter_end_month_idnt,
     cal.fiscal_halfyear_num AS half_idnt,
     cal.half_label,
      CASE
      WHEN cal.fiscal_quarter_num IN (2, 4)
      THEN 1
      ELSE 0
      END AS half_end_quarter_idnt,
     cal.fiscal_year_num,
      CASE
      WHEN cal.week_idnt < (SELECT week_idnt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
        WHERE day_date = CURRENT_DATE('PST8PDT'))
      THEN 1
      ELSE 0
      END AS hist_ind,
      CASE
      WHEN cal.month_idnt = (SELECT month_idnt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
        WHERE day_date = CURRENT_DATE('PST8PDT'))
      THEN 1
      ELSE 0
      END AS current_month,
      CASE
      WHEN cal.week_idnt = (SELECT week_idnt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
        WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 7 DAY))
      THEN 1
      ELSE 0
      END AS last_completed_week,
      CASE
      WHEN nm.row_num = 1
      THEN 1
      ELSE 0
      END AS last_completed_month,
      CASE
      WHEN nm.row_num < 4
      THEN 1
      ELSE 0
      END AS last_completed_3_months,
      CASE
      WHEN nm.row_num BETWEEN 5 AND 7
      THEN 1
      ELSE 0
      END AS next_3_months,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS update_timestamp,
     `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS update_timestamp_tz
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS cal
     LEFT JOIN numbered_months AS nm ON cal.month_idnt = nm.month_idnt
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS ly ON cal.day_date_last_year_realigned = ly.day_date
    WHERE cal.fiscal_year_num BETWEEN (SELECT start_year
       FROM fiscal_year_range) AND (SELECT end_year
       FROM fiscal_year_range);
    
    
    --COLLECT STATS      COLUMN(day_date, week_idnt, month_idnt, quarter_idnt, half_idnt)     ,COLUMN(week_idnt)     ,COLUMN(week_idnt ,month_end_week_idnt, quarter_end_week_idnt)     ,COLUMN(quarter_end_week_idnt)     ,COLUMN(month_end_week_idnt)     ,COLUMN(day_date)     ,COLUMN(hist_ind)     ,COLUMN(hist_ind, week_idnt)     ,COLUMN(ly_week_num_realigned)     ,COLUMN(current_month)     ,COLUMN(last_completed_3_months)     ,COLUMN(half_idnt)     ,COLUMN(month_idnt)     ,COLUMN(month_idnt ,half_idnt)      ,COLUMN(last_completed_week)     ,COLUMN(hist_ind ,day_date)      ,COLUMN(last_completed_month)     ,COLUMN(fiscal_year_num)     ,COLUMN(half_end_quarter_idnt)     ,COLUMN(week_idnt ,week_label, ly_week_num_realigned ,ly_month_idnt_realigned, ly_half_idnt_realigned ,month_idnt, month_label ,month_end_date ,month_end_week_idnt ,quarter_idnt , quarter_label,              quarter_end_week_idnt ,quarter_end_month_idnt ,half_idnt, half_label ,half_end_quarter_idnt ,fiscal_year_num ,hist_ind, current_month ,last_completed_week ,last_completed_month,              last_completed_3_months)     ON `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal
    
    
    TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.locations;
    
    
    INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.locations
    (SELECT DISTINCT store_num,
      store_name,
      channel_num,
      banner,
      selling_channel,
      store_address_state,
      store_dma_desc,
      store_location_latitude,
      store_location_longitude,
      region_desc,
      region_short_desc,
      update_timestamp,
      update_timestamp_tz
     FROM (SELECT st.store_num,
        st.store_name,
        st.channel_num,
        st.channel_brand AS banner,
        st.selling_channel,
        st.store_address_state,
        st.store_dma_desc,
        st.store_location_latitude,
        st.store_location_longitude,
        st.region_desc,
        sd.region_short_desc,
        CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
        update_timestamp,
        `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON st.store_num = sd.store_num
       WHERE st.channel_num IN (110, 120, 210, 250)
        AND LOWER(st.store_type_code) <> LOWER('VS')
        AND st.store_open_date < (SELECT MIN(day_date)
          FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal
          WHERE last_completed_3_months = 1)
        AND LOWER(st.store_name) NOT LIKE LOWER('CLOSED%')) AS t2
     WHERE NOT EXISTS (SELECT 1 AS `A12180`
       FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.locations
       WHERE store_num = t2.store_num)
     QUALIFY (ROW_NUMBER() OVER (PARTITION BY store_num)) = 1);