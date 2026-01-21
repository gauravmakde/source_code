
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08300;
DAG_ID=mta_email_performance_11521_ACE_ENG;
---     Task_Name=mta_email_performance;'*/
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS bottom_funnel AS WITH base AS (SELECT activity_date_pacific,
    CASE
    WHEN LENGTH(REGEXP_EXTRACT_ALL(RPAD(utm_content, 20, ' '), '[^_]+')[SAFE_OFFSET(0)]) = 8 AND LOWER(REGEXP_EXTRACT_ALL(RPAD(utm_content
         , 20, ' '), '[^_]+')[SAFE_OFFSET(0)]) LIKE LOWER('202%')
    THEN PARSE_DATE('%Y%m%d', REGEXP_EXTRACT_ALL(RPAD(utm_content, 20, ' ') , r'[^_]+') [OFFSET ( 0 ) ])
    ELSE NULL
    END AS deploy_date,
    CASE
    WHEN LOWER(utm_source) IN (LOWER('NR_planned'), LOWER('NR_Transactional'), LOWER('NR_triggered'), LOWER('NR_triggered_bc'
       ))
    THEN 'Nordstrom Rack'
    WHEN LOWER(utm_source) IN (LOWER('N_transactional'), LOWER('N_triggered'), LOWER('N_triggered_bc'), LOWER('N_planned'
       ))
    THEN 'Nordstrom'
    ELSE NULL
    END AS brand,
    CASE
    WHEN LOWER(utm_source) IN (LOWER('N_planned'), LOWER('NR_planned')) OR LOWER(utm_source) IN (LOWER('N_transactional'
         ), LOWER('NR_transactional')) AND LOWER(utm_term) LIKE LOWER('%_%')
    THEN COALESCE(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(0)], FORMAT('%4d', 0))
    ELSE utm_campaign
    END AS campaign_id_version,
    CASE
    WHEN LOWER(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(1)]) <> LOWER('0')
    THEN COALESCE(TRIM(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(1)]), FORMAT('%4d', 0))
    ELSE COALESCE(TRIM(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(0)]), FORMAT('%4d', 0))
    END AS job_id,
   utm_source,
   channel AS order_channel,
    CASE
    WHEN LOWER(arrived_channel) = LOWER('R.COM')
    THEN 'Nordstrom Rack'
    WHEN LOWER(arrived_channel) = LOWER('N.COM')
    THEN 'Nordstrom'
    ELSE NULL
    END AS arrived_channel,
   SUM(gross) AS gross_demand,
   SUM(net_sales) AS net_demand,
   SUM(units) AS units,
   SUM(orders) AS orders,
   SUM(sessions) AS sessions,
   SUM(session_orders) AS session_orders,
   SUM(session_demand) AS session_demand,
   SUM(bounced_sessions) AS bounced_sessions
  FROM `{{params.gcp_project_id}}`.t2dl_das_mta.mkt_utm_agg
  WHERE LOWER(finance_detail) IN (LOWER('EMAIL_MARKETING'), LOWER('EMAIL_TRANSACT'), LOWER('EMAIL_TRIGGER'))
   AND LOWER(utm_source) IN (LOWER('NR_planned'), LOWER('NR_Transactional'), LOWER('NR_triggered'), LOWER('NR_triggered_bc'
      ), LOWER('N_transactional'), LOWER('N_triggered'), LOWER('N_triggered_bc'), LOWER('N_planned'))
   AND LOWER(channelcountry) = LOWER('US')
   AND activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
   AND UPPER(CASE
      WHEN LOWER(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(1)]) <> LOWER('0')
      THEN COALESCE(TRIM(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(1)]), FORMAT('%4d', 0))
      ELSE COALESCE(TRIM(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(0)]), FORMAT('%4d', 0))
      END) = LOWER(CASE
      WHEN LOWER(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(1)]) <> LOWER('0')
      THEN COALESCE(TRIM(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(1)]), FORMAT('%4d', 0))
      ELSE COALESCE(TRIM(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(0)]), FORMAT('%4d', 0))
      END)
   AND LENGTH(CASE
      WHEN LOWER(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(1)]) <> LOWER('0')
      THEN COALESCE(TRIM(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(1)]), FORMAT('%4d', 0))
      ELSE COALESCE(TRIM(REGEXP_EXTRACT_ALL(RPAD(utm_term, 20, ' '), '[^_]+')[SAFE_OFFSET(0)]), FORMAT('%4d', 0))
      END) <= 6
  GROUP BY activity_date_pacific,
   deploy_date,
   brand,
   campaign_id_version,
   job_id,
   utm_source,
   order_channel,
   arrived_channel
  HAVING activity_date_pacific >= deploy_date) (SELECT activity_date_pacific,
   deploy_date,
   brand,
   campaign_id_version,
   job_id,
   utm_source,
   order_channel,
   arrived_channel,
   gross_demand,
   net_demand,
   units,
   orders,
   sessions,
   session_orders,
   session_demand,
   bounced_sessions,
   DATE_DIFF(activity_date_pacific, deploy_date, DAY) AS measurement_days
  FROM base);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS top_funnel_1
AS
SELECT message_id,
  CASE
  WHEN message_id IN (655030, 655031)
  THEN FORMAT('%11d', 600827)
  WHEN message_id IN (655152, 655153)
  THEN FORMAT('%11d', 600830)
  WHEN LOWER(campaign_name) = LOWER('07_750_DAM_Credit')
  THEN 'tdamsendcomm'
  WHEN version > 0
  THEN FORMAT('%11d', version)
  ELSE campaign_id
  END AS campaign_id_version,
 brand,
 campaign_type,
 last_updated_date AS deploy_date,
  CASE
  WHEN LOWER(campaign_type) LIKE LOWER('%Planned')
  THEN DATE_DIFF(engagement_date, last_updated_date, DAY)
  WHEN LOWER(campaign_type) LIKE LOWER('%Triggered') AND last_updated_date <= engagement_date
  THEN DATE_DIFF(engagement_date, last_updated_date, DAY)
  ELSE NULL
  END AS measurement_days,
  CASE
  WHEN LOWER(campaign_id) = LOWER('RACKwindowshop')
  THEN 'RACKwindowshop'
  WHEN message_id IN (655030, 655031)
  THEN 'CTR-reminder-womens'
  WHEN message_id IN (655152, 655153)
  THEN 'CTR-last-day-womens'
  ELSE campaign_name
  END AS campaign_name,
 MIN(subject_line) AS subject_line,
 SUM(total_sent_count) AS total_sent_count,
 SUM(total_delivered_count) AS total_delivered_count,
 SUM(total_clicked_count) AS total_clicked_count,
 SUM(total_opened_count) AS total_opened_count,
 SUM(total_bounced_count) AS total_bounced_count,
  SUM(cpp_total_unsubscribed_count) + SUM(ced_total_unsubscribed_count) AS total_unsubscribed_count,
 SUM(unique_clicked_count) AS unique_clicked_count,
 SUM(unique_opened_count) AS unique_opened_count,
  SUM(ced_unique_unsubscribed_count) + SUM(cpp_total_unsubscribed_count) AS unique_unsubscribed_count
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.email_contact_event_hdr_fct AS a
WHERE LOWER(campaign_type) <> LOWER('')
 AND LOWER(campaign_segment) <> LOWER('CA')
 AND last_updated_date BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND last_updated_date IS NOT NULL
GROUP BY message_id,
 campaign_id_version,
 brand,
 campaign_type,
 deploy_date,
 measurement_days,
 campaign_name;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS impacted_message_id
AS
SELECT campaign_id_version,
 message_id
FROM (SELECT campaign_id_version,
   message_id,
   SUM(total_delivered_count) AS total_delivered_count,
   SUM(unique_opened_count) AS unique_opened_count,
   SUM(unique_clicked_count) AS unique_clicked_count,
   SUM(total_bounced_count) AS total_bounced_count
  FROM top_funnel_1
  GROUP BY message_id,
   campaign_id_version
  HAVING total_delivered_count < 0 OR unique_opened_count > total_delivered_count OR unique_clicked_count >
      unique_opened_count OR unique_clicked_count > total_delivered_count OR total_bounced_count > total_delivered_count
    ) AS t1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS top_funnel
AS
SELECT a.message_id,
 a.campaign_id_version,
 a.brand,
 a.campaign_type,
 a.deploy_date,
 a.measurement_days,
 a.campaign_name,
 a.subject_line,
 a.total_sent_count,
 a.total_delivered_count,
 a.total_clicked_count,
 a.total_opened_count,
 a.total_bounced_count,
 a.total_unsubscribed_count,
 a.unique_clicked_count,
 a.unique_opened_count,
 a.unique_unsubscribed_count,
  CASE
  WHEN b.campaign_id_version IS NOT NULL AND b.message_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS impacted_flag
FROM top_funnel_1 AS a
 LEFT JOIN impacted_message_id AS b ON LOWER(a.campaign_id_version) = LOWER(b.campaign_id_version) AND a.message_id = b
   .message_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS top_funnel_1_day
AS
SELECT message_id,
 TRIM(campaign_id_version) AS campaign_id_version,
 brand,
 TRIM(campaign_type) AS campaign_type,
 deploy_date,
 campaign_name,
 MAX(impacted_flag) AS impacted_flag,
 MIN(subject_line) AS subject_line,
 SUM(total_sent_count) AS total_sent_count,
 SUM(total_delivered_count) AS total_delivered_count,
 SUM(total_clicked_count) AS total_clicked_count,
 SUM(total_opened_count) AS total_opened_count,
 SUM(total_bounced_count) AS total_bounced_count,
 SUM(total_unsubscribed_count) AS total_unsubscribed_count,
 SUM(unique_clicked_count) AS unique_clicked_count,
 SUM(unique_opened_count) AS unique_opened_count,
 SUM(unique_unsubscribed_count) AS unique_unsubscribed_count
FROM top_funnel AS a
WHERE measurement_days BETWEEN 0 AND 1
GROUP BY message_id,
 campaign_id_version,
 brand,
 campaign_type,
 deploy_date,
 campaign_name;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS top_funnel_10_day
AS
SELECT message_id,
 TRIM(campaign_id_version) AS campaign_id_version,
 brand,
 TRIM(campaign_type) AS campaign_type,
 deploy_date,
 campaign_name,
 MAX(impacted_flag) AS impacted_flag,
 MIN(subject_line) AS subject_line,
 SUM(total_sent_count) AS total_sent_count,
 SUM(total_delivered_count) AS total_delivered_count,
 SUM(total_clicked_count) AS total_clicked_count,
 SUM(total_opened_count) AS total_opened_count,
 SUM(total_bounced_count) AS total_bounced_count,
 SUM(total_unsubscribed_count) AS total_unsubscribed_count,
 SUM(unique_clicked_count) AS unique_clicked_count,
 SUM(unique_opened_count) AS unique_opened_count,
 SUM(unique_unsubscribed_count) AS unique_unsubscribed_count
FROM top_funnel AS a
WHERE measurement_days BETWEEN 0 AND 10
GROUP BY message_id,
 campaign_id_version,
 brand,
 campaign_type,
 deploy_date,
 campaign_name;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS top_funnel_30_day
AS
SELECT message_id,
 TRIM(campaign_id_version) AS campaign_id_version,
 brand,
 TRIM(campaign_type) AS campaign_type,
 deploy_date,
 campaign_name,
 MAX(impacted_flag) AS impacted_flag,
 MIN(subject_line) AS subject_line,
 SUM(total_sent_count) AS total_sent_count,
 SUM(total_delivered_count) AS total_delivered_count,
 SUM(total_clicked_count) AS total_clicked_count,
 SUM(total_opened_count) AS total_opened_count,
 SUM(total_bounced_count) AS total_bounced_count,
 SUM(total_unsubscribed_count) AS total_unsubscribed_count,
 SUM(unique_clicked_count) AS unique_clicked_count,
 SUM(unique_opened_count) AS unique_opened_count,
 SUM(unique_unsubscribed_count) AS unique_unsubscribed_count
FROM top_funnel
WHERE measurement_days BETWEEN 0 AND 30
GROUP BY message_id,
 campaign_id_version,
 brand,
 campaign_type,
 deploy_date,
 campaign_name;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS complete_funnel_1_day
AS
SELECT TRIM(a.campaign_id_version) AS campaign_id_version,
 a.message_id AS job_id,
 a.deploy_date,
 a.brand AS box_type,
  CASE
  WHEN LOWER(a.campaign_type) IN (LOWER('Marketing Planned'), LOWER('Marketing Triggered'))
  THEN 'Marketing'
  ELSE 'Transactional'
  END AS program_type,
 TRIM(a.campaign_type) AS email_type,
 a.campaign_name,
 t1.arrived_channel,
  CASE
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Triggered')
  THEN 'N Transactional Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND LOWER(t1.utm_source
     ) = LOWER('N_triggered_bc')
  THEN 'N Bluecore Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered')
  THEN 'N Marketing Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Transactional Triggered')
  THEN 'NR Transactional Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND LOWER(t1.utm_source
     ) = LOWER('NR_triggered_bc')
  THEN 'NR Bluecore Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND (LOWER(t1
       .utm_source) <> LOWER('NR_triggered_bc') OR t1.utm_source IS NULL)
  THEN 'NR Marketing Triggers'
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND c.campaign_category_corrected IS NOT NULL
  THEN c.campaign_category_corrected
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND d.campaign_category_new
   IS NOT NULL
  THEN d.campaign_category_new
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name) LIKE LOWER('%Sale%')
  THEN 'Sale'
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name) LIKE LOWER('%Loyalty%')
  THEN 'Loyalty'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lcactive%')
  THEN 'LCActive'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lclapsed%')
  THEN 'LCLapsed'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%welcome%')
  THEN 'Welcome'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lcreengagement%')
  THEN 'LCReengagement'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%squad%')
  THEN 'Squad'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Pop In%')
  THEN 'Pop-In'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack Loyalty%')
  THEN 'Rack Loyalty'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Holiday%')
  THEN 'Holiday'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack Credit%')
  THEN 'Rack Credit'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens YA Apparel%')
  THEN 'Womens YA Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Contemporary Apparel%')
  THEN 'Womens Contemporary Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Active Shoes%')
  THEN 'Womens Active Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Contemporary Shoes%')
  THEN 'Womens Contemporary Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Active Shoes%')
  THEN 'Mens Active Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Core Apparel%')
  THEN 'Womens Core Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Core Shoes%')
  THEN 'Womens Core Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Active Apparel%')
  THEN 'Womens Active Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Designer%')
  THEN 'Womens Designer'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Accessories%')
  THEN 'Womens Accessories'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Specialized%')
  THEN 'Womens Specialized'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Core Apparel%')
  THEN 'Mens Core Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Core Shoes%')
  THEN 'Mens Core Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Active Apparel%')
  THEN 'Mens Active Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Designer%')
  THEN 'Mens Designer'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Accessories%')
  THEN 'Mens Accessories'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Specialized%')
  THEN 'Mens Specialized'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%XDIV%')
  THEN 'XDIV'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Multi DIV%')
  THEN 'XDIV'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Home%')
  THEN 'Home'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Kids%')
  THEN 'Kids'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Beauty%')
  THEN 'Beauty'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack%')
  THEN 'Rack'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Cyber%')
  THEN 'Cyber'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Hallmark Holidays%')
  THEN 'Hallmark Holidays'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CRM%')
  THEN 'CRM'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CRI%')
  THEN 'CRI'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%NPG%')
  THEN 'NPG'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%New Concepts%')
  THEN 'New Concepts'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Promo%')
  THEN 'Promo'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
         ) LIKE LOWER('%Services') OR LOWER(a.campaign_name) LIKE LOWER('%Styling') OR LOWER(a.campaign_name) LIKE LOWER('%NMS'
        ) OR LOWER(a.campaign_name) LIKE LOWER('%Events'))
  THEN 'Services'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary EA%')
  THEN 'Anniversary EA'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary Preview%')
  THEN 'Anniversary Preview'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary Public%')
  THEN 'Anniversary Public'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
       ) LIKE LOWER('%Anni Awareness%') OR LOWER(a.campaign_name) LIKE LOWER('%Anniversary Awareness%'))
  THEN 'Anniversary Awareness'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
        ) LIKE LOWER('%DIB/CSR%') OR LOWER(a.campaign_name) LIKE LOWER('%DBI%') OR LOWER(a.campaign_name) LIKE LOWER('%DBI/CSR%'
       ))
  THEN 'DIB/CSR'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_name) LIKE LOWER('%Credit%')
  THEN 'Credit'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_name) LIKE LOWER('%GC%')
  THEN 'Gift Card'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned')
  THEN 'N Marketing Planned - Unknown'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned') AND (LOWER(a.campaign_name
       ) LIKE LOWER('%Corporate%') OR LOWER(a.campaign_name) LIKE LOWER('%Service%'))
  THEN 'Corporate'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Loyalty%')
  THEN 'Loyalty-Transactional Planned'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned')
  THEN 'N Transactional Planned'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CTR%')
  THEN 'CTR'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
        ) LIKE LOWER('%merch-store%') OR LOWER(a.campaign_name) LIKE LOWER('%store-driving%') OR LOWER(a.campaign_name)
      LIKE LOWER('%store%'))
  THEN 'Store'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%blowout%')
  THEN 'Blowouts'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Survey%')
  THEN 'Survey'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Dedicated%')
  THEN 'Dedicated'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Markdown%')
  THEN 'Markdowns'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%new-arrivals%')
  THEN 'New Arrivals'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Division-Affinity%')
  THEN 'Division Affinity'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch%')
  THEN 'Merch'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch-dedicated%')
  THEN 'Dedicated'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch-survey%')
  THEN 'Survey'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%flash%')
  THEN 'Flash'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('sundaypm%')
  THEN 'SundayPM'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('clearance-promo%')
  THEN 'Clearance Promo'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned')
  THEN 'NR Marketing Planned - Unknown'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Transactional Planned')
  THEN 'NR Transactional Planned'
  ELSE a.campaign_type
  END AS campaign_category,
 1 AS measurement_days,
 a.impacted_flag,
 a.subject_line,
 a.total_sent_count,
 a.total_delivered_count,
 a.total_clicked_count,
 a.total_opened_count,
 a.total_bounced_count,
 a.total_unsubscribed_count,
 a.unique_clicked_count,
 a.unique_opened_count,
 a.unique_unsubscribed_count,
 COALESCE(SUM(t1.gross_demand), 0) AS gross_sales,
 COALESCE(SUM(t1.net_demand), 0) AS net_sales,
 COALESCE(SUM(t1.orders), 0) AS orders,
 COALESCE(SUM(t1.units), 0) AS units,
 COALESCE(SUM(t1.sessions), 0) AS sessions,
 COALESCE(SUM(t1.session_orders), 0) AS session_orders,
 COALESCE(SUM(t1.session_demand), 0) AS session_demand,
 COALESCE(SUM(t1.bounced_sessions), 0) AS bounced_sessions
FROM top_funnel_1_day AS a
 LEFT JOIN (SELECT deploy_date,
   brand,
   utm_source,
   campaign_id_version,
   job_id,
   arrived_channel,
   measurement_days,
   SUM(gross_demand) AS gross_demand,
   SUM(net_demand) AS net_demand,
   SUM(units) AS units,
   SUM(orders) AS orders,
   SUM(sessions) AS sessions,
   SUM(session_orders) AS session_orders,
   SUM(session_demand) AS session_demand,
   SUM(bounced_sessions) AS bounced_sessions
  FROM bottom_funnel
  WHERE measurement_days BETWEEN 0 AND 1
  GROUP BY deploy_date,
   brand,
   campaign_id_version,
   job_id,
   utm_source,
   arrived_channel,
   measurement_days) AS t1 ON LOWER(RPAD(a.campaign_id_version, 70, ' ')) = LOWER(RPAD(t1.campaign_id_version, 70, ' ')
     ) AND a.message_id = CAST(t1.job_id AS FLOAT64) AND a.deploy_date = t1.deploy_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_email_campaign_category_correction AS c ON LOWER(a.campaign_name) = LOWER(c.campaign_name)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_email_campaign_id_correction AS d ON LOWER(a.campaign_id_version) = LOWER(d.campaign_id_version
   )
WHERE DATE_DIFF(CURRENT_DATE('PST8PDT'), a.deploy_date, DAY) >= 1
GROUP BY campaign_id_version,
 job_id,
 a.deploy_date,
 box_type,
 program_type,
 email_type,
 a.campaign_name,
 t1.arrived_channel,
 campaign_category,
 measurement_days,
 a.impacted_flag,
 a.subject_line,
 a.total_sent_count,
 a.total_delivered_count,
 a.total_clicked_count,
 a.total_opened_count,
 a.total_bounced_count,
 a.total_unsubscribed_count,
 a.unique_clicked_count,
 a.unique_opened_count,
 a.unique_unsubscribed_count;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS complete_funnel_10_day
AS
SELECT TRIM(a.campaign_id_version) AS campaign_id_version,
 a.message_id AS job_id,
 a.deploy_date,
 a.brand AS box_type,
  CASE
  WHEN LOWER(a.campaign_type) IN (LOWER('Marketing Planned'), LOWER('Marketing Triggered'))
  THEN 'Marketing'
  ELSE 'Transactional'
  END AS program_type,
 TRIM(a.campaign_type) AS email_type,
 a.campaign_name,
 b.arrived_channel,
  CASE
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Triggered')
  THEN 'N Transactional Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND LOWER(b.utm_source
     ) = LOWER('N_triggered_bc')
  THEN 'N Bluecore Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered')
  THEN 'N Marketing Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Transactional Triggered')
  THEN 'NR Transactional Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND LOWER(b.utm_source
     ) = LOWER('NR_triggered_bc')
  THEN 'NR Bluecore Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND (LOWER(b.utm_source
       ) <> LOWER('NR_triggered_bc') OR b.utm_source IS NULL)
  THEN 'NR Marketing Triggers'
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND c.campaign_category_corrected IS NOT NULL
  THEN c.campaign_category_corrected
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND d.campaign_category_new
   IS NOT NULL
  THEN d.campaign_category_new
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name) LIKE LOWER('%Sale%')
  THEN 'Sale'
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name) LIKE LOWER('%Loyalty%')
  THEN 'Loyalty'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lcactive%')
  THEN 'LCActive'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lclapsed%')
  THEN 'LCLapsed'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%welcome%')
  THEN 'Welcome'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lcreengagement%')
  THEN 'LCReengagement'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%squad%')
  THEN 'Squad'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Pop In%')
  THEN 'Pop-In'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack Loyalty%')
  THEN 'Rack Loyalty'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Holiday%')
  THEN 'Holiday'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack Credit%')
  THEN 'Rack Credit'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens YA Apparel%')
  THEN 'Womens YA Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Contemporary Apparel%')
  THEN 'Womens Contemporary Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Active Shoes%')
  THEN 'Womens Active Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Contemporary Shoes%')
  THEN 'Womens Contemporary Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Active Shoes%')
  THEN 'Mens Active Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Core Apparel%')
  THEN 'Womens Core Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Core Shoes%')
  THEN 'Womens Core Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Active Apparel%')
  THEN 'Womens Active Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Designer%')
  THEN 'Womens Designer'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Accessories%')
  THEN 'Womens Accessories'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Specialized%')
  THEN 'Womens Specialized'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Core Apparel%')
  THEN 'Mens Core Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Core Shoes%')
  THEN 'Mens Core Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Active Apparel%')
  THEN 'Mens Active Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Designer%')
  THEN 'Mens Designer'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Accessories%')
  THEN 'Mens Accessories'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Specialized%')
  THEN 'Mens Specialized'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%XDIV%')
  THEN 'XDIV'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Multi DIV%')
  THEN 'XDIV'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Home%')
  THEN 'Home'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Kids%')
  THEN 'Kids'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Beauty%')
  THEN 'Beauty'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack%')
  THEN 'Rack'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Cyber%')
  THEN 'Cyber'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Hallmark Holidays%')
  THEN 'Hallmark Holidays'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CRM%')
  THEN 'CRM'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CRI%')
  THEN 'CRI'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%NPG%')
  THEN 'NPG'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%New Concepts%')
  THEN 'New Concepts'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Promo%')
  THEN 'Promo'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
         ) LIKE LOWER('%Services') OR LOWER(a.campaign_name) LIKE LOWER('%Styling') OR LOWER(a.campaign_name) LIKE LOWER('%NMS'
        ) OR LOWER(a.campaign_name) LIKE LOWER('%Events'))
  THEN 'Services'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary EA%')
  THEN 'Anniversary EA'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary Preview%')
  THEN 'Anniversary Preview'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary Public%')
  THEN 'Anniversary Public'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
       ) LIKE LOWER('%Anni Awareness%') OR LOWER(a.campaign_name) LIKE LOWER('%Anniversary Awareness%'))
  THEN 'Anniversary Awareness'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
        ) LIKE LOWER('%DIB/CSR%') OR LOWER(a.campaign_name) LIKE LOWER('%DBI%') OR LOWER(a.campaign_name) LIKE LOWER('%DBI/CSR%'
       ))
  THEN 'DIB/CSR'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_name) LIKE LOWER('%Credit%')
  THEN 'Credit'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_name) LIKE LOWER('%GC%')
  THEN 'Gift Card'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned')
  THEN 'N Marketing Planned - Unknown'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned') AND (LOWER(a.campaign_name
       ) LIKE LOWER('%Corporate%') OR LOWER(a.campaign_name) LIKE LOWER('%Service%'))
  THEN 'Corporate'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Loyalty%')
  THEN 'Loyalty-Transactional Planned'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned')
  THEN 'N Transactional Planned'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CTR%')
  THEN 'CTR'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
        ) LIKE LOWER('%merch-store%') OR LOWER(a.campaign_name) LIKE LOWER('%store-driving%') OR LOWER(a.campaign_name)
      LIKE LOWER('%store%'))
  THEN 'Store'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%blowout%')
  THEN 'Blowouts'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Survey%')
  THEN 'Survey'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Dedicated%')
  THEN 'Dedicated'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Markdown%')
  THEN 'Markdowns'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%new-arrivals%')
  THEN 'New Arrivals'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Division-Affinity%')
  THEN 'Division Affinity'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch%')
  THEN 'Merch'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch-dedicated%')
  THEN 'Dedicated'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch-survey%')
  THEN 'Survey'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%flash%')
  THEN 'Flash'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('sundaypm%')
  THEN 'SundayPM'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('clearance-promo%')
  THEN 'Clearance Promo'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned')
  THEN 'NR Marketing Planned - Unknown'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Transactional Planned')
  THEN 'NR Transactional Planned'
  ELSE a.campaign_type
  END AS campaign_category,
 10 AS measurement_days,
 a.impacted_flag,
 a.subject_line,
 a.total_sent_count,
 a.total_delivered_count,
 a.total_clicked_count,
 a.total_opened_count,
 a.total_bounced_count,
 a.total_unsubscribed_count,
 a.unique_clicked_count,
 a.unique_opened_count,
 a.unique_unsubscribed_count,
 COALESCE(SUM(b.gross_demand), 0) AS gross_sales,
 COALESCE(SUM(b.net_demand), 0) AS net_sales,
 COALESCE(SUM(b.orders), 0) AS orders,
 COALESCE(SUM(b.units), 0) AS units,
 COALESCE(SUM(b.sessions), 0) AS sessions,
 COALESCE(SUM(b.session_orders), 0) AS session_orders,
 COALESCE(SUM(b.session_demand), 0) AS session_demand,
 COALESCE(SUM(b.bounced_sessions), 0) AS bounced_sessions
FROM top_funnel_10_day AS a
 LEFT JOIN (SELECT deploy_date,
   campaign_id_version,
   job_id,
   utm_source,
   arrived_channel,
   measurement_days,
   SUM(gross_demand) AS gross_demand,
   SUM(net_demand) AS net_demand,
   SUM(units) AS units,
   SUM(orders) AS orders,
   SUM(sessions) AS sessions,
   SUM(session_orders) AS session_orders,
   SUM(session_demand) AS session_demand,
   SUM(bounced_sessions) AS bounced_sessions
  FROM bottom_funnel
  WHERE measurement_days BETWEEN 0 AND 10
  GROUP BY deploy_date,
   campaign_id_version,
   job_id,
   utm_source,
   arrived_channel,
   measurement_days) AS b ON LOWER(RPAD(a.campaign_id_version, 70, ' ')) = LOWER(RPAD(b.campaign_id_version, 70, ' '))
   AND a.message_id = CAST(b.job_id AS FLOAT64) AND a.deploy_date = b.deploy_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_email_campaign_category_correction AS c ON LOWER(a.campaign_name) = LOWER(c.campaign_name)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_email_campaign_id_correction AS d ON LOWER(a.campaign_id_version) = LOWER(d.campaign_id_version
   )
WHERE DATE_DIFF(CURRENT_DATE('PST8PDT'), a.deploy_date, DAY) >= 10
GROUP BY campaign_id_version,
 job_id,
 a.deploy_date,
 box_type,
 program_type,
 email_type,
 a.campaign_name,
 b.arrived_channel,
 campaign_category,
 measurement_days,
 a.impacted_flag,
 a.subject_line,
 a.total_sent_count,
 a.total_delivered_count,
 a.total_clicked_count,
 a.total_opened_count,
 a.total_bounced_count,
 a.total_unsubscribed_count,
 a.unique_clicked_count,
 a.unique_opened_count,
 a.unique_unsubscribed_count;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS complete_funnel_30_day
AS
SELECT TRIM(a.campaign_id_version) AS campaign_id_version,
 a.message_id AS job_id,
 a.deploy_date,
 a.brand AS box_type,
  CASE
  WHEN LOWER(a.campaign_type) IN (LOWER('Marketing Planned'), LOWER('Marketing Triggered'))
  THEN 'Marketing'
  ELSE 'Transactional'
  END AS program_type,
 TRIM(a.campaign_type) AS email_type,
 a.campaign_name,
 b.arrived_channel,
  CASE
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Triggered')
  THEN 'N Transactional Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND LOWER(b.utm_source
     ) = LOWER('N_triggered_bc')
  THEN 'N Bluecore Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered')
  THEN 'N Marketing Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Transactional Triggered')
  THEN 'NR Transactional Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND LOWER(b.utm_source
     ) = LOWER('NR_triggered_bc')
  THEN 'NR Bluecore Triggers'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Triggered') AND (LOWER(b.utm_source
       ) <> LOWER('NR_triggered_bc') OR b.utm_source IS NULL)
  THEN 'NR Marketing Triggers'
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND c.campaign_category_corrected IS NOT NULL
  THEN c.campaign_category_corrected
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND d.campaign_category_new
   IS NOT NULL
  THEN d.campaign_category_new
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name) LIKE LOWER('%Sale%')
  THEN 'Sale'
  WHEN LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name) LIKE LOWER('%Loyalty%')
  THEN 'Loyalty'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lcactive%')
  THEN 'LCActive'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lclapsed%')
  THEN 'LCLapsed'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%welcome%')
  THEN 'Welcome'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%lcreengagement%')
  THEN 'LCReengagement'
  WHEN LOWER(a.campaign_name) LIKE LOWER('%squad%')
  THEN 'Squad'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Pop In%')
  THEN 'Pop-In'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack Loyalty%')
  THEN 'Rack Loyalty'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Holiday%')
  THEN 'Holiday'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack Credit%')
  THEN 'Rack Credit'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens YA Apparel%')
  THEN 'Womens YA Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Contemporary Apparel%')
  THEN 'Womens Contemporary Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Active Shoes%')
  THEN 'Womens Active Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Contemporary Shoes%')
  THEN 'Womens Contemporary Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Active Shoes%')
  THEN 'Mens Active Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Core Apparel%')
  THEN 'Womens Core Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Core Shoes%')
  THEN 'Womens Core Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Active Apparel%')
  THEN 'Womens Active Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Designer%')
  THEN 'Womens Designer'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Accessories%')
  THEN 'Womens Accessories'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Womens Specialized%')
  THEN 'Womens Specialized'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Core Apparel%')
  THEN 'Mens Core Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Core Shoes%')
  THEN 'Mens Core Shoes'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Active Apparel%')
  THEN 'Mens Active Apparel'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Designer%')
  THEN 'Mens Designer'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Accessories%')
  THEN 'Mens Accessories'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Mens Specialized%')
  THEN 'Mens Specialized'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%XDIV%')
  THEN 'XDIV'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Multi DIV%')
  THEN 'XDIV'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Home%')
  THEN 'Home'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Kids%')
  THEN 'Kids'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Beauty%')
  THEN 'Beauty'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Rack%')
  THEN 'Rack'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Cyber%')
  THEN 'Cyber'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Hallmark Holidays%')
  THEN 'Hallmark Holidays'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CRM%')
  THEN 'CRM'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CRI%')
  THEN 'CRI'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%NPG%')
  THEN 'NPG'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%New Concepts%')
  THEN 'New Concepts'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Promo%')
  THEN 'Promo'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
         ) LIKE LOWER('%Services') OR LOWER(a.campaign_name) LIKE LOWER('%Styling') OR LOWER(a.campaign_name) LIKE LOWER('%NMS'
        ) OR LOWER(a.campaign_name) LIKE LOWER('%Events'))
  THEN 'Services'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary EA%')
  THEN 'Anniversary EA'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary Preview%')
  THEN 'Anniversary Preview'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('Anniversary Public%')
  THEN 'Anniversary Public'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
       ) LIKE LOWER('%Anni Awareness%') OR LOWER(a.campaign_name) LIKE LOWER('%Anniversary Awareness%'))
  THEN 'Anniversary Awareness'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
        ) LIKE LOWER('%DIB/CSR%') OR LOWER(a.campaign_name) LIKE LOWER('%DBI%') OR LOWER(a.campaign_name) LIKE LOWER('%DBI/CSR%'
       ))
  THEN 'DIB/CSR'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_name) LIKE LOWER('%Credit%')
  THEN 'Credit'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_name) LIKE LOWER('%GC%')
  THEN 'Gift Card'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Marketing Planned')
  THEN 'N Marketing Planned - Unknown'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned') AND (LOWER(a.campaign_name
       ) LIKE LOWER('%Corporate%') OR LOWER(a.campaign_name) LIKE LOWER('%Service%'))
  THEN 'Corporate'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Loyalty%')
  THEN 'Loyalty-Transactional Planned'
  WHEN LOWER(a.brand) = LOWER('Nordstrom') AND LOWER(a.campaign_type) = LOWER('Transactional Planned')
  THEN 'N Transactional Planned'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%CTR%')
  THEN 'CTR'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND (LOWER(a.campaign_name
        ) LIKE LOWER('%merch-store%') OR LOWER(a.campaign_name) LIKE LOWER('%store-driving%') OR LOWER(a.campaign_name)
      LIKE LOWER('%store%'))
  THEN 'Store'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%blowout%')
  THEN 'Blowouts'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Survey%')
  THEN 'Survey'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Dedicated%')
  THEN 'Dedicated'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Markdown%')
  THEN 'Markdowns'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%new-arrivals%')
  THEN 'New Arrivals'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%Division-Affinity%')
  THEN 'Division Affinity'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch%')
  THEN 'Merch'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch-dedicated%')
  THEN 'Dedicated'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%merch-survey%')
  THEN 'Survey'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('%flash%')
  THEN 'Flash'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('sundaypm%')
  THEN 'SundayPM'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned') AND LOWER(a.campaign_name
     ) LIKE LOWER('clearance-promo%')
  THEN 'Clearance Promo'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Marketing Planned')
  THEN 'NR Marketing Planned - Unknown'
  WHEN LOWER(a.brand) = LOWER('Nordstrom Rack') AND LOWER(a.campaign_type) = LOWER('Transactional Planned')
  THEN 'NR Transactional Planned'
  ELSE a.campaign_type
  END AS campaign_category,
 30 AS measurement_days,
 a.impacted_flag,
 a.subject_line,
 a.total_sent_count,
 a.total_delivered_count,
 a.total_clicked_count,
 a.total_opened_count,
 a.total_bounced_count,
 a.total_unsubscribed_count,
 a.unique_clicked_count,
 a.unique_opened_count,
 a.unique_unsubscribed_count,
 COALESCE(SUM(b.gross_demand), 0) AS gross_sales,
 COALESCE(SUM(b.net_demand), 0) AS net_sales,
 COALESCE(SUM(b.orders), 0) AS orders,
 COALESCE(SUM(b.units), 0) AS units,
 COALESCE(SUM(b.sessions), 0) AS sessions,
 COALESCE(SUM(b.session_orders), 0) AS session_orders,
 COALESCE(SUM(b.session_demand), 0) AS session_demand,
 COALESCE(SUM(b.bounced_sessions), 0) AS bounced_sessions
FROM top_funnel_30_day AS a
 LEFT JOIN (SELECT deploy_date,
   campaign_id_version,
   job_id,
   utm_source,
   arrived_channel,
   measurement_days,
   SUM(gross_demand) AS gross_demand,
   SUM(net_demand) AS net_demand,
   SUM(units) AS units,
   SUM(orders) AS orders,
   SUM(sessions) AS sessions,
   SUM(session_orders) AS session_orders,
   SUM(session_demand) AS session_demand,
   SUM(bounced_sessions) AS bounced_sessions
  FROM bottom_funnel
  WHERE measurement_days BETWEEN 0 AND 30
  GROUP BY deploy_date,
   campaign_id_version,
   job_id,
   utm_source,
   arrived_channel,
   measurement_days) AS b ON LOWER(RPAD(a.campaign_id_version, 70, ' ')) = LOWER(RPAD(b.campaign_id_version, 70, ' '))
   AND a.message_id = CAST(b.job_id AS FLOAT64) AND a.deploy_date = b.deploy_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_email_campaign_category_correction AS c ON LOWER(a.campaign_name) = LOWER(c.campaign_name)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_email_campaign_id_correction AS d ON LOWER(a.campaign_id_version) = LOWER(d.campaign_id_version
   )
WHERE DATE_DIFF(CURRENT_DATE('PST8PDT'), a.deploy_date, DAY) >= 30
GROUP BY campaign_id_version,
 job_id,
 a.deploy_date,
 box_type,
 program_type,
 email_type,
 a.campaign_name,
 b.arrived_channel,
 campaign_category,
 measurement_days,
 a.impacted_flag,
 a.subject_line,
 a.total_sent_count,
 a.total_delivered_count,
 a.total_clicked_count,
 a.total_opened_count,
 a.total_bounced_count,
 a.total_unsubscribed_count,
 a.unique_clicked_count,
 a.unique_opened_count,
 a.unique_unsubscribed_count;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS email_campaign_deep_dive
AS
SELECT campaign_id_version,
 job_id,
 deploy_date,
 box_type,
 program_type,
 email_type,
 campaign_name,
 arrived_channel,
 campaign_category,
 measurement_days,
 impacted_flag,
 subject_line,
 total_sent_count,
 total_delivered_count,
 total_clicked_count,
 total_opened_count,
 total_bounced_count,
 total_unsubscribed_count,
 unique_clicked_count,
 unique_opened_count,
 unique_unsubscribed_count,
 gross_sales,
 net_sales,
 orders,
 units,
 sessions,
 session_orders,
 session_demand,
 bounced_sessions
FROM (SELECT campaign_id_version,
    job_id,
    deploy_date,
    box_type,
    program_type,
    email_type,
    campaign_name,
    arrived_channel,
    campaign_category,
    measurement_days,
    impacted_flag,
    subject_line,
    total_sent_count,
    total_delivered_count,
    total_clicked_count,
    total_opened_count,
    total_bounced_count,
    total_unsubscribed_count,
    unique_clicked_count,
    unique_opened_count,
    unique_unsubscribed_count,
    gross_sales,
    net_sales,
    orders,
    units,
    sessions,
    session_orders,
    session_demand,
    bounced_sessions,
     CASE
     WHEN total_sent_count < 10 AND (LOWER(email_type) = LOWER('Marketing Planned') OR LOWER(campaign_name) LIKE LOWER('%test%'
          ))
     THEN 1
     ELSE 0
     END AS dropflag
   FROM complete_funnel_1_day
   UNION ALL
   SELECT campaign_id_version,
    job_id,
    deploy_date,
    box_type,
    program_type,
    email_type,
    campaign_name,
    arrived_channel,
    campaign_category,
    measurement_days,
    impacted_flag,
    subject_line,
    total_sent_count,
    total_delivered_count,
    total_clicked_count,
    total_opened_count,
    total_bounced_count,
    total_unsubscribed_count,
    unique_clicked_count,
    unique_opened_count,
    unique_unsubscribed_count,
    gross_sales,
    net_sales,
    orders,
    units,
    sessions,
    session_orders,
    session_demand,
    bounced_sessions,
     CASE
     WHEN total_sent_count < 10 AND (LOWER(email_type) = LOWER('Marketing Planned') OR LOWER(campaign_name) LIKE LOWER('%test%'
          ))
     THEN 1
     ELSE 0
     END AS dropflag
   FROM complete_funnel_10_day
   UNION ALL
   SELECT campaign_id_version,
    job_id,
    deploy_date,
    box_type,
    program_type,
    email_type,
    campaign_name,
    arrived_channel,
    campaign_category,
    measurement_days,
    impacted_flag,
    subject_line,
    total_sent_count,
    total_delivered_count,
    total_clicked_count,
    total_opened_count,
    total_bounced_count,
    total_unsubscribed_count,
    unique_clicked_count,
    unique_opened_count,
    unique_unsubscribed_count,
    gross_sales,
    net_sales,
    orders,
    units,
    sessions,
    session_orders,
    session_demand,
    bounced_sessions,
     CASE
     WHEN total_sent_count < 10 AND (LOWER(email_type) = LOWER('Marketing Planned') OR LOWER(campaign_name) LIKE LOWER('%test%'
          ))
     THEN 1
     ELSE 0
     END AS dropflag
   FROM complete_funnel_30_day) AS a
WHERE dropflag = 0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_email_performance
WHERE deploy_date BETWEEN {{params.start_date}} AND {{params.end_date}};
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_email_performance
(SELECT campaign_id_version,
  CAST(job_id AS STRING) AS job_id,
  deploy_date,
  box_type,
  program_type,
  email_type,
  campaign_name,
  arrived_channel,
  campaign_category,
  measurement_days,
  impacted_flag,
  subject_line,
  total_sent_count,
  total_delivered_count,
  total_clicked_count,
  total_opened_count,
  total_bounced_count,
  total_unsubscribed_count,
  unique_clicked_count,
  unique_opened_count,
  unique_unsubscribed_count,
  gross_sales,
  net_sales,
  orders,
  units,
  sessions,
  session_orders,
  session_demand,
  bounced_sessions,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM email_campaign_deep_dive);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATISTICS  COLUMN (PARTITION), COLUMN (campaign_id_version, job_id, deploy_date, subject_line, measurement_days), COLUMN (deploy_date) on {{params.mta_t2_schema}}.mta_email_performance;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
