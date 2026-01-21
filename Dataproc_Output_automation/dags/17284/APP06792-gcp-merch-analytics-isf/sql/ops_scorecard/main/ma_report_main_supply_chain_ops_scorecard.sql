BEGIN;

CREATE TEMPORARY TABLE IF NOT EXISTS  weekly_ops_standup 
(
  ops_name STRING,
  banner STRING,
  channel STRING,
  metric_name STRING,
  fiscal_num INT64,
  fiscal_desc STRING,
  label STRING,
  rolling_fiscal_ind INT64,
  plan_op FLOAT64,
  plan_cp FLOAT64,
  ty FLOAT64,
  ly FLOAT64,
  day_date DATE
)
;

--  WK Identifier for automating weekly plots
CREATE TEMPORARY TABLE IF NOT EXISTS curr_wk 
AS
   SELECT
      day_cal_454_dim.week_idnt,
      day_cal_454_dim.week_start_day_date,
      day_cal_454_dim.week_end_day_date,
      day_cal_454_dim.month_idnt,
      day_cal_454_dim.month_start_day_date,
      day_cal_454_dim.month_end_day_date
   FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_cal_454_dim.day_date = date_sub({{params.start_date}}, interval 7 DAY);


CREATE TEMPORARY TABLE IF NOT EXISTS  cal_lkup
AS 
   SELECT DISTINCT
      a.week_num,
      a.week_of_fyr,
      a.week_454_num,
      a.month_short_desc,
      b.week_start_day_date,
      b.week_end_day_date,
      a.month_num,
      b.month_start_day_date,
      b.month_end_day_date,
      a.year_num
   FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS a
      LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b 
      ON a.week_num = b.week_idnt
         AND a.month_num = b.month_idnt
   WHERE b.day_date BETWEEN date_sub({{params.start_date}}, interval 42 DAY) AND date_add({{params.start_date}}, interval 21 DAY);


CREATE TEMPORARY TABLE IF NOT EXISTS dc_inbound
AS 
   SELECT
      week_num,
      month_short_desc,
      week_454_num,
      CASE
         WHEN week_start_day_date <= date_sub({{params.start_date}}, interval 7 DAY) THEN 1
         ELSE 0
      END AS rolling_fiscal_ind,
      b.days_of_backlog,
      b.actual_backlog
   FROM
      (
         SELECT
            a.date,
            CAST(avg(a.days_of_backlog) as FLOAT64) AS days_of_backlog,
            sum(a.actual_backlog) AS actual_backlog
         FROM
            (
               SELECT
                  dc_inbound_daily_projections_fct_vw.date,
                  ss_ops_site_dc_only_locations,
                  sum(actual_backlog) AS actual_backlog,
                  (sum(days_to_process_numerator) / sum(actual_backlog)) AS days_of_backlog -- To find sum of values at site and date level
               FROM                  
                  `{{params.gcp_project_id}}`.t2dl_sca_vws.dc_inbound_daily_projections_fct_vw
               WHERE lower(rtrim(dow, ' ')) = lower('SAT')
                  AND lower(ss_ops_site_dc_only_locations) LIKE lower('%DC%')
                  AND actual_backlog <> 0
                  AND dc_inbound_daily_projections_fct_vw.date BETWEEN date_sub({{params.start_date}}, interval 50 DAY) AND date_add({{params.start_date}}, interval 50 DAY)
               GROUP BY 1, 2
            ) AS a
         GROUP BY 1
      ) AS b
      INNER JOIN cal_lkup AS c 
      ON b.date = c.week_end_day_date
   WHERE week_start_day_date BETWEEN (
      SELECT
         week_start_day_date - 14
         FROM
         curr_wk
   ) AND (
      SELECT
         week_start_day_date + 27
         FROM
         curr_wk
   );

INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'DC Inbound Days of Backlog (DAYS)' AS metric_name,
      dc_inbound.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(dc_inbound.month_short_desc, ' ', 'WK ', CAST(dc_inbound.week_454_num as STRING)) AS label,
      dc_inbound.rolling_fiscal_ind,
      3.5 AS plan_op,
      NULL AS plan_cp,
      dc_inbound.days_of_backlog AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      dc_inbound;

INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'DC Inbound Days of Backlog (UNITS)' AS metric_name,
      dc_inbound.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(dc_inbound.month_short_desc, ' ', 'WK ', CAST(dc_inbound.week_454_num as STRING)) AS label,
      dc_inbound.rolling_fiscal_ind,
      NULL AS plan_op,
      NULL AS plan_cp,
      dc_inbound.actual_backlog AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      dc_inbound;

CREATE TEMPORARY TABLE IF NOT EXISTS fc_inbound
AS
   SELECT
      week_num,
      month_short_desc,
      week_454_num,
      CASE
         WHEN week_start_day_date <= date_sub({{params.start_date}}, interval 7 DAY) THEN 1
         ELSE 0
      END AS rolling_fiscal_ind,
      b.days_of_backlog,
      b.actual_backlog
      FROM
      (
         SELECT
            a.date,
            CAST(avg(a.days_of_backlog) as FLOAT64) AS days_of_backlog,
            sum(a.actual_backlog) AS actual_backlog
            -- To find average of (days of backlog) at site level
         FROM            
            (
               SELECT
                  daily_inbound_fct_vw.date,
                  daily_inbound_fct_vw.ss_ops_site_outbound_ops,
                  sum(daily_inbound_fct_vw.actual_backlog) AS actual_backlog,
                  (sum(daily_inbound_fct_vw.days_to_process_numerator) / sum(daily_inbound_fct_vw.actual_backlog)) AS days_of_backlog
                  -- To find sum of values at site and date level
               FROM
                  `{{params.gcp_project_id}}`.t2dl_sca_vws.daily_inbound_fct_vw
               WHERE lower(rtrim(daily_inbound_fct_vw.dow, ' ')) = lower('SAT')
                  AND LOWER(rtrim(daily_inbound_fct_vw.ss_business_unit_21_day_labor, ' ')) <> 'TC'
                  AND daily_inbound_fct_vw.actual_backlog <> 0
                  AND daily_inbound_fct_vw.date  BETWEEN date_sub({{params.start_date}}, interval 50 DAY) AND date_add({{params.start_date}}, interval 50 DAY)
               GROUP BY 1, 2
            ) AS a
         GROUP BY 1
      ) AS b
      INNER JOIN cal_lkup AS c ON b.date = c.week_end_day_date
      WHERE week_start_day_date BETWEEN (
         SELECT
            week_start_day_date - 14
         FROM
            curr_wk
      ) AND (
         SELECT
            week_start_day_date + 27
         FROM
            curr_wk
      );

INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'FC Inbound Days of Backlog (DAYS)' AS metric_name,
      fc_inbound.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(fc_inbound.month_short_desc, ' ', 'WK ', CAST(fc_inbound.week_454_num as STRING)) AS label,
      fc_inbound.rolling_fiscal_ind,
      cast(3.5 as NUMERIC) AS plan_op,
      NULL AS plan_cp,
      fc_inbound.days_of_backlog AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      fc_inbound;


INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'FC Inbound Days of Backlog (UNITS)' AS metric_name,
      fc_inbound.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(fc_inbound.month_short_desc, ' ', 'WK ', CAST(fc_inbound.week_454_num as STRING)) AS label,
      fc_inbound.rolling_fiscal_ind,
      NULL AS plan_op,
      NULL AS plan_cp,
      fc_inbound.actual_backlog AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      fc_inbound;


CREATE TEMPORARY TABLE IF NOT EXISTS fc_outbound
AS
   SELECT
      week_num,
      month_short_desc,
      week_454_num,
      CASE
         WHEN week_start_day_date <= date_sub({{params.start_date}}, interval 7 DAY) THEN 1
         ELSE 0
      END AS rolling_fiscal_ind,
      b.days_of_backlog,
      b.actual_backlog
   FROM
      (
         SELECT
            a.date,
            CAST(avg(a.days_of_backlog) as FLOAT64) AS days_of_backlog,
            sum(a.actual_backlog) AS actual_backlog
            -- To find average of (days of backlog) at site level
         FROM            
            (
               SELECT
                  daily_outbound_fct_vw.date,
                  daily_outbound_fct_vw.ss_ops_site_outbound_ops,
                  sum(daily_outbound_fct_vw.actual_backlog) AS actual_backlog,
                  (sum(daily_outbound_fct_vw.days_to_process_numerator) / sum(daily_outbound_fct_vw.actual_backlog)) AS days_of_backlog
                  -- To find sum of values at site and date level
               FROM                
                  `{{params.gcp_project_id}}`.t2dl_sca_vws.daily_outbound_fct_vw
               WHERE lower(rtrim(daily_outbound_fct_vw.dow, ' ')) = lower('SAT')
                  AND daily_outbound_fct_vw.actual_backlog <> 0
                  AND daily_outbound_fct_vw.date BETWEEN date_sub({{params.start_date}}, interval 50 DAY) AND date_add({{params.start_date}}, interval 50 DAY)
               GROUP BY 1, 2
            ) AS a
         GROUP BY 1
      ) AS b
      INNER JOIN cal_lkup AS c ON b.date = c.week_end_day_date
   WHERE week_start_day_date BETWEEN (
         SELECT
            week_start_day_date - 14
            FROM
            curr_wk
      ) AND (
         SELECT
            week_start_day_date + 27
            FROM
            curr_wk
      );


INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'FC Outbound Days of Backlog (DAYS)' AS metric_name,
      fc_outbound.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(fc_outbound.month_short_desc, ' ', 'WK ', CAST(fc_outbound.week_454_num as STRING)) AS label,
      fc_outbound.rolling_fiscal_ind,
      2 AS plan_op,
      NULL AS plan_cp,
      fc_outbound.days_of_backlog AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      fc_outbound;

INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'FC Outbound Days of Backlog (UNITS)' AS metric_name,
      fc_outbound.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(fc_outbound.month_short_desc, ' ', 'WK ', CAST(fc_outbound.week_454_num as STRING)) AS label,
      fc_outbound.rolling_fiscal_ind,
      NULL AS plan_op,
      NULL AS plan_cp,
      fc_outbound.actual_backlog AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      fc_outbound
;


--  Store Returns Inspect Days of Backlog
CREATE TEMPORARY TABLE IF NOT EXISTS store_returns
AS
   SELECT
      week_num,
      month_short_desc,
      week_454_num,
      CASE
         WHEN week_start_day_date <= date_sub({{params.start_date}}, interval 7 DAY) THEN 1
         ELSE 0
      END AS rolling_fiscal_ind,
      b.days_of_backlog,
      b.actual_backlog
   FROM
      (
         SELECT
            a.date,
            CAST(avg(a.days_of_backlog) as FLOAT64) AS days_of_backlog,
            sum(a.actual_backlog) AS actual_backlog
            -- To find average of (days of backlog) at site level
         FROM
            (
               SELECT
                  daily_returns_inspections_fct_vw.date,
                  daily_returns_inspections_fct_vw.ss_ops_site_returns_inspect_ops,
                  sum(daily_returns_inspections_fct_vw.actual_backlog) AS actual_backlog,
                  (sum(daily_returns_inspections_fct_vw.days_to_process_numerator) / sum(daily_returns_inspections_fct_vw.actual_backlog)) AS days_of_backlog
                  -- To find sum of values at site and date level
               FROM
                  `{{params.gcp_project_id}}`.t2dl_sca_vws.daily_returns_inspections_fct_vw
               WHERE lower(rtrim(daily_returns_inspections_fct_vw.dow, ' ')) = lower('SAT')
                  AND daily_returns_inspections_fct_vw.actual_backlog <> 0
                  AND lower(rtrim(daily_returns_inspections_fct_vw.return_locations, ' ')) <> lower('MAIL')
                  AND daily_returns_inspections_fct_vw.date  BETWEEN date_sub({{params.start_date}}, interval 50 DAY) AND date_add({{params.start_date}}, interval 50 DAY)
               GROUP BY 1, 2
            ) AS a
         GROUP BY 1
      ) AS b
      INNER JOIN cal_lkup AS c 
      ON b.date = c.week_end_day_date
   WHERE week_start_day_date BETWEEN (
      SELECT
         week_start_day_date - 14
         FROM
         curr_wk
   ) AND (
      SELECT
         week_start_day_date + 27
         FROM
         curr_wk
   );

INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'Store Returns Inspect Days of Backlog (DAYS)' AS metric_name,
      store_returns.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(store_returns.month_short_desc, ' ', 'WK ', CAST(store_returns.week_454_num as STRING)) AS label,
      store_returns.rolling_fiscal_ind,
      CAST(3.5 as NUMERIC) AS plan_op,
      NULL AS plan_cp,
      store_returns.days_of_backlog AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      store_returns;

INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'Store Returns Inspect Days of Backlog (UNITS)' AS metric_name,
      store_returns.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(store_returns.month_short_desc, ' ', 'WK ', CAST(store_returns.week_454_num as STRING)) AS label,
      store_returns.rolling_fiscal_ind,
      NULL AS plan_op,
      NULL AS plan_cp,
      store_returns.actual_backlog AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      store_returns;

CREATE TEMPORARY TABLE IF NOT EXISTS t1
AS
   SELECT
      a.tracking_num,
      a.rma_id,
      a.carrier_code,
      a.shipment_id,
      min(a.activity_tmstp) AS usps_fedex_scan_min_activity
   FROM
      (
         SELECT
            tracking_num,
            rma_id,
            carrier_code,
            shipment_id,
            coalesce(activity_tmstp_pacific, activity_tmstp_local) AS activity_tmstp,
            CASE
               WHEN LOWER(rtrim(carrier_code, ' ')) = LOWER('FDEG')
               AND LOWER(rtrim(shipment_status, ' ')) = LOWER('IN_TRANSIT')
               AND LOWER(shipment_description) NOT LIKE LOWER('%LABEL%') THEN 'USPS_FEDEX_SCAN'
               WHEN LOWER(rtrim(carrier_code, ' ')) = LOWER('NSFV')
               AND LOWER(rtrim(shipment_status, ' ')) = LOWER('IN_TRANSIT')
               AND LOWER(shipment_description) NOT LIKE LOWER('%LABEL%') THEN 'USPS_FEDEX_SCAN'
               WHEN LOWER(rtrim(carrier_code, ' ')) = LOWER('FDEG')
               AND LOWER(rtrim(shipment_status, ' ')) = LOWER('OUT_FOR_DELIVERY') THEN 'USPS_FEDEX_SCAN'
               WHEN LOWER(shipment_description) LIKE LOWER('%NEWIGISTCS%') THEN 'PB_SCAN'
               WHEN LOWER(shipment_description) LIKE LOWER('%PITNEY BOWES%')THEN 'PB_SCAN'
               WHEN lower(rtrim(shipment_description, ' ')) IN(
               lower('Delivered to FDR facility'), lower('Container prepared for shipment')
               ) THEN 'PB_SCAN'
               WHEN LOWER(rtrim(shipment_status, ' ')) = 'DELIVERED' THEN 'DELIVERY_SCAN'
               WHEN LOWER(shipment_description) LIKE lower('%LABEL%')
               OR LOWER(shipment_description) LIKE lower('%PICK%') THEN 'NOT_USED_SCAN'
               ELSE 'USPS_FEDEX_SCAN'
            END AS scanned_status
         FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_shipment_tracking_event_fact AS fact
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc 
            ON CAST(fact.activity_tmstp as DATE) = dc.day_date
         WHERE lower(rtrim(carrier_code, ' ')) IN(lower('nfsv'), lower('fdeg'))
            AND LOWER(rtrim(mail_in_return_ind, ' ')) = lower('Y')
            AND LOWER(rtrim(event_type, ' ')) = lower('SHIPMENT_TRACKING')
            AND rma_id IS NOT NULL
            AND week_num IN( SELECT DISTINCT week_num FROM cal_lkup )
      ) AS a
      WHERE LOWER(rtrim(a.scanned_status, ' ')) = lower('USPS_FEDEX_SCAN')
       AND a.activity_tmstp IS NOT NULL
      GROUP BY 1, 2, 3, 4;


--  To get timestamps for when the return was first scanned and when the return invoice was created.
CREATE TEMPORARY TABLE IF NOT EXISTS   t2
AS
   SELECT
      rma_id,
      tracking_num,
      purchase_id,
      sku_id,
      line_item_id,
      max(shipment_id) AS shipment_id,
      max(return_invoiced_tmstp_pacific) AS return_invoiced_tmstp_pacific,
      max(shipment_first_scan_tmstp_pacific) AS shipment_first_scan_tmstp_pacific
   FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_item_return_fact AS fact
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc 
      ON CAST(fact.return_invoiced_tmstp_pacific as DATE) = dc.day_date
   WHERE rma_id IS NOT NULL
      AND return_invoiced_tmstp_pacific IS NOT NULL
      AND week_num IN( SELECT DISTINCT week_num FROM cal_lkup )
   GROUP BY 1, 2, 3, 4, 5;


CREATE TEMPORARY TABLE IF NOT EXISTS t3
AS
   SELECT DISTINCT
      r.rma_id,
      r.tracking_num,
      carrier_code,
      return_invoiced_tmstp_pacific,
      coalesce(r.shipment_first_scan_tmstp_pacific, cte.usps_fedex_scan_min_activity) AS usps_fedex_first_scan_mstp_pacific,
      CAST(return_invoiced_tmstp_pacific as DATE) AS date,
      week_idnt
   FROM
      t2 AS r
      LEFT OUTER JOIN t1 AS cte 
      ON r.tracking_num = cte.tracking_num
      AND r.rma_id = cte.rma_id
      AND r.shipment_id = cte.shipment_id
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS q 
      ON CAST(return_invoiced_tmstp_pacific as DATE) = q.day_date
   WHERE week_start_day_date BETWEEN (
      SELECT week_start_day_date - 35 FROM curr_wk
   ) AND (
      SELECT week_start_day_date FROM curr_wk
   );

CREATE TEMPORARY TABLE IF NOT EXISTS  mail_returns
AS
	SELECT
			a.week_idnt,
         ROUND(NUMERIC '1.00' * CAST(CAST(1000 as NUMERIC) * approx_quantiles(a.mail_returns_days, 100)[OFFSET(50)] as INT64) / 1000, 4, 'ROUND_HALF_EVEN') AS p50,
			ROUND(NUMERIC '1.00' * CAST(CAST(1000 as NUMERIC) * approx_quantiles(a.mail_returns_days, 100)[OFFSET(90)] as INT64) / 1000, 4, 'ROUND_HALF_EVEN') AS p90
		FROM
			(
				SELECT
						a_0.date,
						a_0.rma_id,
						a_0.tracking_num,
						a_0.week_idnt,
                  round(a_0.mail_returns_seconds / 86400, 2) AS mail_returns_days
					FROM
						(
							SELECT
									t3.date,
									t3.week_idnt,
									t3.rma_id,
									t3.tracking_num,
									max(timestamp_diff(cast(t3.return_invoiced_tmstp_pacific as timestamp), cast(t3.usps_fedex_first_scan_mstp_pacific as timestamp), DAY) * 86400 + (extract(HOUR from CAST(t3.return_invoiced_tmstp_pacific as TIMESTAMP)) - extract(HOUR from CAST(t3.usps_fedex_first_scan_mstp_pacific as TIMESTAMP))) * 3600 + (extract(MINUTE from CAST(t3.return_invoiced_tmstp_pacific as TIMESTAMP)) - extract(MINUTE from CAST(t3.usps_fedex_first_scan_mstp_pacific as TIMESTAMP))) * 60 + (extract(SECOND from CAST(t3.return_invoiced_tmstp_pacific as TIMESTAMP)) - extract(SECOND from CAST(t3.usps_fedex_first_scan_mstp_pacific as TIMESTAMP)))) AS mail_returns_seconds
								FROM
									t3
								GROUP BY 1, 2, 3, 4
						) AS a_0
			) AS a
		GROUP BY 1;

INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'Mail Returns Drop-Off to Credit (P50)' AS metric_name,
      x.week_idnt AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST(week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      CAST(7.5 AS NUMERIC ) AS plan_op,
      NULL AS plan_cp,
      p50 AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      mail_returns AS x
   INNER JOIN cal_lkup AS y 
   ON x.week_idnt = y.week_num;


INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'Mail Returns Drop-Off to Credit (P90)' AS metric_name,
      x.week_idnt AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST(week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      CAST(11.5 AS NUMERIC ) AS plan_op,
      NULL AS plan_cp,
      p90 AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      mail_returns AS x
   INNER JOIN cal_lkup AS y 
   ON x.week_idnt = y.week_num;

CREATE TEMPORARY TABLE IF NOT EXISTS store_ontime
AS
   SELECT
      week_idnt AS week_num,
      (sum(a.on_time + a.ontime + a.thresh) * 100.0 / (sum(a.on_time + a.ontime + a.thresh + a.critical + a.critical_late + a.late) * 1.0)) AS ontime_delivery
      FROM
      (
         SELECT
            storedeliveryoperationaldata_vw.appt_date,
            count(CASE
               WHEN LOWER(rtrim(storedeliveryoperationaldata_vw.delivery_status, ' ')) = LOWER('ON-TIME') THEN 1
            END) AS on_time,
            count(CASE
               WHEN LOWER(rtrim(storedeliveryoperationaldata_vw.delivery_status, ' ')) = LOWER('ON TIME') THEN 1
            END) AS ontime,
            count(CASE
               WHEN LOWER(rtrim(storedeliveryoperationaldata_vw.delivery_status, ' ')) = LOWER('THRESHOLD') THEN 1
            END) AS thresh,
            count(CASE
               WHEN LOWER(rtrim(storedeliveryoperationaldata_vw.delivery_status, ' ')) = LOWER('CRITICAL LATE') THEN 1
            END) AS critical,
            count(CASE
               WHEN LOWER(rtrim(storedeliveryoperationaldata_vw.delivery_status, ' ')) = LOWER('CRITICALLATE') THEN 1
            END) AS critical_late,
            count(CASE
               WHEN LOWER(rtrim(storedeliveryoperationaldata_vw.delivery_status, ' ')) = lower('LATE') THEN 1
            END) AS late
            FROM
            `{{params.gcp_project_id}}`.t3dl_ctran_scm.storedeliveryoperationaldata_vw
            WHERE storedeliveryoperationaldata_vw.delivery_status IS NOT NULL
            AND storedeliveryoperationaldata_vw.order_number IS NOT NULL
            GROUP BY 1
      ) AS a
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b 
      ON a.appt_date = b.day_date
      WHERE week_start_day_date BETWEEN (
      SELECT
            week_start_day_date - 35
         FROM
            curr_wk
      ) AND (
      SELECT
            week_start_day_date
         FROM
            curr_wk
      )
      GROUP BY 1;

INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'Store On Time Delivery' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST(week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      95 AS plan_op,
      NULL AS plan_cp,
      ontime_delivery AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      store_ontime AS x
   INNER JOIN cal_lkup AS y 
      ON x.week_num = y.week_num;


--  FC On Time Delivery to Promise
CREATE TEMPORARY TABLE IF NOT EXISTS fc_ontime
AS
   SELECT
      week_idnt AS week_num,
      CAST(sum(b.y_pkg) as FLOAT64) AS y_pkg,
      CAST(sum(b.total_pkg) as FLOAT64) AS total_pkg
   FROM
      (
         SELECT DISTINCT
            a.delivery_date,
            sum(CASE
               WHEN LOWER(rtrim(a.on_tm_fst_dlvry_atmpt, ' ')) = LOWER('Y' )THEN a.pkg
               ELSE 0
            END) AS y_pkg,
            sum(a.pkg) AS total_pkg
         FROM
            (
               SELECT
                  CAST(cssf.carrier_first_attempted_delivery_tmstp_pacific as DATE) AS delivery_date,
                  count(cssf.tracking_num) AS pkg,
                  CAST(cssf.requested_max_promise_tmstp_pacific as DATE) AS promise_date,
                  CASE
                     WHEN CAST(cssf.carrier_first_attempted_delivery_tmstp_pacific as DATE) > CAST(cssf.requested_max_promise_tmstp_pacific as DATE) THEN 'N'
                     WHEN CAST(cssf.carrier_first_attempted_delivery_tmstp_pacific as DATE) <= CAST(cssf.requested_max_promise_tmstp_pacific as DATE) THEN 'Y'
                     ELSE NULL
                  END AS on_tm_fst_dlvry_atmpt
               FROM
                  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_shipment_summary_fact AS cssf
                  INNER JOIN (
                     SELECT DISTINCT
                        oldf.carrier_tracking_num,
                        max(coalesce(oldf.carrier_first_attempted_delivery_tmstp_pacific, oldf.carrier_first_attempted_delivery_tmstp_local)) AS del_dt,
                        oldf.carrier_tracking_num AS trck_no,
                        promise_type_code,
                        oldf.shipped_date_pacific,
                        rqlos.ord_los_med_desc
                     FROM
                        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf
                     LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_osu.ord_los_dim AS rqlos 
                     ON oldf.requested_level_of_service_code = rqlos.ord_los_cd
                     WHERE 1 = 1
                        AND LOWER(rtrim(shipped_node_type_code, ' ')) = LOWER('FC')
                        AND rtrim(rqlos.ord_los_med_desc, ' ') IN(
                        'Ground Service', 'Standard Shipping', 'Standard Sign Reqd', 'Cedar Pick Up', 'Saturday', 'Saturday Sign Reqd'
                     )
                        AND oldf.shipped_date_pacific >= (
                        SELECT
                           week_start_day_date - 50
                           FROM
                           curr_wk
                     )
                     GROUP BY 1, 3, 4, 5, 6
                  ) AS ca 
                  ON cssf.tracking_num = ca.trck_no
                  AND  CAST(cssf.carrier_first_attempted_delivery_tmstp_pacific as DATE) = ca.del_dt
               GROUP BY 1, 3, 4
            ) AS a
         GROUP BY 1
      ) AS b
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c 
      ON b.delivery_date = c.day_date
   WHERE week_start_day_date BETWEEN (
      SELECT
         week_start_day_date - 35
         FROM
         curr_wk
   ) AND (
      SELECT
         week_start_day_date
         FROM
         curr_wk
   )
   GROUP BY 1;

INSERT INTO weekly_ops_standup 
   SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'FC On Time Delivery to Promise' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST(week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      97 AS plan_op,
      NULL AS plan_cp,
      CASE
        WHEN total_pkg = 0 THEN 0
        ELSE y_pkg * 100 / total_pkg
      END AS ty,
      NULL AS ly,
      NULL AS day_date
   FROM
      fc_ontime AS x
   INNER JOIN cal_lkup AS y 
   ON x.week_num = y.week_num;

--  Click to Delivery
CREATE TEMPORARY TABLE IF NOT EXISTS click_to_delivery
AS
   SELECT
      week_idnt AS week_num,
      max(a.channel) AS channel,
      max(a.node_type) AS node_type,
      CAST(avg(a.p50) as FLOAT64) AS p50,
      CAST(avg(a.p90) as FLOAT64) AS p90
   FROM
      (
         SELECT
            a_0.delivery_date,
            max(a_0.channel) AS channel,
            max(a_0.node_type) AS node_type,
            ROUND(NUMERIC '1.00' * CAST(CAST(1000 as NUMERIC) * approx_quantiles(a_0.click_to_delivery, 100)[OFFSET(50)] as INT64) / 1000, 4, 'ROUND_HALF_EVEN') AS p50,
            ROUND(NUMERIC '1.00' * CAST(CAST(1000 as NUMERIC) * approx_quantiles(a_0.click_to_delivery, 100)[OFFSET(90)] as INT64) / 1000, 4, 'ROUND_HALF_EVEN') AS p90
         FROM
            (
               SELECT
                  a_1.delivery_date,
                  a_1.channel,
                  a_1.node_type,
                  a_1.order_num,
                  a_1.carrier_tracking_num,
                  a_1.order_date_pacific,
                  CASE
                     WHEN a_1.click_to_delivery_seconds IS NULL THEN NULL
                     WHEN a_1.click_to_delivery_seconds / 86400 > 20 THEN 20
                     WHEN a_1.click_to_delivery_seconds / 86400 <= 0 THEN NULL
                     ELSE round(a_1.click_to_delivery_seconds / 86400, 2)
                  END AS click_to_delivery
               FROM
                  (
                     SELECT
                        order_line_detail_fact.order_num,
                        order_line_detail_fact.carrier_tracking_num,
                        order_line_detail_fact.order_date_pacific,
                        max(CAST(order_line_detail_fact.carrier_first_attempted_delivery_tmstp_pacific as DATE)) AS delivery_date,
                        max(CASE
                           WHEN LOWER(rtrim(order_line_detail_fact.source_channel_code, ' ')) = LOWER('FULL_LINE') THEN 'N.COM'
                           WHEN LOWER(rtrim(order_line_detail_fact.source_channel_code, ' ')) = LOWER('RACK') THEN 'R.COM'
                        END) AS channel,
                        max(CASE
                           WHEN LOWER(rtrim(order_line_detail_fact.last_released_node_type_code, ' ')) = LOWER('DS') THEN 'DROPSHIP'
                           ELSE 'OWNED'
                        END) AS node_type,
                        max(timestamp_diff(cast(order_line_detail_fact.carrier_first_attempted_delivery_tmstp_pacific as timestamp), current_timestamp(), DAY) * 86400 
                        + (extract(HOUR from CAST(order_line_detail_fact.carrier_first_attempted_delivery_tmstp_pacific as TIMESTAMP)) - extract(HOUR from current_timestamp())) * 3600 
                        + (extract(MINUTE from CAST(order_line_detail_fact.carrier_first_attempted_delivery_tmstp_pacific as TIMESTAMP)) - extract(MINUTE from current_timestamp())) * 60 
                        + (extract(SECOND from CAST(order_line_detail_fact.carrier_first_attempted_delivery_tmstp_pacific as TIMESTAMP)) - extract(SECOND from current_timestamp()))) AS click_to_delivery_seconds
                        FROM
                          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
                        WHERE cast(order_line_detail_fact.carrier_first_attempted_delivery_tmstp_pacific as timestamp) > cast(order_line_detail_fact.shipped_tmstp_pacific as timestamp)
                         AND cast(order_line_detail_fact.shipped_tmstp_pacific as timestamp) > current_timestamp()
                         AND CAST( order_line_detail_fact.carrier_first_attempted_delivery_tmstp_pacific as DATE) > date_sub({{params.start_date}}, interval 52 DAY)
                         AND CAST( order_line_detail_fact.carrier_first_attempted_delivery_tmstp_pacific as DATE) < date_add({{params.start_date}}, interval 1 DAY)
                        GROUP BY 1, 2, 3, LOWER((CASE
                          WHEN upper(rtrim(order_line_detail_fact.source_channel_code, ' ')) = LOWER('FULL_LINE') THEN 'N.COM'
                          WHEN upper(rtrim(order_line_detail_fact.source_channel_code, ' ')) = LOWER('RACK') THEN 'R.COM'
                        END)), LOWER((CASE
                          WHEN lower(rtrim(order_line_detail_fact.last_released_node_type_code, ' ')) = LOWER('DS') THEN 'DROPSHIP'
                          ELSE 'OWNED'
                        END))
                    ) AS a_1
              ) AS a_0
            GROUP BY 1, upper(a_0.channel), upper(a_0.node_type)
      ) AS a
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b 
      ON a.delivery_date = b.day_date
   WHERE week_start_day_date BETWEEN (
      SELECT
         week_start_day_date - 35
         FROM
         curr_wk
   ) AND (
      SELECT
         week_start_day_date
         FROM
         curr_wk
   )
   GROUP BY 1, LOWER(a.channel), LOWER(a.node_type);

INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      CASE
        WHEN LOWER(rtrim(channel, ' ')) = LOWER('N.COM') THEN 'NORDSTROM'
        ELSE 'NORDSTROM RACK'
      END AS banner,
      channel AS channel,
      'Click to Delivery DROPSHIP (P50)' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      CAST(5.2 as numeric) AS plan_op,
      NULL AS plan_cp,
      p50 AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      click_to_delivery AS x
      INNER JOIN cal_lkup AS y 
      ON x.week_num = y.week_num
    WHERE LOWER(rtrim(node_type, ' ')) = LOWER('DROPSHIP');

INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      CASE
        WHEN LOWER(rtrim(channel, ' ')) = 'N.COM' THEN 'NORDSTROM'
        ELSE 'NORDSTROM RACK'
      END AS banner,
      channel AS channel,
      'Click to Delivery DROPSHIP (P90)' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      8 AS plan_op,
      NULL AS plan_cp,
      p90 AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      click_to_delivery AS x
      INNER JOIN cal_lkup AS y ON x.week_num = y.week_num
    WHERE LOWER(rtrim(node_type, ' ')) = 'DROPSHIP'
;


INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      CASE
        WHEN LOWER(rtrim(channel, ' ')) = 'N.COM' THEN 'NORDSTROM'
        ELSE 'NORDSTROM RACK'
      END AS banner,
      channel AS channel,
      'Click to Delivery OWNED (P50)' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      CAST(5.2 AS NUMERIC) AS plan_op,
      NULL AS plan_cp,
      p50 AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      click_to_delivery AS x
      INNER JOIN cal_lkup AS y ON x.week_num = y.week_num
    WHERE LOWER(rtrim(node_type, ' ')) = LOWER('OWNED')
;


INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      CASE
        WHEN LOWER(rtrim(channel, ' ')) = 'N.COM' THEN 'NORDSTROM'
        ELSE 'NORDSTROM RACK'
      END AS banner,
      channel AS channel,
      'Click to Delivery OWNED (P90)' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      8 AS plan_op,
      NULL AS plan_cp,
      p90 AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      click_to_delivery AS x
      INNER JOIN cal_lkup AS y ON x.week_num = y.week_num
    WHERE LOWER(rtrim(node_type, ' ')) = 'OWNED'
;


--  PPH
INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'PPH' AS metric_name,
      b.week_num AS fiscal_num,
      'Week' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      15 AS plan_op,
      NULL AS plan_cp,
      a.actual_pph AS ty,
      a.wig_pph AS ly,
      NULL AS day_date
    FROM
      (
        SELECT
            a_0.fiscalyear,
            a_0.fiscalweek,
            CASE
              WHEN a_0.actual_hours = 0 THEN 0
              ELSE div(a_0.actual_units, a_0.actual_hours)
            END AS actual_pph,
            CASE
              WHEN a_0.wig_hours = 0 THEN 0
              ELSE a_0.wig_units / a_0.wig_hours
            END AS wig_pph
          FROM
            -- Wildly Important Goal (LY)
            (
              SELECT
                  pph_wbr_fct_vw.fiscalyear,
                  pph_wbr_fct_vw.fiscalweek,
                  sum(CASE
                    WHEN LOWER(rtrim(pph_wbr_fct_vw.scenario, ' ')) = LOWER('ACTUAL') THEN pph_wbr_fct_vw.units
                    ELSE 0
                  END) AS actual_units,
                  sum(CASE
                    WHEN LOWER(rtrim(pph_wbr_fct_vw.scenario, ' ')) = LOWER('ACTUAL') THEN pph_wbr_fct_vw.totalhours
                    ELSE 0
                  END) AS actual_hours,
                  sum(CASE
                    WHEN LOWER(rtrim(pph_wbr_fct_vw.scenario, ' ')) = LOWER('WIG BASELINE') THEN pph_wbr_fct_vw.units
                    ELSE 0
                  END) AS wig_units,
                  sum(CASE
                    WHEN LOWER(rtrim(pph_wbr_fct_vw.scenario, ' ')) = LOWER('WIG BASELINE') THEN pph_wbr_fct_vw.hours
                    ELSE 0
                  END) AS wig_hours
                FROM
                  `{{params.gcp_project_id}}`.t2dl_sca_vws.pph_wbr_fct_vw
                WHERE rtrim(pph_wbr_fct_vw.building, ' ') NOT IN(
                  'NQC', '0', 'LA LOH'
                )
                 AND rtrim(pph_wbr_fct_vw.department, ' ') NOT IN(
                  'Customer Returns', 'Rack Warehouse', 'Returns Inpection', 'Shipping Dock'
                )
                 AND pph_wbr_fct_vw.date  BETWEEN date_sub({{params.start_date}}, interval 45 DAY) AND {{params.start_date}}
                GROUP BY 1, 2
            ) AS a_0
      ) AS a
      INNER JOIN -- Inpection not a typo
      cal_lkup AS b 
      ON a.fiscalyear = b.year_num
      AND a.fiscalweek = b.week_of_fyr
      WHERE week_start_day_date BETWEEN (
         SELECT
            week_start_day_date - 35
         FROM
             curr_wk
      ) AND (      SELECT
          week_start_day_date
        FROM
          curr_wk
    )
;



--  DC Forecast Accuracy
CREATE TEMPORARY TABLE IF NOT EXISTS dc_forecast
AS
   SELECT
      a.week_num,
      sum(a.incoming_actual) AS incoming_actual,
      sum(a.incoming_projected) AS incoming_projected,
      (avg(error) * 100.00) AS mape
   FROM
      (
         SELECT
            week_idnt AS week_num,
            a_0.ss_ops_site_dc_only_locations,
            sum(a_0.incoming_actual) AS incoming_actual,
            sum(a_0.incoming_projected) AS incoming_projected,
            abs(CASE
               WHEN sum(a_0.incoming_projected) = 0 THEN 0
               ELSE (sum(a_0.incoming_actual) - sum(a_0.incoming_projected)) / sum(a_0.incoming_projected)
            END) AS error
         FROM
            (
               SELECT
                  dc_inbound_daily_projections_fct_vw.date,
                  dc_inbound_daily_projections_fct_vw.ss_ops_site_dc_only_locations,
                  sum(dc_inbound_daily_projections_fct_vw.incoming_actual) AS incoming_actual,
                  sum(dc_inbound_daily_projections_fct_vw.incoming_projected) AS incoming_projected
               FROM
                  `{{params.gcp_project_id}}`.t2dl_sca_vws.dc_inbound_daily_projections_fct_vw
               WHERE dc_inbound_daily_projections_fct_vw.date BETWEEN date_sub({{params.start_date}}, interval 50 DAY) AND {{params.start_date}}
                  AND LOWER(dc_inbound_daily_projections_fct_vw.ss_ops_site_dc_only_locations) LIKE LOWER('DC%')
               GROUP BY 1, 2
            ) AS a_0
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b 
            ON a_0.date = b.day_date
         WHERE week_start_day_date BETWEEN (
            SELECT
               week_start_day_date - 35
               FROM
               curr_wk
         ) AND (
            SELECT
               week_start_day_date
               FROM
               curr_wk
         )
         GROUP BY 1, 2
      ) AS a
   GROUP BY 1
;


INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'DC Forecast Accuracy' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      incoming_projected AS plan_op,
      NULL AS plan_cp,
      incoming_actual AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      dc_forecast AS x
      INNER JOIN cal_lkup AS y ON x.week_num = y.week_num
;


INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'DC Forecast Accuracy (MAPE)' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      NULL AS plan_op,
      15 AS plan_cp,
      mape AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      dc_forecast AS x
      INNER JOIN cal_lkup AS y ON x.week_num = y.week_num
;


--  P&H Inventory Speed
CREATE TEMPORARY TABLE IF NOT EXISTS   inv_speed
AS
   SELECT
      a.weeknumreceipt AS week_num,
      ROUND(cast(1.00 * CAST(CAST(1000 as NUMERIC) * approx_quantiles(a.datediff, 100)[OFFSET(50)] as INT64) / 1000 as NUMERIC), 4, 'ROUND_HALF_EVEN') AS p50,
      ROUND(cast(1.00 * CAST(CAST(1000 as NUMERIC) * approx_quantiles(a.datediff, 100)[OFFSET(90)] as INT64) / 1000 as NUMERIC), 4, 'ROUND_HALF_EVEN') AS p90
   FROM
      (
         SELECT
            a_0.operation_number,
            a_0.shipment_number,
            a_0.item_product_id,
            --CAST(create_time AS DATE AT SOURCE TIME ZONE) AS create_dt, 
            CAST( a_0.create_time as DATE) AS create_dt,
            CAST( a_0.receipt_date_utc as DATE) AS receipt_dt,
            a_0.weeknumreceipt,
            date_diff(CAST( a_0.receipt_date_utc as DATE), CAST( a_0.create_time as DATE), DAY) AS datediff
         FROM
            (
               SELECT
                  rcisf.operation_number,
                  rcisf.shipment_number,
                  rcisf.item_product_id,
                  p.create_time,
                  rcisf.receipt_date_utc,
                  rd.week_num AS weeknumreceipt
               FROM
                  (
                     SELECT DISTINCT
                        rms_cost_internal_shipment_fact.operation_number,
                        rms_cost_internal_shipment_fact.operation_type,
                        rms_cost_internal_shipment_fact.shipment_number,
                        rms_cost_internal_shipment_fact.item_product_id,
                        rms_cost_internal_shipment_fact.from_location_logical,
                        rms_cost_internal_shipment_fact.from_location_facility,
                        rms_cost_internal_shipment_fact.to_location_facility,
                        rms_cost_internal_shipment_fact.shipment_date_utc,
                        rms_cost_internal_shipment_fact.shipment_quantity,
                        rms_cost_internal_shipment_fact.receipt_date_utc,
                        rms_cost_internal_shipment_fact.receipt_quantity
                     FROM
                        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rms_cost_internal_shipment_fact
                     WHERE (rms_cost_internal_shipment_fact.from_location_logical = 697
                        OR rms_cost_internal_shipment_fact.from_location_logical = 297)
                        AND rms_cost_internal_shipment_fact.receipt_date_utc >= DATE('2023-05-01')
                        AND LOWER(rtrim(rms_cost_internal_shipment_fact.operation_type, ' ')) = LOWER('TRANSFER')
                  ) AS rcisf
                  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rms_cost_transfers_fact AS p 
                  ON rcisf.operation_number = p.transfer_num
                  AND p.rms_sku_num = rcisf.item_product_id
                  AND p.from_location_logical = rcisf.from_location_logical
                  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS t 
                  ON rcisf.to_location_facility = cast( t.store_num as string)
                  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS rd 
                  ON rcisf.receipt_date_utc = rd.day_date
                  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS crd 
                  ON CAST( p.create_time as DATE) = crd.day_date
               WHERE rtrim(p.transfer_context_type, ' ') IN( 'RACK_PACK_AND_HOLD')
            ) AS a_0
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b 
            ON a_0.receipt_date_utc = b.day_date
         WHERE week_start_day_date BETWEEN (
            SELECT
               week_start_day_date - 35
               FROM
               curr_wk
         ) AND (
            SELECT
               week_start_day_date
               FROM
               curr_wk
         )
      ) AS a
      GROUP BY 1
;


INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'P&H Inventory Speed (P50)' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      35 AS plan_op,
      NULL AS plan_cp,
      p50 AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      inv_speed AS x
      INNER JOIN cal_lkup AS y ON x.week_num = y.week_num
;


INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'P&H Inventory Speed (P90)' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      35 AS plan_op,
      NULL AS plan_cp,
      p90 AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      inv_speed AS x
      INNER JOIN cal_lkup AS y ON x.week_num = y.week_num
;



-- In Yard to Trailer Empty
CREATE TEMPORARY TABLE IF NOT EXISTS tchk
AS
   SELECT
      ytef.location_id,
      CASE
         WHEN ytef.location_id IN(
         89
         ) THEN 'Portland, OR'
         WHEN ytef.location_id IN(
         299
         ) THEN 'Dubuque, IA'
         WHEN ytef.location_id IN(
         399
         ) THEN 'Ontario, CA'
         WHEN ytef.location_id IN(
         499
         ) THEN 'Newark, CA'
         WHEN ytef.location_id IN(
         599
         ) THEN 'Cedar Rapids, IA'
         WHEN ytef.location_id IN(
         699
         ) THEN 'LOWER Marlboro, MD'
         WHEN ytef.location_id IN(
         799
         ) THEN 'Gainesville, FL'
         WHEN ytef.location_id IN(
         569
         ) THEN 'ECFC'
         WHEN ytef.location_id IN(
         584
         ) THEN 'WCOC'
         WHEN ytef.location_id IN(
         808
         ) THEN 'MWFC'
         WHEN ytef.location_id IN(
         879
         ) THEN 'SBFC'
      END AS location_name,
      ytef.event_name,
      min(ytef.event_time) AS event_time,
      MIN(event_time_tz) AS event_time_tz,
      ytef.carrier_bill_of_lading,
      ytef.trailer_num,
      ytef.trailer_type,
      ytef.appointment_id
   FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_v2_event_fact AS ytef
   WHERE 1 = 1
      AND LOWER(rtrim(ytef.trailer_type, ' ')) = LOWER('INBOUND')
      AND LOWER(rtrim(ytef.event_name, ' ')) = LOWER('TRAILERCHECKEDINTOYARD')
   GROUP BY 1, 3, 6, 7, 8, 9
;

CREATE TEMPORARY TABLE IF NOT EXISTS   temp
AS
   SELECT
      ytef.location_id,
      ytef.event_name,
      max(ytef.event_time) AS event_time,
      MAX(event_time_tz) AS event_time_tz,
      ytef.carrier_bill_of_lading,
      ytef.trailer_num,
      ytef.trailer_type,
      ytef.appointment_id
   FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_v2_event_fact AS ytef
   WHERE 1 = 1
      AND LOWER(rtrim(ytef.event_name, ' ')) = LOWER('TRAILERMARKEDEMPTY')
   GROUP BY 1, 2, 5, 6, 7, 8
;


CREATE TEMPORARY TABLE IF NOT EXISTS   chk2emp
  AS
    SELECT
        tchk.location_id,
        tchk.location_name,
        tchk.event_name,
        tchk.event_time AS chk_event,
        CAST(tchk.event_time_tz AS DATE ) AS chk_event_dt,
        temp.event_time AS emp_event,
        temp.event_time as event_time,
        CAST(temp.event_time_tz AS DATE) AS event_time_tz,
        (CAST((cast(temp.event_time_tz as float64) - cast(tchk.event_time_tz as float64) ) as float64 ) * 60 * 24) + (extract(HOUR from CAST(temp.event_time as TIMESTAMP)) - extract(HOUR from CAST(tchk.event_time as TIMESTAMP))) * 60 + (extract(MINUTE from CAST(temp.event_time as TIMESTAMP)) - extract(MINUTE from CAST(tchk.event_time as TIMESTAMP))) + div(extract(SECOND from CAST(temp.event_time as TIMESTAMP)) - extract(SECOND from CAST(tchk.event_time as TIMESTAMP)), 60) AS `difference in minutes`,
        tchk.carrier_bill_of_lading,
        tchk.trailer_num,
        tchk.trailer_type,
        tchk.appointment_id
      FROM
        tchk
        INNER JOIN temp ON tchk.appointment_id = temp.appointment_id
      WHERE 1 = 1
       AND tchk.event_time < temp.event_time
;

CREATE TEMPORARY TABLE IF NOT EXISTS   dcfc_p90
  AS
    SELECT
        chk2emp.location_id as location_id, 
        chk2emp.location_name as location_name,
        chk2emp.event_time_tz,
        chk2emp.event_time,
        cal.week_idnt as week_idnt ,
        ROUND(cast(approx_quantiles(chk2emp.`difference in minutes`, 100)[OFFSET(90)] / CAST(1440 as NUMERIC) as numeric), 1, 'ROUND_HALF_EVEN') AS p90_dd

      FROM
        chk2emp
        INNER JOIN `{{params.gcp_project_id}}`.t2dl_sca_vws.day_cal_454_dim_vw AS cal ON chk2emp.event_time = cal.day_date
      WHERE 1 = 1
       AND cal.trailing_future_week >= -53
       AND cal.trailing_future_week <= 0
       AND chk2emp.location_id IN(
        89, 299, 399, 499, 699, 799, 569, 584, 808, 879
      )
       AND `difference in minutes` < 43200
      GROUP BY 1, 2, 3,4,5
;

CREATE TEMPORARY TABLE IF NOT EXISTS   dc_iyte
  AS
    SELECT
        week_idnt AS week_num,
        CAST(avg(p90_dd) as FLOAT64) AS dc_p90,
        a.week_idnt,
		a.event_time_tz
      FROM
        dcfc_p90 AS a
        INNER JOIN cal_lkup AS b ON a.week_idnt = b.week_num
      WHERE week_start_day_date BETWEEN (
        SELECT
            week_start_day_date - 35
          FROM
            curr_wk
      ) AND (
        SELECT
            week_start_day_date
          FROM
            curr_wk
      )
       AND location_id IN(
        89, 299, 399, 499, 699, 799
      )
      GROUP BY 1,3,4
;


CREATE TEMPORARY TABLE IF NOT EXISTS   fc_iyte
  AS
    SELECT
        dcfc_p90.week_idnt AS week_num,
        CAST(avg(dcfc_p90.p90_dd) as FLOAT64) AS fc_p90
      FROM
        dcfc_p90
      WHERE 1 = 1
       AND dcfc_p90.location_id IN(
        569, 584, 808, 879
      )
       AND dcfc_p90.week_idnt IN(
        SELECT DISTINCT
            dcfc_p90.week_idnt AS week_num
          FROM
            cal_lkup
      )
      GROUP BY 1
;


CREATE TEMPORARY TABLE IF NOT EXISTS   dcfc_diff
  AS
    SELECT
        a.week_num,
        1 - ROUND(cast(a.dcfc_p90 / CAST(8.4 AS NUMERIC) as numeric), 1, 'ROUND_HALF_EVEN') AS dcfc_diff,
        a.event_time_tz
      FROM
        (
          SELECT
              a_0.week_num,
              (dc_p90 + fc_p90) / 2 AS dcfc_p90,
              b.event_time_tz
            FROM
              fc_iyte AS a_0
              INNER JOIN dc_iyte AS b ON a_0.week_num = b.week_num
        ) AS a
;


-- Click to ship
CREATE TEMPORARY TABLE IF NOT EXISTS   cts
  AS
    SELECT
        a.week_num,
        1 - ROUND(a.cts_p90 / CAST(2.7 AS NUMERIC), 4, 'ROUND_HALF_EVEN') AS cts_diff
      FROM
        (
          SELECT
              a_0.week_num,
              ROUND(CAST(1.00 AS NUMERIC) * CAST(CAST(1000 as NUMERIC) * approx_quantiles(a_0.click_to_ship, 100)[OFFSET(90)] as INT64) / 1000, 4, 'ROUND_HALF_EVEN') AS cts_p90
            FROM
              (
                SELECT
                    tbl.order_num,
                    tbl.shipped_date_pacific,
                    tbl.week_num,
                    tbl.source_channel_code,
                    tbl.node_type AS node_type,
                    tbl.node AS node,
                    tbl.click_to_ship
                  FROM
                    (
                      SELECT
                          ol.order_num,
                          ol.carrier_tracking_num,
                          ol.source_channel_code,
                          ol.node_type,
                          coalesce(n.node_alias, n.node_abbr, cast(OL.NODE as string), ol.node_type) AS node,
                          ddd.week_idnt AS week_num,
                          ol.shipped_date_pacific,
                          CASE
                            WHEN ol.click_to_ship_seconds IS NULL THEN NULL
                            WHEN ol.click_to_ship_seconds / CAST(86400 as FLOAT64) > 20 THEN 20
                            WHEN ol.click_to_ship_seconds / CAST(86400 as FLOAT64) <= 0 THEN NULL
                            ELSE round(ol.click_to_ship_seconds / CAST(86400 as FLOAT64), 2)
                          END AS click_to_ship
                        FROM
                          (
                            SELECT
                                oldf.order_num,
                                oldf.carrier_tracking_num,
                                oldf.source_channel_code,
                                oldf.shipped_date_pacific,
                                CASE
                                  WHEN LOWER(rtrim(oldf.last_released_node_type_code, ' ')) = LOWER('RK') THEN 'RACK_STORE'
                                  WHEN LOWER(rtrim(oldf.last_released_node_type_code, ' ')) = LOWER('FL') THEN 'FLS'
                                  WHEN LOWER(rtrim(oldf.last_released_node_type_code, ' ')) = LOWER('DS') THEN 'DS'
                                  WHEN rtrim(oldf.last_released_node_type_code, ' ') IN(
                                    'OF', 'LH', 'CR', 'OC', 'SS', 'FC', 'RS', 'DC'
                                  ) THEN 'FC'
                                  ELSE 'UNKNOWN'
                                END AS node_type,
                                CASE
                                  WHEN LOWER(rtrim(CASE
                                    WHEN LOWER(rtrim(oldf.last_released_node_type_code, ' ')) = 'RK' THEN 'RACK_STORE'
                                    WHEN LOWER(rtrim(oldf.last_released_node_type_code, ' ')) = 'FL' THEN 'FLS'
                                    WHEN LOWER(rtrim(oldf.last_released_node_type_code, ' ')) = 'DS' THEN 'DS'
                                    WHEN rtrim(oldf.last_released_node_type_code, ' ') IN(
                                      'OF', 'LH', 'CR', 'OC', 'SS', 'FC', 'RS', 'DC'
                                    ) THEN 'FC'
                                    ELSE 'UNKNOWN'
                                  END, ' ')) IN(
                                    'FC', 'UNKNOWN'
                                  ) THEN oldf.last_released_node_num
                                  ELSE NULL
                                END AS node,
                                max(oldf.created_to_shipped_lag_secs) AS click_to_ship_seconds
                              FROM
                                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf
                              GROUP BY 1, 2, 3, 4, 5, 6
                          ) AS ol
                          INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS ddd ON ddd.day_date = ol.shipped_date_pacific
                          LEFT OUTER JOIN t2dl_sca_vws.node_vw AS n ON n.node_number = CAST(ol.node as INT64)
                    ) AS tbl
                  WHERE 1 = 1
                   AND LOWER(rtrim(tbl.node_type, ' ')) = 'FC'
                   AND tbl.week_num IN(
                    SELECT DISTINCT
                        tbl.week_num
                      FROM
                        cal_lkup
                  )
                  GROUP BY 1, 2, 3, 4, 5 ,6 , 7
              ) AS a_0
            GROUP BY 1
        ) AS a
;


-- Deliver to Credit
CREATE TEMPORARY TABLE IF NOT EXISTS   delivered
  AS
    SELECT
        customer_shipment_tracking_event_fact.tracking_num,
        customer_shipment_tracking_event_fact.rma_id,
        customer_shipment_tracking_event_fact.carrier_code,
        customer_shipment_tracking_event_fact.shipment_id,
        max(coalesce(customer_shipment_tracking_event_fact.activity_tmstp_pacific, customer_shipment_tracking_event_fact.activity_tmstp_local)) AS max_activity_tmstp
      FROM
        -- Delivered Timestamp
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_shipment_tracking_event_fact
      WHERE rtrim(customer_shipment_tracking_event_fact.carrier_code, ' ') IN(
        'nfsv', 'fdeg'
      )
       AND LOWER(rtrim(customer_shipment_tracking_event_fact.mail_in_return_ind, ' ')) = LOWER('Y')
       AND LOWER(rtrim(customer_shipment_tracking_event_fact.event_type, ' ')) = LOWER('SHIPMENT_TRACKING')
       AND customer_shipment_tracking_event_fact.rma_id IS NOT NULL
       AND LOWER(rtrim(customer_shipment_tracking_event_fact.shipment_status, ' ')) = LOWER('DELIVERED')
      GROUP BY 1, 2, 3, 4
;


CREATE TEMPORARY TABLE IF NOT EXISTS   credit
  AS
    SELECT
        customer_item_return_fact.rma_id,
        customer_item_return_fact.tracking_num,
        customer_item_return_fact.purchase_id,
        customer_item_return_fact.sku_id,
        customer_item_return_fact.line_item_id,
        max(customer_item_return_fact.shipment_id) AS shipment_id,
        max(customer_item_return_fact.return_invoiced_tmstp_pacific) AS max_return_invoiced_tmsp,
        -- Credit Timestamp
        max(customer_item_return_fact.shipment_delivered_tmstp_pacific) AS max_delivered_tmstp
      FROM
        -- Delivered Timestamp
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_item_return_fact
      GROUP BY 1, 2, 3, 4, 5
;


CREATE TEMPORARY TABLE IF NOT EXISTS   dtc
  AS
    SELECT
        a.week_num,
        1 - ROUND(a.dtc_p90 / CAST(4.0 as numeric), 4, 'ROUND_HALF_EVEN') AS dtc_diff
      FROM
        (
          SELECT
              a_0.week_num,
              ROUND(CAST(1.00 as numeric) * CAST(CAST(1000 as NUMERIC) * approx_quantiles(a_0.diff_in_day, 100)[OFFSET(90)] as INT64) / 1000, 4, 'ROUND_HALF_EVEN') AS dtc_p90
            FROM
              (
                SELECT
                    a_1.credit_date,
                    a_1.week_num,
                    a_1.rma_id,
                    a_1.sku_id,
                    a_1.purchase_id,
                    diff_in_min AS diff_in_min,
                    ROUND(diff_in_min / 1440, 24, 'ROUND_HALF_EVEN') AS diff_in_day
                  FROM
                    (
                      SELECT
                          c.rma_id,
                          c.tracking_num,
                          c.purchase_id,
                          c.sku_id,
                          c.line_item_id,
                          c.shipment_id,
                          coalesce(c.max_delivered_tmstp, d.max_activity_tmstp) AS delivered_tmstp,
                          c.max_return_invoiced_tmsp,
                          x.day_date AS credit_date,
                          x.week_idnt AS week_num
                        FROM
                          credit AS c
                          LEFT OUTER JOIN delivered AS d ON c.tracking_num = d.tracking_num
                           AND c.rma_id = d.rma_id
                           AND c.shipment_id = d.shipment_id
                          LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS x ON x.day_date = CAST( c.max_return_invoiced_tmsp as DATE)
                        WHERE 1 = 1
                         AND c.rma_id IS NOT NULL
                         AND x.week_idnt IN(
                          SELECT DISTINCT
                              x.week_idnt AS week_num
                            FROM
                              cal_lkup
                        )
                        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
                    ) AS a_1
                    CROSS JOIN UNNEST(ARRAY[
                      date_diff(CAST( a_1.max_return_invoiced_tmsp as DATE), CAST( a_1.delivered_tmstp as DATE), DAY) * CAST(60 as NUMERIC) * 24 + (extract(HOUR from CAST(a_1.max_return_invoiced_tmsp as TIMESTAMP)) - extract(HOUR from CAST(a_1.delivered_tmstp as TIMESTAMP))) * 60 + (extract(MINUTE from CAST(a_1.max_return_invoiced_tmsp as TIMESTAMP)) - extract(MINUTE from CAST(a_1.delivered_tmstp as TIMESTAMP))) + div(extract(SECOND from CAST(a_1.max_return_invoiced_tmsp as TIMESTAMP)) - extract(SECOND from CAST(a_1.delivered_tmstp as TIMESTAMP)), 60)
                    ]) AS diff_in_min
              ) AS a_0
            GROUP BY 1
        ) AS a
;


-- Received to Palletized
CREATE TEMPORARY TABLE IF NOT EXISTS   rtp
  AS
    SELECT
        a.week_num,
        1 - ROUND(a.rtp_p90 / CAST(7.8 AS NUMERIC), 1, 'ROUND_HALF_EVEN') AS rtp_diff
      FROM
        (
          SELECT
              dc_cycletime_percentile_by_wk_vw.year_week AS week_num,
              dc_cycletime_percentile_by_wk_vw.p90_dd AS rtp_p90
            FROM
              `{{params.gcp_project_id}}`.t2dl_sca_vws.dc_cycletime_percentile_by_wk_vw
            WHERE LOWER(rtrim(dc_cycletime_percentile_by_wk_vw.stage, ' ')) = LOWER('0C: RECEIVED TO PALLETIZED')
             AND LOWER(rtrim(dc_cycletime_percentile_by_wk_vw.location_id, ' ')) = LOWER('DC NETWORK')
             AND dc_cycletime_percentile_by_wk_vw.year_week IN(
              SELECT DISTINCT
                  dc_cycletime_percentile_by_wk_vw.year_week AS week_num
                FROM
                  cal_lkup
            )
             AND LOWER(rtrim(dc_cycletime_percentile_by_wk_vw.banner, ' ')) = LOWER('ALL UP')
             AND LOWER(rtrim(dc_cycletime_percentile_by_wk_vw.channel, ' ')) = LOWER('ALL UP')
             AND LOWER(rtrim(dc_cycletime_percentile_by_wk_vw.po_type, ' ')) = LOWER('ALL UP')
             AND LOWER(rtrim(dc_cycletime_percentile_by_wk_vw.pack_type, ' ')) = LOWER('ALL UP')
        ) AS a
;


-- Total Network Speed
CREATE TEMPORARY TABLE IF NOT EXISTS   total_network_speed
  AS
    SELECT
        a.week_num,
        (rtp_diff + dtc_diff + cts_diff + dcfc_diff) / 4 AS total_network_speed,
        d.event_time_tz
      FROM
        rtp AS a
        INNER JOIN dtc AS b ON a.week_num = b.week_num
        INNER JOIN cts AS c ON a.week_num = c.week_num
        INNER JOIN dcfc_diff AS d ON a.week_num = d.week_num
;


INSERT INTO weekly_ops_standup 
  SELECT
      'SUPPLYCHAIN' AS ops_name,
      NULL AS banner,
      NULL AS channel,
      'TOTAL NETWORK SPEED' AS metric_name,
      x.week_num AS fiscal_num,
      'WEEK' AS fiscal_desc,
      concat(month_short_desc, ' ', 'WK ', CAST( week_454_num as STRING)) AS label,
      NULL AS rolling_fiscal_ind,
      NUMERIC '0.35' AS plan_op,
      NULL AS plan_cp,
      total_network_speed AS ty,
      NULL AS ly,
      NULL AS day_date
    FROM
      total_network_speed AS x
      INNER JOIN cal_lkup AS y ON x.week_num = y.week_num
    WHERE week_start_day_date BETWEEN (
      SELECT
          week_start_day_date - 35
        FROM
          curr_wk
    ) AND (
      SELECT
          week_start_day_date
        FROM
          curr_wk
    )
;


DELETE FROM `{{params.gcp_project_id}}`.t2dl_das_osu.weekly_ops_standup WHERE LOWER(rtrim(weekly_ops_standup.ops_name, ' ')) = 'SUPPLYCHAIN'
 AND weekly_ops_standup.updated_week = (
  SELECT
      --  Adding an extra filter since all Ops Scorecard will be using the same table
      week_idnt
    FROM
      curr_wk
);

INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_osu.weekly_ops_standup (
  ops_name	,
banner	,
channel	,
metric_name	,
fiscal_num	,
fiscal_desc	,
label	,
rolling_fiscal_ind	,
plan_op	,
plan_cp	,
ty	,
ly	,
updated_week	,
update_timestamp	,
update_timestamp_tz	,
day_date	
)
  SELECT
      ops_name,
      banner,
      channel,
      metric_name,
      fiscal_num,
      fiscal_desc,
      label,
      rolling_fiscal_ind,
      plan_op,
      plan_cp,
      ty,
      ly,
      week_idnt AS updated_week,
      timestamp(current_datetime('PST8PDT')) AS update_timestamp,
      `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst(),
      day_date
    FROM
      weekly_ops_standup AS a
      CROSS JOIN curr_wk
;

commit transaction;

