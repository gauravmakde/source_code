BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=app08818;
DAG_ID=trips_sandbox_weekly_cust_11521_ACE_ENG;
---     Task_Name=trips_sandbox_monthly_yoy;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
--CLUSTER BY week_num
AS
SELECT dc.week_num,
 dc.month_num,
 dc.quarter_num,
 dc.year_num,
  CASE
  WHEN dc.month_num >= WN.month_num - 100 AND dc.month_num <= WN.month_num
  THEN 'TY'
  WHEN dc.month_num >= WN.month_num - 200 AND dc.month_num < WN.month_num - 100
  THEN 'LY'
  ELSE NULL
  END AS year_id,
 MIN(dc.day_date) AS ty_start_dt,
 MAX(dc.day_date) AS ty_end_dt
FROM  `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
 LEFT JOIN (SELECT DISTINCT month_num
  FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
  WHERE day_date = current_date('PST8PDT')) AS WN ON TRUE
WHERE dc.month_num >= (SELECT DISTINCT month_num FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc 
WHERE day_date = current_date('PST8PDT')) - 200
 AND dc.month_num <= (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = current_date('PST8PDT'))
GROUP BY dc.week_num,
 dc.month_num,
 dc.quarter_num,
 dc.year_num,
 year_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS upc_lookup_table
--CLUSTER BY upc_num
AS
SELECT DISTINCT LTRIM(upc.upc_num, '0') AS upc_num,
  CASE
  WHEN CAST(trunc(sku.div_num) AS INTEGER) IN (310, 340, 345, 351, 360, 365, 600, 700, 800, 900)
  THEN sku.div_num
  ELSE - 1
  END AS div_num,
  CASE
  WHEN sku.div_num IN (310, 340, 345, 351, 360, 365, 600, 700, 800, 900)
  THEN sku.div_desc
  ELSE 'OTHER'
  END AS div_desc,
 CAST(trunc(sku.grp_num) AS INTEGER) AS subdiv_num,
 sku.grp_desc AS subdiv_name,
 CAST(trunc(sku.dept_num) AS INTEGER) AS dept_num,
 sku.dept_desc AS dept_name,
 sku.class_num,
 sku.sbclass_num,
 sku.brand_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS upc ON LOWER(sku.rms_sku_num) = LOWER(upc.rms_sku_num) AND LOWER(sku.channel_country
    ) = LOWER(upc.channel_country)
WHERE LOWER(sku.channel_country) = LOWER('US')
 AND sku.div_num IN (310, 345, 360, 340, 365, 351, 700);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_single_attribute
--CLUSTER BY acp_id, month_num_realigned, quarter_num_realigned, year_num_realigned
AS
SELECT month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 acp_id,
 region,
 dma,
 aec AS engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 new_to_jwn
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute
WHERE LOWER(time_granularity) = LOWER('MONTH');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sales_information
--CLUSTER BY acp_id, week_num, quarter_num, month_num
AS
SELECT scf.sale_date,
 rc.week_num,
 rc.month_num,
 rc.quarter_num,
 rc.year_num,
 dl.year_id,
 scf.global_tran_id,
 scf.line_item_seq_num,
 scf.store_num,
 scf.acp_id,
 scf.sku_num,
 scf.upc_num,
 COALESCE(div.div_desc, 'OTHER') AS div_desc,
 scf.trip_id,
 scf.employee_discount_flag,
 scf.transaction_type_id,
 scf.device_id,
 scf.ship_method_id,
 scf.price_type_id,
 scf.line_net_usd_amt,
 scf.giftcard_flag,
 scf.items,
 scf.returned_sales,
 scf.returned_items,
 scf.non_gc_amt,
 tsa.region,
 tsa.dma,
 tsa.engagement_cohort,
 tsa.predicted_segment,
 tsa.loyalty_level,
 tsa.loyalty_type,
 tsa.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
 LEFT JOIN customer_single_attribute AS tsa ON LOWER(tsa.acp_id) = LOWER(scf.acp_id) AND rc.month_num = tsa.month_num_realigned
      AND rc.quarter_num = tsa.quarter_num_realigned AND rc.year_num = tsa.year_num_realigned
WHERE rc.month_num >= (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(current_date('PST8PDT'), INTERVAL - 8 MONTH))
 AND rc.month_num <= (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = current_date('PST8PDT'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT month_num,
 quarter_num,
 year_num,
 NULL AS year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id,
 MAX(new_to_jwn) AS new_to_jwn,
 SUM(line_net_usd_amt) AS gross_spend,
 SUM(non_gc_amt) AS non_gc_spend,
 COUNT(DISTINCT trip_id) AS trips,
 SUM(items) AS items,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_accessories,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN trip_id
   ELSE NULL
   END) AS trips_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN items
   ELSE NULL
   END) AS items_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_apparel,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN trip_id
   ELSE NULL
   END) AS trips_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN items
   ELSE NULL
   END) AS items_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_beauty,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN trip_id
   ELSE NULL
   END) AS trips_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN items
   ELSE NULL
   END) AS items_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_designer,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN trip_id
   ELSE NULL
   END) AS trips_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN items
   ELSE NULL
   END) AS items_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_home,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN trip_id
   ELSE NULL
   END) AS trips_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN items
   ELSE NULL
   END) AS items_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_merch,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN trip_id
   ELSE NULL
   END) AS trips_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN items
   ELSE NULL
   END) AS items_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_shoes,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN trip_id
   ELSE NULL
   END) AS trips_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN items
   ELSE NULL
   END) AS items_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_other,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN trip_id
   ELSE NULL
   END) AS trips_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN items
   ELSE NULL
   END) AS items_other
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY month_num,
 quarter_num,
 year_num,
 year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT month_num,
 quarter_num,
 year_num,
 NULL AS year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id,
 MAX(new_to_jwn) AS new_to_jwn,
 SUM(line_net_usd_amt) AS return_spend,
 SUM(items) AS return_items,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN items
   ELSE NULL
   END) AS return_items_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN items
   ELSE NULL
   END) AS return_items_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN items
   ELSE NULL
   END) AS return_items_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN items
   ELSE NULL
   END) AS return_items_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN items
   ELSE NULL
   END) AS return_items_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN items
   ELSE NULL
   END) AS return_items_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN items
   ELSE NULL
   END) AS return_items_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN items
   ELSE NULL
   END) AS return_items_other
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY month_num,
 quarter_num,
 year_num,
 year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT COALESCE(a.month_num, b.month_num) AS month_num,
 COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
 COALESCE(a.year_num, b.year_num) AS year_num,
 COALESCE(a.year_id, b.year_id) AS year_id,
 COALESCE(a.acp_id, b.acp_id) AS acp_id,
 COALESCE(a.channel, b.channel) AS channel,
 COALESCE(a.banner, b.banner) AS banner,
 COALESCE(a.region, b.region) AS region,
 COALESCE(a.dma, b.dma) AS dma,
 COALESCE(a.engagement_cohort, b.engagement_cohort) AS aec,
 COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment,
 COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level,
 COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type,
  CASE
  WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1
  THEN 1
  ELSE 0
  END AS new_to_jwn,
 COALESCE(a.gross_spend, 0) AS gross_spend,
  COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend, 0) AS net_spend,
 COALESCE(a.trips, 0) AS trips,
 COALESCE(a.items, 0) AS gross_units,
  COALESCE(a.items, 0) - COALESCE(b.return_items, 0) AS net_units,
 COALESCE(a.gross_spend_accessories, 0) AS gross_spend_accessories,
  COALESCE(a.non_gc_spend_accessories, 0) + COALESCE(b.return_spend_accessories, 0) AS net_spend_accessories,
 COALESCE(a.trips_accessories, 0) AS trips_accessories,
 COALESCE(a.items_accessories, 0) AS gross_units_accessories,
  COALESCE(a.items_accessories, 0) - COALESCE(b.return_items_accessories, 0) AS net_units_accessories,
 COALESCE(a.gross_spend_apparel, 0) AS gross_spend_apparel,
  COALESCE(a.non_gc_spend_apparel, 0) + COALESCE(b.return_spend_apparel, 0) AS net_spend_apparel,
 COALESCE(a.trips_apparel, 0) AS trips_apparel,
 COALESCE(a.items_apparel, 0) AS gross_units_apparel,
  COALESCE(a.items_apparel, 0) - COALESCE(b.return_items_apparel, 0) AS net_units_apparel,
 COALESCE(a.gross_spend_beauty, 0) AS gross_spend_beauty,
  COALESCE(a.non_gc_spend_beauty, 0) + COALESCE(b.return_spend_beauty, 0) AS net_spend_beauty,
 COALESCE(a.trips_beauty, 0) AS trips_beauty,
 COALESCE(a.items_beauty, 0) AS gross_units_beauty,
  COALESCE(a.items_beauty, 0) - COALESCE(b.return_items_beauty, 0) AS net_units_beauty,
 COALESCE(a.gross_spend_designer, 0) AS gross_spend_designer,
  COALESCE(a.non_gc_spend_designer, 0) + COALESCE(b.return_spend_designer, 0) AS net_spend_designer,
 COALESCE(a.trips_designer, 0) AS trips_designer,
 COALESCE(a.items_designer, 0) AS gross_units_designer,
  COALESCE(a.items_designer, 0) - COALESCE(b.return_items_designer, 0) AS net_units_designer,
 COALESCE(a.gross_spend_home, 0) AS gross_spend_home,
  COALESCE(a.non_gc_spend_home, 0) + COALESCE(b.return_spend_home, 0) AS net_spend_home,
 COALESCE(a.trips_home, 0) AS trips_home,
 COALESCE(a.items_home, 0) AS gross_units_home,
  COALESCE(a.items_home, 0) - COALESCE(b.return_items_home, 0) AS net_units_home,
 COALESCE(a.gross_spend_merch, 0) AS gross_spend_merch,
  COALESCE(a.non_gc_spend_merch, 0) + COALESCE(b.return_spend_merch, 0) AS net_spend_merch,
 COALESCE(a.trips_merch, 0) AS trips_merch,
 COALESCE(a.items_merch, 0) AS gross_units_merch,
  COALESCE(a.items_merch, 0) - COALESCE(b.return_items_merch, 0) AS net_units_merch,
 COALESCE(a.gross_spend_shoes, 0) AS gross_spend_shoes,
  COALESCE(a.non_gc_spend_shoes, 0) + COALESCE(b.return_spend_shoes, 0) AS net_spend_shoes,
 COALESCE(a.trips_shoes, 0) AS trips_shoes,
 COALESCE(a.items_shoes, 0) AS gross_units_shoes,
  COALESCE(a.items_shoes, 0) - COALESCE(b.return_items_shoes, 0) AS net_units_shoes,
 COALESCE(a.gross_spend_other, 0) AS gross_spend_other,
  COALESCE(a.non_gc_spend_other, 0) + COALESCE(b.return_spend_other, 0) AS net_spend_other,
 COALESCE(a.trips_other, 0) AS trips_other,
 COALESCE(a.items_other, 0) AS gross_units_other,
  COALESCE(a.items_other, 0) - COALESCE(b.return_items_other, 0) AS net_units_other
FROM ty_positive AS a
 FULL JOIN ty_negative AS b ON a.month_num = b.month_num AND a.quarter_num = b.quarter_num AND a.year_num = b.year_num
           AND a.year_id = b.year_id AND LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b.channel) AND
         LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region) AND LOWER(a.dma) = LOWER(b.dma) AND
      LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment) = LOWER(b.predicted_segment
      ) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) = LOWER(b.loyalty_type);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS trip_summary_overall
--CLUSTER BY quarter_num, month_num, year_num
AS
SELECT month_num,
 quarter_num,
 year_num,
 year_id,
 region,
 dma,
 aec,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 new_to_jwn,
 COUNT(DISTINCT CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN acp_id
   ELSE NULL
   END) AS cust_count_fls,
 COUNT(DISTINCT CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN acp_id
   ELSE NULL
   END) AS cust_count_ncom,
 COUNT(DISTINCT CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN acp_id
   ELSE NULL
   END) AS cust_count_rs,
 COUNT(DISTINCT CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN acp_id
   ELSE NULL
   END) AS cust_count_rcom,
 COUNT(DISTINCT CASE
   WHEN LOWER(channel) IN (LOWER('1) Nordstrom Stores'), LOWER('3) Rack Stores'))
   THEN acp_id
   ELSE NULL
   END) AS cust_count_stores,
 COUNT(DISTINCT CASE
   WHEN LOWER(channel) IN (LOWER('2) Nordstrom.com'), LOWER('4) Rack.com'))
   THEN acp_id
   ELSE NULL
   END) AS cust_count_digital,
 COUNT(DISTINCT CASE
   WHEN LOWER(banner) = LOWER('1) Nordstrom Banner')
   THEN acp_id
   ELSE NULL
   END) AS cust_count_nord,
 COUNT(DISTINCT CASE
   WHEN LOWER(banner) = LOWER('2) Rack Banner')
   THEN acp_id
   ELSE NULL
   END) AS cust_count_rack,
 COUNT(DISTINCT acp_id) AS cust_count_jwn,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips
   ELSE NULL
   END) AS trips_fls,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips
   ELSE NULL
   END) AS trips_ncom,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips
   ELSE NULL
   END) AS trips_rs,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips
   ELSE NULL
   END) AS trips_rcom,
 SUM(trips) AS trips_jwn,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend
   ELSE NULL
   END) AS net_spend_fls,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend
   ELSE NULL
   END) AS net_spend_ncom,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend
   ELSE NULL
   END) AS net_spend_rs,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend
   ELSE NULL
   END) AS net_spend_rcom,
 SUM(net_spend) AS net_spend_jwn,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN gross_spend
   ELSE NULL
   END) AS gross_spend_fls,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN gross_spend
   ELSE NULL
   END) AS gross_spend_ncom,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN gross_spend
   ELSE NULL
   END) AS gross_spend_rs,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN gross_spend
   ELSE NULL
   END) AS gross_spend_rcom,
 SUM(gross_spend) AS gross_spend_jwn,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units
   ELSE NULL
   END) AS net_units_fls,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units
   ELSE NULL
   END) AS net_units_ncom,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units
   ELSE NULL
   END) AS net_units_rs,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units
   ELSE NULL
   END) AS net_units_rcom,
 SUM(net_units) AS net_units_jwn,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN gross_units
   ELSE NULL
   END) AS gross_units_fls,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN gross_units
   ELSE NULL
   END) AS gross_units_ncom,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN gross_units
   ELSE NULL
   END) AS gross_units_rs,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN gross_units
   ELSE NULL
   END) AS gross_units_rcom,
 SUM(gross_units) AS gross_units_jwn,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips_accessories
   ELSE NULL
   END) AS ns_accessories_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips_accessories
   ELSE NULL
   END) AS ncom_accessories_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips_accessories
   ELSE NULL
   END) AS rs_accessories_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips_accessories
   ELSE NULL
   END) AS rcom_accessories_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend_accessories
   ELSE NULL
   END) AS ns_accessories_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend_accessories
   ELSE NULL
   END) AS ncom_accessories_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend_accessories
   ELSE NULL
   END) AS rs_accessories_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend_accessories
   ELSE NULL
   END) AS rcom_accessories_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units_accessories
   ELSE NULL
   END) AS ns_accessories_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units_accessories
   ELSE NULL
   END) AS ncom_accessories_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units_accessories
   ELSE NULL
   END) AS rs_accessories_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units_accessories
   ELSE NULL
   END) AS rcom_accessories_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips_apparel
   ELSE NULL
   END) AS ns_apparel_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips_apparel
   ELSE NULL
   END) AS ncom_apparel_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips_apparel
   ELSE NULL
   END) AS rs_apparel_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips_apparel
   ELSE NULL
   END) AS rcom_apparel_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend_apparel
   ELSE NULL
   END) AS ns_apparel_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend_apparel
   ELSE NULL
   END) AS ncom_apparel_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend_apparel
   ELSE NULL
   END) AS rs_apparel_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend_apparel
   ELSE NULL
   END) AS rcom_apparel_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units_apparel
   ELSE NULL
   END) AS ns_apparel_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units_apparel
   ELSE NULL
   END) AS ncom_apparel_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units_apparel
   ELSE NULL
   END) AS rs_apparel_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units_apparel
   ELSE NULL
   END) AS rcom_apparel_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips_beauty
   ELSE NULL
   END) AS ns_beauty_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips_beauty
   ELSE NULL
   END) AS ncom_beauty_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips_beauty
   ELSE NULL
   END) AS rs_beauty_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips_beauty
   ELSE NULL
   END) AS rcom_beauty_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend_beauty
   ELSE NULL
   END) AS ns_beauty_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend_beauty
   ELSE NULL
   END) AS ncom_beauty_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend_beauty
   ELSE NULL
   END) AS rs_beauty_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend_beauty
   ELSE NULL
   END) AS rcom_beauty_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units_beauty
   ELSE NULL
   END) AS ns_beauty_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units_beauty
   ELSE NULL
   END) AS ncom_beauty_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units_beauty
   ELSE NULL
   END) AS rs_beauty_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units_beauty
   ELSE NULL
   END) AS rcom_beauty_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips_designer
   ELSE NULL
   END) AS ns_designer_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips_designer
   ELSE NULL
   END) AS ncom_designer_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips_designer
   ELSE NULL
   END) AS rs_designer_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips_designer
   ELSE NULL
   END) AS rcom_designer_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend_designer
   ELSE NULL
   END) AS ns_designer_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend_designer
   ELSE NULL
   END) AS ncom_designer_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend_designer
   ELSE NULL
   END) AS rs_designer_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend_designer
   ELSE NULL
   END) AS rcom_designer_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units_designer
   ELSE NULL
   END) AS ns_designer_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units_designer
   ELSE NULL
   END) AS ncom_designer_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units_designer
   ELSE NULL
   END) AS rs_designer_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units_designer
   ELSE NULL
   END) AS rcom_designer_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips_home
   ELSE NULL
   END) AS ns_home_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips_home
   ELSE NULL
   END) AS ncom_home_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips_home
   ELSE NULL
   END) AS rs_home_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips_home
   ELSE NULL
   END) AS rcom_home_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend_home
   ELSE NULL
   END) AS ns_home_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend_home
   ELSE NULL
   END) AS ncom_home_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend_home
   ELSE NULL
   END) AS rs_home_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend_home
   ELSE NULL
   END) AS rcom_home_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units_home
   ELSE NULL
   END) AS ns_home_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units_home
   ELSE NULL
   END) AS ncom_home_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units_home
   ELSE NULL
   END) AS rs_home_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units_home
   ELSE NULL
   END) AS rcom_home_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips_merch
   ELSE NULL
   END) AS ns_merch_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips_merch
   ELSE NULL
   END) AS ncom_merch_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips_merch
   ELSE NULL
   END) AS rs_merch_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips_merch
   ELSE NULL
   END) AS rcom_merch_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend_merch
   ELSE NULL
   END) AS ns_merch_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend_merch
   ELSE NULL
   END) AS ncom_merch_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend_merch
   ELSE NULL
   END) AS rs_merch_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend_merch
   ELSE NULL
   END) AS rcom_merch_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units_merch
   ELSE NULL
   END) AS ns_merch_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units_merch
   ELSE NULL
   END) AS ncom_merch_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units_merch
   ELSE NULL
   END) AS rs_merch_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units_merch
   ELSE NULL
   END) AS rcom_merch_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips_shoes
   ELSE NULL
   END) AS ns_shoes_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips_shoes
   ELSE NULL
   END) AS ncom_shoes_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips_shoes
   ELSE NULL
   END) AS rs_shoes_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips_shoes
   ELSE NULL
   END) AS rcom_shoes_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend_shoes
   ELSE NULL
   END) AS ns_shoes_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend_shoes
   ELSE NULL
   END) AS ncom_shoes_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend_shoes
   ELSE NULL
   END) AS rs_shoes_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend_shoes
   ELSE NULL
   END) AS rcom_shoes_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units_shoes
   ELSE NULL
   END) AS ns_shoes_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units_shoes
   ELSE NULL
   END) AS ncom_shoes_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units_shoes
   ELSE NULL
   END) AS rs_shoes_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units_shoes
   ELSE NULL
   END) AS rcom_shoes_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN trips_other
   ELSE NULL
   END) AS ns_other_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN trips_other
   ELSE NULL
   END) AS ncom_other_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN trips_other
   ELSE NULL
   END) AS rs_other_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN trips_other
   ELSE NULL
   END) AS rcom_other_weekly_trips,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_spend_other
   ELSE NULL
   END) AS ns_other_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_spend_other
   ELSE NULL
   END) AS ncom_other_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_spend_other
   ELSE NULL
   END) AS rs_other_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_spend_other
   ELSE NULL
   END) AS rcom_other_weekly_net_spend,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN net_units_other
   ELSE NULL
   END) AS ns_other_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN net_units_other
   ELSE NULL
   END) AS ncom_other_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN net_units_other
   ELSE NULL
   END) AS rs_other_weekly_net_units,
 SUM(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN net_units_other
   ELSE NULL
   END) AS rcom_other_weekly_net_units
FROM ty
GROUP BY month_num,
 quarter_num,
 year_num,
 year_id,
 region,
 dma,
 aec,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 new_to_jwn;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (month_num, quarter_num, year_num) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (region) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (dma) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (aec) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (predicted_segment) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (loyalty_level) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (loyalty_type) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (new_to_jwn) ON trip_summary_overall;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS sales_information;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sales_information
--CLUSTER BY acp_id, week_num, quarter_num, month_num
AS
SELECT scf.sale_date,
 rc.week_num,
 rc.month_num,
 rc.quarter_num,
 rc.year_num,
 dl.year_id,
 scf.global_tran_id,
 scf.line_item_seq_num,
 scf.store_num,
 scf.acp_id,
 scf.sku_num,
 scf.upc_num,
 COALESCE(div.div_desc, 'OTHER') AS div_desc,
 scf.trip_id,
 scf.employee_discount_flag,
 scf.transaction_type_id,
 scf.device_id,
 scf.ship_method_id,
 scf.price_type_id,
 scf.line_net_usd_amt,
 scf.giftcard_flag,
 scf.items,
 scf.returned_sales,
 scf.returned_items,
 scf.non_gc_amt,
 tsa.region,
 tsa.dma,
 tsa.engagement_cohort,
 tsa.predicted_segment,
 tsa.loyalty_level,
 tsa.loyalty_type,
 tsa.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
 LEFT JOIN customer_single_attribute AS tsa ON LOWER(tsa.acp_id) = LOWER(scf.acp_id) AND rc.month_num = tsa.month_num_realigned
      AND rc.quarter_num = tsa.quarter_num_realigned AND rc.year_num = tsa.year_num_realigned
WHERE rc.month_num >= (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(current_date('PST8PDT'), INTERVAL - 16 MONTH))
 AND rc.month_num < (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(current_date('PST8PDT'), INTERVAL - 8 MONTH));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty_positive;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT month_num,
 quarter_num,
 year_num,
 NULL AS year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id,
 MAX(new_to_jwn) AS new_to_jwn,
 SUM(line_net_usd_amt) AS gross_spend,
 SUM(non_gc_amt) AS non_gc_spend,
 COUNT(DISTINCT trip_id) AS trips,
 SUM(items) AS items,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_accessories,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN trip_id
   ELSE NULL
   END) AS trips_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN items
   ELSE NULL
   END) AS items_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_apparel,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN trip_id
   ELSE NULL
   END) AS trips_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN items
   ELSE NULL
   END) AS items_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_beauty,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN trip_id
   ELSE NULL
   END) AS trips_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN items
   ELSE NULL
   END) AS items_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_designer,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN trip_id
   ELSE NULL
   END) AS trips_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN items
   ELSE NULL
   END) AS items_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_home,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN trip_id
   ELSE NULL
   END) AS trips_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN items
   ELSE NULL
   END) AS items_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_merch,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN trip_id
   ELSE NULL
   END) AS trips_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN items
   ELSE NULL
   END) AS items_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_shoes,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN trip_id
   ELSE NULL
   END) AS trips_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN items
   ELSE NULL
   END) AS items_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_other,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN trip_id
   ELSE NULL
   END) AS trips_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN items
   ELSE NULL
   END) AS items_other
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY month_num,
 quarter_num,
 year_num,
 year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty_negative;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT month_num,
 quarter_num,
 year_num,
 NULL AS year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id,
 MAX(new_to_jwn) AS new_to_jwn,
 SUM(line_net_usd_amt) AS return_spend,
 SUM(items) AS return_items,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN items
   ELSE NULL
   END) AS return_items_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN items
   ELSE NULL
   END) AS return_items_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN items
   ELSE NULL
   END) AS return_items_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN items
   ELSE NULL
   END) AS return_items_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN items
   ELSE NULL
   END) AS return_items_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN items
   ELSE NULL
   END) AS return_items_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN items
   ELSE NULL
   END) AS return_items_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN items
   ELSE NULL
   END) AS return_items_other
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY month_num,
 quarter_num,
 year_num,
 year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT COALESCE(a.month_num, b.month_num) AS month_num,
 COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
 COALESCE(a.year_num, b.year_num) AS year_num,
 COALESCE(a.year_id, b.year_id) AS year_id,
 COALESCE(a.acp_id, b.acp_id) AS acp_id,
 COALESCE(a.channel, b.channel) AS channel,
 COALESCE(a.banner, b.banner) AS banner,
 COALESCE(a.region, b.region) AS region,
 COALESCE(a.dma, b.dma) AS dma,
 COALESCE(a.engagement_cohort, b.engagement_cohort) AS aec,
 COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment,
 COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level,
 COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type,
  CASE
  WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1
  THEN 1
  ELSE 0
  END AS new_to_jwn,
 COALESCE(a.gross_spend, 0) AS gross_spend,
  COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend, 0) AS net_spend,
 COALESCE(a.trips, 0) AS trips,
 COALESCE(a.items, 0) AS gross_units,
  COALESCE(a.items, 0) - COALESCE(b.return_items, 0) AS net_units,
 COALESCE(a.gross_spend_accessories, 0) AS gross_spend_accessories,
  COALESCE(a.non_gc_spend_accessories, 0) + COALESCE(b.return_spend_accessories, 0) AS net_spend_accessories,
 COALESCE(a.trips_accessories, 0) AS trips_accessories,
 COALESCE(a.items_accessories, 0) AS gross_units_accessories,
  COALESCE(a.items_accessories, 0) - COALESCE(b.return_items_accessories, 0) AS net_units_accessories,
 COALESCE(a.gross_spend_apparel, 0) AS gross_spend_apparel,
  COALESCE(a.non_gc_spend_apparel, 0) + COALESCE(b.return_spend_apparel, 0) AS net_spend_apparel,
 COALESCE(a.trips_apparel, 0) AS trips_apparel,
 COALESCE(a.items_apparel, 0) AS gross_units_apparel,
  COALESCE(a.items_apparel, 0) - COALESCE(b.return_items_apparel, 0) AS net_units_apparel,
 COALESCE(a.gross_spend_beauty, 0) AS gross_spend_beauty,
  COALESCE(a.non_gc_spend_beauty, 0) + COALESCE(b.return_spend_beauty, 0) AS net_spend_beauty,
 COALESCE(a.trips_beauty, 0) AS trips_beauty,
 COALESCE(a.items_beauty, 0) AS gross_units_beauty,
  COALESCE(a.items_beauty, 0) - COALESCE(b.return_items_beauty, 0) AS net_units_beauty,
 COALESCE(a.gross_spend_designer, 0) AS gross_spend_designer,
  COALESCE(a.non_gc_spend_designer, 0) + COALESCE(b.return_spend_designer, 0) AS net_spend_designer,
 COALESCE(a.trips_designer, 0) AS trips_designer,
 COALESCE(a.items_designer, 0) AS gross_units_designer,
  COALESCE(a.items_designer, 0) - COALESCE(b.return_items_designer, 0) AS net_units_designer,
 COALESCE(a.gross_spend_home, 0) AS gross_spend_home,
  COALESCE(a.non_gc_spend_home, 0) + COALESCE(b.return_spend_home, 0) AS net_spend_home,
 COALESCE(a.trips_home, 0) AS trips_home,
 COALESCE(a.items_home, 0) AS gross_units_home,
  COALESCE(a.items_home, 0) - COALESCE(b.return_items_home, 0) AS net_units_home,
 COALESCE(a.gross_spend_merch, 0) AS gross_spend_merch,
  COALESCE(a.non_gc_spend_merch, 0) + COALESCE(b.return_spend_merch, 0) AS net_spend_merch,
 COALESCE(a.trips_merch, 0) AS trips_merch,
 COALESCE(a.items_merch, 0) AS gross_units_merch,
  COALESCE(a.items_merch, 0) - COALESCE(b.return_items_merch, 0) AS net_units_merch,
 COALESCE(a.gross_spend_shoes, 0) AS gross_spend_shoes,
  COALESCE(a.non_gc_spend_shoes, 0) + COALESCE(b.return_spend_shoes, 0) AS net_spend_shoes,
 COALESCE(a.trips_shoes, 0) AS trips_shoes,
 COALESCE(a.items_shoes, 0) AS gross_units_shoes,
  COALESCE(a.items_shoes, 0) - COALESCE(b.return_items_shoes, 0) AS net_units_shoes,
 COALESCE(a.gross_spend_other, 0) AS gross_spend_other,
  COALESCE(a.non_gc_spend_other, 0) + COALESCE(b.return_spend_other, 0) AS net_spend_other,
 COALESCE(a.trips_other, 0) AS trips_other,
 COALESCE(a.items_other, 0) AS gross_units_other,
  COALESCE(a.items_other, 0) - COALESCE(b.return_items_other, 0) AS net_units_other
FROM ty_positive AS a
 FULL JOIN ty_negative AS b ON a.month_num = b.month_num AND a.quarter_num = b.quarter_num AND a.year_num = b.year_num
           AND a.year_id = b.year_id AND LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b.channel) AND
         LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region) AND LOWER(a.dma) = LOWER(b.dma) AND
      LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment) = LOWER(b.predicted_segment
      ) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) = LOWER(b.loyalty_type);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO trip_summary_overall
(SELECT month_num,
  quarter_num,
  year_num,
  year_id,
  region,
  dma,
  aec,
  predicted_segment,
  loyalty_level,
  loyalty_type,
  new_to_jwn,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_fls,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_ncom,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_rs,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_rcom,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) IN (LOWER('1) Nordstrom Stores'), LOWER('3) Rack Stores'))
    THEN acp_id
    ELSE NULL
    END) AS cust_count_stores,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) IN (LOWER('2) Nordstrom.com'), LOWER('4) Rack.com'))
    THEN acp_id
    ELSE NULL
    END) AS cust_count_digital,
  COUNT(DISTINCT CASE
    WHEN LOWER(banner) = LOWER('1) Nordstrom Banner')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_nord,
  COUNT(DISTINCT CASE
    WHEN LOWER(banner) = LOWER('2) Rack Banner')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_rack,
  COUNT(DISTINCT acp_id) AS cust_count_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips
    ELSE NULL
    END) AS trips_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips
    ELSE NULL
    END) AS trips_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips
    ELSE NULL
    END) AS trips_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips
    ELSE NULL
    END) AS trips_rcom,
  SUM(trips) AS trips_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend
    ELSE NULL
    END) AS net_spend_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend
    ELSE NULL
    END) AS net_spend_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend
    ELSE NULL
    END) AS net_spend_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend
    ELSE NULL
    END) AS net_spend_rcom,
  SUM(net_spend) AS net_spend_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN gross_spend
    ELSE NULL
    END) AS gross_spend_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN gross_spend
    ELSE NULL
    END) AS gross_spend_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN gross_spend
    ELSE NULL
    END) AS gross_spend_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN gross_spend
    ELSE NULL
    END) AS gross_spend_rcom,
  SUM(gross_spend) AS gross_spend_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units
    ELSE NULL
    END) AS net_units_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units
    ELSE NULL
    END) AS net_units_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units
    ELSE NULL
    END) AS net_units_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units
    ELSE NULL
    END) AS net_units_rcom,
  SUM(net_units) AS net_units_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN gross_units
    ELSE NULL
    END) AS gross_units_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN gross_units
    ELSE NULL
    END) AS gross_units_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN gross_units
    ELSE NULL
    END) AS gross_units_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN gross_units
    ELSE NULL
    END) AS gross_units_rcom,
  SUM(gross_units) AS gross_units_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_accessories
    ELSE NULL
    END) AS ns_accessories_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_accessories
    ELSE NULL
    END) AS ncom_accessories_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_accessories
    ELSE NULL
    END) AS rs_accessories_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_accessories
    ELSE NULL
    END) AS rcom_accessories_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_accessories
    ELSE NULL
    END) AS ns_accessories_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_accessories
    ELSE NULL
    END) AS ncom_accessories_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_accessories
    ELSE NULL
    END) AS rs_accessories_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_accessories
    ELSE NULL
    END) AS rcom_accessories_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_accessories
    ELSE NULL
    END) AS ns_accessories_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_accessories
    ELSE NULL
    END) AS ncom_accessories_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_accessories
    ELSE NULL
    END) AS rs_accessories_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_accessories
    ELSE NULL
    END) AS rcom_accessories_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_apparel
    ELSE NULL
    END) AS ns_apparel_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_apparel
    ELSE NULL
    END) AS ncom_apparel_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_apparel
    ELSE NULL
    END) AS rs_apparel_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_apparel
    ELSE NULL
    END) AS rcom_apparel_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_apparel
    ELSE NULL
    END) AS ns_apparel_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_apparel
    ELSE NULL
    END) AS ncom_apparel_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_apparel
    ELSE NULL
    END) AS rs_apparel_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_apparel
    ELSE NULL
    END) AS rcom_apparel_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_apparel
    ELSE NULL
    END) AS ns_apparel_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_apparel
    ELSE NULL
    END) AS ncom_apparel_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_apparel
    ELSE NULL
    END) AS rs_apparel_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_apparel
    ELSE NULL
    END) AS rcom_apparel_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_beauty
    ELSE NULL
    END) AS ns_beauty_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_beauty
    ELSE NULL
    END) AS ncom_beauty_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_beauty
    ELSE NULL
    END) AS rs_beauty_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_beauty
    ELSE NULL
    END) AS rcom_beauty_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_beauty
    ELSE NULL
    END) AS ns_beauty_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_beauty
    ELSE NULL
    END) AS ncom_beauty_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_beauty
    ELSE NULL
    END) AS rs_beauty_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_beauty
    ELSE NULL
    END) AS rcom_beauty_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_beauty
    ELSE NULL
    END) AS ns_beauty_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_beauty
    ELSE NULL
    END) AS ncom_beauty_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_beauty
    ELSE NULL
    END) AS rs_beauty_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_beauty
    ELSE NULL
    END) AS rcom_beauty_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_designer
    ELSE NULL
    END) AS ns_designer_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_designer
    ELSE NULL
    END) AS ncom_designer_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_designer
    ELSE NULL
    END) AS rs_designer_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_designer
    ELSE NULL
    END) AS rcom_designer_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_designer
    ELSE NULL
    END) AS ns_designer_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_designer
    ELSE NULL
    END) AS ncom_designer_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_designer
    ELSE NULL
    END) AS rs_designer_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_designer
    ELSE NULL
    END) AS rcom_designer_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_designer
    ELSE NULL
    END) AS ns_designer_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_designer
    ELSE NULL
    END) AS ncom_designer_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_designer
    ELSE NULL
    END) AS rs_designer_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_designer
    ELSE NULL
    END) AS rcom_designer_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_home
    ELSE NULL
    END) AS ns_home_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_home
    ELSE NULL
    END) AS ncom_home_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_home
    ELSE NULL
    END) AS rs_home_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_home
    ELSE NULL
    END) AS rcom_home_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_home
    ELSE NULL
    END) AS ns_home_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_home
    ELSE NULL
    END) AS ncom_home_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_home
    ELSE NULL
    END) AS rs_home_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_home
    ELSE NULL
    END) AS rcom_home_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_home
    ELSE NULL
    END) AS ns_home_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_home
    ELSE NULL
    END) AS ncom_home_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_home
    ELSE NULL
    END) AS rs_home_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_home
    ELSE NULL
    END) AS rcom_home_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_merch
    ELSE NULL
    END) AS ns_merch_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_merch
    ELSE NULL
    END) AS ncom_merch_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_merch
    ELSE NULL
    END) AS rs_merch_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_merch
    ELSE NULL
    END) AS rcom_merch_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_merch
    ELSE NULL
    END) AS ns_merch_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_merch
    ELSE NULL
    END) AS ncom_merch_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_merch
    ELSE NULL
    END) AS rs_merch_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_merch
    ELSE NULL
    END) AS rcom_merch_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_merch
    ELSE NULL
    END) AS ns_merch_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_merch
    ELSE NULL
    END) AS ncom_merch_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_merch
    ELSE NULL
    END) AS rs_merch_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_merch
    ELSE NULL
    END) AS rcom_merch_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_shoes
    ELSE NULL
    END) AS ns_shoes_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_shoes
    ELSE NULL
    END) AS ncom_shoes_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_shoes
    ELSE NULL
    END) AS rs_shoes_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_shoes
    ELSE NULL
    END) AS rcom_shoes_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_shoes
    ELSE NULL
    END) AS ns_shoes_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_shoes
    ELSE NULL
    END) AS ncom_shoes_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_shoes
    ELSE NULL
    END) AS rs_shoes_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_shoes
    ELSE NULL
    END) AS rcom_shoes_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_shoes
    ELSE NULL
    END) AS ns_shoes_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_shoes
    ELSE NULL
    END) AS ncom_shoes_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_shoes
    ELSE NULL
    END) AS rs_shoes_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_shoes
    ELSE NULL
    END) AS rcom_shoes_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_other
    ELSE NULL
    END) AS ns_other_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_other
    ELSE NULL
    END) AS ncom_other_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_other
    ELSE NULL
    END) AS rs_other_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_other
    ELSE NULL
    END) AS rcom_other_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_other
    ELSE NULL
    END) AS ns_other_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_other
    ELSE NULL
    END) AS ncom_other_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_other
    ELSE NULL
    END) AS rs_other_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_other
    ELSE NULL
    END) AS rcom_other_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_other
    ELSE NULL
    END) AS ns_other_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_other
    ELSE NULL
    END) AS ncom_other_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_other
    ELSE NULL
    END) AS rs_other_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_other
    ELSE NULL
    END) AS rcom_other_weekly_net_units
 FROM ty
 GROUP BY month_num,
  quarter_num,
  year_num,
  year_id,
  region,
  dma,
  aec,
  predicted_segment,
  loyalty_level,
  loyalty_type,
  new_to_jwn);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (month_num, quarter_num, year_num) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (region) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (dma) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (aec) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (predicted_segment) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (loyalty_level) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (loyalty_type) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (new_to_jwn) ON trip_summary_overall;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS sales_information;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sales_information
--CLUSTER BY acp_id, week_num, quarter_num, month_num
AS
SELECT scf.sale_date,
 rc.week_num,
 rc.month_num,
 rc.quarter_num,
 rc.year_num,
 dl.year_id,
 scf.global_tran_id,
 scf.line_item_seq_num,
 scf.store_num,
 scf.acp_id,
 scf.sku_num,
 scf.upc_num,
 COALESCE(div.div_desc, 'OTHER') AS div_desc,
 scf.trip_id,
 scf.employee_discount_flag,
 scf.transaction_type_id,
 scf.device_id,
 scf.ship_method_id,
 scf.price_type_id,
 scf.line_net_usd_amt,
 scf.giftcard_flag,
 scf.items,
 scf.returned_sales,
 scf.returned_items,
 scf.non_gc_amt,
 tsa.region,
 tsa.dma,
 tsa.engagement_cohort,
 tsa.predicted_segment,
 tsa.loyalty_level,
 tsa.loyalty_type,
 tsa.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
 LEFT JOIN customer_single_attribute AS tsa ON LOWER(tsa.acp_id) = LOWER(scf.acp_id) AND rc.month_num = tsa.month_num_realigned
      AND rc.quarter_num = tsa.quarter_num_realigned AND rc.year_num = tsa.year_num_realigned
WHERE rc.month_num >= (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(current_date('PST8PDT'), INTERVAL - 24 MONTH))
 AND rc.month_num < (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(current_date('PST8PDT'), INTERVAL - 16 MONTH));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty_positive;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT month_num,
 quarter_num,
 year_num,
 NULL AS year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id,
 MAX(new_to_jwn) AS new_to_jwn,
 SUM(line_net_usd_amt) AS gross_spend,
 SUM(non_gc_amt) AS non_gc_spend,
 COUNT(DISTINCT trip_id) AS trips,
 SUM(items) AS items,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_accessories,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN trip_id
   ELSE NULL
   END) AS trips_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN items
   ELSE NULL
   END) AS items_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_apparel,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN trip_id
   ELSE NULL
   END) AS trips_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN items
   ELSE NULL
   END) AS items_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_beauty,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN trip_id
   ELSE NULL
   END) AS trips_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN items
   ELSE NULL
   END) AS items_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_designer,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN trip_id
   ELSE NULL
   END) AS trips_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN items
   ELSE NULL
   END) AS items_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_home,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN trip_id
   ELSE NULL
   END) AS trips_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN items
   ELSE NULL
   END) AS items_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_merch,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN trip_id
   ELSE NULL
   END) AS trips_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN items
   ELSE NULL
   END) AS items_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_shoes,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN trip_id
   ELSE NULL
   END) AS trips_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN items
   ELSE NULL
   END) AS items_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS gross_spend_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN non_gc_amt
   ELSE NULL
   END) AS non_gc_spend_other,
 COUNT(DISTINCT CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN trip_id
   ELSE NULL
   END) AS trips_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN items
   ELSE NULL
   END) AS items_other
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY month_num,
 quarter_num,
 year_num,
 year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty_negative;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT month_num,
 quarter_num,
 year_num,
 NULL AS year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id,
 MAX(new_to_jwn) AS new_to_jwn,
 SUM(line_net_usd_amt) AS return_spend,
 SUM(items) AS return_items,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN items
   ELSE NULL
   END) AS return_items_accessories,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN items
   ELSE NULL
   END) AS return_items_apparel,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN items
   ELSE NULL
   END) AS return_items_beauty,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN items
   ELSE NULL
   END) AS return_items_designer,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN items
   ELSE NULL
   END) AS return_items_home,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN items
   ELSE NULL
   END) AS return_items_merch,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN items
   ELSE NULL
   END) AS return_items_shoes,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN line_net_usd_amt
   ELSE NULL
   END) AS return_spend_other,
 SUM(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN items
   ELSE NULL
   END) AS return_items_other
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY month_num,
 quarter_num,
 year_num,
 year_id,
 channel,
 banner,
 region,
 dma,
 engagement_cohort,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty
--CLUSTER BY acp_id, quarter_num, month_num, year_num
AS
SELECT COALESCE(a.month_num, b.month_num) AS month_num,
 COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
 COALESCE(a.year_num, b.year_num) AS year_num,
 COALESCE(a.year_id, b.year_id) AS year_id,
 COALESCE(a.acp_id, b.acp_id) AS acp_id,
 COALESCE(a.channel, b.channel) AS channel,
 COALESCE(a.banner, b.banner) AS banner,
 COALESCE(a.region, b.region) AS region,
 COALESCE(a.dma, b.dma) AS dma,
 COALESCE(a.engagement_cohort, b.engagement_cohort) AS aec,
 COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment,
 COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level,
 COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type,
  CASE
  WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1
  THEN 1
  ELSE 0
  END AS new_to_jwn,
 COALESCE(a.gross_spend, 0) AS gross_spend,
  COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend, 0) AS net_spend,
 COALESCE(a.trips, 0) AS trips,
 COALESCE(a.items, 0) AS gross_units,
  COALESCE(a.items, 0) - COALESCE(b.return_items, 0) AS net_units,
 COALESCE(a.gross_spend_accessories, 0) AS gross_spend_accessories,
  COALESCE(a.non_gc_spend_accessories, 0) + COALESCE(b.return_spend_accessories, 0) AS net_spend_accessories,
 COALESCE(a.trips_accessories, 0) AS trips_accessories,
 COALESCE(a.items_accessories, 0) AS gross_units_accessories,
  COALESCE(a.items_accessories, 0) - COALESCE(b.return_items_accessories, 0) AS net_units_accessories,
 COALESCE(a.gross_spend_apparel, 0) AS gross_spend_apparel,
  COALESCE(a.non_gc_spend_apparel, 0) + COALESCE(b.return_spend_apparel, 0) AS net_spend_apparel,
 COALESCE(a.trips_apparel, 0) AS trips_apparel,
 COALESCE(a.items_apparel, 0) AS gross_units_apparel,
  COALESCE(a.items_apparel, 0) - COALESCE(b.return_items_apparel, 0) AS net_units_apparel,
 COALESCE(a.gross_spend_beauty, 0) AS gross_spend_beauty,
  COALESCE(a.non_gc_spend_beauty, 0) + COALESCE(b.return_spend_beauty, 0) AS net_spend_beauty,
 COALESCE(a.trips_beauty, 0) AS trips_beauty,
 COALESCE(a.items_beauty, 0) AS gross_units_beauty,
  COALESCE(a.items_beauty, 0) - COALESCE(b.return_items_beauty, 0) AS net_units_beauty,
 COALESCE(a.gross_spend_designer, 0) AS gross_spend_designer,
  COALESCE(a.non_gc_spend_designer, 0) + COALESCE(b.return_spend_designer, 0) AS net_spend_designer,
 COALESCE(a.trips_designer, 0) AS trips_designer,
 COALESCE(a.items_designer, 0) AS gross_units_designer,
  COALESCE(a.items_designer, 0) - COALESCE(b.return_items_designer, 0) AS net_units_designer,
 COALESCE(a.gross_spend_home, 0) AS gross_spend_home,
  COALESCE(a.non_gc_spend_home, 0) + COALESCE(b.return_spend_home, 0) AS net_spend_home,
 COALESCE(a.trips_home, 0) AS trips_home,
 COALESCE(a.items_home, 0) AS gross_units_home,
  COALESCE(a.items_home, 0) - COALESCE(b.return_items_home, 0) AS net_units_home,
 COALESCE(a.gross_spend_merch, 0) AS gross_spend_merch,
  COALESCE(a.non_gc_spend_merch, 0) + COALESCE(b.return_spend_merch, 0) AS net_spend_merch,
 COALESCE(a.trips_merch, 0) AS trips_merch,
 COALESCE(a.items_merch, 0) AS gross_units_merch,
  COALESCE(a.items_merch, 0) - COALESCE(b.return_items_merch, 0) AS net_units_merch,
 COALESCE(a.gross_spend_shoes, 0) AS gross_spend_shoes,
  COALESCE(a.non_gc_spend_shoes, 0) + COALESCE(b.return_spend_shoes, 0) AS net_spend_shoes,
 COALESCE(a.trips_shoes, 0) AS trips_shoes,
 COALESCE(a.items_shoes, 0) AS gross_units_shoes,
  COALESCE(a.items_shoes, 0) - COALESCE(b.return_items_shoes, 0) AS net_units_shoes,
 COALESCE(a.gross_spend_other, 0) AS gross_spend_other,
  COALESCE(a.non_gc_spend_other, 0) + COALESCE(b.return_spend_other, 0) AS net_spend_other,
 COALESCE(a.trips_other, 0) AS trips_other,
 COALESCE(a.items_other, 0) AS gross_units_other,
  COALESCE(a.items_other, 0) - COALESCE(b.return_items_other, 0) AS net_units_other
FROM ty_positive AS a
 FULL JOIN ty_negative AS b ON a.month_num = b.month_num AND a.quarter_num = b.quarter_num AND a.year_num = b.year_num
           AND a.year_id = b.year_id AND LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b.channel) AND
         LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region) AND LOWER(a.dma) = LOWER(b.dma) AND
      LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment) = LOWER(b.predicted_segment
      ) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) = LOWER(b.loyalty_type);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO trip_summary_overall
(SELECT month_num,
  quarter_num,
  year_num,
  year_id,
  region,
  dma,
  aec,
  predicted_segment,
  loyalty_level,
  loyalty_type,
  new_to_jwn,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_fls,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_ncom,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_rs,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_rcom,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) IN (LOWER('1) Nordstrom Stores'), LOWER('3) Rack Stores'))
    THEN acp_id
    ELSE NULL
    END) AS cust_count_stores,
  COUNT(DISTINCT CASE
    WHEN LOWER(channel) IN (LOWER('2) Nordstrom.com'), LOWER('4) Rack.com'))
    THEN acp_id
    ELSE NULL
    END) AS cust_count_digital,
  COUNT(DISTINCT CASE
    WHEN LOWER(banner) = LOWER('1) Nordstrom Banner')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_nord,
  COUNT(DISTINCT CASE
    WHEN LOWER(banner) = LOWER('2) Rack Banner')
    THEN acp_id
    ELSE NULL
    END) AS cust_count_rack,
  COUNT(DISTINCT acp_id) AS cust_count_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips
    ELSE NULL
    END) AS trips_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips
    ELSE NULL
    END) AS trips_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips
    ELSE NULL
    END) AS trips_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips
    ELSE NULL
    END) AS trips_rcom,
  SUM(trips) AS trips_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend
    ELSE NULL
    END) AS net_spend_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend
    ELSE NULL
    END) AS net_spend_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend
    ELSE NULL
    END) AS net_spend_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend
    ELSE NULL
    END) AS net_spend_rcom,
  SUM(net_spend) AS net_spend_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN gross_spend
    ELSE NULL
    END) AS gross_spend_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN gross_spend
    ELSE NULL
    END) AS gross_spend_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN gross_spend
    ELSE NULL
    END) AS gross_spend_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN gross_spend
    ELSE NULL
    END) AS gross_spend_rcom,
  SUM(gross_spend) AS gross_spend_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units
    ELSE NULL
    END) AS net_units_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units
    ELSE NULL
    END) AS net_units_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units
    ELSE NULL
    END) AS net_units_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units
    ELSE NULL
    END) AS net_units_rcom,
  SUM(net_units) AS net_units_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN gross_units
    ELSE NULL
    END) AS gross_units_fls,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN gross_units
    ELSE NULL
    END) AS gross_units_ncom,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN gross_units
    ELSE NULL
    END) AS gross_units_rs,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN gross_units
    ELSE NULL
    END) AS gross_units_rcom,
  SUM(gross_units) AS gross_units_jwn,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_accessories
    ELSE NULL
    END) AS ns_accessories_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_accessories
    ELSE NULL
    END) AS ncom_accessories_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_accessories
    ELSE NULL
    END) AS rs_accessories_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_accessories
    ELSE NULL
    END) AS rcom_accessories_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_accessories
    ELSE NULL
    END) AS ns_accessories_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_accessories
    ELSE NULL
    END) AS ncom_accessories_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_accessories
    ELSE NULL
    END) AS rs_accessories_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_accessories
    ELSE NULL
    END) AS rcom_accessories_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_accessories
    ELSE NULL
    END) AS ns_accessories_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_accessories
    ELSE NULL
    END) AS ncom_accessories_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_accessories
    ELSE NULL
    END) AS rs_accessories_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_accessories
    ELSE NULL
    END) AS rcom_accessories_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_apparel
    ELSE NULL
    END) AS ns_apparel_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_apparel
    ELSE NULL
    END) AS ncom_apparel_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_apparel
    ELSE NULL
    END) AS rs_apparel_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_apparel
    ELSE NULL
    END) AS rcom_apparel_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_apparel
    ELSE NULL
    END) AS ns_apparel_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_apparel
    ELSE NULL
    END) AS ncom_apparel_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_apparel
    ELSE NULL
    END) AS rs_apparel_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_apparel
    ELSE NULL
    END) AS rcom_apparel_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_apparel
    ELSE NULL
    END) AS ns_apparel_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_apparel
    ELSE NULL
    END) AS ncom_apparel_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_apparel
    ELSE NULL
    END) AS rs_apparel_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_apparel
    ELSE NULL
    END) AS rcom_apparel_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_beauty
    ELSE NULL
    END) AS ns_beauty_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_beauty
    ELSE NULL
    END) AS ncom_beauty_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_beauty
    ELSE NULL
    END) AS rs_beauty_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_beauty
    ELSE NULL
    END) AS rcom_beauty_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_beauty
    ELSE NULL
    END) AS ns_beauty_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_beauty
    ELSE NULL
    END) AS ncom_beauty_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_beauty
    ELSE NULL
    END) AS rs_beauty_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_beauty
    ELSE NULL
    END) AS rcom_beauty_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_beauty
    ELSE NULL
    END) AS ns_beauty_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_beauty
    ELSE NULL
    END) AS ncom_beauty_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_beauty
    ELSE NULL
    END) AS rs_beauty_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_beauty
    ELSE NULL
    END) AS rcom_beauty_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_designer
    ELSE NULL
    END) AS ns_designer_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_designer
    ELSE NULL
    END) AS ncom_designer_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_designer
    ELSE NULL
    END) AS rs_designer_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_designer
    ELSE NULL
    END) AS rcom_designer_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_designer
    ELSE NULL
    END) AS ns_designer_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_designer
    ELSE NULL
    END) AS ncom_designer_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_designer
    ELSE NULL
    END) AS rs_designer_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_designer
    ELSE NULL
    END) AS rcom_designer_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_designer
    ELSE NULL
    END) AS ns_designer_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_designer
    ELSE NULL
    END) AS ncom_designer_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_designer
    ELSE NULL
    END) AS rs_designer_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_designer
    ELSE NULL
    END) AS rcom_designer_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_home
    ELSE NULL
    END) AS ns_home_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_home
    ELSE NULL
    END) AS ncom_home_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_home
    ELSE NULL
    END) AS rs_home_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_home
    ELSE NULL
    END) AS rcom_home_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_home
    ELSE NULL
    END) AS ns_home_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_home
    ELSE NULL
    END) AS ncom_home_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_home
    ELSE NULL
    END) AS rs_home_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_home
    ELSE NULL
    END) AS rcom_home_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_home
    ELSE NULL
    END) AS ns_home_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_home
    ELSE NULL
    END) AS ncom_home_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_home
    ELSE NULL
    END) AS rs_home_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_home
    ELSE NULL
    END) AS rcom_home_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_merch
    ELSE NULL
    END) AS ns_merch_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_merch
    ELSE NULL
    END) AS ncom_merch_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_merch
    ELSE NULL
    END) AS rs_merch_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_merch
    ELSE NULL
    END) AS rcom_merch_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_merch
    ELSE NULL
    END) AS ns_merch_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_merch
    ELSE NULL
    END) AS ncom_merch_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_merch
    ELSE NULL
    END) AS rs_merch_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_merch
    ELSE NULL
    END) AS rcom_merch_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_merch
    ELSE NULL
    END) AS ns_merch_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_merch
    ELSE NULL
    END) AS ncom_merch_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_merch
    ELSE NULL
    END) AS rs_merch_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_merch
    ELSE NULL
    END) AS rcom_merch_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_shoes
    ELSE NULL
    END) AS ns_shoes_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_shoes
    ELSE NULL
    END) AS ncom_shoes_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_shoes
    ELSE NULL
    END) AS rs_shoes_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_shoes
    ELSE NULL
    END) AS rcom_shoes_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_shoes
    ELSE NULL
    END) AS ns_shoes_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_shoes
    ELSE NULL
    END) AS ncom_shoes_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_shoes
    ELSE NULL
    END) AS rs_shoes_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_shoes
    ELSE NULL
    END) AS rcom_shoes_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_shoes
    ELSE NULL
    END) AS ns_shoes_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_shoes
    ELSE NULL
    END) AS ncom_shoes_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_shoes
    ELSE NULL
    END) AS rs_shoes_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_shoes
    ELSE NULL
    END) AS rcom_shoes_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN trips_other
    ELSE NULL
    END) AS ns_other_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN trips_other
    ELSE NULL
    END) AS ncom_other_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN trips_other
    ELSE NULL
    END) AS rs_other_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN trips_other
    ELSE NULL
    END) AS rcom_other_weekly_trips,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_spend_other
    ELSE NULL
    END) AS ns_other_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_spend_other
    ELSE NULL
    END) AS ncom_other_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_spend_other
    ELSE NULL
    END) AS rs_other_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_spend_other
    ELSE NULL
    END) AS rcom_other_weekly_net_spend,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
    THEN net_units_other
    ELSE NULL
    END) AS ns_other_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
    THEN net_units_other
    ELSE NULL
    END) AS ncom_other_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('3) Rack Stores')
    THEN net_units_other
    ELSE NULL
    END) AS rs_other_weekly_net_units,
  SUM(CASE
    WHEN LOWER(channel) = LOWER('4) Rack.com')
    THEN net_units_other
    ELSE NULL
    END) AS rcom_other_weekly_net_units
 FROM ty
 GROUP BY month_num,
  quarter_num,
  year_num,
  year_id,
  region,
  dma,
  aec,
  predicted_segment,
  loyalty_level,
  loyalty_type,
  new_to_jwn);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (month_num, quarter_num, year_num) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (region) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (dma) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (aec) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (predicted_segment) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (loyalty_level) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (loyalty_type) ON trip_summary_overall;
--COLLECT STATISTICS COLUMN (new_to_jwn) ON trip_summary_overall;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy
(SELECT COALESCE(tso.month_num, tso2.month_num + 100) AS month_num,
  COALESCE(tso.quarter_num, tso2.quarter_num + 10) AS quarter_num,
  COALESCE(tso.year_num, tso2.year_num + 1) AS year_num,
  COALESCE(tso.region, tso2.region) AS region,
  COALESCE(tso.dma, tso2.dma) AS dma,
  COALESCE(tso.aec, tso2.aec) AS aec,
  COALESCE(tso.predicted_segment, tso2.predicted_segment) AS predicted_segment,
  COALESCE(tso.loyalty_level, tso2.loyalty_level) AS loyalty_level,
  COALESCE(tso.loyalty_type, tso2.loyalty_type) AS loyalty_type,
  COALESCE(tso.new_to_jwn, tso2.new_to_jwn) AS new_to_jwn,
  COALESCE(tso.cust_count_fls, 0) AS cust_count_fls_ty,
  COALESCE(tso.cust_count_ncom, 0) AS cust_count_ncom_ty,
  COALESCE(tso.cust_count_rs, 0) AS cust_count_rs_ty,
  COALESCE(tso.cust_count_rcom, 0) AS cust_count_rcom_ty,
  COALESCE(tso.cust_count_stores, 0) AS cust_count_stores_ty,
  COALESCE(tso.cust_count_digital, 0) AS cust_count_digital_ty,
  COALESCE(tso.cust_count_nord, 0) AS cust_count_nord_ty,
  COALESCE(tso.cust_count_rack, 0) AS cust_count_rack_ty,
  COALESCE(tso.cust_count_jwn, 0) AS cust_count_jwn_ty,
  COALESCE(tso.trips_fls, 0) AS trips_fls_ty,
  COALESCE(tso.trips_ncom, 0) AS trips_ncom_ty,
  COALESCE(tso.trips_rs, 0) AS trips_rs_ty,
  COALESCE(tso.trips_rcom, 0) AS trips_rcom_ty,
  COALESCE(tso.trips_jwn, 0) AS trips_jwn_ty,
  CAST(COALESCE(tso.net_spend_fls, 0) AS NUMERIC) AS net_spend_fls_ty,
  CAST(COALESCE(tso.net_spend_ncom, 0) AS NUMERIC) AS net_spend_ncom_ty,
  CAST(COALESCE(tso.net_spend_rs, 0) AS NUMERIC) AS net_spend_rs_ty,
  CAST(COALESCE(tso.net_spend_rcom, 0) AS NUMERIC) AS net_spend_rcom_ty,
  COALESCE(tso.net_spend_jwn, 0) AS net_spend_jwn_ty,
  CAST(COALESCE(tso.gross_spend_fls, 0) AS NUMERIC) AS gross_spend_fls_ty,
  CAST(COALESCE(tso.gross_spend_ncom, 0) AS NUMERIC) AS gross_spend_ncom_ty,
  CAST(COALESCE(tso.gross_spend_rs, 0) AS NUMERIC) AS gross_spend_rs_ty,
  CAST(COALESCE(tso.gross_spend_rcom, 0) AS NUMERIC) AS gross_spend_rcom_ty,
  COALESCE(tso.gross_spend_jwn, 0) AS gross_spend_jwn_ty,
  CAST(COALESCE(tso.net_units_fls, 0) AS NUMERIC) AS net_units_fls_ty,
  CAST(COALESCE(tso.net_units_ncom, 0) AS NUMERIC) AS net_units_ncom_ty,
  CAST(COALESCE(tso.net_units_rs, 0) AS NUMERIC) AS net_units_rs_ty,
  CAST(COALESCE(tso.net_units_rcom, 0) AS NUMERIC) AS net_units_rcom_ty,
  COALESCE(tso.net_units_jwn, 0) AS net_units_jwn_ty,
  CAST(COALESCE(tso.gross_units_fls, 0) AS NUMERIC) AS gross_units_fls_ty,
  CAST(COALESCE(tso.gross_units_ncom, 0) AS NUMERIC) AS gross_units_ncom_ty,
  CAST(COALESCE(tso.gross_units_rs, 0) AS NUMERIC) AS gross_units_rs_ty,
  CAST(COALESCE(tso.gross_units_rcom, 0) AS NUMERIC) AS gross_units_rcom_ty,
  COALESCE(tso.gross_units_jwn, 0) AS gross_units_jwn_ty,
  COALESCE(tso.ns_accessories_weekly_trips, 0) AS ns_accessories_trips_ty,
  COALESCE(tso.ncom_accessories_weekly_trips, 0) AS ncom_accessories_trips_ty,
  COALESCE(tso.rs_accessories_weekly_trips, 0) AS rs_accessories_trips_ty,
  COALESCE(tso.rcom_accessories_weekly_trips, 0) AS rcom_accessories_trips_ty,
  CAST(COALESCE(tso.ns_accessories_weekly_net_spend, 0) AS NUMERIC) AS ns_accessories_net_spend_ty,
  CAST(COALESCE(tso.ncom_accessories_weekly_net_spend, 0) AS NUMERIC) AS ncom_accessories_net_spend_ty,
  CAST(COALESCE(tso.rs_accessories_weekly_net_spend, 0) AS NUMERIC) AS rs_accessories_net_spend_ty,
  CAST(COALESCE(tso.rcom_accessories_weekly_net_spend, 0) AS NUMERIC) AS rcom_accessories_net_spend_ty,
  CAST(COALESCE(trunc(tso.ns_accessories_weekly_net_units, 0)) AS INTEGER) AS ns_accessories_net_units_ty,
  CAST(COALESCE(trunc(tso.ncom_accessories_weekly_net_units, 0)) AS INTEGER) AS ncom_accessories_net_units_ty,
  CAST(COALESCE(trunc(tso.rs_accessories_weekly_net_units, 0)) AS INTEGER) AS rs_accessories_net_units_ty,
  CAST(COALESCE(trunc(tso.rcom_accessories_weekly_net_units, 0)) AS INTEGER) AS rcom_accessories_net_units_ty,
  COALESCE(tso.ns_apparel_weekly_trips, 0) AS ns_apparel_trips_ty,
  COALESCE(tso.ncom_apparel_weekly_trips, 0) AS ncom_apparel_trips_ty,
  COALESCE(tso.rs_apparel_weekly_trips, 0) AS rs_apparel_trips_ty,
  COALESCE(tso.rcom_apparel_weekly_trips, 0) AS rcom_apparel_trips_ty,
  CAST(COALESCE(tso.ns_apparel_weekly_net_spend, 0) AS NUMERIC) AS ns_apparel_net_spend_ty,
  CAST(COALESCE(tso.ncom_apparel_weekly_net_spend, 0) AS NUMERIC) AS ncom_apparel_net_spend_ty,
  CAST(COALESCE(tso.rs_apparel_weekly_net_spend, 0) AS NUMERIC) AS rs_apparel_net_spend_ty,
  CAST(COALESCE(tso.rcom_apparel_weekly_net_spend, 0) AS NUMERIC) AS rcom_apparel_net_spend_ty,
  CAST(COALESCE(tso.ns_apparel_weekly_net_units, 0) AS NUMERIC) AS ns_apparel_net_units_ty,
  CAST(COALESCE(tso.ncom_apparel_weekly_net_units, 0) AS NUMERIC) AS ncom_apparel_net_units_ty,
  CAST(COALESCE(tso.rs_apparel_weekly_net_units, 0) AS NUMERIC) AS rs_apparel_net_units_ty,
  CAST(COALESCE(tso.rcom_apparel_weekly_net_units, 0) AS NUMERIC) AS rcom_apparel_net_units_ty,
  COALESCE(tso.ns_beauty_weekly_trips, 0) AS ns_beauty_trips_ty,
  COALESCE(tso.ncom_beauty_weekly_trips, 0) AS ncom_beauty_trips_ty,
  COALESCE(tso.rs_beauty_weekly_trips, 0) AS rs_beauty_trips_ty,
  COALESCE(tso.rcom_beauty_weekly_trips, 0) AS rcom_beauty_trips_ty,
  CAST(COALESCE(tso.ns_beauty_weekly_net_spend, 0) AS NUMERIC) AS ns_beauty_net_spend_ty,
  CAST(COALESCE(tso.ncom_beauty_weekly_net_spend, 0) AS NUMERIC) AS ncom_beauty_net_spend_ty,
  CAST(COALESCE(tso.rs_beauty_weekly_net_spend, 0) AS NUMERIC) AS rs_beauty_net_spend_ty,
  CAST(COALESCE(tso.rcom_beauty_weekly_net_spend, 0) AS NUMERIC) AS rcom_beauty_net_spend_ty,
  CAST(COALESCE(tso.ns_beauty_weekly_net_units, 0) AS NUMERIC) AS ns_beauty_net_units_ty,
  CAST(COALESCE(tso.ncom_beauty_weekly_net_units, 0) AS NUMERIC) AS ncom_beauty_net_units_ty,
  CAST(COALESCE(tso.rs_beauty_weekly_net_units, 0) AS NUMERIC) AS rs_beauty_net_units_ty,
  CAST(COALESCE(tso.rcom_beauty_weekly_net_units, 0) AS NUMERIC) AS rcom_beauty_net_units_ty,
  COALESCE(tso.ns_designer_weekly_trips, 0) AS ns_designer_trips_ty,
  COALESCE(tso.ncom_designer_weekly_trips, 0) AS ncom_designer_trips_ty,
  COALESCE(tso.rs_designer_weekly_trips, 0) AS rs_designer_trips_ty,
  COALESCE(tso.rcom_designer_weekly_trips, 0) AS rcom_designer_trips_ty,
  CAST(COALESCE(tso.ns_designer_weekly_net_spend, 0) AS NUMERIC) AS ns_designer_net_spend_ty,
  CAST(COALESCE(tso.ncom_designer_weekly_net_spend, 0) AS NUMERIC) AS ncom_designer_net_spend_ty,
  CAST(COALESCE(tso.rs_designer_weekly_net_spend, 0) AS NUMERIC) AS rs_designer_net_spend_ty,
  CAST(COALESCE(tso.rcom_designer_weekly_net_spend, 0) AS NUMERIC) AS rcom_designer_net_spend_ty,
  CAST(COALESCE(tso.ns_designer_weekly_net_units, 0) AS NUMERIC) AS ns_designer_net_units_ty,
  CAST(COALESCE(tso.ncom_designer_weekly_net_units, 0) AS NUMERIC) AS ncom_designer_net_units_ty,
  CAST(COALESCE(tso.rs_designer_weekly_net_units, 0) AS NUMERIC) AS rs_designer_net_units_ty,
  CAST(COALESCE(tso.rcom_designer_weekly_net_units, 0) AS NUMERIC) AS rcom_designer_net_units_ty,
  COALESCE(tso.ns_home_weekly_trips, 0) AS ns_home_trips_ty,
  COALESCE(tso.ncom_home_weekly_trips, 0) AS ncom_home_trips_ty,
  COALESCE(tso.rs_home_weekly_trips, 0) AS rs_home_trips_ty,
  COALESCE(tso.rcom_home_weekly_trips, 0) AS rcom_home_trips_ty,
  CAST(COALESCE(tso.ns_home_weekly_net_spend, 0) AS NUMERIC) AS ns_home_net_spend_ty,
  CAST(COALESCE(tso.ncom_home_weekly_net_spend, 0) AS NUMERIC) AS ncom_home_net_spend_ty,
  CAST(COALESCE(tso.rs_home_weekly_net_spend, 0) AS NUMERIC) AS rs_home_net_spend_ty,
  CAST(COALESCE(tso.rcom_home_weekly_net_spend, 0) AS NUMERIC) AS rcom_home_net_spend_ty,
  CAST(COALESCE(tso.ns_home_weekly_net_units, 0) AS NUMERIC) AS ns_home_net_units_ty,
  CAST(COALESCE(tso.ncom_home_weekly_net_units, 0) AS NUMERIC) AS ncom_home_net_units_ty,
  CAST(COALESCE(tso.rs_home_weekly_net_units, 0) AS NUMERIC) AS rs_home_net_units_ty,
  CAST(COALESCE(tso.rcom_home_weekly_net_units, 0) AS NUMERIC) AS rcom_home_net_units_ty,
  COALESCE(tso.ns_merch_weekly_trips, 0) AS ns_merch_trips_ty,
  COALESCE(tso.ncom_merch_weekly_trips, 0) AS ncom_merch_trips_ty,
  COALESCE(tso.rs_merch_weekly_trips, 0) AS rs_merch_trips_ty,
  COALESCE(tso.rcom_merch_weekly_trips, 0) AS rcom_merch_trips_ty,
  CAST(COALESCE(tso.ns_merch_weekly_net_spend, 0) AS NUMERIC) AS ns_merch_net_spend_ty,
  CAST(COALESCE(tso.ncom_merch_weekly_net_spend, 0) AS NUMERIC) AS ncom_merch_net_spend_ty,
  CAST(COALESCE(tso.rs_merch_weekly_net_spend, 0) AS NUMERIC) AS rs_merch_net_spend_ty,
  CAST(COALESCE(tso.rcom_merch_weekly_net_spend, 0) AS NUMERIC) AS rcom_merch_net_spend_ty,
  CAST(COALESCE(tso.ns_merch_weekly_net_units, 0) AS NUMERIC) AS ns_merch_net_units_ty,
  CAST(COALESCE(tso.ncom_merch_weekly_net_units, 0) AS NUMERIC) AS ncom_merch_net_units_ty,
  CAST(COALESCE(tso.rs_merch_weekly_net_units, 0) AS NUMERIC) AS rs_merch_net_units_ty,
  CAST(COALESCE(tso.rcom_merch_weekly_net_units, 0) AS NUMERIC) AS rcom_merch_net_units_ty,
  COALESCE(tso.ns_shoes_weekly_trips, 0) AS ns_shoes_trips_ty,
  COALESCE(tso.ncom_shoes_weekly_trips, 0) AS ncom_shoes_trips_ty,
  COALESCE(tso.rs_shoes_weekly_trips, 0) AS rs_shoes_trips_ty,
  COALESCE(tso.rcom_shoes_weekly_trips, 0) AS rcom_shoes_trips_ty,
  CAST(COALESCE(tso.ns_shoes_weekly_net_spend, 0) AS NUMERIC) AS ns_shoes_net_spend_ty,
  CAST(COALESCE(tso.ncom_shoes_weekly_net_spend, 0) AS NUMERIC) AS ncom_shoes_net_spend_ty,
  CAST(COALESCE(tso.rs_shoes_weekly_net_spend, 0) AS NUMERIC) AS rs_shoes_net_spend_ty,
  CAST(COALESCE(tso.rcom_shoes_weekly_net_spend, 0) AS NUMERIC) AS rcom_shoes_net_spend_ty,
  CAST(COALESCE(tso.ns_shoes_weekly_net_units, 0) AS NUMERIC) AS ns_shoes_net_units_ty,
  CAST(COALESCE(tso.ncom_shoes_weekly_net_units, 0) AS NUMERIC) AS ncom_shoes_net_units_ty,
  CAST(COALESCE(tso.rs_shoes_weekly_net_units, 0) AS NUMERIC) AS rs_shoes_net_units_ty,
  CAST(COALESCE(tso.rcom_shoes_weekly_net_units, 0) AS NUMERIC) AS rcom_shoes_net_units_ty,
  COALESCE(tso.ns_other_weekly_trips, 0) AS ns_other_trips_ty,
  COALESCE(tso.ncom_other_weekly_trips, 0) AS ncom_other_trips_ty,
  COALESCE(tso.rs_other_weekly_trips, 0) AS rs_other_trips_ty,
  COALESCE(tso.rcom_other_weekly_trips, 0) AS rcom_other_trips_ty,
  CAST(COALESCE(tso.ns_other_weekly_net_spend, 0) AS NUMERIC) AS ns_other_net_spend_ty,
  CAST(COALESCE(tso.ncom_other_weekly_net_spend, 0) AS NUMERIC) AS ncom_other_net_spend_ty,
  CAST(COALESCE(tso.rs_other_weekly_net_spend, 0) AS NUMERIC) AS rs_other_net_spend_ty,
  CAST(COALESCE(tso.rcom_other_weekly_net_spend, 0) AS NUMERIC) AS rcom_other_net_spend_ty,
  CAST(COALESCE(tso.ns_other_weekly_net_units, 0) AS NUMERIC) AS ns_other_net_units_ty,
  CAST(COALESCE(tso.ncom_other_weekly_net_units, 0) AS NUMERIC) AS ncom_other_net_units_ty,
  CAST(COALESCE(tso.rs_other_weekly_net_units, 0) AS NUMERIC) AS rs_other_net_units_ty,
  CAST(COALESCE(tso.rcom_other_weekly_net_units, 0) AS NUMERIC) AS rcom_other_net_units_ty,
  COALESCE(tso2.cust_count_fls, 0) AS cust_count_fls_ly,
  COALESCE(tso2.cust_count_ncom, 0) AS cust_count_ncom_ly,
  COALESCE(tso2.cust_count_rs, 0) AS cust_count_rs_ly,
  COALESCE(tso2.cust_count_rcom, 0) AS cust_count_rcom_ly,
  COALESCE(tso2.cust_count_stores, 0) AS cust_count_stores_ly,
  COALESCE(tso2.cust_count_digital, 0) AS cust_count_digital_ly,
  COALESCE(tso2.cust_count_nord, 0) AS cust_count_nord_ly,
  COALESCE(tso2.cust_count_rack, 0) AS cust_count_rack_ly,
  COALESCE(tso2.cust_count_jwn, 0) AS cust_count_jwn_ly,
  COALESCE(tso2.trips_fls, 0) AS trips_fls_ly,
  COALESCE(tso2.trips_ncom, 0) AS trips_ncom_ly,
  COALESCE(tso2.trips_rs, 0) AS trips_rs_ly,
  COALESCE(tso2.trips_rcom, 0) AS trips_rcom_ly,
  COALESCE(tso2.trips_jwn, 0) AS trips_jwn_ly,
  CAST(COALESCE(tso2.net_spend_fls, 0) AS NUMERIC) AS net_spend_fls_ly,
  CAST(COALESCE(tso2.net_spend_ncom, 0) AS NUMERIC) AS net_spend_ncom_ly,
  CAST(COALESCE(tso2.net_spend_rs, 0) AS NUMERIC) AS net_spend_rs_ly,
  CAST(COALESCE(tso2.net_spend_rcom, 0) AS NUMERIC) AS net_spend_rcom_ly,
  COALESCE(tso2.net_spend_jwn, 0) AS net_spend_jwn_ly,
  CAST(COALESCE(tso2.gross_spend_fls, 0) AS NUMERIC) AS gross_spend_fls_ly,
  CAST(COALESCE(tso2.gross_spend_ncom, 0) AS NUMERIC) AS gross_spend_ncom_ly,
  CAST(COALESCE(tso2.gross_spend_rs, 0) AS NUMERIC) AS gross_spend_rs_ly,
  CAST(COALESCE(tso2.gross_spend_rcom, 0) AS NUMERIC) AS gross_spend_rcom_ly,
  COALESCE(tso2.gross_spend_jwn, 0) AS gross_spend_jwn_ly,
  CAST(COALESCE(tso2.net_units_fls, 0) AS NUMERIC) AS net_units_fls_ly,
  CAST(COALESCE(tso2.net_units_ncom, 0) AS NUMERIC) AS net_units_ncom_ly,
  CAST(COALESCE(tso2.net_units_rs, 0) AS NUMERIC) AS net_units_rs_ly,
  CAST(COALESCE(tso2.net_units_rcom, 0) AS NUMERIC) AS net_units_rcom_ly,
  COALESCE(tso2.net_units_jwn, 0) AS net_units_jwn_ly,
  CAST(COALESCE(tso2.gross_units_fls, 0) AS NUMERIC) AS gross_units_fls_ly,
  CAST(COALESCE(tso2.gross_units_ncom, 0) AS NUMERIC) AS gross_units_ncom_ly,
  CAST(COALESCE(tso2.gross_units_rs, 0) AS NUMERIC) AS gross_units_rs_ly,
  CAST(COALESCE(tso2.gross_units_rcom, 0) AS NUMERIC) AS gross_units_rcom_ly,
  COALESCE(tso2.gross_units_jwn, 0) AS gross_units_jwn_ly,
  COALESCE(tso2.ns_accessories_weekly_trips, 0) AS ns_accessories_trips_ly,
  COALESCE(tso2.ncom_accessories_weekly_trips, 0) AS ncom_accessories_trips_ly,
  COALESCE(tso2.rs_accessories_weekly_trips, 0) AS rs_accessories_trips_ly,
  COALESCE(tso2.rcom_accessories_weekly_trips, 0) AS rcom_accessories_trips_ly,
  CAST(COALESCE(tso2.ns_accessories_weekly_net_spend, 0) AS NUMERIC) AS ns_accessories_net_spend_ly,
  CAST(COALESCE(tso2.ncom_accessories_weekly_net_spend, 0) AS NUMERIC) AS ncom_accessories_net_spend_ly,
  CAST(COALESCE(tso2.rs_accessories_weekly_net_spend, 0) AS NUMERIC) AS rs_accessories_net_spend_ly,
  CAST(COALESCE(tso2.rcom_accessories_weekly_net_spend, 0) AS NUMERIC) AS rcom_accessories_net_spend_ly,
  CAST(COALESCE(trunc(tso2.ns_accessories_weekly_net_units, 0)) AS INTEGER) AS ns_accessories_net_units_ly,
  CAST(COALESCE(trunc(tso2.ncom_accessories_weekly_net_units, 0)) AS INTEGER) AS ncom_accessories_net_units_ly,
  CAST(COALESCE(trunc(tso2.rs_accessories_weekly_net_units, 0)) AS INTEGER) AS rs_accessories_net_units_ly,
  CAST(COALESCE(trunc(tso2.rcom_accessories_weekly_net_units, 0)) AS INTEGER) AS rcom_accessories_net_units_ly,
  COALESCE(tso2.ns_apparel_weekly_trips, 0) AS ns_apparel_trips_ly,
  COALESCE(tso2.ncom_apparel_weekly_trips, 0) AS ncom_apparel_trips_ly,
  COALESCE(tso2.rs_apparel_weekly_trips, 0) AS rs_apparel_trips_ly,
  COALESCE(tso2.rcom_apparel_weekly_trips, 0) AS rcom_apparel_trips_ly,
  CAST(COALESCE(tso2.ns_apparel_weekly_net_spend, 0) AS NUMERIC) AS ns_apparel_net_spend_ly,
  CAST(COALESCE(tso2.ncom_apparel_weekly_net_spend, 0) AS NUMERIC) AS ncom_apparel_net_spend_ly,
  CAST(COALESCE(tso2.rs_apparel_weekly_net_spend, 0) AS NUMERIC) AS rs_apparel_net_spend_ly,
  CAST(COALESCE(tso2.rcom_apparel_weekly_net_spend, 0) AS NUMERIC) AS rcom_apparel_net_spend_ly,
  CAST(COALESCE(tso2.ns_apparel_weekly_net_units, 0) AS NUMERIC) AS ns_apparel_net_units_ly,
  CAST(COALESCE(tso2.ncom_apparel_weekly_net_units, 0) AS NUMERIC) AS ncom_apparel_net_units_ly,
  CAST(COALESCE(tso2.rs_apparel_weekly_net_units, 0) AS NUMERIC) AS rs_apparel_net_units_ly,
  CAST(COALESCE(tso2.rcom_apparel_weekly_net_units, 0) AS NUMERIC) AS rcom_apparel_net_units_ly,
  COALESCE(tso2.ns_beauty_weekly_trips, 0) AS ns_beauty_trips_ly,
  COALESCE(tso2.ncom_beauty_weekly_trips, 0) AS ncom_beauty_trips_ly,
  COALESCE(tso2.rs_beauty_weekly_trips, 0) AS rs_beauty_trips_ly,
  COALESCE(tso2.rcom_beauty_weekly_trips, 0) AS rcom_beauty_trips_ly,
  CAST(COALESCE(tso2.ns_beauty_weekly_net_spend, 0) AS NUMERIC) AS ns_beauty_net_spend_ly,
  CAST(COALESCE(tso2.ncom_beauty_weekly_net_spend, 0) AS NUMERIC) AS ncom_beauty_net_spend_ly,
  CAST(COALESCE(tso2.rs_beauty_weekly_net_spend, 0) AS NUMERIC) AS rs_beauty_net_spend_ly,
  CAST(COALESCE(tso2.rcom_beauty_weekly_net_spend, 0) AS NUMERIC) AS rcom_beauty_net_spend_ly,
  CAST(COALESCE(tso2.ns_beauty_weekly_net_units, 0) AS NUMERIC) AS ns_beauty_net_units_ly,
  CAST(COALESCE(tso2.ncom_beauty_weekly_net_units, 0) AS NUMERIC) AS ncom_beauty_net_units_ly,
  CAST(COALESCE(tso2.rs_beauty_weekly_net_units, 0) AS NUMERIC) AS rs_beauty_net_units_ly,
  CAST(COALESCE(tso2.rcom_beauty_weekly_net_units, 0) AS NUMERIC) AS rcom_beauty_net_units_ly,
  COALESCE(tso2.ns_designer_weekly_trips, 0) AS ns_designer_trips_ly,
  COALESCE(tso2.ncom_designer_weekly_trips, 0) AS ncom_designer_trips_ly,
  COALESCE(tso2.rs_designer_weekly_trips, 0) AS rs_designer_trips_ly,
  COALESCE(tso2.rcom_designer_weekly_trips, 0) AS rcom_designer_trips_ly,
  CAST(COALESCE(tso2.ns_designer_weekly_net_spend, 0) AS NUMERIC) AS ns_designer_net_spend_ly,
  CAST(COALESCE(tso2.ncom_designer_weekly_net_spend, 0) AS NUMERIC) AS ncom_designer_net_spend_ly,
  CAST(COALESCE(tso2.rs_designer_weekly_net_spend, 0) AS NUMERIC) AS rs_designer_net_spend_ly,
  CAST(COALESCE(tso2.rcom_designer_weekly_net_spend, 0) AS NUMERIC) AS rcom_designer_net_spend_ly,
  CAST(COALESCE(tso2.ns_designer_weekly_net_units, 0) AS NUMERIC) AS ns_designer_net_units_ly,
  CAST(COALESCE(tso2.ncom_designer_weekly_net_units, 0) AS NUMERIC) AS ncom_designer_net_units_ly,
  CAST(COALESCE(tso2.rs_designer_weekly_net_units, 0) AS NUMERIC) AS rs_designer_net_units_ly,
  CAST(COALESCE(tso2.rcom_designer_weekly_net_units, 0) AS NUMERIC) AS rcom_designer_net_units_ly,
  COALESCE(tso2.ns_home_weekly_trips, 0) AS ns_home_trips_ly,
  COALESCE(tso2.ncom_home_weekly_trips, 0) AS ncom_home_trips_ly,
  COALESCE(tso2.rs_home_weekly_trips, 0) AS rs_home_trips_ly,
  COALESCE(tso2.rcom_home_weekly_trips, 0) AS rcom_home_trips_ly,
  CAST(COALESCE(tso2.ns_home_weekly_net_spend, 0) AS NUMERIC) AS ns_home_net_spend_ly,
  CAST(COALESCE(tso2.ncom_home_weekly_net_spend, 0) AS NUMERIC) AS ncom_home_net_spend_ly,
  CAST(COALESCE(tso2.rs_home_weekly_net_spend, 0) AS NUMERIC) AS rs_home_net_spend_ly,
  CAST(COALESCE(tso2.rcom_home_weekly_net_spend, 0) AS NUMERIC) AS rcom_home_net_spend_ly,
  CAST(COALESCE(tso2.ns_home_weekly_net_units, 0) AS NUMERIC) AS ns_home_net_units_ly,
  CAST(COALESCE(tso2.ncom_home_weekly_net_units, 0) AS NUMERIC) AS ncom_home_net_units_ly,
  CAST(COALESCE(tso2.rs_home_weekly_net_units, 0) AS NUMERIC) AS rs_home_net_units_ly,
  CAST(COALESCE(tso2.rcom_home_weekly_net_units, 0) AS NUMERIC) AS rcom_home_net_units_ly,
  COALESCE(tso2.ns_merch_weekly_trips, 0) AS ns_merch_trips_ly,
  COALESCE(tso2.ncom_merch_weekly_trips, 0) AS ncom_merch_trips_ly,
  COALESCE(tso2.rs_merch_weekly_trips, 0) AS rs_merch_trips_ly,
  COALESCE(tso2.rcom_merch_weekly_trips, 0) AS rcom_merch_trips_ly,
  CAST(COALESCE(tso2.ns_merch_weekly_net_spend, 0) AS NUMERIC) AS ns_merch_net_spend_ly,
  CAST(COALESCE(tso2.ncom_merch_weekly_net_spend, 0) AS NUMERIC) AS ncom_merch_net_spend_ly,
  CAST(COALESCE(tso2.rs_merch_weekly_net_spend, 0) AS NUMERIC) AS rs_merch_net_spend_ly,
  CAST(COALESCE(tso2.rcom_merch_weekly_net_spend, 0) AS NUMERIC) AS rcom_merch_net_spend_ly,
  CAST(COALESCE(tso2.ns_merch_weekly_net_units, 0) AS NUMERIC) AS ns_merch_net_units_ly,
  CAST(COALESCE(tso2.ncom_merch_weekly_net_units, 0) AS NUMERIC) AS ncom_merch_net_units_ly,
  CAST(COALESCE(tso2.rs_merch_weekly_net_units, 0) AS NUMERIC) AS rs_merch_net_units_ly,
  CAST(COALESCE(tso2.rcom_merch_weekly_net_units, 0) AS NUMERIC) AS rcom_merch_net_units_ly,
  COALESCE(tso2.ns_shoes_weekly_trips, 0) AS ns_shoes_trips_ly,
  COALESCE(tso2.ncom_shoes_weekly_trips, 0) AS ncom_shoes_trips_ly,
  COALESCE(tso2.rs_shoes_weekly_trips, 0) AS rs_shoes_trips_ly,
  COALESCE(tso2.rcom_shoes_weekly_trips, 0) AS rcom_shoes_trips_ly,
  CAST(COALESCE(tso2.ns_shoes_weekly_net_spend, 0) AS NUMERIC) AS ns_shoes_net_spend_ly,
  CAST(COALESCE(tso2.ncom_shoes_weekly_net_spend, 0) AS NUMERIC) AS ncom_shoes_net_spend_ly,
  CAST(COALESCE(tso2.rs_shoes_weekly_net_spend, 0) AS NUMERIC) AS rs_shoes_net_spend_ly,
  CAST(COALESCE(tso2.rcom_shoes_weekly_net_spend, 0) AS NUMERIC) AS rcom_shoes_net_spend_ly,
  CAST(COALESCE(tso2.ns_shoes_weekly_net_units, 0) AS NUMERIC) AS ns_shoes_net_units_ly,
  CAST(COALESCE(tso2.ncom_shoes_weekly_net_units, 0) AS NUMERIC) AS ncom_shoes_net_units_ly,
  CAST(COALESCE(tso2.rs_shoes_weekly_net_units, 0) AS NUMERIC) AS rs_shoes_net_units_ly,
  CAST(COALESCE(tso2.rcom_shoes_weekly_net_units, 0) AS NUMERIC) AS rcom_shoes_net_units_ly,
  COALESCE(tso2.ns_other_weekly_trips, 0) AS ns_other_trips_ly,
  COALESCE(tso2.ncom_other_weekly_trips, 0) AS ncom_other_trips_ly,
  COALESCE(tso2.rs_other_weekly_trips, 0) AS rs_other_trips_ly,
  COALESCE(tso2.rcom_other_weekly_trips, 0) AS rcom_other_trips_ly,
  CAST(COALESCE(tso2.ns_other_weekly_net_spend, 0) AS NUMERIC) AS ns_other_net_spend_ly,
  CAST(COALESCE(tso2.ncom_other_weekly_net_spend, 0) AS NUMERIC) AS ncom_other_net_spend_ly,
  CAST(COALESCE(tso2.rs_other_weekly_net_spend, 0) AS NUMERIC) AS rs_other_net_spend_ly,
  CAST(COALESCE(tso2.rcom_other_weekly_net_spend, 0) AS NUMERIC) AS rcom_other_net_spend_ly,
  CAST(COALESCE(tso2.ns_other_weekly_net_units, 0) AS NUMERIC) AS ns_other_net_units_ly,
  CAST(COALESCE(tso2.ncom_other_weekly_net_units, 0) AS NUMERIC) AS ncom_other_net_units_ly,
  CAST(COALESCE(tso2.rs_other_weekly_net_units, 0) AS NUMERIC) AS rs_other_net_units_ly,
  CAST(COALESCE(tso2.rcom_other_weekly_net_units, 0) AS NUMERIC) AS rcom_other_net_units_ly,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM trip_summary_overall AS tso
  FULL JOIN trip_summary_overall AS tso2 ON MOD(tso.month_num, 100) = MOD(tso2.month_num, 100) AND tso.quarter_num =
              tso2.quarter_num + 10 AND tso.year_num = tso2.year_num + 1 AND tso.year_id = tso2.year_id AND LOWER(tso.region
           ) = LOWER(tso2.region) AND LOWER(tso.dma) = LOWER(tso2.dma) AND LOWER(tso.aec) = LOWER(tso2.aec) AND LOWER(tso
        .predicted_segment) = LOWER(tso2.predicted_segment) AND LOWER(tso.loyalty_level) = LOWER(tso2.loyalty_level) AND
     LOWER(tso.loyalty_type) = LOWER(tso2.loyalty_type) AND tso.new_to_jwn = tso2.new_to_jwn
 WHERE COALESCE(tso.month_num, tso2.month_num + 100) IN (SELECT month_num
    FROM date_lookup));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (month_num, quarter_num, year_num) ON `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
--COLLECT STATISTICS COLUMN (region) ON `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
--COLLECT STATISTICS COLUMN (dma) ON `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
--COLLECT STATISTICS COLUMN (aec) ON `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
--COLLECT STATISTICS COLUMN (predicted_segment) ON `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
--COLLECT STATISTICS COLUMN (loyalty_level) ON `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
--COLLECT STATISTICS COLUMN (loyalty_type) ON `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
--COLLECT STATISTICS COLUMN (new_to_jwn) ON `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_monthly_yoy;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
