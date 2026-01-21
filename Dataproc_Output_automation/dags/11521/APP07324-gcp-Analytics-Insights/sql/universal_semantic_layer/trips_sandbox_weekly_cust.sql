
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup

AS
SELECT dc.week_num,
 dc.month_num,
 dc.quarter_num,
 dc.year_num,
  CASE
  WHEN dc.week_num >= WN.week_num - 100 AND dc.week_num <= WN.week_num
  THEN 'TY'
  WHEN dc.week_num >= WN.week_num - 200 AND dc.week_num < WN.week_num - 100
  THEN 'LY'
  ELSE 'NA'
  END AS year_id,
 MIN(dc.day_date) AS ty_start_dt,
 MAX(dc.day_date) AS ty_end_dt
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
 LEFT JOIN (SELECT DISTINCT week_num
  FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
  WHERE day_date = CURRENT_DATE('PST8PDT')) AS WN ON TRUE
WHERE dc.week_num >= (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = CURRENT_DATE('PST8PDT')) - 300
 AND dc.week_num <= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = CURRENT_DATE('PST8PDT'))
GROUP BY dc.week_num,
 dc.month_num,
 dc.quarter_num,
 dc.year_num,
 year_id ;

 
CREATE TEMPORARY TABLE IF NOT EXISTS upc_lookup_table

AS
SELECT DISTINCT LTRIM(upc.upc_num, '0') AS upc_num,
  CASE
  WHEN 
  
  cast(trunc(sku.div_num) as integer)
  IN (310, 340, 345, 351, 360, 365, 600, 700, 800, 900)
  THEN sku.div_num
  ELSE - 1
  END AS div_num,
  CASE
  WHEN sku.div_num IN (310, 340, 345, 351, 360, 365, 600, 700, 800, 900)
  THEN sku.div_desc
  ELSE 'OTHER'
  END AS div_desc,
  cast(trunc(sku.grp_num ) as integer)AS subdiv_num,
 
 sku.grp_desc AS subdiv_name,
 cast(trunc(sku.dept_num) as integer) AS dept_num,

 sku.dept_desc AS dept_name,
 sku.class_num,
 sku.sbclass_num,
 sku.brand_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS upc ON LOWER(sku.rms_sku_num) = LOWER(upc.rms_sku_num) AND LOWER(sku.channel_country
    ) = LOWER(upc.channel_country)
WHERE LOWER(sku.channel_country) = LOWER('US')
 AND sku.div_num IN (310, 345, 360, 340, 365, 351, 700);

CREATE TEMPORARY TABLE IF NOT EXISTS sales_information

AS
SELECT scf.sale_date,
 rc.week_num AS week_num_realigned,
 rc.month_num AS month_num_realigned,
 rc.quarter_num AS quarter_num_realigned,
 rc.year_num AS year_num_realigned,
 scf.week_num,
 scf.month_num,
 scf.quarter_num,
 scf.year_num,
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
 scf.region,
 scf.dma,
 scf.engagement_cohort,
 scf.predicted_segment,
 scf.loyalty_level,
 scf.loyalty_type,
 scf.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
WHERE rc.week_num >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 4 MONTH))
 AND rc.week_num <= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = CURRENT_DATE('PST8PDT'));

CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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


CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

CREATE TEMPORARY TABLE IF NOT EXISTS usl_trips_sandbox_weekly_cust

AS
SELECT COALESCE(a.week_num, b.week_num) AS week_num,
 COALESCE(a.month_num, b.month_num) AS month_num,
 COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
 COALESCE(a.year_num, b.year_num) AS year_num,
 COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned,
 COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned,
 COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned,
 COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned,
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
  CASE
  WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1
  THEN 1
  ELSE 0
  END AS div_accessories_flag,
  CASE
  WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1
  THEN 1
  ELSE 0
  END AS div_apparel_flag,
  CASE
  WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1
  THEN 1
  ELSE 0
  END AS div_beauty_flag,
  CASE
  WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1
  THEN 1
  ELSE 0
  END AS div_designer_flag,
  CASE
  WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1
  THEN 1
  ELSE 0
  END AS div_home_flag,
  CASE
  WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1
  THEN 1
  ELSE 0
  END AS div_merch_flag,
  CASE
  WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1
  THEN 1
  ELSE 0
  END AS div_shoes_flag,
  CASE
  WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1
  THEN 1
  ELSE 0
  END AS div_other_flag
FROM ty_positive AS a
 FULL JOIN ty_negative AS b ON a.week_num = b.week_num AND a.month_num = b.month_num AND a.quarter_num = b.quarter_num
                AND a.year_num = b.year_num AND a.week_num_realigned = b.week_num_realigned AND a.month_num_realigned =
               b.month_num_realigned AND a.quarter_num_realigned = b.quarter_num_realigned AND a.year_num_realigned = b
             .year_num_realigned AND LOWER(a.year_id) = LOWER(b.year_id) AND LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a
           .channel) = LOWER(b.channel) AND LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region) AND
       LOWER(a.dma) = LOWER(b.dma) AND LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment
      ) = LOWER(b.predicted_segment) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) =
   LOWER(b.loyalty_type);

DROP TABLE IF EXISTS sales_information;


CREATE TEMPORARY TABLE IF NOT EXISTS sales_information

AS
SELECT scf.sale_date,
 rc.week_num AS week_num_realigned,
 rc.month_num AS month_num_realigned,
 rc.quarter_num AS quarter_num_realigned,
 rc.year_num AS year_num_realigned,
 scf.week_num,
 scf.month_num,
 scf.quarter_num,
 scf.year_num,
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
 scf.region,
 scf.dma,
 scf.engagement_cohort,
 scf.predicted_segment,
 scf.loyalty_level,
 scf.loyalty_type,
 scf.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
WHERE rc.week_num >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
 AND rc.week_num < (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 4 MONTH));


DROP TABLE IF EXISTS ty_positive;


CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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


DROP TABLE IF EXISTS ty_negative;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

INSERT INTO usl_trips_sandbox_weekly_cust
(SELECT COALESCE(a.week_num, b.week_num) AS week_num,
  COALESCE(a.month_num, b.month_num) AS month_num,
  COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
  COALESCE(a.year_num, b.year_num) AS year_num,
  COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned,
  COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned,
  COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned,
  COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned,
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
   CASE
   WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1
   THEN 1
   ELSE 0
   END AS div_accessories_flag,
   CASE
   WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1
   THEN 1
   ELSE 0
   END AS div_apparel_flag,
   CASE
   WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1
   THEN 1
   ELSE 0
   END AS div_beauty_flag,
   CASE
   WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1
   THEN 1
   ELSE 0
   END AS div_designer_flag,
   CASE
   WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1
   THEN 1
   ELSE 0
   END AS div_home_flag,
   CASE
   WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1
   THEN 1
   ELSE 0
   END AS div_merch_flag,
   CASE
   WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1
   THEN 1
   ELSE 0
   END AS div_shoes_flag,
   CASE
   WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1
   THEN 1
   ELSE 0
   END AS div_other_flag
 FROM ty_positive AS a
  FULL JOIN ty_negative AS b ON a.week_num = b.week_num AND a.month_num = b.month_num AND a.quarter_num = b.quarter_num
                 AND a.year_num = b.year_num AND a.week_num_realigned = b.week_num_realigned AND a.month_num_realigned =
                b.month_num_realigned AND a.quarter_num_realigned = b.quarter_num_realigned AND a.year_num_realigned = b
              .year_num_realigned AND LOWER(a.year_id) = LOWER(b.year_id) AND LOWER(a.acp_id) = LOWER(b.acp_id) AND
           LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region
          ) AND LOWER(a.dma) = LOWER(b.dma) AND LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment
       ) = LOWER(b.predicted_segment) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) =
    LOWER(b.loyalty_type));


DROP TABLE IF EXISTS sales_information;

CREATE TEMPORARY TABLE IF NOT EXISTS sales_information

AS
SELECT scf.sale_date,
 rc.week_num AS week_num_realigned,
 rc.month_num AS month_num_realigned,
 rc.quarter_num AS quarter_num_realigned,
 rc.year_num AS year_num_realigned,
 scf.week_num,
 scf.month_num,
 scf.quarter_num,
 scf.year_num,
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
 scf.region,
 scf.dma,
 scf.engagement_cohort,
 scf.predicted_segment,
 scf.loyalty_level,
 scf.loyalty_type,
 scf.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
WHERE rc.week_num >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 12 MONTH))
 AND rc.week_num < (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH));


DROP TABLE IF EXISTS ty_positive;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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


DROP TABLE IF EXISTS ty_negative;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

INSERT INTO usl_trips_sandbox_weekly_cust
(SELECT COALESCE(a.week_num, b.week_num) AS week_num,
  COALESCE(a.month_num, b.month_num) AS month_num,
  COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
  COALESCE(a.year_num, b.year_num) AS year_num,
  COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned,
  COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned,
  COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned,
  COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned,
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
   CASE
   WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1
   THEN 1
   ELSE 0
   END AS div_accessories_flag,
   CASE
   WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1
   THEN 1
   ELSE 0
   END AS div_apparel_flag,
   CASE
   WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1
   THEN 1
   ELSE 0
   END AS div_beauty_flag,
   CASE
   WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1
   THEN 1
   ELSE 0
   END AS div_designer_flag,
   CASE
   WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1
   THEN 1
   ELSE 0
   END AS div_home_flag,
   CASE
   WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1
   THEN 1
   ELSE 0
   END AS div_merch_flag,
   CASE
   WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1
   THEN 1
   ELSE 0
   END AS div_shoes_flag,
   CASE
   WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1
   THEN 1
   ELSE 0
   END AS div_other_flag
 FROM ty_positive AS a
  FULL JOIN ty_negative AS b ON a.week_num = b.week_num AND a.month_num = b.month_num AND a.quarter_num = b.quarter_num
                 AND a.year_num = b.year_num AND a.week_num_realigned = b.week_num_realigned AND a.month_num_realigned =
                b.month_num_realigned AND a.quarter_num_realigned = b.quarter_num_realigned AND a.year_num_realigned = b
              .year_num_realigned AND LOWER(a.year_id) = LOWER(b.year_id) AND LOWER(a.acp_id) = LOWER(b.acp_id) AND
           LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region
          ) AND LOWER(a.dma) = LOWER(b.dma) AND LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment
       ) = LOWER(b.predicted_segment) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) =
    LOWER(b.loyalty_type));

DROP TABLE IF EXISTS sales_information;

CREATE TEMPORARY TABLE IF NOT EXISTS sales_information

AS
SELECT scf.sale_date,
 rc.week_num AS week_num_realigned,
 rc.month_num AS month_num_realigned,
 rc.quarter_num AS quarter_num_realigned,
 rc.year_num AS year_num_realigned,
 scf.week_num,
 scf.month_num,
 scf.quarter_num,
 scf.year_num,
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
 scf.region,
 scf.dma,
 scf.engagement_cohort,
 scf.predicted_segment,
 scf.loyalty_level,
 scf.loyalty_type,
 scf.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
WHERE rc.week_num >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
 AND rc.week_num < (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 12 MONTH));

DROP TABLE IF EXISTS ty_positive;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

DROP TABLE IF EXISTS ty_negative;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

INSERT INTO usl_trips_sandbox_weekly_cust
(SELECT COALESCE(a.week_num, b.week_num) AS week_num,
  COALESCE(a.month_num, b.month_num) AS month_num,
  COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
  COALESCE(a.year_num, b.year_num) AS year_num,
  COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned,
  COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned,
  COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned,
  COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned,
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
   CASE
   WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1
   THEN 1
   ELSE 0
   END AS div_accessories_flag,
   CASE
   WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1
   THEN 1
   ELSE 0
   END AS div_apparel_flag,
   CASE
   WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1
   THEN 1
   ELSE 0
   END AS div_beauty_flag,
   CASE
   WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1
   THEN 1
   ELSE 0
   END AS div_designer_flag,
   CASE
   WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1
   THEN 1
   ELSE 0
   END AS div_home_flag,
   CASE
   WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1
   THEN 1
   ELSE 0
   END AS div_merch_flag,
   CASE
   WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1
   THEN 1
   ELSE 0
   END AS div_shoes_flag,
   CASE
   WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1
   THEN 1
   ELSE 0
   END AS div_other_flag
 FROM ty_positive AS a
  FULL JOIN ty_negative AS b ON a.week_num = b.week_num AND a.month_num = b.month_num AND a.quarter_num = b.quarter_num
                 AND a.year_num = b.year_num AND a.week_num_realigned = b.week_num_realigned AND a.month_num_realigned =
                b.month_num_realigned AND a.quarter_num_realigned = b.quarter_num_realigned AND a.year_num_realigned = b
              .year_num_realigned AND LOWER(a.year_id) = LOWER(b.year_id) AND LOWER(a.acp_id) = LOWER(b.acp_id) AND
           LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region
          ) AND LOWER(a.dma) = LOWER(b.dma) AND LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment
       ) = LOWER(b.predicted_segment) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) =
    LOWER(b.loyalty_type));

DROP TABLE IF EXISTS sales_information;

CREATE TEMPORARY TABLE IF NOT EXISTS sales_information

AS
SELECT scf.sale_date,
 rc.week_num AS week_num_realigned,
 rc.month_num AS month_num_realigned,
 rc.quarter_num AS quarter_num_realigned,
 rc.year_num AS year_num_realigned,
 scf.week_num,
 scf.month_num,
 scf.quarter_num,
 scf.year_num,
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
 scf.region,
 scf.dma,
 scf.engagement_cohort,
 scf.predicted_segment,
 scf.loyalty_level,
 scf.loyalty_type,
 scf.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
WHERE rc.week_num >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 20 MONTH))
 AND rc.week_num < (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH));

DROP TABLE IF EXISTS ty_positive;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

DROP TABLE IF EXISTS ty_negative;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

INSERT INTO usl_trips_sandbox_weekly_cust
(SELECT COALESCE(a.week_num, b.week_num) AS week_num,
  COALESCE(a.month_num, b.month_num) AS month_num,
  COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
  COALESCE(a.year_num, b.year_num) AS year_num,
  COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned,
  COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned,
  COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned,
  COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned,
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
   CASE
   WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1
   THEN 1
   ELSE 0
   END AS div_accessories_flag,
   CASE
   WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1
   THEN 1
   ELSE 0
   END AS div_apparel_flag,
   CASE
   WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1
   THEN 1
   ELSE 0
   END AS div_beauty_flag,
   CASE
   WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1
   THEN 1
   ELSE 0
   END AS div_designer_flag,
   CASE
   WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1
   THEN 1
   ELSE 0
   END AS div_home_flag,
   CASE
   WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1
   THEN 1
   ELSE 0
   END AS div_merch_flag,
   CASE
   WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1
   THEN 1
   ELSE 0
   END AS div_shoes_flag,
   CASE
   WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1
   THEN 1
   ELSE 0
   END AS div_other_flag
 FROM ty_positive AS a
  FULL JOIN ty_negative AS b ON a.week_num = b.week_num AND a.month_num = b.month_num AND a.quarter_num = b.quarter_num
                 AND a.year_num = b.year_num AND a.week_num_realigned = b.week_num_realigned AND a.month_num_realigned =
                b.month_num_realigned AND a.quarter_num_realigned = b.quarter_num_realigned AND a.year_num_realigned = b
              .year_num_realigned AND LOWER(a.year_id) = LOWER(b.year_id) AND LOWER(a.acp_id) = LOWER(b.acp_id) AND
           LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region
          ) AND LOWER(a.dma) = LOWER(b.dma) AND LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment
       ) = LOWER(b.predicted_segment) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) =
    LOWER(b.loyalty_type));


DROP TABLE IF EXISTS sales_information;

CREATE TEMPORARY TABLE IF NOT EXISTS sales_information

AS
SELECT scf.sale_date,
 rc.week_num AS week_num_realigned,
 rc.month_num AS month_num_realigned,
 rc.quarter_num AS quarter_num_realigned,
 rc.year_num AS year_num_realigned,
 scf.week_num,
 scf.month_num,
 scf.quarter_num,
 scf.year_num,
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
 scf.region,
 scf.dma,
 scf.engagement_cohort,
 scf.predicted_segment,
 scf.loyalty_level,
 scf.loyalty_type,
 scf.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
WHERE rc.week_num >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH))
 AND rc.week_num < (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 20 MONTH));

DROP TABLE IF EXISTS ty_positive;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

DROP TABLE IF EXISTS ty_negative;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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

INSERT INTO usl_trips_sandbox_weekly_cust
(SELECT COALESCE(a.week_num, b.week_num) AS week_num,
  COALESCE(a.month_num, b.month_num) AS month_num,
  COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
  COALESCE(a.year_num, b.year_num) AS year_num,
  COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned,
  COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned,
  COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned,
  COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned,
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
   CASE
   WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1
   THEN 1
   ELSE 0
   END AS div_accessories_flag,
   CASE
   WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1
   THEN 1
   ELSE 0
   END AS div_apparel_flag,
   CASE
   WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1
   THEN 1
   ELSE 0
   END AS div_beauty_flag,
   CASE
   WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1
   THEN 1
   ELSE 0
   END AS div_designer_flag,
   CASE
   WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1
   THEN 1
   ELSE 0
   END AS div_home_flag,
   CASE
   WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1
   THEN 1
   ELSE 0
   END AS div_merch_flag,
   CASE
   WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1
   THEN 1
   ELSE 0
   END AS div_shoes_flag,
   CASE
   WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1
   THEN 1
   ELSE 0
   END AS div_other_flag
 FROM ty_positive AS a
  FULL JOIN ty_negative AS b ON a.week_num = b.week_num AND a.month_num = b.month_num AND a.quarter_num = b.quarter_num
                 AND a.year_num = b.year_num AND a.week_num_realigned = b.week_num_realigned AND a.month_num_realigned =
                b.month_num_realigned AND a.quarter_num_realigned = b.quarter_num_realigned AND a.year_num_realigned = b
              .year_num_realigned AND LOWER(a.year_id) = LOWER(b.year_id) AND LOWER(a.acp_id) = LOWER(b.acp_id) AND
           LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region
          ) AND LOWER(a.dma) = LOWER(b.dma) AND LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment
       ) = LOWER(b.predicted_segment) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) =
    LOWER(b.loyalty_type));

DROP TABLE IF EXISTS sales_information;

CREATE TEMPORARY TABLE IF NOT EXISTS sales_information

AS
SELECT scf.sale_date,
 rc.week_num AS week_num_realigned,
 rc.month_num AS month_num_realigned,
 rc.quarter_num AS quarter_num_realigned,
 rc.year_num AS year_num_realigned,
 scf.week_num,
 scf.month_num,
 scf.quarter_num,
 scf.year_num,
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
 scf.region,
 scf.dma,
 scf.engagement_cohort,
 scf.predicted_segment,
 scf.loyalty_level,
 scf.loyalty_type,
 scf.new_to_jwn,
 scf.channel,
 scf.banner,
 scf.business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact AS scf
 LEFT JOIN upc_lookup_table AS div ON LOWER(div.upc_num) = LOWER(scf.upc_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS rc ON scf.sale_date = rc.day_date
 INNER JOIN date_lookup AS dl ON rc.week_num = dl.week_num
WHERE rc.week_num >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 25 MONTH))
 AND rc.week_num < (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH));

DROP TABLE IF EXISTS ty_positive;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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


DROP TABLE IF EXISTS ty_negative;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative

AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 year_id,
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
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('ACCESSORIES')
   THEN 1
   ELSE 0
   END) AS div_accessories_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('APPAREL')
   THEN 1
   ELSE 0
   END) AS div_apparel_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('BEAUTY')
   THEN 1
   ELSE 0
   END) AS div_beauty_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('DESIGNER')
   THEN 1
   ELSE 0
   END) AS div_designer_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('HOME')
   THEN 1
   ELSE 0
   END) AS div_home_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('MERCH PROJECTS')
   THEN 1
   ELSE 0
   END) AS div_merch_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('SHOES')
   THEN 1
   ELSE 0
   END) AS div_shoes_flag,
 MAX(CASE
   WHEN LOWER(div_desc) = LOWER('OTHER')
   THEN 1
   ELSE 0
   END) AS div_other_flag
FROM sales_information AS sf
WHERE sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND line_net_usd_amt <= 0
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'
    ), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND acp_id IS NOT NULL
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
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


INSERT INTO usl_trips_sandbox_weekly_cust
(SELECT COALESCE(a.week_num, b.week_num) AS week_num,
  COALESCE(a.month_num, b.month_num) AS month_num,
  COALESCE(a.quarter_num, b.quarter_num) AS quarter_num,
  COALESCE(a.year_num, b.year_num) AS year_num,
  COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned,
  COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned,
  COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned,
  COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned,
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
   CASE
   WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1
   THEN 1
   ELSE 0
   END AS div_accessories_flag,
   CASE
   WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1
   THEN 1
   ELSE 0
   END AS div_apparel_flag,
   CASE
   WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1
   THEN 1
   ELSE 0
   END AS div_beauty_flag,
   CASE
   WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1
   THEN 1
   ELSE 0
   END AS div_designer_flag,
   CASE
   WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1
   THEN 1
   ELSE 0
   END AS div_home_flag,
   CASE
   WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1
   THEN 1
   ELSE 0
   END AS div_merch_flag,
   CASE
   WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1
   THEN 1
   ELSE 0
   END AS div_shoes_flag,
   CASE
   WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1
   THEN 1
   ELSE 0
   END AS div_other_flag
 FROM ty_positive AS a
  FULL JOIN ty_negative AS b ON a.week_num = b.week_num AND a.month_num = b.month_num AND a.quarter_num = b.quarter_num
                 AND a.year_num = b.year_num AND a.week_num_realigned = b.week_num_realigned AND a.month_num_realigned =
                b.month_num_realigned AND a.quarter_num_realigned = b.quarter_num_realigned AND a.year_num_realigned = b
              .year_num_realigned AND LOWER(a.year_id) = LOWER(b.year_id) AND LOWER(a.acp_id) = LOWER(b.acp_id) AND
           LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.region) = LOWER(b.region
          ) AND LOWER(a.dma) = LOWER(b.dma) AND LOWER(a.engagement_cohort) = LOWER(b.engagement_cohort) AND LOWER(a.predicted_segment
       ) = LOWER(b.predicted_segment) AND LOWER(a.loyalty_level) = LOWER(b.loyalty_level) AND LOWER(a.loyalty_type) =
    LOWER(b.loyalty_type));

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_weekly_cust;

INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_weekly_cust
(SELECT week_num,
  month_num,
  quarter_num,
  year_num,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  year_id,
  acp_id,
  channel,
  banner,
  region,
  dma,
  COALESCE(aec, 'UNDEFINED') AS aec,
  predicted_segment,
  loyalty_level,
  loyalty_type,
  new_to_jwn,
  CAST(NULL AS STRING) AS cust_age_bucket,
  gross_spend,
  net_spend,
  trips,
  gross_units,
  net_units,
  div_accessories_flag,
  div_apparel_flag,
  div_beauty_flag,
  div_designer_flag,
  div_home_flag,
  div_merch_flag,
  div_shoes_flag,
  div_other_flag,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM usl_trips_sandbox_weekly_cust);

