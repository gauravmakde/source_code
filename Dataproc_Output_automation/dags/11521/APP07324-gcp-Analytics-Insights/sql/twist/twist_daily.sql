BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;


BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS product_views
--  cluster by style_id, event_date_pacific, business_unit_desc, channelcountry
AS
SELECT event_date_pacific,
  CASE
  WHEN LOWER(channel) = LOWER('FULL_LINE') AND LOWER(channelcountry) = LOWER('US')
  THEN 'N.COM'
  WHEN LOWER(channel) = LOWER('FULL_LINE') AND LOWER(channelcountry) = LOWER('CA')
  THEN 'N.CA'
  WHEN LOWER(channel) = LOWER('RACK') AND LOWER(channelcountry) = LOWER('US')
  THEN 'OFFPRICE ONLINE'
  ELSE NULL
  END AS business_unit_desc,
 channelcountry,
 style_id,
 SUM(product_views) AS product_views
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_funnel_daily
WHERE event_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND LOWER(site_source) = LOWER('MODERN')
GROUP BY event_date_pacific,
 business_unit_desc,
 channelcountry,
 style_id
UNION ALL
SELECT event_date_pacific,
  CASE
  WHEN LOWER(channel) = LOWER('FULL_LINE') AND LOWER(channelcountry) = LOWER('US')
  THEN 'N.COM'
  WHEN LOWER(channel) = LOWER('FULL_LINE') AND LOWER(channelcountry) = LOWER('CA')
  THEN 'N.CA'
  WHEN LOWER(channel) = LOWER('RACK') AND LOWER(channelcountry) = LOWER('US')
  THEN 'OFFPRICE ONLINE'
  ELSE NULL
  END AS business_unit_desc,
 channelcountry,
 web_style_id AS style_id,
 SUM(product_views) AS product_views
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_funnel_daily_history
WHERE event_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY event_date_pacific,
 business_unit_desc,
 channelcountry,
 style_id;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS pre_store_traffic
--  cluster by store_number, day_date
AS
SELECT stdn.store_num AS store_number,
 stdn.day_date,
 dc.week_num,
 stdn.channel AS business_unit_desc,
 SUM(stdn.purchase_trips) AS purchase_trips,
 SUM(CASE
   WHEN LOWER(stdn.traffic_source) IN (LOWER('retailnext_cam'))
   THEN stdn.traffic
   ELSE 0
   END) AS traffic_rn_cam,
 SUM(CASE
   WHEN LOWER(stdn.traffic_source) IN (LOWER('placer_channel_scaled_to_retailnext'), LOWER('placer_closed_store_scaled_to_retailnext'
      ), LOWER('placer_vertical_interference_scaled_to_retailnext'), LOWER('placer_vertical_interference_scaled_to_est_retailnext'
      ), LOWER('placer_store_scaled_to_retailnext'))
   THEN stdn.traffic
   ELSE 0
   END) AS traffic_scaled_placer
FROM `{{params.gcp_project_id}}`.t2dl_das_fls_traffic_model.store_traffic_daily AS stdn
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc ON stdn.day_date = dc.day_date
WHERE dc.year_num >= 2021
 AND stdn.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY store_number,
 stdn.day_date,
 dc.week_num,
 business_unit_desc;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS store_traffic
--  cluster by store_number, day_date
AS
SELECT t.day_date,
 t.business_unit_desc,
 t.store_number,
 SUM(CASE
   WHEN t.traffic_rn_cam > 0
   THEN t.traffic_rn_cam
   ELSE t.traffic_scaled_placer
   END) AS traffic
FROM pre_store_traffic AS t
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS s ON t.store_number = s.store_num
WHERE t.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND LOWER(s.store_type_code) IN (LOWER('FL'), LOWER('RK'))
GROUP BY t.day_date,
 t.business_unit_desc,
 t.store_number;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS demand
--  cluster by rms_sku_num, rms_style_num, store_num, business_day_date
AS
SELECT trn.business_day_date,
 sku.web_style_num,
 sku.rms_style_num,
 trn.rms_sku_num,
  CASE
  WHEN LOWER(st.business_unit_desc) = LOWER('N.COM')
  THEN '808'
  WHEN LOWER(st.business_unit_desc) = LOWER('OFFPRICE ONLINE')
  THEN '828'
  ELSE SUBSTR(CAST(trn.intent_store_num AS STRING), 1, 5)
  END AS store_num,
 st.business_unit_desc,
 SUM(trn.line_net_usd_amt) AS demand,
 SUM(trn.line_item_quantity) AS items
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact AS trn
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st 
 ON trn.intent_store_num = st.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku 
 ON LOWER(COALESCE(trn.sku_num, trn.hl_sku_num)) = LOWER(sku.rms_sku_num
    ) 
	AND LOWER(CASE
     WHEN LOWER(st.business_unit_desc) LIKE LOWER('%CANADA%')
     THEN 'CA'
     ELSE 'US'
     END) = LOWER(sku.channel_country)
WHERE trn.business_day_date BETWEEN CAST(DATE_SUB({{params.start_date}}, INTERVAL 60 DAY) AS DATE) AND CAST({{params.end_date}} AS DATE)
 AND trn.line_net_usd_amt > 0
 AND trn.line_item_quantity > 0
 AND LOWER(st.business_unit_desc) IN (LOWER('N.COM'), LOWER('FULL LINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'
    ))
 AND LOWER(trn.tran_type_code) <> LOWER('PAID')
GROUP BY trn.business_day_date,
 sku.web_style_num,
 sku.rms_style_num,
 trn.rms_sku_num,
 store_num,
 st.business_unit_desc;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, rms_style_num, store_num, business_day_date) , column(rms_sku_num) , column(rms_style_num) , column(store_num) , column(business_day_date) on demand;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS rp_lkp
  as (
      select
          rms_sku_num
          , cast(st.store_num as STRING) as loc_idnt
          , day_date
          , business_unit_desc
          , 'Y' as aip_rp_fl
      from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
      inner join (
                select distinct day_date
                from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
                where day_date between CAST(DATE_SUB({{params.start_date}}, INTERVAL 60 DAY) AS DATE)  and CAST({{params.end_date}} AS DATE)
            ) d
      on d.day_date between RANGE_START(rp_period) and RANGE_END(rp_period)
      inner join (
        select
            str.*
            , case when store_num in (210, 212) then 209 else store_num end as store_num_stg
        from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim str
        where UPPER(business_unit_desc) IN ('N.COM', 'FULL LINE',  'RACK', 'RACK CANADA', 'OFFPRICE ONLINE', 'N.CA')
      ) st
         on rp.location_num = st.store_num_stg
      group by 1,2,3,4,5 );
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS eoh_base
--  cluster by day_date, store_num, rms_sku_num
AS
SELECT base.snapshot_date AS day_date,
  CASE
  WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
  THEN base.location_type
  ELSE st.business_unit_desc
  END AS bu,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
     THEN base.location_type
     ELSE st.business_unit_desc
     END) IN (LOWER('N.COM'), LOWER('OMNI.COM'), LOWER('DS'), LOWER('ECONCESSION'))
  THEN '808'
  WHEN LOWER(CASE
     WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
     THEN base.location_type
     ELSE st.business_unit_desc
     END) IN (LOWER('N.CA'), LOWER('FULL LINE CANADA'))
  THEN '867'
  WHEN LOWER(CASE
     WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
     THEN base.location_type
     ELSE st.business_unit_desc
     END) IN (LOWER('OFFPRICE ONLINE'), LOWER('DS_OP'))
  THEN '828'
  ELSE base.location_id
  END AS store_num,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
     THEN base.location_type
     ELSE st.business_unit_desc
     END) IN (LOWER('N.COM'), LOWER('N.CA'), LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('OMNI.COM'), LOWER('DS'
     ), LOWER('ECONCESSION'))
  THEN 'NORD'
  ELSE 'RACK'
  END AS banner,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
     THEN base.location_type
     ELSE st.business_unit_desc
     END) = LOWER('DS')
  THEN 'N.COM'
  WHEN LOWER(CASE
     WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
     THEN base.location_type
     ELSE st.business_unit_desc
     END) = LOWER('ECONCESSION')
  THEN 'N.COM'
  WHEN LOWER(CASE
     WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
     THEN base.location_type
     ELSE st.business_unit_desc
     END) = LOWER('DS_OP')
  THEN 'OFFPRICE ONLINE'
  WHEN LOWER(CASE
     WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
     THEN base.location_type
     ELSE st.business_unit_desc
     END) = LOWER('OMNI.COM')
  THEN 'N.COM'
  ELSE CASE
   WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
   THEN base.location_type
   ELSE st.business_unit_desc
   END
  END AS business_unit_desc,
 'US' AS country,
 base.rms_sku_id AS rms_sku_num,
 base.stock_on_hand_qty,
 base.immediately_sellable_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact AS base
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CAST(base.location_id AS FLOAT64) = st.store_num
WHERE base.snapshot_date BETWEEN CAST(DATE_SUB({{params.start_date}}, INTERVAL 61 DAY) AS DATE) AND CAST(DATE_SUB({{params.end_date}}, INTERVAL 1 DAY) AS DATE) -- go one day back for BOH casting
 AND base.stock_on_hand_qty > 0
 AND LOWER(CASE
    WHEN LOWER(base.location_type) IN (LOWER('DS'), LOWER('DS_OP'), LOWER('ECONCESSION'))
    THEN base.location_type
    ELSE st.business_unit_desc
    END) IN (LOWER('N.COM'), LOWER('FULL LINE'), LOWER('RACK'), LOWER('OFFPRICE ONLINE'), LOWER('OMNI.COM'), LOWER('DS'
    ), LOWER('DS_OP'), LOWER('ECONCESSION'));
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (day_date, store_num, rms_sku_num) , column(rms_sku_num) , column(store_num) , column(day_date) on eoh_base;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS eoh_stg_mc
--  cluster by day_date, store_num, rms_sku_num, business_unit_desc
AS
SELECT DISTINCT inv.day_date,
 inv.bu,
 inv.store_num,
 inv.banner,
 inv.business_unit_desc,
 inv.country,
 inv.rms_sku_num,
 inv.stock_on_hand_qty,
 inv.immediately_sellable_qty,
 fls.stock_on_hand_qty AS fls_eoh,
 fls.immediately_sellable_qty AS fls_asoh,
  COALESCE(inv.stock_on_hand_qty, 0) + COALESCE(fls.stock_on_hand_qty, 0) AS mc_stock_on_hand_qty,
  COALESCE(inv.immediately_sellable_qty, 0) + COALESCE(fls.immediately_sellable_qty, 0) AS mc_immediately_sellable_qty
FROM eoh_base AS inv
 LEFT JOIN (SELECT day_date,
   rms_sku_num,
   SUM(stock_on_hand_qty) AS stock_on_hand_qty,
   SUM(immediately_sellable_qty) AS immediately_sellable_qty
  FROM eoh_base AS fls
  WHERE LOWER(business_unit_desc) = LOWER('FULL LINE')
  GROUP BY day_date,
   rms_sku_num) AS fls ON LOWER(fls.rms_sku_num) = LOWER(inv.rms_sku_num) AND inv.day_date = fls.day_date
WHERE LOWER(inv.business_unit_desc) = LOWER('N.COM');
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (day_date, store_num, rms_sku_num, business_unit_desc) , column(rms_sku_num) , column(store_num) , column(day_date) , column(business_unit_desc) on eoh_stg_mc;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS eoh_stg_tmp
--  cluster by day_date, store_num, rms_sku_num, business_unit_desc
AS
SELECT *
FROM eoh_stg_mc
UNION ALL
SELECT day_date,
 bu,
 store_num,
 banner,
 business_unit_desc,
 country,
 rms_sku_num,
 stock_on_hand_qty,
 immediately_sellable_qty,
 0 AS fls_eoh,
 0 AS fls_asoh,
 0 AS mc_stock_on_hand_qty,
 0 AS mc_immediately_sellable_qty
FROM eoh_base
WHERE LOWER(business_unit_desc) NOT IN (LOWER('N.COM'));
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (day_date, store_num, rms_sku_num, business_unit_desc) , column(rms_sku_num) , column(store_num) , column(day_date) , column(business_unit_desc) on eoh_stg_tmp;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS eoh_stg
--  cluster by rms_sku_num, web_style_num, rms_style_num, day_date
AS
SELECT DATE_ADD(inv.day_date, INTERVAL 1 DAY) AS day_date,
 inv.day_date AS lag_day,
  CASE
  WHEN inv.day_date <= DATE '2022-04-01'
  THEN CAST(CASE
    WHEN COALESCE(pcdb.web_style_id, FORMAT('%20d', sku.web_style_num)) = ''
    THEN '0'
    ELSE COALESCE(pcdb.web_style_id, FORMAT('%20d', sku.web_style_num))
    END AS BIGINT)
  ELSE CAST(sku.web_style_num AS BIGINT)
  END AS web_style_num,
 COALESCE(sku.style_group_num, pcdb.rms_style_group_num) AS rms_style_group_num,
 sku.rms_style_num,
 inv.rms_sku_num,
 inv.store_num,
 inv.banner,
 inv.business_unit_desc,
 inv.country,
 SUM(inv.stock_on_hand_qty) AS eoh,
 SUM(inv.immediately_sellable_qty) AS asoh,
 SUM(inv.mc_stock_on_hand_qty) AS eoh_mc,
 SUM(inv.mc_immediately_sellable_qty) AS asoh_mc
FROM eoh_stg_tmp AS inv
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku ON LOWER(inv.rms_sku_num) = LOWER(sku.rms_sku_num) AND LOWER(sku.channel_country
    ) = LOWER('US')
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ete_instrumentation.live_on_site_pcdb_us_opr AS pcdb ON LOWER(inv.rms_sku_num) = LOWER(pcdb.rms_sku_id
   )
WHERE COALESCE(FORMAT('%20d', sku.web_style_num), sku.rms_style_num) IS NOT NULL
GROUP BY day_date,
 lag_day,
 web_style_num,
 rms_style_group_num,
 sku.rms_style_num,
 inv.rms_sku_num,
 inv.store_num,
 inv.banner,
 inv.business_unit_desc,
 inv.country;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc) , column(rms_sku_num) , column(web_style_num) , column(rms_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) on eoh_stg;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS twist_blowout
--  cluster by rms_sku_num, web_style_num, rms_style_num, day_date
AS
SELECT base.web_style_num,
 base.rms_style_group_num,
 base.rms_style_num,
 base.rms_sku_num,
 base.store_num,
 base.business_unit_desc,
 base.country,
 base.banner,
 base.min_date,
 base.max_date,
 base.max_units,
 base.max_style,
 xday.day_date
FROM (SELECT web_style_num,
   rms_style_group_num,
   rms_style_num,
   rms_sku_num,
   store_num,
   business_unit_desc,
   country,
   banner,
   min_date,
   max_date,
   max_units,
   SUM(A1825587398) OVER (PARTITION BY banner, business_unit_desc, store_num, COALESCE(rms_style_group_num, FORMAT('%20d'
        , web_style_num), rms_style_num) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_style
  FROM (SELECT web_style_num,
     rms_style_group_num,
     rms_style_num,
     rms_sku_num,
     store_num,
     business_unit_desc,
     country,
     banner,
     day_date,
     eoh,
     asoh,
     MIN(day_date) AS min_date,
     MAX(day_date) AS max_date,
     MAX(eoh) AS max_units,
     MAX(asoh) AS A1825587398
    FROM eoh_stg
    GROUP BY day_date,
     web_style_num,
     rms_style_group_num,
     rms_style_num,
     rms_sku_num,
     store_num,
     banner,
     business_unit_desc,
     country,
     eoh,
     asoh) AS t0) AS base
 INNER JOIN (SELECT day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE day_date BETWEEN CAST({{params.start_date}} AS DATE) AND CAST({{params.end_date}} AS DATE)) AS xday ON base.min_date <= xday.day_date
WHERE DATE_DIFF(xday.day_date, base.max_date, DAY) <= 60
 AND base.max_style > 1;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc) , column(rms_sku_num) , column(web_style_num) , column(rms_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) on twist_blowout;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS combine
--  cluster by rms_sku_num, web_style_num, rms_style_num, day_date
AS
SELECT base.web_style_num,
 base.rms_style_group_num,
 base.rms_style_num,
 base.rms_sku_num,
 base.store_num,
 base.business_unit_desc,
 base.country,
 base.banner,
 base.min_date,
 base.max_date,
 base.max_units,
 base.max_style,
 base.day_date,
 COALESCE(dm.dma_code, 0) AS dma,
 COALESCE(eoh.eoh, 0) AS eoh,
 COALESCE(eoh.asoh, 0) AS asoh,
 COALESCE(eoh.eoh_mc, 0) AS eoh_mc,
 COALESCE(eoh.asoh_mc, 0) AS asoh_mc,
  CASE
  WHEN LOWER(rp.aip_rp_fl) = LOWER('Y')
  THEN 1
  ELSE 0
  END AS rp_idnt,
  CASE
  WHEN LOWER(base.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('FULL LINE')) AND eoh.asoh > 0
  THEN 1
  WHEN LOWER(base.business_unit_desc) IN (LOWER('N.CA'), LOWER('OFFPRICE ONLINE')) AND eoh.asoh > 1
  THEN 1
  WHEN LOWER(base.business_unit_desc) IN (LOWER('N.COM')) AND eoh.asoh_mc > 1
  THEN 1
  ELSE 0
  END AS mc_instock_ind,
  CASE
  WHEN LOWER(base.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('FULL LINE')) AND eoh.asoh > 0
  THEN 1
  WHEN LOWER(base.business_unit_desc) IN (LOWER('N.COM'), LOWER('N.CA'), LOWER('OFFPRICE ONLINE')) AND eoh.asoh > 1
  THEN 1
  ELSE 0
  END AS fc_instock_ind,
 COALESCE(dmd.demand, 0) AS demand,
 COALESCE(dmd.items, 0) AS items,
 COALESCE(pv.product_views, 0) AS product_views,
 COALESCE(st.traffic, 0) AS traffic,
 sr.price_store_num
FROM twist_blowout AS base
 LEFT JOIN eoh_stg AS eoh ON base.day_date = eoh.day_date AND LOWER(base.rms_sku_num) = LOWER(eoh.rms_sku_num) AND LOWER(base
    .store_num) = LOWER(eoh.store_num)
 LEFT JOIN rp_lkp AS rp ON base.rms_sku_num = CAST(rp.rms_sku_num AS STRING) AND LOWER(base.store_num) = LOWER(rp.loc_idnt
     ) AND base.day_date = rp.day_date
 LEFT JOIN demand AS dmd ON base.day_date = dmd.business_day_date AND LOWER(base.rms_sku_num) = LOWER(dmd.rms_sku_num)
  AND CAST(base.store_num AS FLOAT64) = CAST(CASE
     WHEN dmd.store_num = ''
     THEN '0'
     ELSE dmd.store_num
     END AS INTEGER)
 LEFT JOIN product_views AS pv ON base.day_date = pv.event_date_pacific AND base.web_style_num = pv.style_id AND LOWER(base
     .country) = LOWER(pv.channelcountry) AND LOWER(base.banner) = LOWER(CASE
     WHEN LOWER(pv.business_unit_desc) IN (LOWER('N.COM'), LOWER('N.CA'), LOWER('FULL LINE'))
     THEN 'NORD'
     ELSE 'RACK'
     END)
 LEFT JOIN store_traffic AS st ON base.day_date = st.day_date AND CAST(base.store_num AS FLOAT64) = st.store_number AND
   LOWER(base.business_unit_desc) = LOWER(st.business_unit_desc)
 LEFT JOIN (SELECT DISTINCT store_num,
    us_dma_code AS dma_code
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_store_us_dma
   UNION ALL
   SELECT DISTINCT store_num,
    ca_dma_code AS dma_code
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_store_ca_dma) AS dm ON CAST(base.store_num AS FLOAT64) = dm.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS sr ON CAST(base.store_num AS FLOAT64) = sr.store_num;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc, dma, price_store_num) , column(rms_sku_num) , column(web_style_num) , column(rms_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) , column(dma) , column(price_store_num) on combine;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS combine_prc
--  cluster by rms_sku_num, web_style_num, rms_style_num, day_date
AS
SELECT base.web_style_num,
 base.rms_style_group_num,
 base.rms_style_num,
 base.rms_sku_num,
 base.store_num,
 base.business_unit_desc,
 base.country,
 base.banner,
 base.min_date,
 base.max_date,
 base.max_units,
 base.max_style,
 base.day_date,
 base.dma,
 base.eoh,
 base.asoh,
 base.eoh_mc,
 base.asoh_mc,
 base.rp_idnt,
 base.mc_instock_ind,
 base.fc_instock_ind,
 base.demand,
 base.items,
 base.product_views,
 base.traffic,
 base.price_store_num,
  CASE
  WHEN LOWER(pr.ownership_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  WHEN LOWER(pr.ownership_retail_price_type_code) = LOWER('REGULAR')
  THEN 'R'
  ELSE NULL
  END AS ownership_price_type,
  CASE
  WHEN LOWER(pr.selling_retail_price_type_code) = LOWER('CLEARANCE') OR LOWER(pr.ownership_retail_price_type_code) =
    LOWER('CLEARANCE')
  THEN 'C'
  WHEN LOWER(pr.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'R'
  WHEN LOWER(pr.selling_retail_price_type_code) = LOWER('PROMOTION')
  THEN 'P'
  ELSE NULL
  END AS current_price_type,
 pr.selling_retail_price_amt AS current_price_amt
FROM combine AS base
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS pr 
 ON LOWER(base.rms_sku_num) = LOWER(pr.rms_sku_num) 
 AND base.price_store_num = CAST(pr.store_num AS FLOAT64)
 AND base.day_date between pr.EFF_BEGIN_TMSTP AND pr.EFF_END_TMSTP - interval '0.001' second;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc, dma) , column(rms_sku_num) , column(web_style_num) , column(rms_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) , column(dma) on combine_prc;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS combine_pvs
--  cluster by rms_sku_num, web_style_num, day_date, store_num
AS
SELECT c.web_style_num,
 c.rms_style_group_num,
 c.rms_style_num,
 c.rms_sku_num,
 c.store_num,
 c.business_unit_desc,
 c.country,
 c.banner,
 c.min_date,
 c.max_date,
 c.max_units,
 c.max_style,
 c.day_date,
 c.dma,
 c.eoh_mc,
 c.eoh,
 c.asoh_mc,
 c.asoh,
 c.rp_idnt,
 c.mc_instock_ind,
 c.fc_instock_ind,
 c.demand,
 c.items,
 c.product_views,
 c.traffic,
 d.dept_num,
 d.dept_pvs,
 c.ownership_price_type,
 c.current_price_type,
 c.current_price_amt
FROM combine_prc AS c
 LEFT JOIN (SELECT day_date,
   web_style_num,
   dept_num,
   product_views,
   store_num,
   SUM(product_views) OVER (PARTITION BY day_date, dept_num, store_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS dept_pvs
  FROM (SELECT DISTINCT c0.day_date,
     c0.web_style_num,
     s.dept_num,
     c0.product_views,
     c0.store_num
    FROM combine AS c0
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS s ON LOWER(c0.rms_sku_num) = LOWER(s.rms_sku_num) AND LOWER(c0.country
        ) = LOWER(s.channel_country)
    WHERE LOWER(c0.business_unit_desc) IN (LOWER('N.COM'), LOWER('FULL LINE'))) AS stg) AS d ON c.day_date = d.day_date
   AND c.web_style_num = d.web_style_num AND LOWER(c.store_num) = LOWER(d.store_num)
WHERE LOWER(c.business_unit_desc) IN (LOWER('N.COM'), LOWER('FULL LINE'));

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) , column(rms_sku_num) , column(web_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) , column(dma) on combine_pvs;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS combine_fls
--  cluster by rms_sku_num, web_style_num, day_date, store_num
AS
SELECT t1.web_style_num,
 t1.rms_style_group_num,
 t1.rms_style_num,
 t1.rms_sku_num,
 t1.store_num,
 t1.business_unit_desc,
 t1.country,
 t1.banner,
 t1.min_date,
 t1.max_date,
 t1.max_units,
 t1.max_style,
 t1.day_date,
 t1.dma,
 t1.eoh_mc,
 t1.eoh,
 t1.asoh_mc,
 t1.asoh,
 t1.rp_idnt,
 t1.ownership_price_type,
 t1.current_price_type,
 t1.current_price_amt,
 t1.mc_instock_ind,
 t1.fc_instock_ind,
 t1.demand,
 t1.items,
 t1.product_views,
 t1.traffic,
 t1.dept_num,
 t1.dept_pvs,
 COALESCE(t1.A1607046073, 0) AS hist_items,
 COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.web_style_num
     RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS hist_items_style,
  CASE
  WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1
       .web_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
  THEN 0
  ELSE CAST(COALESCE(t1.A1607046073, 0) * 1.000 / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.web_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS BIGNUMERIC)
  END AS pct_items,
 COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.dept_num
     RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS hist_items_dept,
 COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
   RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS hist_items_loc,
 COALESCE(SUM(1) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.web_style_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS rn,
  CASE
  WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
     RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
  THEN 0
  ELSE CAST(COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS NUMERIC)
   / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
     RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)
  END AS pct_dept,
  t1.traffic * CASE
   WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
      RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
   THEN 0
   ELSE CAST(COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS NUMERIC)
    / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
      RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)
   END AS dept_traffic,
  CAST(t1.product_views AS BIGNUMERIC) / t1.dept_pvs AS dept_pvs_pct,
   CAST(t1.product_views AS BIGNUMERIC) / t1.dept_pvs * (t1.traffic * CASE
     WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
     THEN 0
     ELSE CAST(COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS NUMERIC)
      / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)
     END) AS style_traffic
FROM (SELECT cmb.web_style_num,
   cmb.rms_style_group_num,
   cmb.rms_style_num,
   cmb.rms_sku_num,
   cmb.store_num,
   cmb.business_unit_desc,
   cmb.country,
   cmb.banner,
   cmb.min_date,
   cmb.max_date,
   cmb.max_units,
   cmb.max_style,
   cmb.day_date,
   cmb.dma,
   cmb.eoh_mc,
   cmb.eoh,
   cmb.asoh_mc,
   cmb.asoh,
   cmb.rp_idnt,
   cmb.ownership_price_type,
   cmb.current_price_type,
   cmb.current_price_amt,
   cmb.mc_instock_ind,
   cmb.fc_instock_ind,
   cmb.demand,
   cmb.items,
   cmb.product_views,
   cmb.traffic,
   cmb.dept_num,
   cmb.dept_pvs,
   SUM(dmd.items) AS A1607046073
  FROM combine_pvs AS cmb
   LEFT JOIN demand AS dmd ON dmd.business_day_date BETWEEN DATE_SUB(cmb.day_date, INTERVAL 60 DAY) AND (DATE_SUB(cmb.day_date
           ,INTERVAL 1 DAY)) AND cmb.web_style_num = dmd.web_style_num AND LOWER(cmb.rms_style_num) = LOWER(dmd.rms_style_num
         ) AND LOWER(cmb.rms_sku_num) = LOWER(dmd.rms_sku_num) AND LOWER(cmb.business_unit_desc) = LOWER(dmd.business_unit_desc
       ) AND LOWER(cmb.store_num) = LOWER(dmd.store_num)
  WHERE LOWER(cmb.business_unit_desc) = LOWER('FULL LINE')
   AND cmb.dept_pvs > 0
  GROUP BY cmb.web_style_num,
   cmb.rms_style_group_num,
   cmb.rms_style_num,
   cmb.rms_sku_num,
   cmb.store_num,
   cmb.business_unit_desc,
   cmb.country,
   cmb.banner,
   cmb.min_date,
   cmb.max_date,
   cmb.max_units,
   cmb.max_style,
   cmb.day_date,
   cmb.dma,
   cmb.eoh_mc,
   cmb.eoh,
   cmb.asoh_mc,
   cmb.asoh,
   cmb.rp_idnt,
   cmb.mc_instock_ind,
   cmb.fc_instock_ind,
   cmb.demand,
   cmb.items,
   cmb.product_views,
   cmb.traffic,
   cmb.dept_num,
   cmb.dept_pvs,
   cmb.ownership_price_type,
   cmb.current_price_type,
   cmb.current_price_amt) AS t1;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) , column(rms_sku_num) , column(web_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) , column(dma) on combine_fls;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS fls_full
--  cluster by rms_sku_num, web_style_num, day_date, store_num
AS
SELECT cf.web_style_num,
 cf.rms_style_group_num,
 cf.rms_style_num,
 cf.rms_sku_num,
 cf.store_num,
 cf.business_unit_desc,
 cf.country,
 cf.banner,
 cf.min_date,
 cf.max_date,
 cf.max_units,
 cf.max_style,
 cf.day_date,
 cf.dma,
 cf.eoh_mc,
 cf.eoh,
 cf.asoh_mc,
 cf.asoh,
 cf.rp_idnt,
 cf.ownership_price_type,
 cf.current_price_type,
 cf.current_price_amt,
 cf.mc_instock_ind,
 cf.fc_instock_ind,
 cf.demand,
 cf.items,
 cf.product_views,
 cf.traffic,
 cf.dept_num,
 cf.dept_pvs,
 cf.hist_items,
 cf.hist_items_style,
 cf.pct_items,
 cf.hist_items_dept,
 cf.hist_items_loc,
 cf.rn,
 cf.pct_dept,
 cf.dept_traffic,
 cf.dept_pvs_pct,
 cf.style_traffic,
 zero.style_total,
 zero.zero_ind,
  CASE
  WHEN cf.hist_items_style = 0 AND cf.rn = 1
  THEN CAST(cf.style_traffic AS NUMERIC)
  WHEN cf.hist_items_style = 0 AND zero.zero_ind = 1
  THEN 1.000 / cf.rn * cf.style_traffic
  WHEN cf.hist_items_style = 0
  THEN 0.000
  ELSE CAST(cf.style_traffic * cf.pct_items AS NUMERIC)
  END AS allocated_traffic
FROM combine_fls AS cf
 LEFT JOIN (SELECT day_date,
   business_unit_desc,
   web_style_num,
   store_num,
   SUM(pct_items) AS style_total,
    CASE
    WHEN SUM(pct_items) = 0
    THEN 1
    ELSE 0
    END AS zero_ind
  FROM combine_fls
  GROUP BY web_style_num,
   store_num,
   business_unit_desc,
   day_date) AS zero ON cf.day_date = zero.day_date AND LOWER(cf.store_num) = LOWER(zero.store_num) AND cf.web_style_num
    = zero.web_style_num;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) , column(rms_sku_num) , column(web_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) , column(dma) on fls_full;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS combine_rack
--  cluster by rms_sku_num, web_style_num, rms_style_num, day_date
AS
SELECT t1.web_style_num,
 t1.rms_style_group_num,
 t1.rms_style_num,
 t1.rms_sku_num,
 t1.store_num,
 t1.business_unit_desc,
 t1.country,
 t1.banner,
 t1.min_date,
 t1.max_date,
 t1.max_units,
 t1.max_style,
 t1.day_date,
 t1.dma,
 t1.eoh_mc,
 t1.eoh,
 t1.asoh_mc,
 t1.asoh,
 t1.rp_idnt,
 t1.ownership_price_type,
 t1.current_price_type,
 t1.current_price_amt,
 t1.mc_instock_ind,
 t1.fc_instock_ind,
 t1.demand,
 t1.items,
 t1.product_views,
 t1.traffic,
 COALESCE(t1.A1607046073, 0) AS hist_items,
 COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.rms_style_num
     RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS hist_items_style,
  CASE
  WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1
       .rms_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
  THEN 0
  ELSE CAST(COALESCE(t1.A1607046073, 0) * 1.000 / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.rms_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS BIGNUMERIC)
  END AS pct_items,
 COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
   RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS hist_items_loc,
 COALESCE(SUM(1) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.rms_style_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS rn,
  CASE
  WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
     RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
  THEN 0
  ELSE CAST(COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.rms_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS NUMERIC)
   / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
     RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)
  END AS pct_style,
  t1.traffic * CASE
   WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
      RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
   THEN 0
   ELSE CAST(COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.rms_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS NUMERIC)
    / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
      RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)
   END AS style_traffic,
   t1.traffic * CASE
    WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
    THEN 0
    ELSE CAST(COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.rms_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS NUMERIC)
     / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)
    END * CASE
   WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num,
        t1.rms_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
   THEN 0
   ELSE CAST(COALESCE(t1.A1607046073, 0) * 1.000 / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.day_date, t1.business_unit_desc, t1.store_num, t1.rms_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS BIGNUMERIC)
   END AS allocated_traffic
FROM (SELECT cmb.web_style_num,
   cmb.rms_style_group_num,
   cmb.rms_style_num,
   cmb.rms_sku_num,
   cmb.store_num,
   cmb.business_unit_desc,
   cmb.country,
   cmb.banner,
   cmb.min_date,
   cmb.max_date,
   cmb.max_units,
   cmb.max_style,
   cmb.day_date,
   cmb.dma,
   cmb.eoh_mc,
   cmb.eoh,
   cmb.asoh_mc,
   cmb.asoh,
   cmb.rp_idnt,
   cmb.ownership_price_type,
   cmb.current_price_type,
   cmb.current_price_amt,
   cmb.mc_instock_ind,
   cmb.fc_instock_ind,
   cmb.demand,
   cmb.items,
   cmb.product_views,
   cmb.traffic,
   SUM(dmd.items) AS A1607046073
  FROM combine_prc AS cmb
   LEFT JOIN demand AS dmd ON dmd.business_day_date BETWEEN DATE_SUB(cmb.day_date, INTERVAL 60 DAY) AND (DATE_SUB(cmb.day_date ,INTERVAL 1 DAY)) AND LOWER(cmb.rms_style_num) = LOWER(dmd.rms_style_num) AND LOWER(cmb.rms_sku_num) = LOWER(dmd
        .rms_sku_num) AND LOWER(cmb.business_unit_desc) = LOWER(dmd.business_unit_desc) AND LOWER(cmb.store_num) = LOWER(dmd
      .store_num)
  WHERE LOWER(cmb.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  GROUP BY cmb.web_style_num,
   cmb.rms_style_group_num,
   cmb.rms_style_num,
   cmb.rms_sku_num,
   cmb.store_num,
   cmb.business_unit_desc,
   cmb.country,
   cmb.banner,
   cmb.min_date,
   cmb.max_date,
   cmb.max_units,
   cmb.max_style,
   cmb.day_date,
   cmb.dma,
   cmb.eoh,
   cmb.asoh,
   cmb.eoh_mc,
   cmb.asoh_mc,
   cmb.rp_idnt,
   cmb.mc_instock_ind,
   cmb.fc_instock_ind,
   cmb.demand,
   cmb.items,
   cmb.product_views,
   cmb.traffic,
   cmb.ownership_price_type,
   cmb.current_price_type,
   cmb.current_price_amt) AS t1;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc, dma) , column(rms_sku_num) , column(web_style_num) , column(rms_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) , column(dma) on combine_rack;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS combine_digital
--  cluster by rms_sku_num, web_style_num, day_date, store_num
AS
SELECT t1.web_style_num,
 t1.rms_style_group_num,
 t1.rms_style_num,
 t1.rms_sku_num,
 t1.store_num,
 t1.business_unit_desc,
 t1.country,
 t1.banner,
 t1.min_date,
 t1.max_date,
 t1.max_units,
 t1.max_style,
 t1.day_date,
 t1.dma,
 t1.eoh_mc,
 t1.eoh,
 t1.asoh_mc,
 t1.asoh,
 t1.rp_idnt,
 t1.ownership_price_type,
 t1.current_price_type,
 t1.current_price_amt,
 t1.mc_instock_ind,
 t1.fc_instock_ind,
 t1.demand,
 t1.items,
 t1.product_views,
 t1.traffic,
 COALESCE(t1.A1607046073, 0) AS hist_items,
 COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.business_unit_desc, t1.country, t1.day_date, t1.web_style_num
     RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS hist_items_style,
 COALESCE(SUM(1) OVER (PARTITION BY t1.business_unit_desc, t1.day_date, t1.store_num, t1.web_style_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) AS rn,
  CASE
  WHEN COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.business_unit_desc, t1.country, t1.day_date, t1.web_style_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) = 0
  THEN 0
  ELSE CAST(COALESCE(t1.A1607046073, 0) AS NUMERIC) / COALESCE(SUM(COALESCE(t1.A1607046073, 0)) OVER (PARTITION BY t1.business_unit_desc
       , t1.country, t1.day_date, t1.web_style_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)
  END AS pct_items
FROM (SELECT cmb.web_style_num,
   cmb.rms_style_group_num,
   cmb.rms_style_num,
   cmb.rms_sku_num,
   cmb.store_num,
   cmb.business_unit_desc,
   cmb.country,
   cmb.banner,
   cmb.min_date,
   cmb.max_date,
   cmb.max_units,
   cmb.max_style,
   cmb.day_date,
   cmb.dma,
   cmb.eoh_mc,
   cmb.eoh,
   cmb.asoh_mc,
   cmb.asoh,
   cmb.rp_idnt,
   cmb.ownership_price_type,
   cmb.current_price_type,
   cmb.current_price_amt,
   cmb.mc_instock_ind,
   cmb.fc_instock_ind,
   cmb.demand,
   cmb.items,
   cmb.product_views,
   cmb.traffic,
   SUM(dmd.items) AS A1607046073
  FROM combine_prc AS cmb
   LEFT JOIN demand AS dmd ON dmd.business_day_date BETWEEN DATE_SUB(cmb.day_date, INTERVAL 60 DAY) AND (DATE_SUB(cmb.day_date
           ,INTERVAL 1 DAY)) AND cmb.web_style_num = dmd.web_style_num AND LOWER(cmb.rms_style_num) = LOWER(dmd.rms_style_num
         ) AND LOWER(cmb.rms_sku_num) = LOWER(dmd.rms_sku_num) AND LOWER(cmb.business_unit_desc) = LOWER(dmd.business_unit_desc
       ) AND LOWER(cmb.store_num) = LOWER(dmd.store_num)
  WHERE LOWER(cmb.business_unit_desc) IN (LOWER('N.COM'), LOWER('N.CA'), LOWER('OFFPRICE ONLINE'))
  GROUP BY cmb.web_style_num,
   cmb.rms_style_group_num,
   cmb.rms_style_num,
   cmb.rms_sku_num,
   cmb.store_num,
   cmb.business_unit_desc,
   cmb.country,
   cmb.banner,
   cmb.min_date,
   cmb.max_date,
   cmb.max_units,
   cmb.max_style,
   cmb.day_date,
   cmb.dma,
   cmb.eoh,
   cmb.asoh,
   cmb.eoh_mc,
   cmb.asoh_mc,
   cmb.rp_idnt,
   cmb.mc_instock_ind,
   cmb.fc_instock_ind,
   cmb.demand,
   cmb.items,
   cmb.product_views,
   cmb.traffic,
   cmb.ownership_price_type,
   cmb.current_price_type,
   cmb.current_price_amt) AS t1;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) , column(rms_sku_num) , column(web_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) , column(dma) on combine_digital;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS digital_full
--  cluster by rms_sku_num, web_style_num, day_date, store_num
AS
SELECT cdg.web_style_num,
 cdg.rms_style_group_num,
 cdg.rms_style_num,
 cdg.rms_sku_num,
 cdg.store_num,
 cdg.business_unit_desc,
 cdg.country,
 cdg.banner,
 cdg.min_date,
 cdg.max_date,
 cdg.max_units,
 cdg.max_style,
 cdg.day_date,
 cdg.dma,
 cdg.eoh_mc,
 cdg.eoh,
 cdg.asoh_mc,
 cdg.asoh,
 cdg.rp_idnt,
 cdg.ownership_price_type,
 cdg.current_price_type,
 cdg.current_price_amt,
 cdg.mc_instock_ind,
 cdg.fc_instock_ind,
 cdg.demand,
 cdg.items,
 cdg.product_views,
 cdg.traffic,
 cdg.hist_items,
 cdg.hist_items_style,
 cdg.rn,
 cdg.pct_items,
 zero.style_total,
 zero.zero_ind,
  CASE
  WHEN cdg.hist_items_style = 0 AND cdg.rn = 1
  THEN CAST(cdg.product_views AS NUMERIC)
  WHEN cdg.hist_items_style = 0 AND zero.zero_ind = 1
  THEN 1.00000 / cdg.rn * cdg.product_views
  WHEN cdg.hist_items_style = 0
  THEN 0.00000
  ELSE CAST(cdg.product_views * cdg.pct_items AS NUMERIC)
  END AS allocated_traffic
FROM combine_digital AS cdg
 LEFT JOIN (SELECT day_date,
   business_unit_desc,
   web_style_num,
   store_num,
   SUM(pct_items) AS style_total,
    CASE
    WHEN SUM(pct_items) = 0
    THEN 1
    ELSE 0
    END AS zero_ind
  FROM combine_digital
  GROUP BY web_style_num,
   store_num,
   business_unit_desc,
   day_date) AS zero ON cdg.day_date = zero.day_date AND LOWER(cdg.store_num) = LOWER(zero.store_num) AND cdg.web_style_num
    = zero.web_style_num;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) , column(rms_sku_num) , column(web_style_num) , column(day_date) , column(store_num) , column(business_unit_desc) , column(dma) on digital_full;
BEGIN
SET ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily
WHERE day_date BETWEEN CAST({{params.start_date}} AS DATE) AND CAST({{params.end_date}} AS DATE);

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily
(SELECT day_date,
  country,
  banner,
  business_unit_desc,
  dma,
  store_num,
  CAST(web_style_num AS STRING) AS web_style_num,
  rms_style_num,
  rms_sku_num,
  rp_idnt,
  ownership_price_type,
  current_price_type,
  current_price_amt,
  eoh_mc,
  eoh,
  asoh_mc,
  asoh,
  mc_instock_ind,
  fc_instock_ind,
  demand,
  CAST(TRUNC(items) AS INTEGER) AS items,
  CAST(TRUNC(product_views) AS INTEGER) AS product_views,
  CAST(traffic AS BIGINT) AS traffic,
  CAST(TRUNC(hist_items) AS INTEGER) AS hist_items,
  CAST(TRUNC(hist_items_style) AS INTEGER) AS hist_items_style,
  CAST(pct_items AS NUMERIC) AS pct_items,
  CAST(allocated_traffic AS NUMERIC) AS allocated_traffic,
  dw_sys_load_tmstp
 FROM (SELECT DISTINCT day_date,
     country,
     banner,
     business_unit_desc,
     dma,
     store_num,
     web_style_num,
     rms_style_num,
     rms_sku_num,
     rp_idnt,
     ownership_price_type,
     current_price_type,
     current_price_amt,
     eoh_mc,
     eoh,
     asoh_mc,
     asoh,
     mc_instock_ind,
     fc_instock_ind,
     demand,
     items,
     product_views,
     traffic,
     hist_items,
     hist_items_style,
     pct_items,
     allocated_traffic,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM fls_full
    UNION ALL
    SELECT DISTINCT day_date,
     country,
     banner,
     business_unit_desc,
     dma,
     store_num,
     web_style_num,
     rms_style_num,
     rms_sku_num,
     rp_idnt,
     ownership_price_type,
     current_price_type,
     current_price_amt,
     eoh_mc,
     eoh,
     asoh_mc,
     asoh,
     mc_instock_ind,
     fc_instock_ind,
     demand,
     items,
     product_views,
     traffic,
     hist_items,
     hist_items_style,
     pct_items,
     allocated_traffic,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM digital_full
    WHERE LOWER(business_unit_desc) = LOWER('N.COM')
     OR LOWER(business_unit_desc) = LOWER('OFFPRICE ONLINE') AND day_date >= DATE '2021-01-31'
    UNION ALL
    SELECT DISTINCT day_date,
     country,
     banner,
     business_unit_desc,
     dma,
     store_num,
     web_style_num,
     rms_style_num,
     rms_sku_num,
     rp_idnt,
     ownership_price_type,
     current_price_type,
     current_price_amt,
     eoh_mc,
     eoh,
     asoh_mc,
     asoh,
     mc_instock_ind,
     fc_instock_ind,
     demand,
     items,
     product_views,
     traffic,
     hist_items,
     hist_items_style,
     pct_items,
     allocated_traffic,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM combine_rack
    WHERE LOWER(business_unit_desc) = LOWER('RACK') AND day_date >= DATE '2020-02-01'
     OR LOWER(business_unit_desc) = LOWER('RACK') AND rp_idnt = 1 AND day_date < DATE '2020-02-01') AS t7);
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
