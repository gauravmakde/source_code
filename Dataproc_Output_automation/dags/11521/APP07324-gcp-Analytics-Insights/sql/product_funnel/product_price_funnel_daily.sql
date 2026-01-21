BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08227;
DAG_ID=product_price_funnel_daily_11521_ACE_ENG;
---     Task_Name=product_price_funnel_daily;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pfd_base
AS
SELECT event_date_pacific,
 channelcountry,
 channel,
 platform,
 site_source,
 style_id,
 sku_id,
 averagerating,
 review_count,
 order_quantity,
 order_demand,
 order_sessions,
 add_to_bag_quantity,
 add_to_bag_sessions,
 product_views,
 product_view_sessions
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_funnel_daily
WHERE event_date_pacific BETWEEN {{params.start_date}}

 AND ({{params.end_date}}

)
 AND LOWER(site_source) = LOWER('MODERN')
UNION ALL
SELECT event_date_pacific,
 channelcountry,
 channel,
 platform,
 site_source,
 web_style_id AS style_id,
 rms_sku_num AS sku_id,
 averagerating,
 review_count,
 order_units AS order_quantity,
 demand AS order_demand,
 order_sessions,
 cart_adds AS add_to_bag_quantity,
 add_to_bag_sessions,
 product_views,
 product_view_sessions
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_funnel_daily_history
WHERE event_date_pacific BETWEEN {{params.start_date}}

 AND ({{params.end_date}}

);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (sku_id, style_id, event_date_pacific, channel, channelcountry) , column(sku_id) , column(style_id) , column(event_date_pacific) , column(channel) , column(channelcountry) on pfd_base;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pfd_los

AS
SELECT b.event_date_pacific,
 b.channelcountry,
 b.channel,
 b.platform,
 b.site_source,
 b.style_id,
 COALESCE(l.sku_id, 'unknown') AS sku_id,
 b.averagerating,
 b.review_count,
 b.order_quantity,
 b.order_demand,
 b.order_sessions,
 b.add_to_bag_quantity,
 b.add_to_bag_sessions,
  b.product_views / COALESCE(l.sku_cnt, 1) AS product_views,
 b.product_view_sessions
FROM pfd_base AS b
 LEFT JOIN (SELECT DISTINCT los.day_date,
   los.channel_country,
    CASE
    WHEN LOWER(los.channel_brand) = LOWER('NORDSTROM')
    THEN 'FULL_LINE'
    WHEN LOWER(los.channel_brand) = LOWER('NORDSTROM_RACK')
    THEN 'RACK'
    ELSE NULL
    END AS channel_brand,
    CASE
    WHEN los.day_date <= DATE '2022-05-01'
    THEN CAST(CASE
      WHEN COALESCE(e.web_style_id, FORMAT('%20d', s.web_style_num)) = ''
      THEN '0'
      ELSE COALESCE(e.web_style_id, FORMAT('%20d', s.web_style_num))
      END AS BIGINT)
    ELSE CAST(CASE
      WHEN COALESCE(FORMAT('%20d', s.web_style_num), e.web_style_id) = ''
      THEN '0'
      ELSE COALESCE(FORMAT('%20d', s.web_style_num), e.web_style_id)
      END AS BIGINT)
    END AS web_style,
   los.sku_id,
   COUNT(los.sku_id) OVER (PARTITION BY los.day_date, los.channel_country, los.channel_brand, CASE
       WHEN los.day_date <= DATE '2022-05-01'
       THEN CAST(CASE
         WHEN COALESCE(e.web_style_id, FORMAT('%20d', s.web_style_num)) = ''
         THEN '0'
         ELSE COALESCE(e.web_style_id, FORMAT('%20d', s.web_style_num))
         END AS BIGINT)
       ELSE CAST(CASE
         WHEN COALESCE(FORMAT('%20d', s.web_style_num), e.web_style_id) = ''
         THEN '0'
         ELSE COALESCE(FORMAT('%20d', s.web_style_num), e.web_style_id)
         END AS BIGINT)
       END RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sku_cnt
  FROM `{{params.gcp_project_id}}`.t2dl_das_site_merch.live_on_site_daily AS los
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS s ON LOWER(los.sku_id) = LOWER(s.rms_sku_num) AND LOWER(los.channel_country
      ) = LOWER(s.channel_country)
   LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ete_instrumentation.live_on_site_pcdb_us_opr AS e ON LOWER(los.sku_id) = LOWER(e.rms_sku_id)
  WHERE los.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
) AS l
 ON b.event_date_pacific = l.day_date AND LOWER(b.channelcountry
      ) = LOWER(l.channel_country) AND LOWER(b.channel) = LOWER(l.channel_brand) AND b.style_id = l.web_style
WHERE LOWER(b.sku_id) = LOWER('unknown')
UNION ALL
SELECT *
FROM pfd_base
WHERE LOWER(sku_id) <> LOWER('unknown');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (sku_id, style_id, event_date_pacific, channel, channelcountry) , column(sku_id) , column(style_id) , column(event_date_pacific) , column(channel) , column(channelcountry) on pfd_los;



CREATE TEMPORARY TABLE IF NOT EXISTS  rp_lkp
as (
select
rms_sku_num
, case when business_unit_desc  in ('N.COM', 'N.CA') then LOWER('FULL_LINE') else 'RACK' end as channel
, case when business_unit_desc  in ('N.COM', 'OFFPRICE ONLINE') then LOWER('US') else 'CA' end as channelcountry
, day_date
, 'Y' as aip_rp_fl
from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
inner join (
select distinct day_date
from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
where day_date between cast({{params.start_date}}

  as date) and cast({{params.end_date}}

 as date)
) d
on d.day_date between RANGE_START(rp_period) and RANGE_END(rp_period)
inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim st
on rp.location_num = st.store_num
where business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE', 'N.CA')
group by 1,2,3,4,5
) ;




--collect statistics primary index (rms_sku_num, day_date, channel, channelcountry) , column(rms_sku_num) , column(day_date) , column(channel) , column(channelcountry) on rp_lkp;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS prc

AS
SELECT DISTINCT c.day_date,
  CASE
  WHEN LOWER(nap.channel_brand) = LOWER('NORDSTROM')
  THEN 'FULL_LINE'
  ELSE 'RACK'
  END AS channel_brand,
 nap.channel_country AS channelcountry,
 nap.rms_sku_num,
 nap.regular_price_amt,
 nap.selling_retail_price_amt AS current_price_amt,
  CASE
  WHEN LOWER(nap.selling_retail_price_type_code) = LOWER('PROMOTION') AND LOWER(nap.ownership_retail_price_type_code) =
    LOWER('CLEARANCE')
  THEN 'C'
  WHEN LOWER(nap.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'R'
  WHEN LOWER(nap.selling_retail_price_type_code) = LOWER('PROMOTION')
  THEN 'P'
  WHEN LOWER(nap.selling_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  ELSE NULL
  END AS current_price_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS nap
 INNER JOIN (SELECT day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE day_date BETWEEN {{params.start_date}}

 AND ({{params.end_date}}

)
   AND day_date >= DATE '2020-10-01') AS c ON c.day_date BETWEEN CAST(nap.eff_begin_tmstp AS DATE) AND (CAST(nap.eff_end_tmstp AS DATE)
   )
WHERE LOWER(nap.selling_channel) = LOWER('ONLINE')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY c.day_date, nap.rms_sku_num, channelcountry, channel_brand ORDER BY nap.eff_begin_tmstp
      DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, day_date, channel_brand, channelcountry) , column(rms_sku_num) , column(day_date) , column(channel_brand) , column(channelcountry) on prc;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS twist

AS
SELECT day_date,
  CASE
  WHEN LOWER(banner) = LOWER('NORD')
  THEN LOWER('FULL_LINE')
  ELSE LOWER('RACK')
  END AS channel,
 country,
 web_style_num,
 SUM(CASE
   WHEN mc_instock_ind = 1
   THEN allocated_traffic
   ELSE 0
   END) AS instock_views,
 SUM(allocated_traffic) AS total_views,
  CASE
  WHEN SUM(allocated_traffic) = 0
  THEN NULL
  ELSE SUM(CASE
     WHEN mc_instock_ind = 1
     THEN allocated_traffic
     ELSE 0
     END) / SUM(allocated_traffic)
  END AS pct_instock
FROM `{{params.gcp_project_id}}`.t2dl_das_twist.twist_daily
WHERE LOWER(business_unit_desc) IN (LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('N.CA'))
 AND product_views > 0
 AND day_date BETWEEN {{params.start_date}}

 AND ({{params.end_date}}

)
GROUP BY day_date,
 channel,
 country,
 web_style_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (web_style_num, day_date, channel, country) , column(web_style_num) , column(day_date) , column(channel) , column(country) on twist;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS flash

AS
SELECT e.sku_num,
 'RACK' AS channel,
 e.channel_country,
 dt.day_date,
 e.selling_event_name AS event_name
FROM (SELECT DISTINCT day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE day_date BETWEEN {{params.start_date}}

 AND ({{params.end_date}}

)) AS dt
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_selling_event_sku_dim AS e ON dt.day_date BETWEEN CAST(e.item_start_tmstp AS DATE)
  AND (CAST(e.item_end_tmstp AS DATE))
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_selling_event_tags_dim AS t3 ON LOWER(e.selling_event_num) = LOWER(t3.selling_event_num
    ) AND LOWER(t3.tag_name) = LOWER('FLASH')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY e.sku_num, LOWER('RACK'), e.channel_country, dt.day_date ORDER BY e.selling_event_name
      DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (sku_num, day_date, channel, channel_country) , column(sku_num) , column(day_date) , column(channel) , column(channel_country) on flash;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.product_price_funnel_daily
WHERE event_date_pacific BETWEEN {{params.start_date}}
 AND ({{params.end_date}}
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


insert into `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.product_price_funnel_daily
select
p.event_date_pacific
, p.channelcountry
, p.channel
, p.platform
, p.site_source
, p.style_id as web_style_num
, p.sku_id as rms_sku_num
, case when p.sku_id = 'unknown' then null
when r.rms_sku_num is not null then 1
else 0 end as rp_ind
, p.averagerating
, p.review_count
, cast(trunc(cast(p.order_quantity as float64)) as int64)
, CAST(p.order_demand as numeric)
, p.order_sessions
, cast(trunc(cast(p.add_to_bag_quantity  as float64))as int64)
, p.add_to_bag_sessions
, cast(p.product_views as numeric)
, p.product_view_sessions
, case when p.sku_id = 'unknown' then null
when c.current_price_type is null then 'R'
else c.current_price_type end as current_price_type
, c.current_price_amt
, c.regular_price_amt
, t.pct_instock
, case when f.sku_num is not null then LOWER('FLASH') else LOWER('PERSISTENT') end as event_type
, upper(coalesce(f.event_name, LOWER('NONE'))) as event_name
,  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp

from pfd_los p
left join rp_lkp r
on p.event_date_pacific = r.day_date
and p.channelcountry = r.channelcountry
and p.channel = r.channel
and p.sku_id = r.rms_sku_num
left join prc c
on p.event_date_pacific = c.day_date
and p.channelcountry = c.channelcountry
and p.channel = c.channel_brand
and p.sku_id = c.rms_sku_num
left join `{{params.gcp_project_id}}`.t2dl_das_ete_instrumentation.live_on_site_pcdb_us_opr e
on p.sku_id = e.rms_sku_id
left join twist t
on p.event_date_pacific = t.day_date
and p.channel = t.channel
and p.channelcountry = t.country
and cast(coalesce(CAST(trunc(cast(p.style_id as float64))  AS INT64),CAST(trunc(Cast(e.web_style_id as float64)) AS INT64)) as bigint) = cast(trunc(cast(t.web_style_num  as float64))as bigint)
left join flash f
on p.event_date_pacific = f.day_date
and p.channel = f.channel
and p.channelcountry = f.channel_country
and p.sku_id = f.sku_num
;




--COLLECT STATS ON {{params.product_funnel_t2_schema}}.product_price_funnel_daily  COLUMN( event_date_pacific ,rms_sku_num);
--COLLECT STATS ON {{params.product_funnel_t2_schema}}.product_price_funnel_daily  COLUMN( event_date_pacific );
--COLLECT STATS ON {{params.product_funnel_t2_schema}}.product_price_funnel_daily  COLUMN( rms_sku_num);
--COLLECT STATS ON {{params.product_funnel_t2_schema}}.product_price_funnel_daily  COLUMN( channelcountry);
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
