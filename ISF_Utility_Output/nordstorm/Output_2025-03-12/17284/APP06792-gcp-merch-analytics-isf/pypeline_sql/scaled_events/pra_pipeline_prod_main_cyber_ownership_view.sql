REPLACE VIEW {environment_schema}.CYBER_OWNERSHIP_VW
AS
LOCK ROW FOR ACCESS

SELECT CURRENT_DATE AS day_date
    , 'NAP' AS data_source
    , loc.country
    , TRIM(loc.chnl_label) AS channel
    , loc.chnl_idnt AS channel_idnt
    , loc.banner
    , TRIM(loc.regn_label) AS region
    , TRIM(loc.loc_label) AS location
    , TRIM(sku.div_num|| ', '||sku.div_desc) AS division
    , TRIM(sku.grp_num|| ', '||sku.grp_desc) AS subdivision
    , TRIM(sku.dept_num|| ', '||sku.dept_desc) AS department
    , TRIM(sku.class_num|| ', '||sku.class_desc) AS "class"
    , TRIM(sku.sbclass_num|| ', '||sku.sbclass_desc) AS subclass
    , sku.style_group_num AS style_group_num
    , sku.style_group_desc
    , sku.web_style_num AS style_num
    , sku.style_desc
    , sku.supp_part_num AS vpn
    , COALESCE(sku.supp_color, 'None') as supp_color
    , sku.color_desc AS nrf_colr
    , spdpt.preferred_partner_desc AS brand_tier
    , vb.vendor_brand_name AS brand
    , sku.prmy_supp_num AS supplier_num
    , ven.vendor_name AS supplier
    , udig.udig
    , coalesce(SKU.npg_ind, 'N') AS npg_ind
    , max(CASE WHEN rp.rp_ind = 1 THEN 'Y' ELSE 'N' END) AS rp_ind
    , coalesce(max(anchor.anchor_brand_ind),'N') AS anchor_brands_ind
    , sum(apo.an_on_order_retail) AS on_order_dollars
    , SUM(apo.an_on_order_cost) AS on_order_cost
    , sum(apo.an_on_order_units) AS on_order_units
    , SUM(apo.an_total_ordered_oo_retail) AS ttl_ordered_dollars
    , SUM(apo.an_total_ordered_oo_cost) AS ttl_ordered_cost
    , SUM(apo.an_total_ordered_oo_units) AS ttl_ordered_units
    , SUM(apo.an_total_canceled_oo_units) AS ttl_canceled_units
    , sum(apo.an_in_transit_retail) AS in_transit_dollars
    , SUM(apo.an_in_transit_cost) AS in_transit_cost
    , sum(apo.an_in_transit_units) AS in_transit_units
    , sum(apo.eoh_retail) AS eoh_dollars
    , sum(apo.eoh_cost) AS eoh_cost
    , sum(apo.eoh_units) AS eoh_units
    , sum(apo.an_receipts_retail) AS receipts_retail
    , sum(apo.an_receipts_cost) AS receipts_cost
    , sum(apo.an_receipts_units) AS receipts_units

FROM {environment_schema}.sku_loc_daily_se_an_on_order apo
INNER JOIN (
    SELECT
    	channel_num||', '||channel_desc AS chnl_label
        , channel_num AS chnl_idnt
        , region_num||', '||region_desc AS regn_label
        , CAST(store_num AS VARCHAR(4))||', '||store_short_name AS loc_label
        , case when chnl_idnt in (110,120) then 'NORDSTROM'
               when chnl_idnt in (210,250) then 'NORDSTROM RACK'
          end as banner
        , store_num AS loc_idnt
        , channel_desc AS chnl_desc
        , store_country_code AS country
    FROM PRD_NAP_USR_VWS.STORE_DIM
    WHERE store_country_code = 'US'
    AND store_close_date IS NULL
)loc
    ON apo.loc_idnt = loc.loc_idnt
INNER JOIN (
  select distinct
  rms_sku_num as sku_idnt
  , 'NORDSTROM RACK' as banner
  from prd_nap_usr_vws.PRODUCT_CATALOG_DIM
  where catalog_num = 'dIk4ii41TzaGo1CKgVOy8Q'
  UNION ALL
  select distinct
  pptd.rms_sku_num as sku_idnt
  , 'NORDSTROM' as banner
  from prd_nap_usr_vws.product_promotion_timeline_dim pptd
  inner join prd_nap_usr_vws.day_cal dt
    on dt.day_date between cast(enticement_start_tmstp as date) and cast(enticement_end_tmstp as date)
    --harcoding beginning and end dates of Cyber period
    where dt.day_date between date'2024-11-10' and date'2024-12-07'
    and pptd.channel_country = 'US'
    and pptd.channel_brand = 'NORDSTROM'
    and enticement_tags = 'NATURAL_BEAUTY'
) cyber_sku
    ON apo.sku_idnt = cyber_sku.sku_idnt
    AND loc.banner = cyber_sku.banner
INNER JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
    ON apo.sku_idnt = sku.rms_sku_num
    AND sku.channel_country = 'US'
LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM ven
	ON ven.vendor_num = SKU.prmy_supp_num
LEFT JOIN PRD_NAP_USR_VWS.VENDOR_BRAND_XREF vb
	ON vb.vendor_num = SKU.prmy_supp_num
  AND vb.vendor_brand_name = SKU.brand_name
LEFT JOIN PRD_NAP_USR_VWS.SUPP_DEPT_MAP_DIM spdpt
	ON spdpt.supplier_num = SKU.prmy_supp_num
		AND spdpt.dept_num = SKU.dept_num
		AND spdpt.banner = loc.banner
		AND CURRENT_DATE BETWEEN CAST(spdpt.eff_begin_tmstp AS DATE FORMAT 'YYYY-MM-DD') AND CAST(spdpt.eff_end_tmstp AS DATE FORMAT 'YYYY-MM-DD')
LEFT JOIN
(
  SELECT distinct rms_sku_num, location_num, 1 AS rp_ind
  FROM prd_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
  WHERE rp_period contains (SELECT distinct day_date FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = (current_date - interval '1' day))
) rp
    ON apo.sku_idnt = rp.rms_sku_num
    AND apo.loc_idnt = rp.location_num
LEFT JOIN
(
  SELECT DISTINCT catalog_name AS udig
  , rms_sku_num as sku_idnt
  , case when catalog_num = 'dIk4ii41TzaGo1CKgVOy8Q' then 'NORDSTROM RACK'
    when catalog_num = 'UTvl_wJWQMqCPEuMCPiCsg' then 'NORDSTROM'
    else 'NA'
    end as banner
  FROM prd_nap_usr_vws.PRODUCT_CATALOG_DIM
  WHERE catalog_num in ('UTvl_wJWQMqCPEuMCPiCsg', 'dIk4ii41TzaGo1CKgVOy8Q')
) udig
    ON apo.sku_idnt = udig.sku_idnt
    AND loc.banner = udig.banner
LEFT JOIN T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.anchor_brands anchor
    ON sku.dept_num = anchor.dept_num
    AND sku.prmy_supp_num = anchor.supplier_idnt
    AND loc.banner = anchor.banner

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
;

GRANT SELECT ON {environment_schema}.CYBER_OWNERSHIP_VW TO PUBLIC;
