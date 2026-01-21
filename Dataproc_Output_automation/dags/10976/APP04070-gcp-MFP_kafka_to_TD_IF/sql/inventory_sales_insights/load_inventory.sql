/* SET QUERY_BAND = 'App_ID=app04070;DAG_ID=inventory_sales_insights_history_10976_tech_nap_merch;Task_Name=execute_load_inventory;'
FOR SESSION VOLATILE;*/

-- ET;

-- Load Daily Inventory metrics


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_sales_insights_by_day_hist_fact
(
    metrics_date,
    rms_sku_id,
    rms_style_num,
    color_num,
    channel_brand,
    selling_channel,
    channel_country,
    location_id,
    total_stock_on_hand_qty,
    total_in_transit_qty,
    anniversary_item_ind,
    promotion_ind,
    ownership_price_amt,
    regular_price_amt,
    base_retail_drop_ship_amt,
    regular_price_percent_off,
    current_price_amt,
    is_online_purchasable,
    online_purchasable_eff_begin_tmstp,
    online_purchasable_eff_begin_tmstp_tz,
    online_purchasable_eff_end_tmstp,
    online_purchasable_eff_end_tmstp_tz,
    weighted_average_cost,
    rack_ind,
    store_type_code,
    store_abbrev_name,
    compare_at_retail_price_amt,
    base_retail_percentage_off_compare_at_value,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz
)
SELECT
    isqbdlf.snapshot_date,
    isqbdlf.rms_sku_id,
    psd.rms_style_num,
    trim(psd.color_num) as color_num,
    psdv.channel_brand,
    psdv.selling_channel,
    psdv.channel_country,
    trim(isqbdlf.location_id) as location_id,
    isqbdlf.stock_on_hand_qty,
    isqbdlf.in_transit_qty,
    case
      WHEN pptd.enticement_tags LIKE '%ANNIVERSARY_SALE%'
      THEN 'Y'
      ELSE 'N'
    END AS anniversary_item_ind,
    CASE
      WHEN LOWER(pptd.selling_retail_price_type_code) = LOWER('P')
      THEN 'Y'
      ELSE 'N'
    END AS promotion_ind,
    pptd.OWNERSHIP_PRICE_AMT,
    pptd.REGULAR_PRICE_AMT,
    pptd.REGULAR_PRICE_AMT AS BASE_RETAIL_DROP_SHIP_AMT,
        trunc(ROUND((pptd.regular_price_amt - pptd.ownership_price_amt) / CAST(ROUND(nullif(pptd.regular_price_amt, NUMERIC '0'), 6, 'ROUND_HALF_EVEN') as NUMERIC), 6, 'ROUND_HALF_EVEN'), 2) AS regular_price_percent_off,
    pptd.current_price_amt,
    purch.is_online_purchasable,
    purch.eff_begin_tmstp as online_purchasable_eff_begin_tmstp,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(purch.eff_begin_tmstp as string)) as online_purchasable_eff_begin_tmstp_tz,
    purch.eff_end_tmstp as online_purchasable_eff_end_tmstp,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(purch.eff_end_tmstp as string)) as online_purchasable_eff_end_tmstp_tz,
    coalesce(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) as weighted_average_cost,
    'N' AS rack_ind, -- init all rack_ind until update query populates first_rack_dates and associated rack_ind
    psdv.store_type_code,
    psdv.store_abbrev_name,
    pptd.compare_at_value,
    pptd.base_retail_percentage_off_compare_at_value,
    timestamp(current_datetime('PST8PDT')),
    `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`()
from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_stock_quantity_by_day_logical_fact as isqbdlf

left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw as psdv
   on cast(trunc(cast(trim(isqbdlf.location_id )as float64)) as integer) = psdv.store_num
inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist as psd
   on LOWER(isqbdlf.rms_sku_id)      = LOWER(psd.rms_sku_num)
  and LOWER(psdv.store_country_code) = LOWER(psd.channel_country)
  AND LOWER(psd.partner_relationship_type_code) <> LOWER('ECONCESSION') -- excluding 'ECONCESSION' inventory
  and range_contains(range(psd.eff_begin_tmstp_utc, psd.eff_end_tmstp_utc) , cast( isqbdlf.snapshot_date as timestamp) + interval 1 day - interval 1 millisecond)
inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control as elt
   ON LOWER(elt.subject_area_nm)   = LOWER('METRICS_DATA_SERVICE_HIST')
  and isqbdlf.snapshot_date between cast(elt.extract_from_tmstp as date) and cast(elt.extract_to_tmstp as date)
left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim as orgstore
   on  cast(trunc(cast(trim(isqbdlf.location_id )as float64)) as integer)  = orgstore.store_num
  
left join (
            select distinct
                pptd_price.rms_sku_num
              , pptd_price.channel_country
              , pptd_price.channel_brand
              , pptd_price.selling_channel
              , pptd_promo.enticement_tags
              , CASE
                  WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('REGULAR')   THEN 'R'
                  WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('PROMOTION') THEN 'P'
                  WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('CLEARANCE') THEN 'C'
                  ELSE pptd_price.selling_retail_price_type_code -- default value (edge case)
                END AS selling_retail_price_type_code
               , pptd_price.ownership_retail_price_amt as ownership_price_amt
               , pptd_price.regular_price_amt
               , pptd_price.selling_retail_price_amt as current_price_amt
               , pptd_price.compare_at_retail_price_amt as compare_at_value
               , pptd_price.base_retail_percentage_off_compare_at_retail_pct as base_retail_percentage_off_compare_at_value
               , pptd_price.eff_begin_tmstp_utc as eff_begin_tmstp
               , pptd_price.eff_end_tmstp_utc as eff_end_tmstp
               from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim as pptd_price
               left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_promotion_timeline_dim as pptd_promo
                 on LOWER(pptd_promo.rms_sku_num)         = LOWER(pptd_price.rms_sku_num)
                and LOWER(pptd_promo.channel_country)     = LOWER(pptd_price.channel_country)
                and LOWER(pptd_promo.selling_channel)     = LOWER(pptd_price.selling_channel)
                and LOWER(pptd_promo.channel_brand)       = LOWER(pptd_price.channel_brand)
                and LOWER(pptd_promo.promo_id)            = LOWER(pptd_price.selling_retail_record_id)
                AND LOWER(pptd_promo.promotion_type_code) = 'SIMPLE'
                and range_overlaps(range(pptd_price.eff_begin_tmstp, pptd_price.eff_end_tmstp) , range(pptd_promo.eff_begin_tmstp, pptd_promo.eff_end_tmstp))
               inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control as elt
                 ON LOWER(elt.subject_area_nm)    = LOWER('METRICS_DATA_SERVICE_HIST')
             where pptd_price.eff_end_tmstp_utc >=     cast(date(date(elt.extract_from_tmstp + interval 1 day) - interval 1 millisecond) as timestamp)
          ) as pptd
   on LOWER(pptd.rms_sku_num)     = LOWER(isqbdlf.rms_sku_id)
  and LOWER(psdv.channel_country) = LOWER(pptd.channel_country)
  and LOWER(pptd.channel_brand)   = LOWER(psdv.channel_brand)
  and LOWER(pptd.selling_channel) = LOWER(psdv.selling_channel)
  and range_contains(range(pptd.eff_begin_tmstp, pptd.eff_end_tmstp) ,     cast( isqbdlf.snapshot_date as timestamp) + interval 1 day - interval 1 millisecond)
left join (
            select distinct
                opid.rms_sku_num
              , opid.channel_country
              , opid.channel_brand
              , opid.selling_channel
              , opid.is_online_purchasable
              , cast(opid.eff_begin_tmstp as timestamp) as eff_begin_tmstp
              , cast(opid.eff_end_tmstp as timestamp) as eff_end_tmstp
              from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_online_purchasable_item_dim as opid
              join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control as elt
                ON LOWER(elt.subject_area_nm)  = LOWER('METRICS_DATA_SERVICE_HIST')
             where opid.eff_end_tmstp >= elt.extract_from_tmstp
          ) as purch
   on LOWER(purch.rms_sku_num) = LOWER(isqbdlf.rms_sku_id)
  and LOWER(purch.channel_country) = LOWER(psdv.channel_country)
  and LOWER(purch.channel_brand) = LOWER(psdv.channel_brand)
 and range_contains(range(purch.eff_begin_tmstp, purch.eff_end_tmstp) ,     cast( isqbdlf.snapshot_date as timestamp) + interval 1 day - interval 1 millisecond)
left join (
            select
                wac1.sku_num,
                trim(wac1.location_num) as location_num,
                wac1.weighted_average_cost,
                wac1.eff_begin_dt,
                wac1.eff_end_dt
              from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim as wac1
            where wac1.eff_end_dt >= (
                                       select cast(elt.extract_from_tmstp as date)
                                         from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control as elt
                                        where LOWER(elt.subject_area_nm) = LOWER('METRICS_DATA_SERVICE_HIST')
                                     )
          ) as wacd
   on LOWER(isqbdlf.rms_sku_id) = LOWER(wacd.sku_num)
  and LOWER(trim(cast(orgstore.store_num as string))) = LOWER(trim(wacd.location_num))
  and isqbdlf.snapshot_date >= wacd.eff_begin_dt
  and isqbdlf.snapshot_date < wacd.eff_end_dt
left join (
            select
                wac2.sku_num,
                wac2.channel_num,
                wac2.weighted_average_cost,
                wac2.eff_begin_dt,
                wac2.eff_end_dt
              from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim as wac2
             where wac2.eff_end_dt >= (
                                        select cast(elt.extract_from_tmstp as date)
                                          from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control as elt
                                         WHERE LOWER(elt.subject_area_nm) = LOWER('METRICS_DATA_SERVICE_HIST')
                                      )
          ) AS wacc
   on LOWER(isqbdlf.rms_sku_id) = LOWER(wacc.sku_num)
  and orgstore.channel_num = wacc.channel_num
  and isqbdlf.snapshot_date >= wacc.eff_begin_dt
  and isqbdlf.snapshot_date < wacc.eff_end_dt
;




UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_sales_insights_by_day_hist_fact AS tgt SET
 anniversary_item_ind = src.anniversary_item_ind,
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(dw_sys_updt_tmstp as string))
 FROM (SELECT DISTINCT isibdf.rms_sku_id,
   isibdf.rms_style_num,
   isibdf.color_num,
   isibdf.channel_country,
   isibdf.selling_channel,
   isibdf.channel_brand,
   isibdf.location_id,
   'Y' AS anniversary_item_ind
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_hist_fact AS isibdf
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pptd_price ON LOWER(isibdf.rms_sku_id) = LOWER(pptd_price.rms_sku_num
        ) AND LOWER(isibdf.channel_country) = LOWER(pptd_price.channel_country) AND LOWER(isibdf.selling_channel) =
      LOWER(pptd_price.selling_channel) AND LOWER(isibdf.channel_brand) = LOWER(pptd_price.channel_brand)
and range_contains(range(pptd_price.eff_begin_tmstp_utc,     CASE
      WHEN pptd_price.eff_end_tmstp_utc < CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) THEN TIMESTAMP(pptd_price.eff_end_tmstp_utc + INTERVAL 45 DAY)
      ELSE pptd_price.eff_end_tmstp_utc
    END) ,     CAST( isibdf.metrics_date as TIMESTAMP) + INTERVAL 1 DAY - INTERVAL 1 MILLISECOND)


   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_promotion_timeline_dim AS pptd_promo ON LOWER(isibdf.rms_sku_id) = LOWER(pptd_promo
          .rms_sku_num) AND LOWER(isibdf.channel_country) = LOWER(pptd_promo.channel_country) AND LOWER(isibdf.selling_channel
         ) = LOWER(pptd_promo.selling_channel) AND LOWER(isibdf.channel_brand) = LOWER(pptd_promo.channel_brand) AND
      LOWER(pptd_price.selling_retail_record_id) = LOWER(pptd_promo.promo_id) AND LOWER(pptd_promo.promotion_type_code)
     = LOWER('SIMPLE')
  and range_contains(range(pptd_promo.eff_begin_tmstp_utc, case
                                                   when pptd_promo.eff_end_tmstp_utc < cast(current_DATETIME('PST8PDT') as timestamp)
                                                   then TIMESTAMP(pptd_promo.eff_end_tmstp_utc  + INTERVAL 45 DAY)
                                                   else pptd_promo.eff_end_tmstp_utc
                                                 end) ,     CAST( isibdf.metrics_date as TIMESTAMP) + INTERVAL 1 DAY - INTERVAL 1 MILLISECOND)


   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control AS eltn ON LOWER(eltn.subject_area_nm) = LOWER('METRICS_DATA_SERVICE_HIST')
  WHERE isibdf.metrics_date BETWEEN CAST(eltn.extract_from_tmstp AS DATE) AND (CAST(eltn.extract_to_tmstp AS DATE))
   AND LOWER(pptd_promo.enticement_tags) LIKE LOWER('%ANNIVERSARY_SALE%')) AS src, `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control AS elt
WHERE tgt.metrics_date BETWEEN CAST(elt.extract_from_tmstp AS DATE) AND (CAST(elt.extract_to_tmstp AS DATE)) AND LOWER(elt
          .subject_area_nm) = LOWER('METRICS_DATA_SERVICE_HIST') AND LOWER(tgt.rms_sku_id) = LOWER(src.rms_sku_id) AND
       LOWER(tgt.rms_style_num) = LOWER(src.rms_style_num) AND LOWER(tgt.color_num) = LOWER(src.color_num) AND LOWER(tgt
      .channel_country) = LOWER(src.channel_country) AND LOWER(tgt.selling_channel) = LOWER(src.selling_channel) AND
   LOWER(tgt.channel_brand) = LOWER(src.channel_brand) AND LOWER(tgt.location_id) = LOWER(src.location_id);


--COLLECT STATISTICS     COLUMN(RMS_SKU_ID),     COLUMN(RMS_STYLE_NUM),     COLUMN(COLOR_NUM),     COLUMN(CHANNEL_COUNTRY),     COLUMN(CHANNEL_BRAND),     COLUMN(SELLING_CHANNEL),     COLUMN(LOCATION_ID),     COLUMN(RMS_SKU_ID, RMS_STYLE_NUM, COLOR_NUM, CHANNEL_COUNTRY, CHANNEL_BRAND, SELLING_CHANNEL, LOCATION_ID),     COLUMN(PARTITION) ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.INVENTORY_SALES_INSIGHTS_BY_DAY_HIST_FACT