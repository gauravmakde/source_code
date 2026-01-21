

DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_rp
WHERE (week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}});

INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_rp
with store as (
    SELECT store_num,
  CASE
  WHEN channel_num IN (110, 120)
  THEN 'NORDSTROM'
  WHEN channel_num IN (210, 250)
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
  CASE
  WHEN channel_num IN (110, 210)
  THEN 'STORE'
  WHEN channel_num IN (120, 250)
  THEN 'ONLINE'
  ELSE NULL
  END AS selling_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE channel_num IN (110, 210, 120, 250)
),

wk as (
   SELECT DISTINCT week_idnt AS week_num,
 CAST(day_date AS DATE) AS day_date,
 CAST(week_start_day_date AS DATE) AS week_start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}}  AND ({{params.end_date}})
), 
sku as ( 
    SELECT DISTINCT rms_sku_num,
 epm_choice_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
WHERE LOWER(channel_country) = LOWER('US')
)
select 
	store.channel_brand,
	store.selling_channel,
	sku.epm_choice_num,
	wk.week_num,
  wk.week_start_date,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS date) as last_updated_utc,
   
	1 as rp_ind,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT') ) AS DATETIME) as dw_sys_load_tmstp
from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
   join wk
		on wk.day_date >= RANGE_START(rp.RP_PERIOD) 
		and wk.day_date <= RANGE_END(rp.RP_PERIOD)
     JOIN store
        ON store.STORE_NUM = CAST(RP.location_num AS INTEGER)
     JOIN sku
        ON RP.rms_sku_num  = sku.rms_sku_num
    WHERE sku.epm_choice_num IS NOT NULL
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        8;
