

CREATE TEMPORARY TABLE IF NOT EXISTS price_type AS 
SELECT
    rms_sku_num,
    DATETIME_ADD(eff_begin_tmstp, INTERVAL 1 SECOND) AS eff_begin_tmstp, -- Add 1 second to prevent duplication
    eff_end_tmstp,
    CASE 
        WHEN ownership_retail_price_type_code = 'CLEARANCE' THEN 'Clearance'
        WHEN selling_retail_price_type_code = 'CLEARANCE' THEN 'Clearance'
        WHEN selling_retail_price_type_code = 'PROMOTION' THEN 'Promotion'
        WHEN selling_retail_price_type_code = 'REGULAR' THEN 'Regular Price'
        ELSE NULL 
    END AS price_type,
    store_num,
    CASE 
        WHEN ppd.channel_brand = 'NORDSTROM_RACK' AND ppd.selling_channel = 'STORE' AND ppd.channel_country = 'CA' THEN 'RACK CANADA'
        WHEN ppd.channel_brand = 'NORDSTROM_RACK' AND ppd.selling_channel = 'STORE' AND ppd.channel_country = 'US' THEN 'RACK'
        WHEN ppd.channel_brand = 'NORDSTROM_RACK' AND ppd.selling_channel = 'ONLINE' AND ppd.channel_country = 'US' THEN 'OFFPRICE ONLINE'
        WHEN ppd.channel_brand = 'NORDSTROM' AND ppd.selling_channel = 'STORE' AND ppd.channel_country = 'CA' THEN 'FULL LINE CANADA'
        WHEN ppd.channel_brand = 'NORDSTROM' AND ppd.selling_channel = 'STORE' AND ppd.channel_country = 'US' THEN 'FULL LINE'
        WHEN ppd.channel_brand = 'NORDSTROM' AND ppd.selling_channel = 'ONLINE' AND ppd.channel_country = 'CA' THEN 'N.CA'
        WHEN ppd.channel_brand = 'NORDSTROM' AND ppd.selling_channel = 'ONLINE' AND ppd.channel_country = 'US' THEN 'N.COM'
        ELSE NULL 
    END AS business_unit_desc
FROM 
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim ppd
WHERE 
    eff_end_tmstp >= CAST({{params.start_date}} AS DATE) 
    AND eff_begin_tmstp <= CAST({{params.end_date}} AS DATE)
    AND (
        ownership_retail_price_type_code = 'CLEARANCE' 
        OR selling_retail_price_type_code IN ('CLEARANCE', 'PROMOTION')
    );





CREATE TEMPORARY TABLE IF NOT EXISTS jdmv

AS
SELECT demand_date,
 business_unit_desc,
 channel,
 line_item_currency_code,
 inventory_business_model,
 price_type,
 remote_selling_ind,
 fulfilled_from_location_type,
 delivery_method,
 rms_sku_num,
 demand_tmstp_pacific,
 jwn_reported_demand_ind,
 demand_units,
 jwn_reported_demand_usd_amt,
 jwn_gross_demand_usd_amt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_demand_metric_vw AS jdmv
WHERE demand_date BETWEEN CAST({{params.start_date}} AS DATE) AND CAST({{params.end_date}} AS DATE)
 AND demand_date BETWEEN DATE '2021-01-31' AND (DATE_SUB(CURRENT_DATE ('PST8PDT'), INTERVAL 1 DAY))
 AND LOWER(zero_value_unit_ind) = LOWER('N')
 AND (LOWER(business_unit_country) <> LOWER('CA') OR business_day_date <= DATE '2023-02-25');



CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_joined AS
SELECT
    jdmv.demand_date,
    jdmv.business_unit_desc,
    jdmv.channel,
    jdmv.line_item_currency_code,
    CAST(
        CASE 
            WHEN jdmv.price_type <> 'NOT_APPLICABLE' THEN jdmv.price_type
            WHEN ptc.price_type IS NOT NULL THEN ptc.price_type
            ELSE 'Regular Price' 
        END AS STRING
    ) AS price_type,
    jdmv.fulfilled_from_location_type,
    jdmv.delivery_method,
    jdmv.jwn_reported_demand_ind,
    jdmv.demand_units,
    jdmv.jwn_reported_demand_usd_amt,
    jdmv.jwn_gross_demand_usd_amt
FROM 
    jdmv AS jdmv
LEFT JOIN 
    price_type AS ptc
ON 
    jdmv.rms_sku_num = ptc.rms_sku_num 
    AND jdmv.business_unit_desc = ptc.business_unit_desc
    AND jdmv.demand_tmstp_pacific BETWEEN ptc.eff_begin_tmstp AND ptc.eff_end_tmstp
    AND jdmv.price_type = 'NOT_APPLICABLE';





CREATE TEMPORARY TABLE IF NOT EXISTS g_bands AS
SELECT
    jdmv.demand_date,
    jdmv.business_unit_desc,
    jdmv.channel,
    jdmv.price_type,
    jdmv.fulfilled_from_location_type,
    jdmv.delivery_method,
    CASE 
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 0.01 AND 5.00 THEN '0-5'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 5.01 AND 10 THEN '5-10'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 10.01 AND 15 THEN '10-15'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 15.01 AND 20 THEN '15-20'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 20.01 AND 25 THEN '20-25'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 25.01 AND 30 THEN '25-30'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 30.01 AND 35 THEN '30-35'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 35.01 AND 40 THEN '35-40'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 40.01 AND 45 THEN '40-45'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 45.01 AND 50 THEN '45-50'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 50.01 AND 55 THEN '50-55'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 55.01 AND 60 THEN '55-60'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 60.01 AND 65 THEN '60-65'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 65.01 AND 70 THEN '65-70'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 70.01 AND 75 THEN '70-75'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 75.01 AND 80 THEN '75-80'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 80.01 AND 85 THEN '80-85'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 85.01 AND 90 THEN '85-90'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 90.01 AND 95 THEN '90-95'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 95.01 AND 100 THEN '95-100'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 100.01 AND 200 THEN '100-200'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 200.01 AND 300 THEN '200-300'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 300.01 AND 400 THEN '300-400'
        WHEN jdmv.jwn_gross_demand_usd_amt BETWEEN 400.01 AND 500 THEN '400-500'
        ELSE 'above 500' 
    END AS gross_demand_usd_price_band,
    SUM(jdmv.jwn_gross_demand_usd_amt) AS gross_demand_usd_amt,
    SUM(jdmv.demand_units) AS gross_demand_units
FROM 
    jdmv_joined AS jdmv
GROUP BY 
    jdmv.demand_date,
    jdmv.business_unit_desc,
    jdmv.channel,
    jdmv.price_type,
    jdmv.fulfilled_from_location_type,
    jdmv.delivery_method,
    gross_demand_usd_price_band;






CREATE TEMPORARY TABLE IF NOT EXISTS r_bands AS
SELECT
    jdmv.demand_date,
    jdmv.business_unit_desc,
    jdmv.channel,
    jdmv.price_type,
    jdmv.fulfilled_from_location_type,
    jdmv.delivery_method,
    CASE 
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 0.01 AND 5.00 THEN '0-5'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 5.01 AND 10 THEN '5-10'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 10.01 AND 15 THEN '10-15'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 15.01 AND 20 THEN '15-20'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 20.01 AND 25 THEN '20-25'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 25.01 AND 30 THEN '25-30'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 30.01 AND 35 THEN '30-35'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 35.01 AND 40 THEN '35-40'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 40.01 AND 45 THEN '40-45'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 45.01 AND 50 THEN '45-50'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 50.01 AND 55 THEN '50-55'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 55.01 AND 60 THEN '55-60'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 60.01 AND 65 THEN '60-65'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 65.01 AND 70 THEN '65-70'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 70.01 AND 75 THEN '70-75'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 75.01 AND 80 THEN '75-80'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 80.01 AND 85 THEN '80-85'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 85.01 AND 90 THEN '85-90'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 90.01 AND 95 THEN '90-95'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 95.01 AND 100 THEN '95-100'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 100.01 AND 200 THEN '100-200'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 200.01 AND 300 THEN '200-300'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 300.01 AND 400 THEN '300-400'
        WHEN jdmv.jwn_reported_demand_usd_amt BETWEEN 400.01 AND 500 THEN '400-500'
        ELSE 'above 500' 
    END AS reported_demand_usd_price_band,
    SUM(jdmv.jwn_reported_demand_usd_amt) AS reported_demand_usd_amt,
    SUM(CASE 
            WHEN jdmv.jwn_reported_demand_ind = 'Y' THEN jdmv.demand_units 
            ELSE 0 
        END) AS reported_demand_units
FROM 
   jdmv_joined AS jdmv
GROUP BY 
    jdmv.demand_date,
    jdmv.business_unit_desc,
    jdmv.channel,
    jdmv.price_type,
    jdmv.fulfilled_from_location_type,
    jdmv.delivery_method,
    reported_demand_usd_price_band;





CREATE TEMPORARY TABLE IF NOT EXISTS jogmv AS
SELECT business_day_date,
 business_unit_desc,
 channel,
 price_type,
 fulfilled_from_location_type,
 delivery_method,
 service_type,
 jwn_fulfilled_demand_ind,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 product_return_ind,
 jwn_operational_gmv_ind,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_operational_gmv_metric_vw AS jogmv
WHERE business_day_date BETWEEN CAST({{params.start_date}} AS DATE) AND CAST({{params.end_date}} AS DATE)
 AND business_day_date BETWEEN DATE '2021-01-31' AND (DATE_SUB(CURRENT_DATE ('PST8PDT'), INTERVAL 1 DAY))
 AND LOWER(zero_value_unit_ind) = LOWER('N')
 AND (LOWER(business_unit_country) <> LOWER('CA') OR business_day_date <= DATE '2023-02-25');


CREATE TEMPORARY TABLE IF NOT EXISTS f_bands AS
SELECT business_day_date,
 business_unit_desc,
 channel,
 price_type,
 fulfilled_from_location_type,
 delivery_method,
  CASE
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 0.01 AND 5.00
  THEN '0-5'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 5.01 AND 10
  THEN '5-10'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 10.01 AND 15
  THEN '10-15'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 15.01 AND 20
  THEN '15-20'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 20.01 AND 25
  THEN '20-25'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 25.01 AND 30
  THEN '25-30'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 30.01 AND 35
  THEN '30-35'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 35.01 AND 40
  THEN '35-40'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 40.01 AND 45
  THEN '40-45'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 45.01 AND 50
  THEN '45-50'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 50.01 AND 55
  THEN '50-55'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 55.01 AND 60
  THEN '55-60'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 60.01 AND 65
  THEN '60-65'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 65.01 AND 70
  THEN '65-70'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 70.01 AND 75
  THEN '70-75'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 75.01 AND 80
  THEN '75-80'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 80.01 AND 85
  THEN '80-85'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 85.01 AND 90
  THEN '85-90'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 90.01 AND 95
  THEN '90-95'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 95.01 AND 100
  THEN '95-100'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 100.01 AND 200
  THEN '100-200'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 200.01 AND 300
  THEN '200-300'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 300.01 AND 400
  THEN '300-400'
  WHEN jwn_fulfilled_demand_usd_amt BETWEEN 400.01 AND 500
  THEN '400-500'
  ELSE 'above 500'
  END AS fulfilled_demand_usd_price_band,
 SUM(jwn_fulfilled_demand_usd_amt) AS fulfilled_demand_usd_amt,
 SUM(line_item_quantity) AS fulfilled_demand_units
FROM jogmv
WHERE LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
GROUP BY business_day_date,
 business_unit_desc,
 channel,
 price_type,
 fulfilled_from_location_type,
 delivery_method,
 fulfilled_demand_usd_price_band;


CREATE TEMPORARY TABLE IF NOT EXISTS ret_bands AS
SELECT business_day_date,
 business_unit_desc,
 channel,
 price_type,
 fulfilled_from_location_type,
 delivery_method,
  CASE
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 0.01 AND 5.00
  THEN '0-5'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 5.01 AND 10
  THEN '5-10'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 10.01 AND 15
  THEN '10-15'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 15.01 AND 20
  THEN '15-20'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 20.01 AND 25
  THEN '20-25'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 25.01 AND 30
  THEN '25-30'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 30.01 AND 35
  THEN '30-35'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 35.01 AND 40
  THEN '35-40'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 40.01 AND 45
  THEN '40-45'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 45.01 AND 50
  THEN '45-50'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 50.01 AND 55
  THEN '50-55'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 55.01 AND 60
  THEN '55-60'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 60.01 AND 65
  THEN '60-65'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 65.01 AND 70
  THEN '65-70'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 70.01 AND 75
  THEN '70-75'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 75.01 AND 80
  THEN '75-80'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 80.01 AND 85
  THEN '80-85'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 85.01 AND 90
  THEN '85-90'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 90.01 AND 95
  THEN '90-95'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 95.01 AND 100
  THEN '95-100'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 100.01 AND 200
  THEN '100-200'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 200.01 AND 300
  THEN '200-300'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 300.01 AND 400
  THEN '300-400'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 400.01 AND 500
  THEN '400-500'
  ELSE 'above 500'
  END AS actual_product_returns_usd_price_band,
 SUM(CASE
   WHEN LOWER(service_type) <> LOWER('Last Chance')
   THEN operational_gmv_usd_amt
   ELSE 0
   END) AS actual_product_returns_usd_amt,
 SUM(CASE
   WHEN LOWER(service_type) <> LOWER('Last Chance')
   THEN line_item_quantity
   ELSE 0
   END) AS actual_product_returns_units
FROM jogmv
WHERE LOWER(product_return_ind) = LOWER('Y')
GROUP BY business_day_date,
 business_unit_desc,
 channel,
 price_type,
 fulfilled_from_location_type,
 delivery_method,
 actual_product_returns_usd_price_band;


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_bands AS
SELECT business_day_date,
 business_unit_desc,
 channel,
 price_type,
 fulfilled_from_location_type,
 delivery_method,
  CASE
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 0.01 AND 5.00
  THEN '0-5'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 5.01 AND 10
  THEN '5-10'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 10.01 AND 15
  THEN '10-15'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 15.01 AND 20
  THEN '15-20'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 20.01 AND 25
  THEN '20-25'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 25.01 AND 30
  THEN '25-30'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 30.01 AND 35
  THEN '30-35'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 35.01 AND 40
  THEN '35-40'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 40.01 AND 45
  THEN '40-45'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 45.01 AND 50
  THEN '45-50'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 50.01 AND 55
  THEN '50-55'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 55.01 AND 60
  THEN '55-60'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 60.01 AND 65
  THEN '60-65'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 65.01 AND 70
  THEN '65-70'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 70.01 AND 75
  THEN '70-75'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 75.01 AND 80
  THEN '75-80'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 80.01 AND 85
  THEN '80-85'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 85.01 AND 90
  THEN '85-90'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 90.01 AND 95
  THEN '90-95'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 95.01 AND 100
  THEN '95-100'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 100.01 AND 200
  THEN '100-200'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 200.01 AND 300
  THEN '200-300'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 300.01 AND 400
  THEN '300-400'
  WHEN ABS(operational_gmv_usd_amt) BETWEEN 400.01 AND 500
  THEN '400-500'
  ELSE 'above 500'
  END AS op_gmv_usd_price_band,
 SUM(operational_gmv_usd_amt) AS op_gmv_usd_amt,
 SUM(operational_gmv_units) AS op_gmv_units
FROM jogmv
WHERE LOWER(jwn_operational_gmv_ind) = LOWER('Y')
GROUP BY business_day_date,
 business_unit_desc,
 channel,
 price_type,
 fulfilled_from_location_type,
 delivery_method,
 op_gmv_usd_price_band;

CREATE TEMPORARY TABLE IF NOT EXISTS combo AS
SELECT
    demand_date AS tran_date,
    business_unit_desc,
    channel,
    price_type,
    fulfilled_from_location_type,
    delivery_method,
    gross_demand_usd_price_band AS price_band,
    gross_demand_usd_amt,
    gross_demand_units,
    CAST(NULL AS NUMERIC) AS reported_demand_usd_amt,
	cast(trunc(NULL) as INT64)AS reported_demand_units,
    
    CAST(NULL AS NUMERIC) AS fulfilled_demand_usd_amt,
	
	cast(trunc(NULL) as INT64)AS fulfilled_demand_units,
    
    CAST(NULL AS NUMERIC) AS actual_product_returns_usd_amt,
	
	cast(trunc(NULL) as INT64)AS actual_product_returns_units,
    
    CAST(NULL AS NUMERIC) AS op_gmv_usd_amt,
	cast(trunc(NULL) as INT64)AS op_gmv_units
	
	
    
FROM
    g_bands

UNION ALL

SELECT
    demand_date AS tran_date,
    business_unit_desc,
    channel,
    price_type,
    fulfilled_from_location_type,
    delivery_method,
    reported_demand_usd_price_band AS price_band,
    CAST(NULL AS NUMERIC) AS gross_demand_usd_amt,
	
	cast(trunc(NULL) as INT64)AS gross_demand_units,
    
    reported_demand_usd_amt,
    reported_demand_units,
    CAST(NULL AS NUMERIC) AS fulfilled_demand_usd_amt,
cast(trunc(NULL) as INT64)AS fulfilled_demand_units,
   
    CAST(NULL AS NUMERIC) AS actual_product_returns_usd_amt,
	
	cast(trunc(NULL) as INT64)AS actual_product_returns_units,
    
    CAST(NULL AS NUMERIC) AS op_gmv_usd_amt,
	
	cast(trunc(NULL) as INT64)AS op_gmv_units
   
FROM
    r_bands

UNION ALL

SELECT
    business_day_date AS tran_date,
    business_unit_desc,
    channel,
    price_type,
    fulfilled_from_location_type,
    delivery_method,
    fulfilled_demand_usd_price_band AS price_band,
    CAST(NULL AS NUMERIC) AS gross_demand_usd_amt,
	
	cast(trunc(NULL) as INT64)AS gross_demand_units,
    
    CAST(NULL AS NUMERIC) AS reported_demand_usd_amt,
	cast(trunc(NULL) as INT64)AS reported_demand_units,

    fulfilled_demand_usd_amt,
    fulfilled_demand_units,
    CAST(NULL AS NUMERIC) AS actual_product_returns_usd_amt,
	
	cast(trunc(NULL) as INT64)AS actual_product_returns_units,
    
    CAST(NULL AS NUMERIC) AS op_gmv_usd_amt,
	
	cast(trunc(NULL) as INT64)AS op_gmv_units
    
FROM
    f_bands

UNION ALL

SELECT
    business_day_date AS tran_date,
    business_unit_desc,
    channel,
    price_type,
    fulfilled_from_location_type,
    delivery_method,
    actual_product_returns_usd_price_band AS price_band,
    CAST(NULL AS NUMERIC) AS gross_demand_usd_amt,
	
	cast(trunc(NULL) as INT64)AS gross_demand_units,
  
    CAST(NULL AS NUMERIC) AS reported_demand_usd_amt,
	
	cast(trunc(NULL) as INT64)AS reported_demand_units,
    
    CAST(NULL AS NUMERIC) AS fulfilled_demand_usd_amt,
	
	cast(trunc(NULL) as INT64)AS fulfilled_demand_units,
    
    actual_product_returns_usd_amt,
    actual_product_returns_units,
    CAST(NULL AS NUMERIC) AS op_gmv_usd_amt,
	
	cast(trunc(NULL) as INT64)AS op_gmv_units
    
FROM
    ret_bands

UNION ALL

SELECT
    business_day_date AS tran_date,
    business_unit_desc,
    channel,
    price_type,
    fulfilled_from_location_type,
    delivery_method,
    op_gmv_usd_price_band AS price_band,
    CAST(NULL AS NUMERIC) AS gross_demand_usd_amt,
	cast(trunc(NULL) as INT64)AS gross_demand_units,
   
    CAST(NULL AS NUMERIC) AS reported_demand_usd_amt,
	
	cast(trunc(NULL) as INT64) AS reported_demand_units,
    
    CAST(NULL AS NUMERIC) AS fulfilled_demand_usd_amt,
	cast(trunc(NULL) as INT64) AS fulfilled_demand_units,
    CAST(NULL AS NUMERIC) AS actual_product_returns_usd_amt,
	
	cast(trunc(NULL) as INT64)AS actual_product_returns_units,
   
    op_gmv_usd_amt,
    op_gmv_units
FROM
    gmv_bands;








CREATE TEMPORARY TABLE IF NOT EXISTS combo_agg AS
SELECT
    tran_date,
    business_unit_desc,
    channel,
    price_type,
    fulfilled_from_location_type,
    delivery_method,
    price_band,
    SUM(gross_demand_usd_amt) AS gross_demand_usd_amt,
    SUM(gross_demand_units) AS gross_demand_units,
    SUM(reported_demand_usd_amt) AS reported_demand_usd_amt,
    SUM(reported_demand_units) AS reported_demand_units,
    SUM(fulfilled_demand_usd_amt) AS fulfilled_demand_usd_amt,
    SUM(fulfilled_demand_units) AS fulfilled_demand_units,
    SUM(actual_product_returns_usd_amt) AS actual_product_returns_usd_amt,
    SUM(actual_product_returns_units) AS actual_product_returns_units,
    SUM(op_gmv_usd_amt) AS op_gmv_usd_amt,
    SUM(op_gmv_units) AS op_gmv_units,
    CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
FROM 
    combo
GROUP BY 
    tran_date,
    business_unit_desc,
    channel,
    price_type,
    fulfilled_from_location_type,
    delivery_method,
    price_band;







DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dsa_ai_fct.finance_sales_demand_price_bands_agg
WHERE tran_date BETWEEN CAST({{params.start_date}} AS DATE) AND CAST({{params.end_date}} AS DATE);



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dsa_ai_fct.finance_sales_demand_price_bands_agg
(
  tran_date
  ,business_unit_desc
  ,channel
  ,price_type
  ,fulfilled_from_location_type
  ,delivery_method
  ,price_band
  ,gross_demand_usd_amt
  ,gross_demand_units
  ,reported_demand_usd_amt
  ,reported_demand_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,dw_sys_load_tmstp
  )
  SELECT
  tran_date
  ,business_unit_desc
  ,channel
  ,price_type
  ,fulfilled_from_location_type
  ,delivery_method
  ,price_band
  ,gross_demand_usd_amt
  ,gross_demand_units
  ,reported_demand_usd_amt
  ,reported_demand_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,dw_sys_load_tmstp
 FROM 
combo_agg;


