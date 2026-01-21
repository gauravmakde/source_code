
/*SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
---Task_Name=customer_store_distance_buckets_build;'*/
---FOR SESSION VOLATILE;

CREATE TEMPORARY TABLE IF NOT EXISTS zip_latlong_driver
AS
SELECT country_code,
 zip_code,
 AVG(latitude) AS latitude,
 AVG(longitude) AS longitude
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.zip_codes_dim
GROUP BY country_code,
 zip_code;

 
CREATE TEMPORARY TABLE IF NOT EXISTS store_latlong_driver
AS
SELECT DISTINCT store_num,
  CASE
  WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'
     ))
  THEN 'NORD'
  WHEN LOWER(business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
  THEN 'RACK'
  ELSE NULL
  END AS store_banner,
 store_postal_code,
 store_location_longitude,
 store_location_latitude
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE store_close_date IS NULL
 AND store_open_date <= CURRENT_DATE('PST8PDT')
 AND LOWER(selling_store_ind) = LOWER('S')
 AND LOWER(store_type_code) IN (LOWER('FL'), LOWER('NL'), LOWER('RK'))
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('RACK'), LOWER('RACK CANADA'))
 AND store_num <> 828
 AND LOWER(store_name) NOT LIKE LOWER('%BULK%')
 AND LOWER(store_name) NOT LIKE LOWER('%TRAINING%')
 AND LOWER(store_name) NOT LIKE LOWER('%EMPLOYEE%')
 AND store_location_latitude IS NOT NULL
 AND store_location_longitude IS NOT NULL;

 
CREATE TEMPORARY TABLE IF NOT EXISTS zip_store_distance_closest
AS
SELECT a.zip_code,
 b.store_num,
 b.store_banner,
ST_GEOGFROMTEXT('POINT(' || cast(a.longitude as NUMERIC)||' '|| cast(a.latitude as NUMERIC) || ')') as zip_location
, ST_GEOGFROMTEXT('POINT(' || cast(b.store_location_longitude as NUMERIC)
||' '|| cast(b.store_location_latitude as NUMERIC) || ')') as store_location
, ST_DISTANCE((ST_GEOGFROMTEXT('POINT(' || cast(a.longitude as NUMERIC)||' '|| cast(a.latitude as NUMERIC) || ')')),(ST_GEOGFROMTEXT('POINT(' || cast(b.store_location_longitude as NUMERIC)
||' '|| cast(b.store_location_latitude as NUMERIC) || ')'))) / 1000 * 0.62137 as zip_store_distance
FROM zip_latlong_driver AS a
 INNER JOIN store_latlong_driver AS b ON TRUE
QUALIFY (ROW_NUMBER() OVER (PARTITION BY a.zip_code ORDER BY NULL)) = 1;


CREATE TEMPORARY TABLE IF NOT EXISTS customer_latlong_driver
AS
SELECT DISTINCT a.acp_id,
 a.billing_postal_code,
 b.longitude AS cust_longitude,
 b.latitude AS cust_latitude,
 a.fls_loyalty_store_num,
 a.rack_loyalty_store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS a
 INNER JOIN zip_latlong_driver AS b ON LOWER(a.billing_postal_code) = LOWER(b.zip_code)
WHERE a.billing_postal_code IS NOT NULL;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_store_distance
AS
SELECT a.acp_id,
 a.billing_postal_code,
 zt.store_num AS closest_store,
 zt.store_banner AS closest_store_banner,
 zt.zip_store_distance AS closest_store_distance,
 a.fls_loyalty_store_num,
 ST_GEOGFROMTEXT('POINT(' || cast(a.cust_longitude as NUMERIC)
          ||' '|| cast(a.cust_latitude as NUMERIC) || ')') as custlocation
     , ST_GEOGFROMTEXT('POINT(' || cast(nsol.store_location_longitude as NUMERIC)
          ||' '|| cast(nsol.store_location_latitude as NUMERIC) || ')') as nord_store_loc
     , ST_DISTANCE((ST_GEOGFROMTEXT('POINT(' || cast(a.cust_longitude as NUMERIC)
          ||' '|| cast(a.cust_latitude as NUMERIC) || ')')),(ST_GEOGFROMTEXT('POINT(' || cast(nsol.store_location_longitude as NUMERIC)
          ||' '|| cast(nsol.store_location_latitude as NUMERIC) || ')'))) / 1000 * 0.62137 as nord_sol_distance
     , a.rack_loyalty_store_num
     , ST_GEOGFROMTEXT('POINT(' || cast(rsol.store_location_longitude as NUMERIC)
          ||' '|| cast(rsol.store_location_latitude as NUMERIC) || ')') as rack_store_loc
     , ST_DISTANCE((ST_GEOGFROMTEXT('POINT(' || cast(a.cust_longitude as NUMERIC)
          ||' '|| cast(a.cust_latitude as NUMERIC) || ')')),(ST_GEOGFROMTEXT('POINT(' || cast(rsol.store_location_longitude as NUMERIC)
          ||' '|| cast(rsol.store_location_latitude as NUMERIC) || ')'))) / 1000 * 0.62137 as rack_sol_distance
FROM customer_latlong_driver AS a
 LEFT JOIN store_latlong_driver AS nsol ON a.fls_loyalty_store_num = nsol.store_num
 LEFT JOIN store_latlong_driver AS rsol ON a.rack_loyalty_store_num = rsol.store_num
 LEFT JOIN zip_store_distance_closest AS zt ON LOWER(a.billing_postal_code) = LOWER(zt.zip_code);

 
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_store_distance_buckets;

INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_store_distance_buckets
(SELECT acp_id,
  billing_postal_code,
  closest_store,
  closest_store_banner,
  CAST(closest_store_distance AS FLOAT64) AS closest_store_distance,
   CASE
   WHEN closest_store_distance IS NULL
   THEN 'missing'
   WHEN CAST(TRUNC(closest_store_distance) AS INTEGER) < 9
   THEN '0' || SUBSTR(CAST(CAST(TRUNC(CEILING(closest_store_distance)) AS INTEGER) AS STRING), 1, 2) || ' miles'
   WHEN CAST(TRUNC(closest_store_distance) AS INTEGER) < 50
   THEN SUBSTR(CAST(CAST(TRUNC(CEILING(closest_store_distance)) AS INTEGER) AS STRING), 1, 2) || ' miles'
   ELSE '51+ miles'
   END AS closest_store_dist_bucket,
  fls_loyalty_store_num AS nord_loyalty_store,
  CAST(nord_sol_distance AS FLOAT64) AS nord_sol_distance,
   CASE
   WHEN nord_sol_distance IS NULL
   THEN 'missing'
   WHEN CAST(TRUNC(nord_sol_distance) AS INTEGER) < 9
   THEN '0' || SUBSTR(CAST(CAST(TRUNC(CEILING(nord_sol_distance)) AS INTEGER) AS STRING), 1, 2) || ' miles'
   WHEN CAST(TRUNC(nord_sol_distance) AS INTEGER) < 50
   THEN SUBSTR(CAST(CAST(TRUNC(CEILING(nord_sol_distance)) AS INTEGER) AS STRING), 1, 2) || ' miles'
   ELSE '51+ miles'
   END AS nord_sol_dist_bucket,
  rack_loyalty_store_num AS rack_loyalty_store,
  CAST(rack_sol_distance AS FLOAT64) AS rack_sol_distance,
   CASE
   WHEN rack_sol_distance IS NULL
   THEN 'missing'
   WHEN CAST(TRUNC(rack_sol_distance) AS INTEGER) < 9
   THEN '0' || SUBSTR(CAST(CAST(TRUNC(CEILING(rack_sol_distance)) AS INTEGER) AS STRING), 1, 2) || ' miles'
   WHEN CAST(TRUNC(rack_sol_distance) AS INTEGER) < 50
   THEN SUBSTR(CAST(CAST(TRUNC(CEILING(rack_sol_distance)) AS INTEGER) AS STRING), 1, 2) || ' miles'
   ELSE '51+ miles'
   END AS rack_sol_dist_bucket,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_store_distance AS a);

