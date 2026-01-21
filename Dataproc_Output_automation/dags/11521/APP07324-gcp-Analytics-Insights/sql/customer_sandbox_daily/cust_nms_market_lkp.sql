TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_nms_market_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_nms_market_lkp
(SELECT cust_nms_market_desc,
  ROW_NUMBER() OVER (ORDER BY cust_nms_market_desc) AS cust_nms_market_num,
   CASE
   WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('CHICAGO'), LOWER('DETROIT'), LOWER('MINNEAPOLIS'))
   THEN 'MIDWEST'
   WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('NEW_YORK'), LOWER('WASHINGTON'), LOWER('BOSTON'), LOWER('PHILADELPHIA'
      ))
   THEN 'NORTHEAST'
   WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('SEATTLE'), LOWER('PORTLAND'), LOWER('BOSTON'), LOWER('SAN_FRANCISCO'
      ))
   THEN 'NORTHWEST'
   WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('LOS_ANGELES'), LOWER('SAN_DIEGO'))
   THEN 'SCAL'
   WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('ATLANTA'), LOWER('MIAMI'))
   THEN 'SOUTHEAST'
   WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('AUSTIN'), LOWER('HOUSTON'), LOWER('DENVER'), LOWER('DALLAS'))
   THEN 'SOUTHWEST'
   ELSE 'NON-NMS REGION'
   END AS cust_nms_region_desc,
   CASE
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('CHICAGO'), LOWER('DETROIT'), LOWER('MINNEAPOLIS'))
      THEN 'MIDWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('NEW_YORK'), LOWER('WASHINGTON'), LOWER('BOSTON'), LOWER('PHILADELPHIA'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('SEATTLE'), LOWER('PORTLAND'), LOWER('BOSTON'), LOWER('SAN_FRANCISCO'
         ))
      THEN 'NORTHWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('LOS_ANGELES'), LOWER('SAN_DIEGO'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('ATLANTA'), LOWER('MIAMI'))
      THEN 'SOUTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('AUSTIN'), LOWER('HOUSTON'), LOWER('DENVER'), LOWER('DALLAS'))
      THEN 'SOUTHWEST'
      ELSE 'NON-NMS REGION'
      END) = LOWER('MIDWEST')
   THEN 1
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('CHICAGO'), LOWER('DETROIT'), LOWER('MINNEAPOLIS'))
      THEN 'MIDWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('NEW_YORK'), LOWER('WASHINGTON'), LOWER('BOSTON'), LOWER('PHILADELPHIA'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('SEATTLE'), LOWER('PORTLAND'), LOWER('BOSTON'), LOWER('SAN_FRANCISCO'
         ))
      THEN 'NORTHWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('LOS_ANGELES'), LOWER('SAN_DIEGO'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('ATLANTA'), LOWER('MIAMI'))
      THEN 'SOUTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('AUSTIN'), LOWER('HOUSTON'), LOWER('DENVER'), LOWER('DALLAS'))
      THEN 'SOUTHWEST'
      ELSE 'NON-NMS REGION'
      END) = LOWER('NORTHEAST')
   THEN 2
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('CHICAGO'), LOWER('DETROIT'), LOWER('MINNEAPOLIS'))
      THEN 'MIDWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('NEW_YORK'), LOWER('WASHINGTON'), LOWER('BOSTON'), LOWER('PHILADELPHIA'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('SEATTLE'), LOWER('PORTLAND'), LOWER('BOSTON'), LOWER('SAN_FRANCISCO'
         ))
      THEN 'NORTHWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('LOS_ANGELES'), LOWER('SAN_DIEGO'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('ATLANTA'), LOWER('MIAMI'))
      THEN 'SOUTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('AUSTIN'), LOWER('HOUSTON'), LOWER('DENVER'), LOWER('DALLAS'))
      THEN 'SOUTHWEST'
      ELSE 'NON-NMS REGION'
      END) = LOWER('NORTHWEST')
   THEN 3
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('CHICAGO'), LOWER('DETROIT'), LOWER('MINNEAPOLIS'))
      THEN 'MIDWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('NEW_YORK'), LOWER('WASHINGTON'), LOWER('BOSTON'), LOWER('PHILADELPHIA'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('SEATTLE'), LOWER('PORTLAND'), LOWER('BOSTON'), LOWER('SAN_FRANCISCO'
         ))
      THEN 'NORTHWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('LOS_ANGELES'), LOWER('SAN_DIEGO'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('ATLANTA'), LOWER('MIAMI'))
      THEN 'SOUTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('AUSTIN'), LOWER('HOUSTON'), LOWER('DENVER'), LOWER('DALLAS'))
      THEN 'SOUTHWEST'
      ELSE 'NON-NMS REGION'
      END) = LOWER('SCAL')
   THEN 4
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('CHICAGO'), LOWER('DETROIT'), LOWER('MINNEAPOLIS'))
      THEN 'MIDWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('NEW_YORK'), LOWER('WASHINGTON'), LOWER('BOSTON'), LOWER('PHILADELPHIA'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('SEATTLE'), LOWER('PORTLAND'), LOWER('BOSTON'), LOWER('SAN_FRANCISCO'
         ))
      THEN 'NORTHWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('LOS_ANGELES'), LOWER('SAN_DIEGO'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('ATLANTA'), LOWER('MIAMI'))
      THEN 'SOUTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('AUSTIN'), LOWER('HOUSTON'), LOWER('DENVER'), LOWER('DALLAS'))
      THEN 'SOUTHWEST'
      ELSE 'NON-NMS REGION'
      END) = LOWER('SOUTHEAST')
   THEN 5
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('CHICAGO'), LOWER('DETROIT'), LOWER('MINNEAPOLIS'))
      THEN 'MIDWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('NEW_YORK'), LOWER('WASHINGTON'), LOWER('BOSTON'), LOWER('PHILADELPHIA'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('SEATTLE'), LOWER('PORTLAND'), LOWER('BOSTON'), LOWER('SAN_FRANCISCO'
         ))
      THEN 'NORTHWEST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('LOS_ANGELES'), LOWER('SAN_DIEGO'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('ATLANTA'), LOWER('MIAMI'))
      THEN 'SOUTHEAST'
      WHEN LOWER(UPPER(cust_nms_market_desc)) IN (LOWER('AUSTIN'), LOWER('HOUSTON'), LOWER('DENVER'), LOWER('DALLAS'))
      THEN 'SOUTHWEST'
      ELSE 'NON-NMS REGION'
      END) = LOWER('SOUTHWEST')
   THEN 6
   ELSE 7
   END AS cust_nms_region_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT local_market AS cust_nms_market_desc
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.local_market_postal_dim) AS a);