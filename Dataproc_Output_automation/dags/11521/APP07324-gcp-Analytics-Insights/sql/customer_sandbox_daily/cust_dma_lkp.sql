CREATE TEMPORARY TABLE IF NOT EXISTS dma_mapping
AS
SELECT DISTINCT us_dma_desc AS cust_dma_desc,
 us_dma_code AS cust_dma_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer
UNION DISTINCT
SELECT DISTINCT ca_dma_desc AS cust_dma_desc,
 ca_dma_code AS cust_dma_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer
WHERE ca_dma_desc IS NOT NULL;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_dma_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_dma_lkp
(SELECT COALESCE(cust_dma_desc, 'Unknown') AS cust_dma_desc,
  COALESCE(cust_dma_num, - 1) AS cust_dma_num,
   CASE
   WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'), LOWER('SAN DIEGO'
      ), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
   THEN 'SCAL'
   WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
      ))
   THEN 'NORTHEAST'
   WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
   THEN 'NORTHEAST'
   WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'),
     LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'), LOWER('PE'
      ), LOWER('QC'), LOWER('SK'), LOWER('YT'))
   THEN 'CANADA'
   WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'),
     LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'), LOWER('NE'
      ), LOWER('OH'), LOWER('SD'), LOWER('WI'))
   THEN 'MIDWEST'
   WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'),
     LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'), LOWER('PA'
      ), LOWER('RI'), LOWER('VT'), LOWER('WV'))
   THEN 'NORTHEAST'
   WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'),
     LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
   THEN 'NORTHWEST'
   WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
   THEN 'SCAL'
   WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'),
     LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
   THEN 'SOUTHEAST'
   WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'),
     LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
   THEN 'SOUTHWEST'
   ELSE 'UNKNOWN'
   END AS cust_region_desc,
   CASE
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('SCAL')
   THEN 1
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('NORTHEAST')
   THEN 2
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('CANADA')
   THEN 3
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('NORTHWEST')
   THEN 4
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('SOUTHEAST')
   THEN 5
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('SOUTHWEST')
   THEN 6
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('MIDWEST')
   THEN 7
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('UNKNOWN')
   THEN 8
   ELSE NULL
   END AS cust_region_num,
   CASE
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) = LOWER('CANADA')
   THEN 'CANADA'
   WHEN LOWER(CASE
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
        LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
      THEN 'SCAL'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
         ))
      THEN 'NORTHEAST'
      WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
         ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
        LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
      THEN 'CANADA'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
         ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
        LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
      THEN 'MIDWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
         ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
        LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
      THEN 'NORTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
         ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
      THEN 'NORTHWEST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI')
      THEN 'SCAL'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
         ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
      THEN 'SOUTHEAST'
      WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
         ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
      THEN 'SOUTHWEST'
      ELSE 'UNKNOWN'
      END) <> LOWER('UNKNOWN')
   THEN 'US'
   ELSE 'UNKNOWN'
   END AS cust_country_desc,
   CASE
   WHEN LOWER(CASE
      WHEN LOWER(CASE
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
           LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
         THEN 'SCAL'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
            ))
         THEN 'NORTHEAST'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
            ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
           LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
         THEN 'CANADA'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
            ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
           LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
         THEN 'MIDWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
            ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
           LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
            ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
         THEN 'NORTHWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI'
           )
         THEN 'SCAL'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
            ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
         THEN 'SOUTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
            ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
         THEN 'SOUTHWEST'
         ELSE 'UNKNOWN'
         END) = LOWER('CANADA')
      THEN 'CANADA'
      WHEN LOWER(CASE
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
           LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
         THEN 'SCAL'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
            ))
         THEN 'NORTHEAST'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
            ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
           LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
         THEN 'CANADA'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
            ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
           LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
         THEN 'MIDWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
            ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
           LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
            ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
         THEN 'NORTHWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI'
           )
         THEN 'SCAL'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
            ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
         THEN 'SOUTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
            ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
         THEN 'SOUTHWEST'
         ELSE 'UNKNOWN'
         END) <> LOWER('UNKNOWN')
      THEN 'US'
      ELSE 'UNKNOWN'
      END) = LOWER('CANADA')
   THEN 2
   WHEN LOWER(CASE
      WHEN LOWER(CASE
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
           LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
         THEN 'SCAL'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
            ))
         THEN 'NORTHEAST'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
            ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
           LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
         THEN 'CANADA'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
            ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
           LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
         THEN 'MIDWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
            ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
           LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
            ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
         THEN 'NORTHWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI'
           )
         THEN 'SCAL'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
            ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
         THEN 'SOUTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
            ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
         THEN 'SOUTHWEST'
         ELSE 'UNKNOWN'
         END) = LOWER('CANADA')
      THEN 'CANADA'
      WHEN LOWER(CASE
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
           LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
         THEN 'SCAL'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
            ))
         THEN 'NORTHEAST'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
            ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
           LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
         THEN 'CANADA'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
            ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
           LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
         THEN 'MIDWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
            ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
           LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
            ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
         THEN 'NORTHWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI'
           )
         THEN 'SCAL'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
            ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
         THEN 'SOUTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
            ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
         THEN 'SOUTHWEST'
         ELSE 'UNKNOWN'
         END) <> LOWER('UNKNOWN')
      THEN 'US'
      ELSE 'UNKNOWN'
      END) = LOWER('US')
   THEN 1
   WHEN LOWER(CASE
      WHEN LOWER(CASE
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
           LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
         THEN 'SCAL'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
            ))
         THEN 'NORTHEAST'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
            ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
           LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
         THEN 'CANADA'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
            ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
           LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
         THEN 'MIDWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
            ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
           LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
            ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
         THEN 'NORTHWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI'
           )
         THEN 'SCAL'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
            ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
         THEN 'SOUTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
            ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
         THEN 'SOUTHWEST'
         ELSE 'UNKNOWN'
         END) = LOWER('CANADA')
      THEN 'CANADA'
      WHEN LOWER(CASE
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'),
           LOWER('SAN DIEGO'), LOWER('PALM SPRI'), LOWER('YUMA AZ-E'))
         THEN 'SCAL'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'
            ))
         THEN 'NORTHEAST'
         WHEN LOWER(UPPER(SUBSTR(cust_dma_desc, 1, 11))) = LOWER('CHARLOTTESV')
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AB'
            ), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'), LOWER('NU'), LOWER('ON'),
           LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
         THEN 'CANADA'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('IA'
            ), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'), LOWER('MO'), LOWER('ND'),
           LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
         THEN 'MIDWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('CT'
            ), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'), LOWER('NJ'), LOWER('NY'),
           LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
         THEN 'NORTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AK'
            ), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'), LOWER('WY'))
         THEN 'NORTHWEST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) = LOWER('HI'
           )
         THEN 'SCAL'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AL'
            ), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'), LOWER('TN'), LOWER('VA'))
         THEN 'SOUTHEAST'
         WHEN LOWER(SUBSTR(REPLACE(cust_dma_desc, ')', ''), LENGTH(REPLACE(cust_dma_desc, ')', '')) - 1, 2)) IN (LOWER('AR'
            ), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'), LOWER('UT'))
         THEN 'SOUTHWEST'
         ELSE 'UNKNOWN'
         END) <> LOWER('UNKNOWN')
      THEN 'US'
      ELSE 'UNKNOWN'
      END) = LOWER('UNKNOWN')
   THEN 3
   ELSE NULL
   END AS cust_country_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM dma_mapping);