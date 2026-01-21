/*
RP Anticipated Spend for OTB
Author: Sara Riker
Date Created: 8/25/22
Date Last Updated: 9/8/22

Purpose: Cleans data from s3 bucket and updates RP anticipated spend tables

Updates tables: 
  - {environment_schema}.rp_anticipated_spend
  - {environment_schema}.rp_anticipated_spend_hist
  - {environment_schema}.rp_anticipated_spend_current
*/

DELETE FROM {environment_schema}.rp_anticipated_spend ALL;
INSERT INTO {environment_schema}.rp_anticipated_spend
SELECT 
     CAST(OREPLACE(week_idnt, '"', '') AS INTEGER) AS week_idnt
    ,CAST(OREPLACE(dept, '"', '') AS INTEGER) AS dept_idnt
    ,CAST(OREPLACE(dept_desc, '"', '') AS VARCHAR(40)) AS dept_desc
    ,CAST(OREPLACE(cls_idnt, '"', '') AS INTEGER) AS cls_idnt
    ,CAST(OREPLACE(scls_idnt, '"', '') AS INTEGER) AS scls_idnt
    ,CAST(OREPLACE(supp_idnt, '"', '') AS INTEGER) AS supp_idnt
    ,CAST(OREPLACE(supp_name, '"', '') AS VARCHAR(40)) AS supp_desc
    ,CAST(OREPLACE(banner_id, '"', '') AS INTEGER) AS banner_id
    ,CAST(OREPLACE(ft_id, '"', '') AS INTEGER) AS ft_id

    ,CAST(OREPLACE(rp_antspnd_u, '"', '') AS DECIMAL(12,4)) AS rp_antspnd_u
    ,CAST(OREPLACE(rp_antspnd_c, '"', '') AS DECIMAL(12,4)) AS rp_antspnd_c
    ,CAST(OREPLACE(rp_antspnd_r, '"', '') AS DECIMAL(12,4)) AS rp_antspnd_r
    ,current_timestamp AS update_timestamp
FROM {environment_schema}.rp_anticipated_spend_staging
;

CREATE MULTISET VOLATILE TABLE rp_spend AS (
SELECT 
     rp.week_idnt
    ,cal.fiscal_year_num AS year_454
    ,cal.month_idnt AS month_454
    ,rp.banner_id
    ,rp.ft_id
    ,rp.dept_idnt
    ,rp.dept_desc
    ,COALESCE(map1.category, map2.category, 'OTHER') AS category
    ,rp.supp_idnt
    ,rp.supp_desc
    ,SUM(rp_antspnd_u) AS rp_antspnd_u
    ,SUM(rp_antspnd_c) AS rp_antspnd_c
    ,SUM(rp_antspnd_r) AS rp_antspnd_r
    ,current_date AS rcd_load_date
    ,current_timestamp AS update_timestamp
FROM {environment_schema}.rp_anticipated_spend rp
JOIN (SELECT DISTINCT week_idnt, month_idnt, fiscal_year_num FROM prd_nap_usr_vws.day_cal_454_dim) cal
  ON rp.week_idnt = cal.week_idnt
LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim map1
  ON rp.dept_idnt = map1.dept_num
 AND rp.cls_idnt = map1.class_num
 AND rp.scls_idnt = map1.sbclass_num
LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim map2
  ON rp.dept_idnt = map2.dept_num
 AND rp.cls_idnt = map2.class_num
 AND map2.sbclass_num = '-1'
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA 
PRIMARY INDEX(week_idnt)
ON COMMIT PRESERVE ROWS
;

-- add to historical view
DELETE FROM {environment_schema}.rp_anticipated_spend_hist 
WHERE rcd_load_date NOT IN (SELECT DISTINCT 
                                d.month_end_day_date 
                            FROM t2dl_das_open_to_buy.rp_anticipated_spend_hist h 
                            JOIN prd_nap_usr_vws.day_cal_454_dim d
                              ON h.rcd_load_date = d.day_date
                          )
;
INSERT INTO {environment_schema}.rp_anticipated_spend_hist 
SELECT * FROM rp_spend
;
 
-- Current data only
DELETE FROM {environment_schema}.rp_anticipated_spend_current WHERE week_idnt >= (SELECT MIN(week_idnt) FROM rp_spend);
INSERT INTO {environment_schema}.rp_anticipated_spend_current
SELECT * 
FROM rp_spend
;

-- clean up staging table
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_anticipated_spend_staging', OUT_RETURN_MSG);