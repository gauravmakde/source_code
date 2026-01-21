-- this process updates elapsed weeks for rp data

CREATE MULTISET VOLATILE TABLE rp_spend_inc AS (
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
FROM t2dl_das_open_to_buy.rp_anticipated_spend_adhoc) 
WITH DATA 
PRIMARY INDEX(week_idnt)
ON COMMIT PRESERVE ROWS
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
FROM rp_spend_inc rp
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

DELETE FROM t2dl_das_open_to_buy.rp_anticipated_spend_current WHERE week_idnt in (SELECT DISTINCT week_idnt from rp_spend);
INSERT INTO t2dl_das_open_to_buy.rp_anticipated_spend_current
SELECT * 
FROM rp_spend;

DELETE FROM t2dl_das_open_to_buy.rp_anticipated_spend_hist 
WHERE week_idnt in (SELECT DISTINCT week_idnt from rp_spend) AND rcd_load_date = current_date 
;

INSERT INTO t2dl_das_open_to_buy.rp_anticipated_spend_hist 
SELECT * FROM rp_spend
;