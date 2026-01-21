/*
Purpose:        Inserts data in {{environment_schema}} tables for live on site daily aggregated
                    los_daily_agg
Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING (prod)
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):      Mayank Sarwa
Date Created/Modified: 11/19/2024
*/

--Drop table dates; 
CREATE MULTISET VOLATILE TABLE dates
AS (
      SELECT
      	distinct day_date
      	, fiscal_year_num AS fiscal_year
      	, 'TY' AS ty_ly_ind
      	, fiscal_week_num as fiscal_week
      	, fiscal_day_num as day_num
    	FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
    	WHERE week_end_day_date <= current_DATE()
    	AND fiscal_year_num >= ( SELECT MAX(fiscal_year_num) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= current_date)
      UNION ALL
    	SELECT
        	distinct ly.day_date
        	, ty.fiscal_year_num - 1 AS fiscal_year
        	, 'LY' AS ty_ly_ind
        	, ty.fiscal_week_num as fiscal_week
        	, ty.fiscal_day_num as day_num
    	FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM ty
    	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM ly
    	 ON ty.day_date_last_year_realigned = ly.day_date 
    	WHERE ty.week_end_day_date <= current_DATE()
    	AND ty.fiscal_year_num >= ( SELECT MAX(fiscal_year_num) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= current_date)
) WITH DATA PRIMARY INDEX(day_date) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (DAY_DATE) ON dates;

--drop table los_ccs;
CREATE MULTISET VOLATILE TABLE los_ccs
AS (
    SELECT
    DISTINCT
    dt.ty_ly_ind,       
    dt.fiscal_year,
    dt.fiscal_week,
    dt.day_num,
    los_skus.day_date,
    los_skus.channel_country,
    los_skus.channel_brand,
    sku_lkp.div_num,
    TRIM(CONCAT(sku_lkp.div_num,', ', sku_lkp.div_desc)) AS division,
    sku_lkp.grp_num,
    TRIM(CONCAT(sku_lkp.grp_num, ', ', sku_lkp.grp_desc)) AS subdivision,
    sku_lkp.dept_num,
    TRIM(CONCAT(sku_lkp.dept_num, ', ', sku_lkp.dept_desc)) AS department,
    sku_lkp.class_num,
    TRIM(CONCAT(sku_lkp.class_num, ', ', sku_lkp.class_desc)) AS "class",
    sku_lkp.sbclass_num,
    TRIM(CONCAT(sku_lkp.sbclass_num, ', ', sku_lkp.sbclass_desc)) AS subclass,
    COALESCE(CASE WHEN lwdc.partner_relationship_type_code = 'ECONCESSION' THEN lwdc.style_group_num ELSE NULL END, sku_lkp.        style_group_num, sku_lkp.rms_style_num) AS style_group_num,
    CASE WHEN lwdc.partner_relationship_type_code = 'ECONCESSION' THEN lwdc.style_group_num || '-' || lwdc.color_num ELSE sku_lkp.cc END AS cc,
    sku_lkp.brand_name AS brand,
    sku_lkp.supp_name as supplier,
    sku_lkp.npg_flag as npg_ind,
    CASE WHEN lwdc.partner_relationship_type_code = 'ECONCESSION' THEN 1 ELSE 0 END AS mp_ind,
    los_skus.price_band_one,
    los_skus.price_band_two,
    los_skus.rp_ind,
    los_skus.ds_ind,
    los_skus.cl_ind,
    los_skus.pm_ind,
    los_skus.reg_ind,
    los_skus.flash_ind
    FROM T2DL_DAS_SITE_MERCH.live_on_site_daily los_skus
    INNER JOIN dates dt
    ON los_skus.day_date = dt.day_date
    LEFT JOIN T2DL_DAS_MARKDOWN.SKU_LKP sku_lkp
    ON los_skus.sku_id = sku_lkp.rms_sku_num 
    AND los_skus.channel_country = sku_lkp.channel_country
    LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw lwdc
    ON los_skus.sku_id = lwdc.rms_sku_num 
    AND los_skus.channel_country = lwdc.channel_country
    WHERE sku_lkp.grp_num IS NOT NULL
) WITH DATA PRIMARY INDEX(day_date, channel_country, channel_brand, cc, subclass) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(day_date), COLUMN(channel_country), COLUMN(channel_brand), COLUMN(cc) ON los_ccs;

COLLECT STATISTICS COLUMN (
TY_LY_IND ,FISCAL_YEAR ,FISCAL_WEEK,DAY_NUM ,DAY_DATE 
,CHANNEL_COUNTRY ,CHANNEL_BRAND ,DIVISION,SUBDIVISION 
,DEPARTMENT ,"CLASS" ,SUBCLASS ,STYLE_GROUP_NUM ,CC,BRAND 
,SUPPLIER ,PRICE_BAND_ONE ,PRICE_BAND_TWO) ON los_ccs;

COLLECT STATISTICS COLUMN (STYLE_GROUP_NUM) ON los_ccs;

COLLECT STATISTICS COLUMN (DIVISION ,SUBDIVISION ,DEPARTMENT,"CLASS" ,SUBCLASS) ON los_ccs;

--drop table product_types;
CREATE MULTISET VOLATILE TABLE product_types
AS (
    SELECT 
    psd.type_level_1_desc AS product_type_1,
    psd.type_level_2_desc AS product_type_2,
    psd.style_group_num
    FROM
    prd_nap_usr_vws.product_style_dim psd
    WHERE
    psd.channel_country <> 'CA' 
    QUALIFY row_number() 
    OVER(
    PARTITION BY psd.style_group_num ORDER BY psd.dw_sys_updt_tmstp DESC
    ) = 1
) WITH DATA PRIMARY INDEX(style_group_num) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (PRODUCT_TYPE_1 ,PRODUCT_TYPE_2) ON product_types;

COLLECT STATISTICS COLUMN (STYLE_GROUP_NUM) ON product_types;

--drop table quantrix_ccs_category;
CREATE MULTISET VOLATILE TABLE quantrix_ccs_category
AS (
    SELECT
    div_idnt,
    div_label AS division,
    sdiv_idnt,
    sdiv_label AS subdivision,
    dept_idnt,
    dept_label AS department,
    class_idnt,
    class_label AS "class",
    sbclass_idnt,
    sbclass_label AS subclass,
    COALESCE(QUANTRIX_CATEGORY, 'OTHER') AS quantrix_category,
    COALESCE(CCS_CATEGORY , 'OTHER/NOT CATEGORIZED') AS CCS_CATEGORY,
    COALESCE(NORD_ROLE_DESC , 'OTHER/NOT CATEGORIZED') AS NORD_ROLE_DESC,
    COALESCE(RACK_ROLE_DESC , 'OTHER/NOT CATEGORIZED') AS RACK_ROLE_DESC
    FROM t2dl_das_ccs_categories.ccs_merch_themes
) WITH DATA PRIMARY INDEX(subclass) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (DIVISION ,SUBDIVISION ,DEPARTMENT,"CLASS" ,SUBCLASS) ON quantrix_ccs_category;

COLLECT STATISTICS COLUMN (SUBCLASS) ON quantrix_ccs_category;

COLLECT STATISTICS COLUMN (QUANTRIX_CATEGORY ,CCS_CATEGORY) ON quantrix_ccs_category;

--drop table los_base;
CREATE MULTISET VOLATILE TABLE los_base
AS (
    SELECT
    los_base.ty_ly_ind,      
    los_base.fiscal_year,
    los_base.fiscal_week,
    los_base.day_date,
    los_base.day_num,
    los_base.channel_country,
    los_base.channel_brand,
    los_base.division,
    los_base.subdivision,
    los_base.department,
    los_base."class",
    los_base.subclass,
    los_base.brand,
    los_base.supplier,
    los_base.price_band_one,
    los_base.price_band_two,
    los_base.style_group_num,
    los_base.cc,
    cat.quantrix_category,
    cat.ccs_category,
    CASE WHEN los_base.channel_brand = 'NORDSTROM' THEN cat.nord_role_desc ELSE cat.rack_role_desc 
    END AS category_role,
    psd.product_type_1,
    psd.product_type_2,
    los_base.npg_ind,
    los_base.rp_ind,
    los_base.ds_ind,
    los_base.mp_ind,
    los_base.cl_ind,
    los_base.pm_ind,
    los_base.reg_ind,
    los_base.flash_ind
FROM los_ccs los_base
LEFT JOIN product_types psd
ON los_base.style_group_num = psd.style_group_num
LEFT JOIN quantrix_ccs_category cat
ON los_base.div_num = cat.div_idnt
AND los_base.grp_num = cat.sdiv_idnt
AND los_base.dept_num = cat.dept_idnt
AND los_base.class_num = cat.class_idnt
AND los_base.sbclass_num = cat.sbclass_idnt
) WITH DATA PRIMARY INDEX(day_date, channel_country, channel_brand, cc, subclass) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (TY_LY_IND,FISCAL_YEAR,FISCAL_WEEK,DAY_DATE,DAY_NUM 
,CHANNEL_COUNTRY,CHANNEL_BRAND,DIVISION,SUBDIVISION,DEPARTMENT 
,"CLASS" ,SUBCLASS ,BRAND ,SUPPLIER,PRICE_BAND_ONE ,PRICE_BAND_TWO 
,STYLE_GROUP_NUM ,CC,QUANTRIX_CATEGORY ,CCS_CATEGORY ,CATEGORY_ROLE 
,PRODUCT_TYPE_1,PRODUCT_TYPE_2) ON los_base;

COLLECT STATISTICS COLUMN (DAY_NUM) ON los_base;

COLLECT STATISTICS COLUMN (DAY_DATE) ON los_base;

COLLECT STATISTICS COLUMN (FISCAL_WEEK) ON los_base;

COLLECT STATISTICS COLUMN (FISCAL_YEAR) ON los_base;

drop table los_ccs;
drop table quantrix_ccs_category; 
drop table product_types; 

DELETE FROM {environment_schema}.los_daily_agg{env_suffix};
INSERT INTO {environment_schema}.los_daily_agg{env_suffix}
SELECT DISTINCT  
        los_daily.ty_ly_ind AS ty_ly_ind,  
        los_daily.fiscal_year AS fiscal_year,  
        los_daily.fiscal_week AS fiscal_week,  
        los_daily.day_num AS day_num,  
        los_daily.day_date AS day_date,  
        los_daily.channel_country AS channel_country,  
        los_daily.channel_brand AS channel_brand,  
        los_daily.division AS division,  
        los_daily.subdivision AS subdivision,  
        los_daily.department AS department,  
        los_daily."class" AS "class",  
        los_daily.subclass AS subclass,
        los_daily.quantrix_category AS quantrix_category,  
        los_daily.ccs_category AS ccs_category,  
        los_daily.category_role AS category_role,  
        los_daily.brand AS brand,  
        los_daily.supplier AS supplier,  
        los_daily.product_type_1 AS product_type_1,  
        los_daily.product_type_2 AS product_type_2,  
        los_daily.price_band_one AS price_band_one,  
        los_daily.price_band_two AS price_band_two,  
        los_daily.style_group_num AS style_group_num,  
        los_daily.cc AS cc,  
        MAX(los_daily.npg_ind) AS npg_ind,  
        MAX(los_daily.rp_ind) AS rp_ind,  
        MAX(los_daily.ds_ind) AS ds_ind,  
        MAX(los_daily.mp_ind) AS mp_ind,  
        MAX(los_daily.cl_ind) AS cl_ind,  
        MAX(los_daily.pm_ind) AS pm_ind,  
        MAX(los_daily.reg_ind) AS reg_ind,  
        MAX(los_daily.flash_ind) AS flash_ind,
        CURRENT_TIMESTAMP(6) as dw_sys_load_tmstp
    FROM  
        los_base los_daily  
    GROUP BY  
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23;
	
COLLECT STATISTICS PRIMARY INDEX(day_date, channel_country, channel_brand, cc, subclass) ON {environment_schema}.los_daily_agg{env_suffix} ;

