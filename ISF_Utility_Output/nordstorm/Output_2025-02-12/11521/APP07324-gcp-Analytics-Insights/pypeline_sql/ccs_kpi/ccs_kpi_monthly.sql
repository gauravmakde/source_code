SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ccs_kpi_monthly_11521_ACE_ENG;
     Task_Name=ccs_kpi_monthly;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_ccs_categories.ccs_kpi_monthly
Team/Owner: Merchandising Insights
Date Created/Modified: 4/29/2024

Note:
-- Purpose: customer metrics on CCS Category and department levels
-- Cadence: Runs weekly on Sunday at 15:30 
*/




CREATE MULTISET VOLATILE TABLE date_lookup AS (
SELECT 
distinct 
re.day_date as day_date
	,case
	when re.ty_ly_lly_ind = 'TY' then  CAST(1 AS integer) 
	when re.ty_ly_lly_ind = 'LY' then  CAST(2 AS integer)
	when re.ty_ly_lly_ind = 'LLY' then  CAST(3 AS integer)
	end as ty_ly_lly_ind
	,re.fiscal_year_num
	,ytd_last_full_month_ind
	
FROM prd_nap_vws.realigned_date_lkup_vw re
LEFT JOIN prd_nap_usr_vws.day_cal_454_dim dcd
	ON dcd.day_date = re.day_date
WHERE
    ytd_last_full_month_ind = 'Y' -- only include completed ytd months
) WITH DATA PRIMARY INDEX(ty_ly_lly_ind) ON COMMIT PRESERVE ROWS;  
	  
COLLECT STATISTICS 
	COLUMN ty_ly_lly_ind
	,COLUMN day_date
ON date_lookup;


/*select * from date_lookup*/

-- Previous purchase table. Used for retained customer count --------------------------------------
CREATE VOLATILE MULTISET TABLE prev_purchase AS (
 SELECT DISTINCT 
 acp_id
 -- Adjust indicator for join to sandbox. Sandbox will see if customer has purchase in previous year.
 ,CASE ty_ly_lly_ind
 	WHEN 2 THEN 1 
 	WHEN 3 THEN 2
 END AS ty_ly_lly_ind
 FROM prd_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 JOIN date_lookup d
 	ON COALESCE(dtl.order_date,dtl.tran_date) = d.day_date
 WHERE 
	(d.ty_ly_lly_ind = 2 OR d.ty_ly_lly_ind = 3)
    AND dtl.line_net_usd_amt <> 0 
    AND NOT dtl.acp_id IS NULL
    AND dtl.banner in ('NORDSTROM','RACK')

) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;
COLLECT STATS 
COLUMN acp_id 
,COLUMN ty_ly_lly_ind
ON prev_purchase;



CREATE VOLATILE MULTISET TABLE acquired AS (
SELECT
  acp_id
FROM PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT ntn
JOIN date_lookup d
 	ON ntn.aare_status_date = d.day_date

) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;
COLLECT STATS 
COLUMN acp_id 
ON acquired
;


--TY and LY YTD acp_id, trip_id, sale date, sku ---------------------------------------------------
CREATE VOLATILE MULTISET TABLE tran AS (

SELECT
	d.ty_ly_lly_ind,
	t.acp_id,
	t.sku_num,
	store_num,
	trip_id
FROM prd_nap_usr_vws.retail_tran_detail_fact_vw t
JOIN date_lookup d ON
	t.tran_date = d.day_date
	and d.ytd_last_full_month_ind = 'Y'
WHERE
	NOT t.acp_id IS NULL
	AND t.error_flag = 'N'
	AND t.tran_latest_version_ind = 'Y'
	AND t.tran_type_code   IN  ('EXCH', 'SALE', 'RETN')
	AND (d.ty_ly_lly_ind = 1 OR d.ty_ly_lly_ind = 2)
    ) WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;-- updated to no PI

COLLECT STATS 
    COLUMN acp_id,
    COLUMN sku_num,
	COLUMN store_num,
	COLUMN TY_LY_LLY_IND
ON tran;


-- sandbox ----------------------------------------------------------------------------------------
CREATE VOLATILE MULTISET TABLE cm_sandbox AS (
  SELECT
	CASE
		WHEN str.business_unit_desc IN ('FULL LINE', 'N.COM') THEN 'N'
		WHEN str.business_unit_desc IN ('RACK', 'OFFPRICE ONLINE') THEN 'NR'
	END AS banner,
	CASE
		WHEN str.business_unit_desc IN ('FULL LINE', 'N.COM') THEN NORD_ROLE_DESC
		WHEN str.business_unit_desc IN ('RACK', 'OFFPRICE ONLINE') THEN RACK_ROLE_DESC
	END AS ccs_role,
	tran.ty_ly_lly_ind,
    tran.acp_id, 
    tran.trip_id, 
-- customer flags
    CASE WHEN acq.acp_id IS NOT NULL THEN 1 ELSE 0 end AS ntn_flg,
    CASE WHEN pr.acp_id IS NOT NULL THEN 1 ELSE 0 end AS retained_flg,
-- CCS fields
    mtm.CCS_SUBCATEGORY,
    mtm.DEPT_LABEL
FROM tran
-- join to sku lookup   
    LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw  pssclv 
        ON tran.sku_num = pssclv.rms_sku_num
-- join for ccs data
    LEFT JOIN T2DL_DAS_CCS_CATEGORIES.ccs_merch_themes mtm 
        ON pssclv.DEPT_num = mtm.DEPT_IDNT
        AND pssclv.CLASS_num = mtm.CLASS_IDNT
        AND pssclv.SBCLASS_num = mtm.SBCLASS_IDNT
--Store data
    INNER JOIN prd_nap_usr_vws.store_dim AS str
        ON tran.store_num = str.store_num
        AND str.business_unit_desc IN 
        ('FULL LINE',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK'
   		)
-- New to Nord
      LEFT JOIN acquired AS acq
      ON tran.acp_id = acq.acp_id
-- Retained 
      LEFT JOIN prev_purchase AS pr
        ON tran.acp_id = pr.acp_id
        AND tran.ty_ly_lly_ind = pr.ty_ly_lly_ind
		
	    --WHERE mtm.CCS_SUBCATEGORY IS NOT NULL

) WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


COLLECT STATS 
--COLUMN (TRIP_ID),
--COLUMN (DEPT_LABEL),
--COLUMN (CCS_SUBCATEGORY),
COLUMN (ACP_ID),
--COLUMN (DEPT_LABEL),
COLUMN (ty_ly_lly_ind),
COLUMN (banner)
ON cm_sandbox;


-- Final query ------------------------------------------------------------------------------------
DELETE FROM {shoe_categories_t2_schema}.ccs_kpi_monthly ALL;

INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('Banner' AS VARCHAR(100)) AS data_level
	, banner
	, CAST('Total' AS VARCHAR(100)) AS ccs_role
	, CAST('Total' AS VARCHAR(100)) AS ccs_subcategory
	, CAST('Total' AS VARCHAR(100)) AS department_label
	, 'TY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox 
	WHERE ty_ly_lly_ind=1
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6;

INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('CCS_Role' AS VARCHAR(100)) AS data_level
	, banner
	, ccs_role
	, CAST('Total' AS VARCHAR(100)) AS CCS_SUBCATEGORY
	, CAST('Total' AS VARCHAR(100)) AS department_label
	, 'TY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox
	WHERE ty_ly_lly_ind=1
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;


INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('CCS_Subcategory' AS VARCHAR(100)) AS data_level
	, banner
	, ccs_role
	, CCS_SUBCATEGORY
	, CAST('Total' AS VARCHAR(100)) AS department_label
	, 'TY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox
	WHERE ty_ly_lly_ind=1
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;
	
INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('Department_No_Subcat' AS VARCHAR(100)) AS data_level
	, banner
	, ccs_role
	, CAST('Total' AS VARCHAR(100)) AS ccs_subcategory
	, DEPT_LABEL AS department_label
	, 'TY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox
	WHERE ty_ly_lly_ind=1
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;

INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('Department' AS VARCHAR(100)) AS data_level
	, banner
	, ccs_role
	, ccs_subcategory
	, DEPT_LABEL AS department_label
	, 'TY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox
	WHERE ty_ly_lly_ind=1
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;
	
	
	-----LY
	
	
INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('Banner' AS VARCHAR(100)) AS data_level
	, banner
	, CAST('Total' AS VARCHAR(100)) AS ccs_role
	, CAST('Total' AS VARCHAR(100)) AS ccs_subcategory
	, CAST('Total' AS VARCHAR(100)) AS department_label
	, 'LY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox 
	WHERE ty_ly_lly_ind=2
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;

INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('CCS_Role' AS VARCHAR(100)) AS data_level
	, banner
	, ccs_role
	, CAST('Total' AS VARCHAR(100)) AS CCS_SUBCATEGORY
	, CAST('Total' AS VARCHAR(100)) AS department_label
	, 'LY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox
	WHERE ty_ly_lly_ind=2
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;

INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('CCS_Subcategory' AS VARCHAR(100)) AS data_level
	, banner
	, ccs_role
	, CCS_SUBCATEGORY
	, CAST('Total' AS VARCHAR(100)) AS department_label
	, 'LY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox
	WHERE ty_ly_lly_ind=2
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;
	
INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('Department_No_Subcat' AS VARCHAR(100)) AS data_level
	, banner
	, ccs_role
	, CAST('Total' AS VARCHAR(100)) AS ccs_subcategory
	, DEPT_LABEL AS department_label
	, 'LY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox
	WHERE ty_ly_lly_ind=2
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;

INSERT INTO {shoe_categories_t2_schema}.ccs_kpi_monthly
SELECT
	CAST('Department' AS VARCHAR(100)) AS data_level
	, banner
	, ccs_role
	, ccs_subcategory
	, DEPT_LABEL AS department_label
	, 'LY' ty_ly_lly_ind
	, Count(DISTINCT TRIP_ID) AS trip_count
	, Count(DISTINCT acp_id) AS total_customer_count
	, Count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END ) AS aqc_customer_count
	, Count(DISTINCT CASE WHEN retained_flg = 1 THEN acp_id END ) AS retained_customer_count
	, CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM
	cm_sandbox
	WHERE ty_ly_lly_ind=2
GROUP BY
	1
	, 2
	, 3
	, 4
	, 5
	, 6
;

COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (ty_ly_lly_ind) -- column names used for primary index
on {shoe_categories_t2_schema}.ccs_kpi_monthly;


SET QUERY_BAND = NONE FOR SESSION;  