/*------------------------------------------------------
 Price Type Planning Summary
 Purpose: Provide department-level in combination with division/subdivision/pricetype/planversion/channel-level price type information at a weekly grain
 
 Last Update: 02/13/24 Rasagnya Avala - Update dates logic to bring in 2023 data
 Contact for logic/code: Rasagnya Avala (Merch Analytics)
 Contact for ETL: Rasagnya Avala (Merch Analytics)
 --------------------------------------------------------*/

-- Table with department level price type details along with ratios to line up with MFP costs
-- Methodology:Obtain MFP actual costs, divide by price type costs to get ratio. Use the ratio to then divide price type costs with and align with each fulfill type, plan version and price type (Use LAST_VALUE())

CREATE MULTISET VOLATILE TABLE base AS (
SELECT 
    DEPARTMENT_NUM
    ,WEEK_NUM
    ,CHANNEL_NUM
    ,PRICE_TYPE
    ,plan_version
    ,ownership
    ,fulfill_type_num
    ,Net_Sales_R/Net_Sales_R_Ratio AS Net_Sales_R
    ,Net_Sales_C/Net_Sales_C_Ratio AS Net_Sales_C
    ,Net_Sales_U/Net_Sales_U_Ratio AS Net_Sales_U
FROM 
(SELECT 
    DEPARTMENT_NUM
    ,WEEK_NUM
    ,CHANNEL_NUM
    ,PRICE_TYPE
    ,plan_version
    ,ownership
    ,fulfill_type_num
    ,Net_Sales_R
    --,Net_Sales_R_MFP
    ,LAST_VALUE(Net_Sales_R_Ratio IGNORE NULLS ) OVER (PARTITION BY week_num,department_num,channel_num,ownership,plan_version ORDER BY price_type DESC,ownership,plan_version) AS Net_Sales_R_Ratio
    ,Net_Sales_C
    ,LAST_VALUE(net_Sales_C_Ratio IGNORE NULLS ) OVER (PARTITION BY week_num,department_num,channel_num,ownership,plan_version ORDER BY price_type DESC,ownership,plan_version) AS Net_Sales_C_Ratio
    ,Net_Sales_U
    ,LAST_VALUE(Net_Sales_U_Ratio IGNORE NULLS ) OVER (PARTITION BY week_num,department_num,channel_num,ownership,plan_version ORDER BY price_type DESC,ownership,plan_version) AS Net_Sales_U_Ratio
FROM 
(
SELECT 
    pd.DEPARTMENT_NUM
    ,pd.WEEK_NUM
    ,pd.CHANNEL_NUM 
    ,PRICE_TYPE
    ,plan_version
    ,ownership
    ,mcpacf.fulfill_type_num
    ,sum(CASE WHEN plan_version = 'Cp' then GROSS_SLS_R-RTN_R END) AS Net_Sales_R
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.CP_NET_SALES_RETAIL_AMT END) AS Net_Sales_R_MFP
    ,NULLIF(sum(GROSS_SLS_R-RTN_R)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.CP_NET_SALES_RETAIL_AMT END)) AS decimal(18,10)),0) AS Net_Sales_R_Ratio
    ,sum(CASE WHEN plan_version = 'Cp' THEN GROSS_SLS_C-RTN_C END) AS Net_Sales_C
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.CP_NET_SALES_COST_AMT END) AS Net_Sales_C_MFP
    ,NULLIF(sum(GROSS_SLS_C-RTN_C)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.CP_NET_SALES_COST_AMT END)) AS decimal(18,10)),0) AS Net_Sales_C_Ratio
    ,sum(CASE WHEN plan_version = 'Cp' THEN GROSS_SLS_U-RTN_U END) AS Net_Sales_U 
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.CP_NET_SALES_QTY END) AS Net_Sales_U_MFP
    ,NULLIF(sum(GROSS_SLS_U-RTN_U)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.CP_NET_SALES_QTY END)) AS decimal(18,10)),0) AS Net_Sales_U_Ratio
FROM T2DL_DAS_PRICE_TYPE_PLAN_OUTPUTS.PRICE_TYPE_PLAN_DATA pd
LEFT JOIN PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_CHANNEL_FACT mcpacf 
    ON mcpacf.dept_num = pd.DEPARTMENT_NUM 
    AND mcpacf.WEEK_NUM = pd.WEEK_NUM AND mcpacf.CHANNEL_NUM = pd.CHANNEL_NUM 
    AND mcpacf.FULFILL_TYPE_NUM = CASE WHEN ownership = 'Own' THEN 3 WHEN ownership = 'DropShip' THEN 1 WHEN OWNERSHIP = 'RevShare' THEN 2 END  
WHERE pd.year_num >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date()) 
    AND pd.channel_num IN (110,120,210,250) 
    AND mcpacf.CHANNEL_NUM IN (110,120,210,250) 
    AND plan_version = 'Cp'
GROUP BY 1,2,3,4,5,6,7 
UNION ALL  
SELECT
    pd.DEPARTMENT_NUM
    ,pd.WEEK_NUM
    ,pd.CHANNEL_NUM
    ,PRICE_TYPE
    ,plan_version
    ,ownership
    ,mcpacf.fulfill_type_num
    ,sum(CASE WHEN plan_version = 'Sp' then GROSS_SLS_R-RTN_R END) AS Net_Sales_R
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.SP_NET_SALES_RETAIL_AMT END) AS Net_Sales_R_MFP
    ,NULLIF(sum(GROSS_SLS_R-RTN_R)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.SP_NET_SALES_RETAIL_AMT END)) AS decimal(18,10)),0) AS Net_Sales_R_Ratio
    ,sum(CASE WHEN plan_version = 'Sp' THEN GROSS_SLS_C-RTN_C END) AS Net_Sales_C
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.SP_NET_SALES_COST_AMT END) AS Net_Sales_C_MFP
    ,NULLIF(sum(GROSS_SLS_C-RTN_C)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.SP_NET_SALES_COST_AMT END)) AS decimal(18,10)),0) AS Net_Sales_C_Ratio
    ,sum(CASE WHEN plan_version = 'Sp' THEN GROSS_SLS_U-RTN_U END) AS Net_Sales_U 
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.SP_NET_SALES_QTY END) AS Net_Sales_U_MFP
    ,NULLIF(sum(GROSS_SLS_U-RTN_U)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.SP_NET_SALES_QTY END)) AS decimal(18,10)),0) AS Net_Sales_U_Ratio
FROM T2DL_DAS_PRICE_TYPE_PLAN_OUTPUTS.PRICE_TYPE_PLAN_DATA pd
LEFT JOIN PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_CHANNEL_FACT mcpacf 
    ON mcpacf.dept_num = pd.DEPARTMENT_NUM 
    AND mcpacf.WEEK_NUM = pd.WEEK_NUM AND mcpacf.CHANNEL_NUM = pd.CHANNEL_NUM 
    AND mcpacf.FULFILL_TYPE_NUM = CASE WHEN ownership = 'Own' THEN 3 WHEN ownership = 'DropShip' THEN 1 WHEN OWNERSHIP = 'RevShare' THEN 2 END 
WHERE pd.year_num >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date()) 
    AND pd.channel_num IN (110,120,210,250) 
    AND mcpacf.CHANNEL_NUM IN (110,120,210,250)
    AND plan_version = 'Sp'
GROUP BY 1,2,3,4,5,6,7 
UNION ALL 
-- OP 
SELECT 
    pd.DEPARTMENT_NUM
    ,pd.WEEK_NUM
    ,pd.CHANNEL_NUM
    ,PRICE_TYPE
    ,plan_version
    ,ownership
    ,mcpacf.fulfill_type_num
    ,sum(CASE WHEN plan_version = 'Op' then GROSS_SLS_R-RTN_R END) AS Net_Sales_R
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.OP_NET_SALES_RETAIL_AMT END) AS Net_Sales_R_MFP
    ,NULLIF(sum(GROSS_SLS_R-RTN_R)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.OP_NET_SALES_RETAIL_AMT END)) AS decimal(18,10)),0) AS Net_Sales_R_Ratio
    ,sum(CASE WHEN plan_version = 'Op' THEN GROSS_SLS_C-RTN_C END) AS Net_Sales_C
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.OP_NET_SALES_COST_AMT END) AS Net_Sales_C_MFP
    ,NULLIF(sum(GROSS_SLS_C-RTN_C)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.OP_NET_SALES_COST_AMT END)) AS decimal(18,10)),0) AS Net_Sales_C_Ratio
    ,sum(CASE WHEN plan_version = 'Op' THEN GROSS_SLS_U-RTN_U END) AS Net_Sales_U 
    ,sum(CASE WHEN price_type = 'Total' THEN mcpacf.OP_NET_SALES_QTY END) AS Net_Sales_U_MFP
    ,NULLIF(sum(GROSS_SLS_U-RTN_U)/CAST(NULLIFZERO(sum(CASE WHEN price_type = 'Total' THEN mcpacf.OP_NET_SALES_QTY END)) AS decimal(18,10)),0) AS Net_Sales_U_Ratio
FROM T2DL_DAS_PRICE_TYPE_PLAN_OUTPUTS.PRICE_TYPE_PLAN_DATA pd
LEFT JOIN PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_CHANNEL_FACT mcpacf 
    ON mcpacf.dept_num = pd.DEPARTMENT_NUM 
    AND mcpacf.WEEK_NUM = pd.WEEK_NUM AND mcpacf.CHANNEL_NUM = pd.CHANNEL_NUM 
    AND mcpacf.FULFILL_TYPE_NUM = CASE WHEN ownership = 'Own' THEN 3 WHEN ownership = 'DropShip' THEN 1 WHEN OWNERSHIP = 'RevShare' THEN 2 END  
WHERE pd.year_num >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date()) 
    AND pd.channel_num IN (110,120,210,250) 
    AND mcpacf.CHANNEL_NUM IN (110,120,210,250)
    AND plan_version = 'Op'
GROUP BY 1,2,3,4,5,6,7
) j ) l 
) 
WITH DATA 
PRIMARY INDEX (WEEK_NUM, DEPARTMENT_NUM, CHANNEL_NUM, price_type, plan_version, fulfill_type_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, WEEK_NUM, price_type, plan_version, fulfill_type_num) ON base;

-- Department lookup table
CREATE MULTISET VOLATILE TABLE dept_lookup AS 
(
SELECT DISTINCT dept_num
,dept_name
,division_num
,division_name
,subdivision_num
,subdivision_name 
FROM PRD_NAP_USR_VWS.DEPARTMENT_DIM
)WITH DATA 
PRIMARY INDEX(dept_num, division_num, subdivision_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
    PRIMARY INDEX(DEPT_NUM, division_num, subdivision_num) 
    ON dept_lookup;

-- Date lookup
CREATE MULTISET VOLATILE TABLE date_lkp AS (
SELECT 
ty.week_idnt AS ty_week_idnt
,ly.week_idnt AS ly_week_idnt
,ty.fiscal_year_num AS year_idnt
,ty.fiscal_halfyear_num  AS half_idnt
,ty.quarter_idnt AS quarter_idnt
,ty.month_idnt AS month_idnt
,TRIM(ty.fiscal_year_num)||' H'||RIGHT(CAST(ty.fiscal_halfyear_num AS CHAR(5)),1) AS half_label
,ty.quarter_label
,TRIM(ty.fiscal_year_num)||  ' ' || TRIM(ty.fiscal_month_num) || ' ' || TRIM(ty.month_abrv) AS month_label
,TRIM(ty.fiscal_year_num)|| ', ' || TRIM(ty.fiscal_month_num) || ', Wk ' || TRIM(ty.week_num_of_fiscal_month) AS week_label
,ty.week_end_day_date 
FROM prd_nap_usr_vws.day_cal_454_dim ty
LEFT JOIN prd_nap_usr_vws.day_cal_454_dim ly
    ON ty.day_date_last_year_realigned = ly.day_date
WHERE ty.fiscal_year_num BETWEEN (SELECT MAX(fiscal_year_num-2) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
        AND (SELECT MAX(fiscal_year_num+1) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)  
) 
WITH DATA 
PRIMARY INDEX(ty_week_idnt) 
ON COMMIT PRESERVE ROWS;

-- Pull in TY, LY actual costs from MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW and union for each price type and plan version combo
CREATE MULTISET VOLATILE TABLE ty_ly AS (
SELECT 
    a.dept_num AS DEPARTMENT_NUM
    ,a.WEEK_NUM 
    ,a.CHANNEL_NUM
    ,'Promotional' AS PRICE_TYPE
    ,'Ty' AS plan_version
    ,sum(jwn_operational_gmv_promo_retail_amt_ty) AS Net_Sales_R
    ,sum(jwn_operational_gmv_promo_cost_amt_ty) AS Net_Sales_C
    ,sum(jwn_operational_gmv_promo_units_ty) AS Net_Sales_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW a
WHERE YEAR_NUM >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date()) 
AND a.CHANNEL_NUM IN (110,120,210,250) GROUP BY 1,2,3,4,5
UNION ALL 
SELECT 
    a.dept_num AS DEPARTMENT_NUM
    ,a.WEEK_NUM
    ,a.CHANNEL_NUM
    ,'Clearance' AS PRICE_TYPE
    ,'Ty' AS plan_version
    ,sum(jwn_operational_gmv_clearance_retail_amt_ty) AS Net_Sales_R
    ,sum(jwn_operational_gmv_clearance_cost_amt_ty) AS Net_Sales_C
    ,sum(jwn_operational_gmv_clearance_units_ty) AS Net_Sales_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW a
WHERE YEAR_NUM >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date()) 
AND a.CHANNEL_NUM IN (110,120,210,250) GROUP BY 1,2,3,4,5
UNION ALL 
SELECT 
    a.dept_num AS DEPARTMENT_NUM
    ,a.WEEK_NUM
    ,a.CHANNEL_NUM
    ,'Total' AS PRICE_TYPE
    ,'Ty' AS plan_version
    ,sum(jwn_operational_gmv_total_retail_amt_ty) AS Net_Sales_R
    ,sum(jwn_operational_gmv_total_cost_amt_ty) AS Net_Sales_C
    ,sum(jwn_operational_gmv_total_units_ty) AS Net_Sales_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW a
WHERE YEAR_NUM >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date()) 
    AND a.CHANNEL_NUM IN (110,120,210,250) GROUP BY 1,2,3,4,5
UNION ALL 
SELECT 
    a.dept_num AS DEPARTMENT_NUM
    ,a.WEEK_NUM
    ,a.CHANNEL_NUM
    ,'Regular' AS PRICE_TYPE
    ,'Ty' AS plan_version
    ,sum(jwn_operational_gmv_regular_retail_amt_ty) AS Net_Sales_R
    ,sum(jwn_operational_gmv_regular_cost_amt_ty) AS Net_Sales_C
    ,sum(jwn_operational_gmv_regular_units_ty) AS Net_Sales_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW a
WHERE YEAR_NUM >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date()) 
    AND a.CHANNEL_NUM IN (110,120,210,250) GROUP BY 1,2,3,4,5
UNION ALL 
SELECT 
    a.dept_num AS DEPARTMENT_NUM
    ,a.WEEK_NUM
    ,a.CHANNEL_NUM
    ,'Total' AS PRICE_TYPE
    ,'Ly' AS plan_version
    ,sum(jwn_operational_gmv_total_retail_amt_ly) AS Net_Sales_R
    ,sum(jwn_operational_gmv_total_cost_amt_ly) AS Net_Sales_C
    ,sum(jwn_operational_gmv_total_units_ly) AS Net_Sales_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW a
WHERE YEAR_NUM >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date())
AND a.CHANNEL_NUM IN (110,120,210,250) GROUP BY 1,2,3,4,5
UNION ALL 
SELECT 
    a.dept_num AS DEPARTMENT_NUM
    ,a.WEEK_NUM
    ,a.CHANNEL_NUM
    ,'Regular' AS PRICE_TYPE
    ,'Ly' AS plan_version
    ,sum(jwn_operational_gmv_regular_retail_amt_ly) AS Net_Sales_R
    ,sum(jwn_operational_gmv_regular_cost_amt_ly) AS Net_Sales_C
    ,sum(jwn_operational_gmv_regular_units_ly) AS Net_Sales_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW a
WHERE YEAR_NUM >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date())
    AND a.CHANNEL_NUM IN (110,120,210,250) GROUP BY 1,2,3,4,5
UNION ALL 
SELECT 
    a.dept_num AS DEPARTMENT_NUM
    ,a.WEEK_NUM
    ,a.CHANNEL_NUM
    ,'Promotional' AS PRICE_TYPE
    ,'Ly' AS plan_version
    ,sum(jwn_operational_gmv_promo_retail_amt_ly) AS Net_Sales_R
    ,sum(jwn_operational_gmv_promo_cost_amt_ly) AS Net_Sales_C
    ,sum(jwn_operational_gmv_promo_units_ly) AS Net_Sales_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW a
WHERE YEAR_NUM >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date())
   AND a.CHANNEL_NUM IN (110,120,210,250) GROUP BY 1,2,3,4,5
UNION ALL 
SELECT 
    a.dept_num AS DEPARTMENT_NUM 
    ,a.WEEK_NUM
    ,a.CHANNEL_NUM
    ,'Clearance' AS PRICE_TYPE
    ,'Ly' AS plan_version
    ,sum(jwn_operational_gmv_clearance_retail_amt_ly) AS Net_Sales_R
    ,sum(jwn_operational_gmv_clearance_cost_amt_ly) AS Net_Sales_C
    ,sum(jwn_operational_gmv_clearance_units_ly) AS Net_Sales_U
FROM PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW a
WHERE YEAR_NUM >= (SELECT DISTINCT fiscal_year_num-1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date())
    AND a.CHANNEL_NUM IN (110,120,210,250) GROUP BY 1,2,3,4,5
) 
WITH DATA 
PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, WEEK_NUM, price_type, plan_version) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
    PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, WEEK_NUM, price_type, plan_version) 
    ON ty_ly;

CREATE MULTISET VOLATILE TABLE ty_ly_mapped AS(
SELECT DISTINCT 
     ty_ly.DEPARTMENT_NUM
    ,d.dept_name AS DEPARTMENT_DESC 
    ,d.division_num AS DIVISION_NUM 
    ,d.division_name AS DIVISION_DESC 
    ,d.subdivision_num AS SUBDIVISION_NUM 
    ,d.subdivision_name AS SUBDIVISION_DESC 
    ,e.year_idnt
    ,e.half_idnt
    ,e.quarter_idnt
    ,e.month_idnt
    ,e.ty_week_idnt AS week_idnt
    ,e.half_label
    ,e.quarter_label
    ,e.month_label
    ,e.week_label
    ,e.week_end_day_date
    ,ty_ly.CHANNEL_NUM
    ,ty_ly.PRICE_TYPE
    ,ty_ly.plan_version
    ,ty_ly.Net_Sales_R
    ,ty_ly.Net_Sales_C
    ,ty_ly.Net_Sales_U
    FROM ty_ly 
    INNER JOIN dept_lookup d
    ON d.dept_num = ty_ly.department_num
    INNER JOIN date_lkp e  
    ON e.ty_week_idnt = ty_ly.week_num 
UNION ALL 
SELECT DISTINCT
     ty_ly.DEPARTMENT_NUM
    ,d.dept_name AS DEPARTMENT_DESC 
    ,d.division_num AS DIVISION_NUM 
    ,d.division_name AS DIVISION_DESC 
    ,d.subdivision_num AS SUBDIVISION_NUM 
    ,d.subdivision_name AS SUBDIVISION_DESC 
    ,e.year_idnt
    ,e.half_idnt
    ,e.quarter_idnt
    ,e.month_idnt
    ,e.ty_week_idnt AS week_idnt
    ,e.half_label
    ,e.quarter_label
    ,e.month_label
    ,e.week_label
    ,e.week_end_day_date
    ,ty_ly.CHANNEL_NUM
    ,ty_ly.PRICE_TYPE
    ,'Ly' AS plan_version
    ,ty_ly.Net_Sales_R
    ,ty_ly.Net_Sales_C
    ,ty_ly.Net_Sales_U
    FROM ty_ly 
    INNER JOIN dept_lookup d
    ON d.dept_num = ty_ly.department_num
    INNER JOIN date_lkp e 
    ON e.ly_week_idnt = ty_ly.week_num WHERE ty_ly.plan_version = 'Ty'
    AND week_idnt > (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date()) 
 )
WITH DATA 
PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
    PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) 
    ON ty_ly_mapped;

-- Combine SP, OP, CP and TY, LY plans data
CREATE MULTISET VOLATILE TABLE plans_base AS 
(
SELECT 
-- CP, OP, SP plans
    base.DEPARTMENT_NUM 
    ,dept_lookup.dept_name AS DEPARTMENT_DESC 
    ,dept_lookup.division_num AS DIVISION_NUM 
    ,dept_lookup.division_name AS DIVISION_DESC 
    ,dept_lookup.subdivision_num AS SUBDIVISION_NUM 
    ,dept_lookup.subdivision_name AS SUBDIVISION_DESC 
    ,ty_ly_mapped.year_idnt
    ,ty_ly_mapped.half_idnt
    ,ty_ly_mapped.quarter_idnt
    ,ty_ly_mapped.month_idnt
    ,base.week_num AS week_idnt
    ,ty_ly_mapped.half_label
    ,ty_ly_mapped.quarter_label
    ,ty_ly_mapped.month_label
    ,ty_ly_mapped.week_label
    ,ty_ly_mapped.week_end_day_date
    ,base.CHANNEL_NUM
    ,base.PRICE_TYPE
    ,base.plan_version
    ,base.Net_Sales_R
    ,base.Net_Sales_C
    ,base.Net_Sales_U
FROM base 
LEFT JOIN ty_ly_mapped 
    ON Ty_ly_mapped.department_num = base.department_num
    AND base.week_num = ty_ly_mapped.WEEK_idnt
    AND base.channel_num = ty_ly_mapped.CHANNEL_NUM 
LEFT JOIN dept_lookup 
ON dept_lookup.dept_num = base.department_num
UNION ALL 
-- TY, LY plans
SELECT 
    base.DEPARTMENT_NUM
    ,DEPARTMENT_DESC 
    ,DIVISION_NUM 
    ,DIVISION_DESC 
    ,SUBDIVISION_NUM 
    ,SUBDIVISION_DESC 
    ,ty_ly_mapped.year_idnt
    ,ty_ly_mapped.half_idnt
    ,ty_ly_mapped.quarter_idnt
    ,ty_ly_mapped.month_idnt
    ,ty_ly_mapped.week_idnt AS week_idnt
    ,ty_ly_mapped.half_label
    ,ty_ly_mapped.quarter_label
    ,ty_ly_mapped.month_label
    ,ty_ly_mapped.week_label
    ,ty_ly_mapped.week_end_day_date
    ,base.CHANNEL_NUM
    ,ty_ly_mapped.PRICE_TYPE
    ,ty_ly_mapped.plan_version
    ,ty_ly_mapped.Net_Sales_R
    ,ty_ly_mapped.Net_Sales_C
    ,ty_ly_mapped.Net_Sales_U
FROM ty_ly_mapped 
inner JOIN base 
    ON Ty_ly_mapped.department_num = base.department_num
    AND ty_ly_mapped.week_idnt = base.week_num
    AND base.channel_num = ty_ly_mapped.CHANNEL_NUM
) 
WITH DATA 
PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) ON plans_base;

CREATE MULTISET VOLATILE TABLE plans_prefinal AS 
(SELECT DISTINCT * 
FROM plans_base
) 
WITH DATA 
PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) ON plans_prefinal;

-- Logic to aling LY plan data with date dimensions;Add Fiscal Date Dimensions and Identifiers
CREATE MULTISET VOLATILE TABLE plans_final AS (
SELECT 
    DEPARTMENT_NUM
    ,DEPARTMENT_DESC 
    ,DIVISION_NUM 
    ,DIVISION_DESC 
    ,SUBDIVISION_NUM 
    ,SUBDIVISION_DESC 
    ,e.year_idnt
    ,e.half_idnt
    ,e.quarter_idnt
    ,e.month_idnt
    ,week_idnt
    ,e.half_label
    ,e.quarter_label
    ,e.month_label
    ,e.week_label
    ,e.week_end_day_date
    ,CHANNEL_NUM
    ,PRICE_TYPE
    ,plan_version
    ,Net_Sales_R
    ,Net_Sales_C
    ,Net_Sales_U
FROM plans_prefinal 
LEFT JOIN date_lkp e 
ON e.ty_week_idnt = plans_prefinal.week_idnt
) 
WITH DATA 
PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) ON plans_final;
    
DELETE FROM {environment_schema}.PRICE_TYPE_PLANNING;
INSERT INTO {environment_schema}.PRICE_TYPE_PLANNING
SELECT DISTINCT * FROM plans_final