/*
Name: Store Customer Counts DDL
APPID-Name: APP09268 Merch Analytics - Store Reporting
Purpose: Populate Weekly Customer Counts Store Reporting T2 Source Data in 
Variable(s):    "environment_schema" - T2DL_DAS_<PROJECT DATALAB> 
                "start_date" - date for previous full week start period for output data 
                        (Default = "(CURRENT_DATE - 1)  - INTERVAL '14' DAY")
                "end_date" - date for previous full week end period for output data 
                        (Default = "(CURRENT_DATE - 1)")
DAG: merch_main_cust_counts_store
Author(s): Alli Moore
Date Created: 2024-02-27
Date Last Updated: 2024-03-15
*/


/********************************************** START DIMENSIONS **************************************************/

-- begin
-- Realigned Dates Lookup
--DROP TABLE DT_LKUP;
CREATE MULTISET VOLATILE TABLE DT_LKUP AS (
WITH ty_lkp
AS (
    SELECT DISTINCT
    a.day_date AS ty_day_date
      , day_idnt
      , day_date_last_year_realigned
      , a.fiscal_week_num
      , a.week_idnt
      , a.week_desc
      , a.week_label
      , a.week_start_day_date
      , a.week_end_day_date
      , a.week_num_of_fiscal_month
      , a.month_idnt
      , a.fiscal_month_num
      , a.month_desc
      , a.month_abrv
      , a.month_label
      , a.month_start_day_date
      , a.month_start_week_idnt
      , a.month_end_day_date
      , a.month_end_week_idnt
      , a.quarter_idnt
      , a.fiscal_quarter_num
      , a.fiscal_year_num
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM a
    WHERE fiscal_year_num = (SELECT DISTINCT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date <= {end_date})
    	AND fiscal_week_num >= (SELECT COALESCE(MAX(fiscal_week_num), 1) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {start_date}
    		AND fiscal_year_num = (SELECT DISTINCT MAX(fiscal_year_num) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {end_date}))
    	AND fiscal_week_num <= (SELECT MAX(fiscal_week_num) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {end_date}
    		AND fiscal_year_num = (SELECT DISTINCT MAX(fiscal_year_num) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {end_date})) 
)
SELECT
    CAST('TY' AS VARCHAR(3)) as ty_ly_lly_ind
    , ty_day_date AS day_date
    , a.day_idnt
    , a.fiscal_week_num
    , a.week_idnt
    , a.week_desc
    , a.week_label
    , a.week_start_day_date
    , a.week_end_day_date
    , a.week_num_of_fiscal_month
    , a.fiscal_month_num
    , a.month_idnt
    , a.month_desc
    , a.month_abrv
    , TRIM(a.fiscal_year_num) || ' ' || TRIM(a.month_abrv) AS month_label
    , a.month_start_day_date
    , a.month_start_week_idnt
    , a.month_end_day_date
    , a.month_end_week_idnt
    , a.fiscal_quarter_num
    , CAST(TRIM(TRIM(a.fiscal_year_num) || TRIM(a.fiscal_quarter_num)) AS INTEGER) AS quarter_idnt
    , a.fiscal_year_num
    , MIN(a.ty_day_date) OVER (PARTITION BY ty_ly_lly_ind) AS period_start_day_date
    , MAX(a.ty_day_date) OVER (PARTITION BY ty_ly_lly_ind) AS period_end_day_date
FROM ty_lkp a
UNION ALL
SELECT
    'LY' as ty_ly_lly_ind
    , a.day_date_last_year_realigned as day_date
    , a.day_idnt - 1000 AS day_idnt
    , a.fiscal_week_num
    , a.week_idnt - 100 AS week_idnt
    , a.week_desc
    , TRIM(TRIM(CAST(a.fiscal_year_num - 1 AS VARCHAR(4))) || ' ' || a.month_abrv || ' ' || a.week_desc) AS week_label
    , b.week_start_day_date
    , b.week_end_day_date
    , a.week_num_of_fiscal_month
    , a.fiscal_month_num
    , a.month_idnt - 100 AS month_idnt
    , a.month_desc
    , a.month_abrv
    , TRIM(a.fiscal_year_num - 1) || ' ' || TRIM(a.month_abrv) AS month_label
    , MIN(a.day_date_last_year_realigned) OVER (PARTITION BY a.fiscal_month_num) AS month_start_day_date
    , MIN(a.week_idnt - 100) OVER (PARTITION BY a.fiscal_month_num) AS month_start_week_idnt
    , MAX(a.day_date_last_year_realigned) OVER (PARTITION BY a.fiscal_month_num) AS month_end_day_date
    , MAX(a.week_idnt - 100) OVER (PARTITION BY a.fiscal_month_num) AS month_end_week_idnt
    , a.fiscal_quarter_num
    , CAST(TRIM(TRIM(a.fiscal_year_num - 1) || TRIM(a.fiscal_quarter_num)) AS INTEGER) AS quarter_idnt
    , a.fiscal_year_num - 1 AS fiscal_year_num
    , MIN(a.day_date_last_year_realigned) OVER (PARTITION BY ty_ly_lly_ind) AS period_start_day_date
    , MAX(a.day_date_last_year_realigned) OVER (PARTITION BY ty_ly_lly_ind) AS period_end_day_date
FROM ty_lkp a
INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM b
    ON a.day_date_last_year_realigned = b.day_date
) WITH DATA PRIMARY INDEX(day_idnt, day_date) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(day_idnt, day_date) ON DT_LKUP;



-- Realigned Week Lookup
--DROP TABLE WK_LKUP;
CREATE MULTISET VOLATILE TABLE WK_LKUP AS (
SELECT DISTINCT
	ty_ly_lly_ind
	, dt.week_idnt
	, dt.week_desc
	, dt.week_label
	, dt.fiscal_week_num
	, dt.week_start_day_date
	, dt.week_end_day_date
    , dt.week_num_of_fiscal_month
	, dt.month_idnt
	, dt.month_desc
	, dt.month_label
	, dt.month_abrv
	, dt.fiscal_month_num
	, dt.month_start_day_date
	, dt.month_start_week_idnt
	, dt.month_end_day_date
	, dt.month_end_week_idnt
	, dt.quarter_idnt
	, dt.fiscal_year_num
FROM dt_lkup dt
) WITH DATA PRIMARY INDEX(week_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(week_idnt) ON WK_LKUP;



-- Store Lookup
--DROP TABLE STORE_LKUP;
CREATE MULTISET VOLATILE TABLE STORE_LKUP AS (
SELECT
	st.store_num
	, st.store_name
	, st.store_type_code
	, st.store_type_desc
	, st.selling_store_ind
	, st.region_num
	, st.region_desc
	, st.business_unit_num
	, st.business_unit_desc
	, st.subgroup_num
	, st.subgroup_desc
	, st.store_dma_code
	, dma.dma_desc
	, dma.dma_shrt_desc
	, st.channel_num
	, st.channel_desc
	, st.comp_status_code
	, st.comp_status_desc
	, MAX(CASE WHEN peer_group_type_code = 'OPD' THEN peer_group_num ELSE NULL END) AS rack_district_num
	, MAX(CASE WHEN peer_group_type_code = 'OPD' THEN peer_group_desc ELSE NULL END) AS rack_district_desc
	, MAX(CASE 
		WHEN st.channel_num = 110 AND peer_group_type_code = 'FPC' THEN peer_group_desc
		WHEN st.channel_num = 210 AND peer_group_type_code = 'OPC' THEN peer_group_desc
		ELSE NULL
	END) AS cluster_climate
	, MAX(CASE 
		WHEN st.channel_num = 110 AND peer_group_type_code = 'FPI' THEN peer_group_desc
		WHEN st.channel_num = 210 AND peer_group_type_code = 'OCP' THEN peer_group_desc
		ELSE NULL
	END) AS cluster_price
	, MAX(CASE 
		WHEN st.channel_num = 110 AND peer_group_type_code = 'FPD' THEN peer_group_desc
		ELSE NULL
	END) AS cluster_designer
	, MAX(CASE 
		WHEN st.channel_num = 110 AND peer_group_type_code = 'FPP' THEN peer_group_desc
		ELSE NULL
	END) AS cluster_presidents_cup
FROM PRD_NAP_USR_VWS.STORE_DIM st
LEFT JOIN PRD_NAP_USR_VWS.STORE_PEER_GROUP_DIM cls
	ON cls.store_num = st.store_num
LEFT JOIN PRD_NAP_USR_VWS.ORG_DMA dma
	ON st.store_dma_code = dma.dma_code
WHERE selling_store_ind = 'S'
	AND store_close_date IS NULL
	AND store_type_code IN ('FL','NL','RK') 
	AND channel_num IN (110, 210) 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(store_num) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(store_num) ON STORE_LKUP;

/********************************************** END DIMENSIONS **************************************************/





/*********************************************** START METRICS **************************************************/

-------------------- ACP LOOKUP ---------------------
-- ACP/LOYALTY LOOKUP - TY Only
--DROP TABLE acp_lkup_ty;
CREATE MULTISET VOLATILE TABLE acp_lkup_ty AS (
    SELECT DISTINCT
		d.day_date
		, d.ty_ly_lly_ind
		, rwd.loyalty_id
		, ac.acp_id
		, rwd.rewards_level
		, ROW_NUMBER() OVER (PARTITION BY rwd.acp_id, d.day_date ORDER BY rwd.start_day_date DESC) AS rn -- needed FOR WHEN reward LEVEL changes BUT old level rwd.end_day_date OVERLAPS WITH NEW level rwd.start_day_date BY >1 day
	FROM PRD_NAP_USR_VWS.LOYALTY_LEVEL_LIFECYCLE_FACT_VW rwd
	INNER JOIN DT_LKUP d 
		ON d.day_date BETWEEN rwd.start_day_date AND (rwd.end_day_date - INTERVAL '1' DAY)
		AND d.ty_ly_lly_ind = 'TY'
	INNER JOIN PRD_NAP_USR_VWS.ANALYTICAL_CUSTOMER ac
		ON ac.acp_loyalty_id = rwd.loyalty_id
	INNER JOIN prd_nap_usr_vws.loyalty_member_dim_vw as lmd
    	ON ac.acp_loyalty_id = lmd.loyalty_id
    	AND COALESCE(lmd.member_enroll_date, DATE '2099-12-31') <= d.period_end_day_date
    	AND COALESCE(lmd.member_close_date, DATE '2099-12-31') >= d.period_start_day_date
	WHERE ac.acp_id IS NOT NULL
		AND COALESCE(lmd.member_enroll_date, DATE '2099-12-31') <  COALESCE(lmd.member_close_date, DATE '2099-12-31')
		AND d.ty_ly_lly_ind = 'TY'
	QUALIFY rn = 1
) WITH DATA PRIMARY INDEX (day_date, loyalty_id, acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(day_date, loyalty_id, acp_id) ON acp_lkup_ty;



-- ACP/LOYALTY LOOKUP - LY Only
--DROP TABLE acp_lkup_ly;
CREATE MULTISET VOLATILE TABLE acp_lkup_ly AS (
    SELECT DISTINCT
		d.day_date
		, d.ty_ly_lly_ind
		, rwd.loyalty_id
		, ac.acp_id
		, rwd.rewards_level
		, ROW_NUMBER() OVER (PARTITION BY rwd.acp_id, d.day_date ORDER BY rwd.start_day_date DESC) AS rn -- needed FOR WHEN reward LEVEL changes BUT old level rwd.end_day_date OVERLAPS WITH NEW level rwd.start_day_date BY >1 day
	FROM PRD_NAP_USR_VWS.LOYALTY_LEVEL_LIFECYCLE_FACT_VW rwd
	INNER JOIN DT_LKUP d 
		ON d.day_date BETWEEN rwd.start_day_date AND (rwd.end_day_date - INTERVAL '1' DAY)
		AND d.ty_ly_lly_ind = 'LY'
	INNER JOIN PRD_NAP_USR_VWS.ANALYTICAL_CUSTOMER ac
		ON ac.acp_loyalty_id = rwd.loyalty_id
	INNER JOIN prd_nap_usr_vws.loyalty_member_dim_vw as lmd
    	ON ac.acp_loyalty_id = lmd.loyalty_id
    	AND COALESCE(lmd.member_enroll_date, DATE '2099-12-31') <= d.period_end_day_date
    	AND COALESCE(lmd.member_close_date, DATE '2099-12-31') >= d.period_start_day_date
	WHERE ac.acp_id IS NOT NULL
		AND COALESCE(lmd.member_enroll_date, DATE '2099-12-31') <  COALESCE(lmd.member_close_date, DATE '2099-12-31')
		AND d.ty_ly_lly_ind = 'LY'
	QUALIFY rn = 1
) WITH DATA PRIMARY INDEX (day_date, loyalty_id, acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(day_date, loyalty_id, acp_id) ON acp_lkup_ly;





-------------------- Staging ---------------------
-- TRANSACTIONS - LOYALTY TY STAGED (~10 min for 1wk/1store)
--DROP TABLE tran_ty_stg;
CREATE MULTISET VOLATILE TABLE tran_ty_stg AS (
SELECT 
	DTL.ORDER_DATE
	, DTL.TRAN_DATE
	, DTL.ACP_ID
	, DTL.LINE_NET_USD_AMT
	, DTL.LINE_ITEM_QUANTITY
	, DTL.LINE_ITEM_ORDER_TYPE
	, DTL.LINE_ITEM_FULFILLMENT_TYPE
	, DTL.LINE_ITEM_SEQ_NUM
	, DATA_SOURCE_CODE
	, DTL.RINGING_STORE_NUM
	, DTL.INTENT_STORE_NUM
	, DTL.GLOBAL_TRAN_ID
	, DTL.SKU_NUM
	-- dates
	, B.DAY_DATE
	, B.WEEK_IDNT
	, B.WEEK_START_DAY_DATE
	, B.WEEK_END_DAY_DATE
	, B.TY_LY_LLY_IND
	-- Store
	, STR.STORE_NUM
FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_VW AS DTL
INNER JOIN STORE_LKUP AS STR
	ON (CASE WHEN DTL.LINE_ITEM_ORDER_TYPE LIKE 'CustInit%' AND DTL.LINE_ITEM_FULFILLMENT_TYPE = 'StorePickUp' AND DTL.DATA_SOURCE_CODE = 'COM'
		THEN DTL.RINGING_STORE_NUM ELSE DTL.INTENT_STORE_NUM END) = STR.STORE_NUM
INNER JOIN DT_LKUP B
	ON (CASE WHEN DTL.LINE_NET_USD_AMT > 0 THEN COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) ELSE DTL.TRAN_DATE END) = B.DAY_DATE
	AND B.ty_ly_lly_ind = 'TY'
WHERE DTL.ACP_ID IS NOT NULL
	AND B.ty_ly_lly_ind = 'TY'
) WITH DATA PRIMARY INDEX (GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON tran_ty_stg;



--DROP TABLE full_tran_ty_stg; 
CREATE MULTISET VOLATILE TABLE full_tran_ty_stg AS (
SELECT 
	DTL.ORDER_DATE
	, DTL.TRAN_DATE
	, DTL.ACP_ID
	, DTL.LINE_NET_USD_AMT
	, DTL.LINE_ITEM_QUANTITY
	, DTL.LINE_ITEM_ORDER_TYPE
	, DTL.LINE_ITEM_FULFILLMENT_TYPE
	, DTL.LINE_ITEM_SEQ_NUM
	, DATA_SOURCE_CODE
	, DTL.RINGING_STORE_NUM
	, DTL.INTENT_STORE_NUM
	, DTL.GLOBAL_TRAN_ID
	-- dates
	, DTL.WEEK_IDNT
	, DTL.WEEK_START_DAY_DATE
	, DTL.WEEK_END_DAY_DATE
	, DTL.TY_LY_LLY_IND
	-- Store
	, DTL.STORE_NUM
	-- Loyalty
	, acp.rewards_level AS nordy_level
	-- NTN
	, CASE WHEN ntn.NTN_INSTANCE = 1 THEN 1 ELSE 0 END AS ntn_ind
FROM tran_ty_stg AS DTL
LEFT JOIN acp_lkup_ty acp
	ON acp.acp_id = DTL.acp_id
	AND acp.day_date = DTL.day_date
	AND acp.ty_ly_lly_ind = DTL.ty_ly_lly_ind
LEFT JOIN PRD_NAP_USR_VWS.customer_ntn_fact ntn 
	ON ntn.ACP_ID = DTL.ACP_ID
	AND ntn.NTN_GLOBAL_TRAN_ID = DTL.GLOBAL_TRAN_ID
	AND ntn.SKU_NUM = DTL.SKU_NUM
	AND ntn.STORE_NUM = DTL.STORE_NUM
	AND COALESCE(ntn.ORDER_DATE, ntn.BUSINESS_DAY_DATE) = DTL.day_date
) WITH DATA PRIMARY INDEX (GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON full_tran_ty_stg;




-- TRANSACTIONS - LOYALTY LY STAGED 
--DROP TABLE tran_ly_stg;
CREATE MULTISET VOLATILE TABLE tran_ly_stg AS (
SELECT
	DTL.ORDER_DATE
	, DTL.TRAN_DATE
	, DTL.ACP_ID
	, DTL.LINE_NET_USD_AMT
	, DTL.LINE_ITEM_QUANTITY
	, DTL.LINE_ITEM_ORDER_TYPE
	, DTL.LINE_ITEM_FULFILLMENT_TYPE
	, DTL.LINE_ITEM_SEQ_NUM
	, DATA_SOURCE_CODE
	, DTL.RINGING_STORE_NUM
	, DTL.INTENT_STORE_NUM
	, DTL.GLOBAL_TRAN_ID
	, DTL.SKU_NUM
	-- dates
	, B.DAY_DATE
	, B.WEEK_IDNT
	, B.WEEK_START_DAY_DATE
	, B.WEEK_END_DAY_DATE
	, B.TY_LY_LLY_IND
	-- Store
	, STR.STORE_NUM
FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_VW AS DTL
INNER JOIN STORE_LKUP AS STR
	ON (CASE WHEN DTL.LINE_ITEM_ORDER_TYPE LIKE 'CustInit%' AND DTL.LINE_ITEM_FULFILLMENT_TYPE = 'StorePickUp' AND DTL.DATA_SOURCE_CODE = 'COM'
		THEN DTL.RINGING_STORE_NUM ELSE DTL.INTENT_STORE_NUM END) = STR.STORE_NUM
INNER JOIN DT_LKUP B
	ON (CASE WHEN DTL.LINE_NET_USD_AMT > 0 THEN COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) ELSE DTL.TRAN_DATE END) = B.DAY_DATE
	AND B.ty_ly_lly_ind = 'LY'
WHERE DTL.ACP_ID IS NOT NULL
	AND B.ty_ly_lly_ind = 'LY'
) WITH DATA PRIMARY INDEX (GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON tran_ly_stg;


--DROP TABLE full_tran_ly_stg;
CREATE MULTISET VOLATILE TABLE full_tran_ly_stg AS (
SELECT 
	DTL.ORDER_DATE
	, DTL.TRAN_DATE
	, DTL.ACP_ID
	, DTL.LINE_NET_USD_AMT
	, DTL.LINE_ITEM_QUANTITY
	, DTL.LINE_ITEM_ORDER_TYPE
	, DTL.LINE_ITEM_FULFILLMENT_TYPE
	, DTL.LINE_ITEM_SEQ_NUM
	, DATA_SOURCE_CODE
	, DTL.RINGING_STORE_NUM
	, DTL.INTENT_STORE_NUM
	, DTL.GLOBAL_TRAN_ID
	-- dates
	, DTL.WEEK_IDNT
	, DTL.WEEK_START_DAY_DATE
	, DTL.WEEK_END_DAY_DATE
	, DTL.TY_LY_LLY_IND
	-- Store
	, DTL.STORE_NUM
	-- Loyalty
	, acp.rewards_level AS nordy_level
	-- NTN
	, CASE WHEN ntn.NTN_INSTANCE = 1 THEN 1 ELSE 0 END AS ntn_ind
FROM tran_ly_stg AS DTL
LEFT JOIN acp_lkup_ly acp
	ON acp.acp_id = DTL.acp_id
	AND acp.day_date = DTL.day_date
	AND acp.ty_ly_lly_ind = DTL.ty_ly_lly_ind
LEFT JOIN PRD_NAP_USR_VWS.customer_ntn_fact ntn 
	ON ntn.ACP_ID = DTL.ACP_ID
	AND ntn.NTN_GLOBAL_TRAN_ID = DTL.GLOBAL_TRAN_ID
	AND ntn.SKU_NUM = DTL.SKU_NUM
	AND ntn.STORE_NUM = DTL.STORE_NUM
	AND COALESCE(ntn.ORDER_DATE, ntn.BUSINESS_DAY_DATE) = DTL.day_date
) WITH DATA PRIMARY INDEX (GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON full_tran_ly_stg;






-- TRANSACTIONS - LOYALTY AGG (~20s. for 1wk/1store TY)
--DROP TABLE tran;
CREATE MULTISET VOLATILE TABLE tran AS (
SELECT 
	WEEK_IDNT
	, WEEK_START_DAY_DATE
	, WEEK_END_DAY_DATE
	, TY_LY_LLY_IND
	, STORE_NUM
	, nordy_level
	, COUNT(DISTINCT DTL.ACP_ID) AS CUST_COUNT
	, COUNT(DISTINCT CASE WHEN ntn_ind = 1 THEN DTL.ACP_ID ELSE NULL END) AS NTN_CUST_COUNT
	, SUM(LINE_NET_USD_AMT) AS NET_SPEND
	, SUM(LINE_ITEM_QUANTITY) AS NET_UNITS
	, COUNT(DISTINCT GLOBAL_TRAN_ID) AS NET_TRAN_CT
	, CASE WHEN NET_TRAN_CT <> 0 THEN CAST(CAST(SUM(LINE_ITEM_QUANTITY) AS FLOAT)/CAST(COUNT(DISTINCT GLOBAL_TRAN_ID) AS FLOAT) AS DECIMAL(5,2)) ELSE NULL END AS NET_UPT
	, SUM(CASE WHEN LINE_NET_USD_AMT > 0 THEN LINE_ITEM_QUANTITY ELSE 0 END) AS GROSS_UNITS
	, COALESCE(COUNT(DISTINCT CASE WHEN LINE_NET_USD_AMT > 0 THEN GLOBAL_TRAN_ID ELSE NULL END), 0) AS GROSS_TRAN_CT
	, CASE WHEN GROSS_TRAN_CT <> 0 THEN CAST((CAST(GROSS_UNITS AS FLOAT)/CAST(GROSS_TRAN_CT AS FLOAT)) AS DECIMAL(5,2)) ELSE NULL END AS GROSS_UPT
	, SUM(CASE WHEN LINE_NET_USD_AMT > 0 THEN LINE_NET_USD_AMT ELSE 0 END) AS GROSS_SPEND
	, COUNT(DISTINCT CASE WHEN LINE_NET_USD_AMT > 0 THEN DTL.ACP_ID||STORE_NUM||COALESCE(ORDER_DATE,TRAN_DATE) END) AS TRIPS
FROM full_tran_ty_stg AS DTL
GROUP BY 1,2,3,4,5,6
UNION ALL 
SELECT 
	WEEK_IDNT
	, WEEK_START_DAY_DATE
	, WEEK_END_DAY_DATE
	, TY_LY_LLY_IND
	, STORE_NUM
	, nordy_level
	, COUNT(DISTINCT DTL.ACP_ID) AS CUST_COUNT
	, COUNT(DISTINCT CASE WHEN ntn_ind = 1 THEN DTL.ACP_ID ELSE NULL END) AS NTN_CUST_COUNT
	, SUM(LINE_NET_USD_AMT) AS NET_SPEND
	, SUM(LINE_ITEM_QUANTITY) AS NET_UNITS
	, COUNT(DISTINCT GLOBAL_TRAN_ID) AS NET_TRAN_CT
	, CASE WHEN NET_TRAN_CT <> 0 THEN CAST(CAST(SUM(LINE_ITEM_QUANTITY) AS FLOAT)/CAST(COUNT(DISTINCT GLOBAL_TRAN_ID) AS FLOAT) AS DECIMAL(5,2)) ELSE NULL END AS NET_UPT
	, SUM(CASE WHEN LINE_NET_USD_AMT > 0 THEN LINE_ITEM_QUANTITY ELSE 0 END) AS GROSS_UNITS
	, COALESCE(COUNT(DISTINCT CASE WHEN LINE_NET_USD_AMT > 0 THEN GLOBAL_TRAN_ID ELSE NULL END), 0) AS GROSS_TRAN_CT
	, CASE WHEN GROSS_TRAN_CT <> 0 THEN CAST((CAST(GROSS_UNITS AS FLOAT)/CAST(GROSS_TRAN_CT AS FLOAT)) AS DECIMAL(5,2)) ELSE NULL END AS GROSS_UPT
	, SUM(CASE WHEN LINE_NET_USD_AMT > 0 THEN LINE_NET_USD_AMT ELSE 0 END) AS GROSS_SPEND
	, COUNT(DISTINCT CASE WHEN LINE_NET_USD_AMT > 0 THEN DTL.ACP_ID||STORE_NUM||COALESCE(ORDER_DATE,TRAN_DATE) END) AS TRIPS
FROM full_tran_ly_stg AS DTL
GROUP BY 1,2,3,4,5,6
) WITH DATA PRIMARY INDEX (WEEK_IDNT,STORE_NUM, NORDY_LEVEL) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(WEEK_IDNT,STORE_NUM, NORDY_LEVEL) ON tran;

DROP TABLE full_tran_ty_stg;
DROP TABLE full_tran_ly_stg;




-------------------- Final Output ---------------------
--DROP TABLE tran_final;
CREATE MULTISET VOLATILE TABLE tran_final AS (
SELECT 
	tr.week_idnt
	, tr.store_num
	, st.store_name
	-- date dimensions
	, dt.ty_ly_lly_ind
	, dt.week_desc
	, dt.week_label
	, dt.fiscal_week_num
	, dt.week_start_day_date
	, dt.week_end_day_date
    , dt.week_num_of_fiscal_month
	, dt.month_idnt
	, dt.month_desc
	, dt.month_label
	, dt.month_abrv
	, dt.fiscal_month_num
	, dt.month_start_day_date
	, dt.month_start_week_idnt
	, dt.month_end_day_date
	, dt.month_end_week_idnt
	, dt.quarter_idnt
	, dt.fiscal_year_num
	-- store dimensions
	, st.store_type_code
	, st.store_type_desc
	, st.selling_store_ind
	, st.region_num
	, st.region_desc
	, st.business_unit_num
	, st.business_unit_desc
	, st.subgroup_num
	, st.subgroup_desc
	, st.store_dma_code
	, st.dma_desc
	, st.dma_shrt_desc
	, st.rack_district_num
	, st.rack_district_desc
	, st.channel_num
	, st.channel_desc
	, st.comp_status_code
	, st.comp_status_desc
	, st.cluster_climate
	, st.cluster_price
	, st.cluster_designer
	, st.cluster_presidents_cup
	-- customer dimensions
    , nordy_level
	-- metrics
	, tr.CUST_COUNT
	, tr.NTN_CUST_COUNT
	, tr.NET_SPEND
	, tr.NET_UNITS
	, tr.NET_TRAN_CT
	, tr.NET_UPT
	, tr.GROSS_UNITS
	, tr.GROSS_TRAN_CT
	, tr.GROSS_UPT
	, tr.GROSS_SPEND
	, tr.TRIPS
FROM tran tr	
LEFT JOIN wk_lkup dt
	ON tr.week_idnt = dt.week_idnt
LEFT JOIN store_lkup st
	ON st.store_num = tr.store_num
) WITH DATA PRIMARY INDEX (WEEK_IDNT,STORE_NUM, NORDY_LEVEL) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(WEEK_IDNT,STORE_NUM, NORDY_LEVEL) ON tran_final;


/*********************************************** END METRICS **************************************************/





/******************************************** START DELETE/INSERT *********************************************/

DELETE FROM {environment_schema}.STORE_CUST_COUNT{env_suffix}
WHERE fiscal_year_num <= (SELECT MAX(fiscal_year_num)-2 FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {end_date}) -- DELETES PREVIOUS LY YRS
	OR (fiscal_year_num = (SELECT MAX(fiscal_year_num)-1 FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {end_date}) -- DELETES PREVIOUS TY YRS
		AND ty_ly_lly_ind = 'TY')

	OR (fiscal_week_num BETWEEN -- DELETES TY from last complete start week up to last completed end week
		(SELECT COALESCE(MAX(fiscal_week_num), 1) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {start_date} 
			AND fiscal_year_num = (SELECT DISTINCT MAX(fiscal_year_num) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {end_date})
		) 
		AND (SELECT MAX(fiscal_week_num) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {end_date} 
			AND fiscal_year_num = (SELECT DISTINCT MAX(fiscal_year_num) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE week_end_day_date <= {end_date})
		)
	)
;


INSERT INTO {environment_schema}.STORE_CUST_COUNT{env_suffix}
SELECT * FROM tran_final;

COLLECT STATISTICS PRIMARY INDEX(WEEK_IDNT,STORE_NUM)
,COLUMN(nordy_level)
ON {environment_schema}.STORE_CUST_COUNT{env_suffix};

-- end
/******************************************** END DELETE/INSERT *********************************************/
