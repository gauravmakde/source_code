/*
MFP Cp Cost
Author: Michelle Du
Owner: Jihyun Yu
Date Created: 10/18/22
Date Last Updated: 2/17/23

Purpose: Create MFP Cp cost at channel and banner level tables.

Tables:
    {environment_schema}.mfp_cp_cost_channel_weekly{env_suffix}
    {environment_schema}.mfp_cp_cost_banner_weekly{env_suffix}
*/

CREATE MULTISET VOLATILE TABLE dates_lkup_mfp AS (
	SELECT DISTINCT
		week_idnt
		, week_start_day_date
		, week_end_day_date
		, month_idnt
		, month_start_day_date
		, month_end_day_date
		, week_num_of_fiscal_month
		, MAX(week_num_of_fiscal_month) OVER (PARTITION BY month_idnt) AS weeks_in_month
		, week_label
		, month_label
		, quarter_label
		, fiscal_year_num
	FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
	WHERE week_end_day_date BETWEEN (current_date - 365*2) AND (current_date + 365*2)
		AND fiscal_year_num <> 2020
) WITH DATA
PRIMARY INDEX(month_idnt, week_idnt) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE dept_lkup_ccp AS (
	SELECT DISTINCT
		dept_num
		, division_num
		, subdivision_num
		, TRIM (dept_num || ', ' ||dept_name) AS dept_label
		, TRIM (division_num || ', ' ||division_name) AS division_label
		, TRIM (subdivision_num || ', ' ||subdivision_name) AS subdivision_label
	FROM prd_nap_usr_vws.department_dim
) WITH DATA
PRIMARY INDEX (dept_num) ON COMMIT PRESERVE ROWS;

DELETE FROM {environment_schema}.mfp_cp_cost_channel_weekly{env_suffix} ALL;
INSERT INTO {environment_schema}.mfp_cp_cost_channel_weekly{env_suffix}
SELECT
	dc.month_idnt
	, dc.month_start_day_date
    , dc.month_end_day_date
	, mfpc.week_num AS week_idnt
    , dc.week_start_day_date
    , dc.week_end_day_date
    , quarter_label
	, fiscal_year_num
	, fulfill_type_num
	, CASE
		WHEN channel_num IN ('110','120','210','220','250','260','310') THEN 'US'
		WHEN channel_num IN ('111','121','211','221','261','311') THEN 'CA'
		END AS channel_country
	, CASE
		WHEN channel_country = 'US' THEN 'USD'
		WHEN channel_country = 'CA' THEN 'CAD'
		END AS currency
	, channel_num AS chnl_idnt
	, mfpc.dept_num AS dept_idnt
	, dept_label
	, division_label
	, subdivision_label
	, SUM(CP_DEMAND_TOTAL_RETAIL_AMT) AS cp_demand_r_dollars
	, SUM(CP_DEMAND_TOTAL_QTY) AS cp_demand_u
	, SUM(CP_GROSS_SALES_RETAIL_AMT) AS cp_gross_sls_r_dollars
	, SUM(CP_GROSS_SALES_QTY) AS cp_gross_sls_u
	, SUM(CP_RETURNS_RETAIL_AMT) AS cp_return_r_dollars
	, SUM(CP_RETURNS_QTY) AS cp_return_u
	, SUM(CP_NET_SALES_RETAIL_AMT) AS cp_net_sls_r_dollars
	, SUM(CP_NET_SALES_COST_AMT) AS cp_net_sls_c_dollars
	, SUM(CP_NET_SALES_QTY) AS cp_net_sls_u
FROM PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_CHANNEL_FACT mfpc
	JOIN dates_lkup_mfp dc
		ON mfpc.week_num = dc.week_idnt
	LEFT JOIN dept_lkup_ccp dp
		ON mfpc.DEPT_NUM  = dp.dept_num
WHERE (FULFILL_TYPE_NUM = 1 OR FULFILL_TYPE_NUM = 3)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;

COLLECT STATS
    PRIMARY INDEX (week_idnt, week_start_day_date, month_idnt, chnl_idnt, channel_country, dept_idnt)
    ON {environment_schema}.mfp_cp_cost_channel_weekly{env_suffix};

DELETE FROM {environment_schema}.mfp_cp_cost_banner_weekly{env_suffix} ALL;
INSERT INTO {environment_schema}.mfp_cp_cost_banner_weekly{env_suffix}
SELECT
	dc.month_idnt
	, dc.month_start_day_date
    , dc.month_end_day_date
	, mfpc.week_num AS week_idnt
	, dc.week_start_day_date
	, dc.week_end_day_date
    , quarter_label
    , fiscal_year_num
    , fulfill_type_num
    , CASE
        WHEN (BANNER_COUNTRY_NUM = 1 OR BANNER_COUNTRY_NUM  = 3) THEN 'US'
		WHEN (BANNER_COUNTRY_NUM  = 2 OR BANNER_COUNTRY_NUM  = 4) THEN 'CA'
		END AS channel_country
	, CASE
	    WHEN BANNER_COUNTRY_NUM = 1 OR BANNER_COUNTRY_NUM  = 2 THEN 'FP'
		WHEN BANNER_COUNTRY_NUM  = 3 OR BANNER_COUNTRY_NUM  = 4 THEN 'OP'
		END AS banner
	, mfpc.dept_num AS dept_idnt
	, dept_label
	, division_label
	, subdivision_label
	, CASE
	    WHEN (BANNER_COUNTRY_NUM = 1 OR BANNER_COUNTRY_NUM  = 3) THEN 'USD'
		WHEN (BANNER_COUNTRY_NUM  = 2 OR BANNER_COUNTRY_NUM  = 4) THEN 'CAD'
		END AS currency
	, SUM(CP_BEGINNING_OF_PERIOD_ACTIVE_COST_AMT) AS cp_bop_ttl_c_dollars
	, SUM(CP_BEGINNING_OF_PERIOD_ACTIVE_QTY) AS cp_bop_ttl_u
	, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN CP_BEGINNING_OF_PERIOD_ACTIVE_COST_AMT END ) AS cp_beginofmonth_bop_c
	, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN CP_BEGINNING_OF_PERIOD_ACTIVE_QTY END ) AS cp_beginofmonth_bop_u
	, SUM(CP_ENDING_OF_PERIOD_ACTIVE_COST_AMT) AS cp_eop_ttl_c_dollars
	, SUM(CP_ENDING_OF_PERIOD_ACTIVE_QTY) AS cp_eop_ttl_u
	, SUM(CASE WHEN dc.week_num_of_fiscal_month = dc.weeks_in_month THEN CP_ENDING_OF_PERIOD_ACTIVE_COST_AMT END ) AS cp_eop_eom_c_dollars
	, SUM(CASE WHEN dc.week_num_of_fiscal_month = dc.weeks_in_month THEN CP_ENDING_OF_PERIOD_ACTIVE_QTY END ) AS cp_eop_eom_u
	, SUM(CP_RECEIPTS_ACTIVE_COST_AMT ) AS cp_rept_need_c_dollars
	, SUM(CP_RECEIPTS_ACTIVE_QTY ) AS cp_rept_need_u
	, SUM(CP_RECEIPTS_ACTIVE_COST_AMT - CP_RECEIPTS_RESERVE_COST_AMT) AS cp_rept_need_lr_c_dollars
	, SUM(CP_RECEIPTS_ACTIVE_QTY - CP_RECEIPTS_RESERVE_QTY) AS cp_rept_need_lr_u
FROM PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_BANNER_COUNTRY_FACT mfpc
	JOIN dates_lkup_mfp dc
		ON mfpc.week_num = dc.week_idnt
	LEFT JOIN dept_lkup_ccp dp
    	ON mfpc.DEPT_NUM  = dp.dept_num
WHERE (FULFILL_TYPE_NUM = 1 OR FULFILL_TYPE_NUM = 3)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;

COLLECT STATS
    PRIMARY INDEX (week_idnt, week_start_day_date, month_idnt, banner, channel_country, dept_idnt)
	, COLUMN (week_idnt, fulfill_type_num, channel_country, dept_idnt)
    	ON {environment_schema}.mfp_cp_cost_banner_weekly{env_suffix};
