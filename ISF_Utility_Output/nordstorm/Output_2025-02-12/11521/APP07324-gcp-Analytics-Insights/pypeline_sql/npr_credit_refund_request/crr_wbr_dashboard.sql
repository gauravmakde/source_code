/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=crr_wbr_dashboard_11521_ACE_ENG;
     Task_Name=crr_wbr_dashboard;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: {ccr_t2_schema}.crr_wbr_dashboard
Team/Owner: Deboleena Ganguly (deboleena.ganguly@nordstrom.com)
Date Created/Modified: 02/09/2023

Note:
-- What is the the purpose of the table

-- What is the update cadence/lookback window

*/

/*
Temp table notes here if applicable
*/
CREATE MULTISET VOLATILE TABLE tmpinsults AS (
SELECT
    DISTINCT
    nefi_npr_id
    , insult_flag
FROM T2DL_DAS_CRR.crr_model_output as o
WHERE nefi_npr_id is not null
AND insult_flag = '1'
)
WITH DATA
PRIMARY INDEX(nefi_npr_id)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE tmpclaims AS (
SELECT
    DISTINCT
         p.nefi_npr_id
       , p.which_table
       , p.order_num
       , p.recommended_action
       , p.claim_date
       , p.npr_amount
       , CASE WHEN p.recommended_action = 'approve' THEN 'approved'
           WHEN p.recommended_action = 'deny' AND p.actual_action  = 'approve' THEN 'approved'
           WHEN p.recommended_action = 'deny' THEN 'denied'
           WHEN p.recommended_action NOT IN ('approve', 'deny') AND actual_action = 'approve' AND p.is_returned = 1 THEN 'approved'
           WHEN p.recommended_action NOT IN ('approve', 'deny') AND actual_action = 'deny' AND p.is_returned = 1 THEN 'denied'
           WHEN p.recommended_action NOT IN ('approve', 'deny') AND actual_action is null AND p.is_returned = 1 THEN 'approved' --these cases are claims that have passed SLA and missed being reviewed.
           ELSE NULL END AS status --all claims here are still waiting to be reviewed by the CFS team.
        , CASE
            WHEN oldf.SOURCE_CHANNEL_CODE = 'FULL_LINE' THEN 'NORDSTROM'
            WHEN oldf.SOURCE_CHANNEL_CODE = 'RACK' THEN 'NORDSTROM_RACK'
            ELSE 'UNKNOWN'
            END AS channel_brand
       FROM T2DL_DAS_CRR.crr_nefi_input as p
       LEFT JOIN tmpinsults as s
         ON p.nefi_npr_id = s.nefi_npr_id
       LEFT JOIN PRD_NAP_USR_VWS.RETAIL_TRAN_HDR_FACT rthf -- pick only SALE order
         ON p.order_num = CAST(RTHF.order_num AS INTEGER)
	   LEFT JOIN PRD_NAP_USR_VWS.ORDER_LINE_DETAIL_FACT oldf -- Join with OLDF for source channel code
         ON rthf.order_num = oldf.ORDER_NUM
	  WHERE p.is_returned = 1
		AND rthf.tran_type_code = '{tran_type_code_sale}'
		AND claim_date >= TRUNC(current_date()- 183, 'D') --
		AND p.which_table = '{which_table}'
)
WITH DATA
PRIMARY INDEX(nefi_npr_id)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE intermediate AS (
SELECT
	CAST(f.day_date as DATE) as cal_date
	, f.year_num as fiscal_year
	, f.quarter_454_num as quarter_num
	, f.month_desc  as fiscal_month
	, f.month_454_num as fiscal_month_nbr
	, f.week_of_fyr as fiscal_week
   , channel_brand
   , which_table
   , SUM(CASE WHEN status = 'denied' THEN 1 ELSE 0 END) AS denied_claims_cnt
   , SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved_claims_cnt
   , SUM(CASE WHEN status = 'denied' THEN npr_amount ELSE null END) AS denied_amt
   , SUM(CASE WHEN status = 'approved' THEN npr_amount ELSE null END) AS approved_amt
   , SUM(CASE WHEN insult_flag = 1 THEN npr_amount ELSE 0 END) as insult_amt
   , SUM(CASE WHEN status in ('approved', 'denied') THEN npr_amount ELSE null END) AS total_claim_amt
   , SUM(CASE WHEN status in ('approved', 'denied') THEN 1 ELSE 0 END) AS total_claims_cnt
   , SUM(CASE WHEN recommended_action not in ('approve', 'deny')  THEN 1 ELSE 0 END) AS reviewed_claims_cnt
   , SUM(CASE WHEN recommended_action not in ('approve', 'deny') THEN npr_amount ELSE null END) AS reviewed_claims_amt
   , SUM(CASE WHEN insult_flag = 1 THEN 1 ELSE 0 END) AS insults_cnt
   FROM tmpclaims AS o
		LEFT JOIN tmpinsults AS i
		ON o.nefi_npr_id = i.nefi_npr_id
		LEFT JOIN prd_nap_usr_vws.day_cal AS f
		ON CAST(o.claim_date AS DATE) = f.day_date
		GROUP BY
		CAST(f.day_date as DATE)
		, f.year_num
        , f.quarter_454_num
        , f.month_desc
        , f.month_454_num
        , f.week_of_fyr
        ,channel_brand
       , which_table
)
WITH DATA
PRIMARY INDEX(cal_date,channel_brand)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE pre_insert AS (
    SELECT cal_date
       , fiscal_week
       , fiscal_month
       , fiscal_year
       , denied_claims_cnt AS denied_claims
       , approved_claims_cnt AS approved_claims
       , denied_amt
       , approved_amt
       , insults_cnt AS insults
       , total_claim_amt AS total_claim_amt
       , total_claims_cnt AS total_claims
       , reviewed_claims_cnt AS reviewed_claims_count
       , reviewed_claims_amt AS reviewed_claims_amount
       , insult_amt AS insult_dollars --
       , denied_amt - 0 AS net_savings --
       , CASE WHEN channel_brand = 'NORDSTROM_RACK' THEN 'NORDSTROM_RACK' ELSE 'NORDSTROM' END AS channel_brand
       , which_table
       , denied_claims_cnt
       , approved_claims_cnt
       , reviewed_claims_cnt
   FROM intermediate
       GROUP BY cal_date
       , fiscal_week
       , fiscal_month
       , fiscal_year
       , denied_claims
       , approved_claims
       , denied_amt
       , approved_amt
       , insults
       , total_claim_amt
       , total_claims
       , reviewed_claims_count
       , reviewed_claims_amount
       , insult_dollars --
       , net_savings --
       , channel_brand
       , which_table
)
WITH DATA
PRIMARY INDEX(cal_date,channel_brand)
ON COMMIT PRESERVE ROWS
;

/*
--------------------------------------------
DELETE any overlapping records from destination
table prior to INSERT of new data
--------------------------------------------
*/
DELETE
FROM    {ccr_t2_schema}.crr_wbr_dashboard
WHERE cal_date >= TRUNC(current_date()-183, 'D')
;

INSERT INTO {ccr_t2_schema}.crr_wbr_dashboard
SELECT  cal_date
       , fiscal_week
       , fiscal_month
       , fiscal_year
       , denied_claims
       , approved_claims
       , denied_amt
       , approved_amt
       , insults
       , total_claim_amt
       , total_claims
       , reviewed_claims_count
       , reviewed_claims_amount
       , insult_dollars --
       , net_savings --
       , channel_brand
       , which_table
       , denied_claims_cnt
       , approved_claims_cnt
       , reviewed_claims_cnt
       , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM	pre_insert
;

-- columns that will be queried in future
COLLECT STATISTICS
                    COLUMN (cal_date), -- column names used for primary index
                    COLUMN (fiscal_week),
                    COLUMN (fiscal_month),
                    COLUMN (fiscal_year),
                    COLUMN (channel_brand),
                    COLUMN (which_table)
on {ccr_t2_schema}.crr_wbr_dashboard;


/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
