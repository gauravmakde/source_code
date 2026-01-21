/*------------------------------------------------------
 NPG Scorecard
 Purpose: Provide customers, loyalty customers, items broken down by npg_ind as a scorecard that renders each month to NPG team
 
 Last Update: 11/06/23 Rasagnya Avala - Initial file
 Contact for logic/code: Rasagnya Avala (Merch Analytics)
 Contact for ETL: Rasagnya Avala (Merch Analytics)
 --------------------------------------------------------*/

-- Date Lookup
CREATE MULTISET VOLATILE TABLE date_lookup AS (
SELECT
     min(day_date) as ty_start_dt
    , max(day_date) as ty_end_dt
    , min(day_date_last_year_realigned) as ly_start_dt
    , max(day_date_last_year_realigned) as ly_end_dt 
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE month_idnt = (SELECT DISTINCT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = CURRENT_DATE()-10)
) WITH DATA ON COMMIT PRESERVE ROWS;

-- Transactions TY

CREATE MULTISET VOLATILE TABLE tran_base_ty AS (  
SELECT
       'TY' AS ty_ly_ind
      , CASE WHEN str.business_unit_desc IN ('FULL LINE','FULL LINE CANADA') THEN 'FLS'
        WHEN str.business_unit_desc IN ('N.CA','N.COM','TRUNK CLUB') THEN 'NCOM'
        WHEN str.business_unit_desc IN ('RACK', 'RACK CANADA') THEN 'RACK'
        WHEN str.business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NRHL'
        END AS channel
      , tran.acp_id
      , tran.sku_num 
      , tran.trip_id  
      , tran.store_num
      , CASE WHEN ntn.acp_id IS NOT NULL THEN 1 ELSE 0 END AS ntn_flg
      , min(sale_date) as sale_date
      , sum(CASE WHEN gc_flag=0 THEN net_amt ELSE 0 END) AS net_spend
      , sum(CASE WHEN gc_flag=0 THEN gross_amt else 0 END) AS gross_spend
      , sum(gross_items) AS gross_items
      , sum(net_items) AS net_items
FROM (
      SELECT -- TODO: , ON the LEFT 
        dtl.acp_id,
        dtl.sku_num,
        dtl.global_tran_id,
        dtl.business_unit_desc,
        dtl.shipped_usd_sales - dtl.return_usd_amt AS net_amt,
        CASE WHEN dtl.shipped_usd_sales>0 THEN dtl.shipped_usd_sales END AS gross_amt, -- TODO: CHANGE aliases TO MATCH WITH reporting needs
        -- remove giftcard purchase in net spend
        CASE WHEN dtl.nonmerch_fee_code = '6666' THEN 1 ELSE 0 END AS gc_flag,
          -- change bopus intent_store_num to ringing_store_num 808 or 867
        CASE WHEN dtl.line_item_order_type = 'CustInitWebOrder'
               AND dtl.line_item_fulfillment_type = 'StorePickUp'
               AND dtl.business_unit_desc = 'FULL LINE'
               THEN 808
               WHEN dtl.line_item_order_type = 'CustInitWebOrder'
               AND dtl.line_item_fulfillment_type = 'StorePickUp'
               AND dtl.business_unit_desc = 'FULL LINE CANADA'
               THEN 867
               ELSE dtl.intent_store_num END AS store_num,
        -- use order date is there is one
        COALESCE(dtl.order_date,dtl.tran_date) AS sale_date,
        -- define trip based on store_num + date
        CASE WHEN dtl.shipped_usd_sales>0 THEN CAST(dtl.acp_id||'_'||store_num||'_'||'_'||sale_date AS varchar(150)) END AS trip_id,
        CASE WHEN dtl.shipped_usd_sales>0 THEN dtl.shipped_qty END AS gross_items,
        CASE WHEN dtl.shipped_usd_sales <  0 THEN dtl.shipped_qty *(-1)
        	 WHEN dtl.shipped_usd_sales = 0 THEN 0
        	 ELSE dtl.shipped_qty END AS net_items
FROM T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT AS dtl 
-- TODO: validate with merch prod sales AND RETURNS
WHERE COALESCE(dtl.order_date,dtl.tran_date) >= (SELECT ty_start_dt FROM date_lookup) --TODO: change to between
      AND COALESCE(dtl.order_date,dtl.tran_date) <= (SELECT ty_end_dt FROM date_lookup)
      AND NOT dtl.acp_id IS NULL AND net_items > 0 AND transaction_type = 'retail'
      AND dtl.business_day_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup)
    ) AS tran
  INNER JOIN prd_nap_usr_vws.store_dim AS str
    ON tran.store_num = str.store_num
    AND str.business_unit_desc IN (
         'FULL LINE',
         'FULL LINE CANADA',
         'N.CA',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK',
         'RACK CANADA',
         'TRUNK CLUB')
  LEFT JOIN prd_nap_usr_vws.customer_ntn_fact AS ntn
        --left join prd_nap_usr_vws.customer_ntn_status_fact as ntn
    ON tran.global_tran_id = ntn.ntn_global_tran_id --ntn.aare_global_tran_id
    AND tran.sku_num = ntn.sku_num  --commented out to test the validity of a new NTN TABLE
    WHERE str.store_country_code = 'US' 
GROUP BY 1,2,3,4,5,6,7
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS; -- USE sku_num AND sale_date
 
COLLECT STATISTICS COLUMN(acp_id) ON tran_base_ty;

-- Transactions LY

CREATE MULTISET VOLATILE TABLE tran_base_ly AS (  
SELECT
       'LY' AS ty_ly_ind
      , CASE WHEN str.business_unit_desc IN ('FULL LINE','FULL LINE CANADA') THEN 'FLS'
        WHEN str.business_unit_desc IN ('N.CA','N.COM','TRUNK CLUB') THEN 'NCOM'
        WHEN str.business_unit_desc IN ('RACK', 'RACK CANADA') THEN 'RACK'
        WHEN str.business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NRHL'
        END AS channel
      , tran.acp_id
      , tran.sku_num 
      , tran.trip_id  
      , tran.store_num
      , CASE WHEN ntn.acp_id IS NOT NULL THEN 1 ELSE 0 END AS ntn_flg
      , min(sale_date) as sale_date
      , sum(CASE WHEN gc_flag=0 THEN net_amt ELSE 0 END) AS net_spend
      , sum(CASE WHEN gc_flag=0 THEN gross_amt else 0 END) AS gross_spend
      , sum(gross_items) AS gross_items
      , sum(net_items) AS net_items
FROM (
      SELECT -- TODO: , ON the LEFT 
        dtl.acp_id,
        dtl.sku_num,
        dtl.global_tran_id,
        dtl.business_unit_desc,
        dtl.shipped_usd_sales - dtl.return_usd_amt AS net_amt,
        CASE WHEN dtl.shipped_usd_sales>0 THEN dtl.shipped_usd_sales END AS gross_amt, -- TODO: CHANGE aliases TO MATCH WITH reporting needs
        -- remove giftcard purchase in net spend
        CASE WHEN dtl.nonmerch_fee_code = '6666' THEN 1 ELSE 0 END AS gc_flag,
          -- change bopus intent_store_num to ringing_store_num 808 or 867
        CASE WHEN dtl.line_item_order_type = 'CustInitWebOrder'
               AND dtl.line_item_fulfillment_type = 'StorePickUp'
               AND dtl.business_unit_desc = 'FULL LINE'
               THEN 808
               WHEN dtl.line_item_order_type = 'CustInitWebOrder'
               AND dtl.line_item_fulfillment_type = 'StorePickUp'
               AND dtl.business_unit_desc = 'FULL LINE CANADA'
               THEN 867
               ELSE dtl.intent_store_num END AS store_num,
        -- use order date is there is one
        COALESCE(dtl.order_date,dtl.tran_date) AS sale_date,
        -- define trip based on store_num + date
        CASE WHEN dtl.shipped_usd_sales>0 THEN CAST(dtl.acp_id||'_'||store_num||'_'||'_'||sale_date AS varchar(150)) END AS trip_id,
        CASE WHEN dtl.shipped_usd_sales>0 THEN dtl.shipped_qty END AS gross_items,
        CASE WHEN dtl.shipped_usd_sales <  0 THEN dtl.shipped_qty *(-1)
        	 WHEN dtl.shipped_usd_sales = 0 THEN 0
        	 ELSE dtl.shipped_qty END AS net_items
FROM T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT AS dtl 
-- TODO: validate with merch prod sales AND RETURNS
WHERE COALESCE(dtl.order_date,dtl.tran_date) >= (SELECT ly_start_dt FROM date_lookup) --TODO: change to between
      AND COALESCE(dtl.order_date,dtl.tran_date) <= (SELECT ly_end_dt FROM date_lookup)
      AND NOT dtl.acp_id IS NULL AND net_items > 0 AND transaction_type = 'retail'
      AND dtl.business_day_date BETWEEN (SELECT ly_start_dt FROM date_lookup) AND (SELECT ly_end_dt FROM date_lookup)
    ) AS tran
  INNER JOIN prd_nap_usr_vws.store_dim AS str
    ON tran.store_num = str.store_num
    AND str.business_unit_desc IN (
         'FULL LINE',
         'FULL LINE CANADA',
         'N.CA',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK',
         'RACK CANADA',
         'TRUNK CLUB')
  LEFT JOIN prd_nap_usr_vws.customer_ntn_fact AS ntn
        --left join prd_nap_usr_vws.customer_ntn_status_fact as ntn
    ON tran.global_tran_id = ntn.ntn_global_tran_id --ntn.aare_global_tran_id
    AND tran.sku_num = ntn.sku_num  --commented out to test the validity of a new NTN TABLE
    WHERE str.store_country_code = 'US' 
GROUP BY 1,2,3,4,5,6,7
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS; -- USE sku_num AND sale_date
 
COLLECT STATISTICS COLUMN(acp_id) ON tran_base_ly;

-- Loyalty Status

 CREATE MULTISET VOLATILE TABLE cm_lyl_status_ty AS (
 SELECT 'TY' AS ty_ly_ind,
   	acp_id,
    CASE WHEN cardmember_fl = 1 THEN 'cardmember'
   		 WHEN member_fl = 1 THEN 'member'
    	 ELSE 'non-member' END AS loyalty_status,
 	CASE WHEN rewards_level=1 THEN '1-member'
  		 WHEN rewards_level=2 THEN '2-insider'
  		 WHEN rewards_level=3 THEN '3-influencer'
  		 WHEN rewards_level=4 THEN '4-ambassador'
   		 WHEN rewards_level=5 THEN '5-icon'
       	 WHEN rewards_level=0 THEN 
       	 	(CASE WHEN loyalty_status='cardmember' THEN '1-member'
                  WHEN loyalty_status='member' THEN '1-member'
                  WHEN loyalty_status='non-member' THEN '0-nonmember' END)
   		 	ELSE '0-nonmember' END AS nordy_level
 FROM (
 SELECT DISTINCT ac.acp_id,
     -- member status
     CASE WHEN cardmember_enroll_date <= (SELECT ty_end_dt FROM date_lookup)
          AND (cardmember_close_date >= (SELECT ty_end_dt FROM date_lookup)
          OR cardmember_close_date IS NULL)
          THEN 1 ELSE 0 END AS cardmember_fl,
     CASE WHEN cardmember_fl = 0
          AND member_enroll_date <= (SELECT ty_end_dt FROM date_lookup)
          AND (member_close_date >= (SELECT ty_end_dt FROM date_lookup)
          OR member_close_date IS NULL)
          THEN 1 ELSE 0 END AS member_fl,
     -- nordy_level
	max(CASE WHEN rwd.rewards_level IN ('MEMBER') THEN 1
             WHEN rwd.rewards_level IN ('INSIDER','L1') THEN 2
             WHEN rwd.rewards_level IN ('INFLUENCER','L2') THEN 3
             WHEN rwd.rewards_level IN ('AMBASSADOR','L3') THEN 4
             WHEN rwd.rewards_level IN ('ICON','L4') THEN 5
             ELSE 0 END) AS rewards_level
FROM prd_nap_usr_vws.analytical_customer AS ac
   INNER JOIN prd_nap_usr_vws.loyalty_member_dim_vw AS lmd
     ON ac.acp_loyalty_id = lmd.loyalty_id
   LEFT JOIN prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
   -- date piece in join for nordy
    ON ac.acp_loyalty_id = rwd.loyalty_id 
    	AND rwd.start_day_date <= (SELECT ty_end_dt FROM date_lookup)
		AND rwd.end_day_date > (SELECT ty_end_dt FROM date_lookup)
	GROUP BY 1,2,3
    ) AS lyl_ty 
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON cm_lyl_status_ty;

-- Loyalty LY

CREATE MULTISET VOLATILE TABLE cm_lyl_status_ly AS (
 SELECT 'TY' AS ty_ly_ind,
   	acp_id,
    CASE WHEN cardmember_fl = 1 THEN 'cardmember'
   		 WHEN member_fl = 1 THEN 'member'
    	 ELSE 'non-member' END AS loyalty_status,
 	CASE WHEN rewards_level=1 THEN '1-member'
  		 WHEN rewards_level=2 THEN '2-insider'
  		 WHEN rewards_level=3 THEN '3-influencer'
  		 WHEN rewards_level=4 THEN '4-ambassador'
   		 WHEN rewards_level=5 THEN '5-icon'
       	 WHEN rewards_level=0 THEN 
       	 	(CASE WHEN loyalty_status='cardmember' THEN '1-member'
                  WHEN loyalty_status='member' THEN '1-member'
                  WHEN loyalty_status='non-member' THEN '0-nonmember' END)
   		 	ELSE '0-nonmember' END AS nordy_level
 FROM (
 SELECT DISTINCT ac.acp_id,
     -- member status
     CASE WHEN cardmember_enroll_date <= (SELECT ly_end_dt FROM date_lookup)
          AND (cardmember_close_date >= (SELECT ly_end_dt FROM date_lookup)
          OR cardmember_close_date IS NULL)
          THEN 1 ELSE 0 END AS cardmember_fl,
     CASE WHEN cardmember_fl = 0
          AND member_enroll_date <= (SELECT ly_end_dt FROM date_lookup)
          AND (member_close_date >= (SELECT ly_end_dt FROM date_lookup)
          OR member_close_date IS NULL)
          THEN 1 ELSE 0 END AS member_fl,
     -- nordy_level
	max(CASE WHEN rwd.rewards_level IN ('MEMBER') THEN 1
             WHEN rwd.rewards_level IN ('INSIDER','L1') THEN 2
             WHEN rwd.rewards_level IN ('INFLUENCER','L2') THEN 3
             WHEN rwd.rewards_level IN ('AMBASSADOR','L3') THEN 4
             WHEN rwd.rewards_level IN ('ICON','L4') THEN 5
             ELSE 0 END) AS rewards_level
FROM prd_nap_usr_vws.analytical_customer AS ac
   INNER JOIN prd_nap_usr_vws.loyalty_member_dim_vw AS lmd
     ON ac.acp_loyalty_id = lmd.loyalty_id
   LEFT JOIN prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
   -- date piece in join for nordy
    ON ac.acp_loyalty_id = rwd.loyalty_id 
    	AND rwd.start_day_date <= (SELECT ly_end_dt FROM date_lookup)
		AND rwd.end_day_date > (SELECT ly_end_dt FROM date_lookup)
	GROUP BY 1,2,3
    ) AS lyl_ty 
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON cm_lyl_status_ly;


-- full data TY

CREATE MULTISET VOLATILE TABLE full_data_ty AS (
SELECT 	e.fiscal_year_num AS year_idnt,
        e.fiscal_halfyear_num  AS half_idnt,
        e.quarter_idnt AS quarter_idnt,
        e.month_idnt AS month_idnt,
        e.week_idnt AS week_idnt,
        TRIM(e.fiscal_year_num) || ' H' || RIGHT(CAST(e.fiscal_halfyear_num AS CHAR(5)),1) AS half_label,
	    e.quarter_label,
	    TRIM(e.fiscal_year_num) || ' ' || TRIM(e.fiscal_month_num) || ' ' || TRIM(e.month_abrv) AS month_label,  
        TRIM(e.fiscal_year_num) || ', ' || TRIM(e.fiscal_month_num) || ', Wk ' || TRIM(e.week_num_of_fiscal_month) AS week_label,
	    a.channel,
		a.ty_ly_ind,
		a.acp_id,
		a.ntn_flg,
		a.trip_id,
		cASE WHEN c.loyalty_status IS NULL THEN 'non-member' ELSE c.loyalty_status END AS loyalty_status,
		d.npg_ind,
		a.net_spend AS net_spend,
		a.gross_spend AS gross_spend,
		a.net_items AS net_items,
		a.gross_items AS gross_items,
		ROW_NUMBER() OVER (PARTITION BY npg_ind,a.ty_ly_ind,d.web_style_num,rms_sku_num,a.trip_id,a.acp_id,loyalty_status,a.channel order by a.acp_id) as rn
FROM tran_base_ty AS a
	LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw AS d
		ON a.sku_num = d.rms_sku_num
	LEFT JOIN cm_lyl_status_ty c
	    ON c.acp_id =  a.acp_id
	INNER JOIN prd_nap_usr_vws.day_cal_454_dim AS e
		on a.sale_date = e.day_date  
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data_ty;

-- full data LY

CREATE MULTISET VOLATILE TABLE full_data_ly AS (
SELECT 	e.fiscal_year_num AS year_idnt,
        e.fiscal_halfyear_num  AS half_idnt,
        e.quarter_idnt AS quarter_idnt,
        e.month_idnt AS month_idnt,
        e.week_idnt AS week_idnt,
        TRIM(e.fiscal_year_num) || ' H' || RIGHT(CAST(e.fiscal_halfyear_num AS CHAR(5)),1) AS half_label,
	    e.quarter_label,
	    TRIM(e.fiscal_year_num) || ' ' || TRIM(e.fiscal_month_num) || ' ' || TRIM(e.month_abrv) AS month_label,  
        TRIM(e.fiscal_year_num) || ', ' || TRIM(e.fiscal_month_num) || ', Wk ' || TRIM(e.week_num_of_fiscal_month) AS week_label,
	    a.channel,
		a.ty_ly_ind,
		a.acp_id,
		a.ntn_flg,
		a.trip_id,
		cASE WHEN c.loyalty_status IS NULL THEN 'non-member' ELSE c.loyalty_status END AS loyalty_status,
		d.npg_ind,
		a.net_spend AS net_spend,
		a.gross_spend AS gross_spend,
		a.net_items AS net_items,
		a.gross_items AS gross_items,
		ROW_NUMBER() OVER (PARTITION BY npg_ind,a.ty_ly_ind,d.web_style_num,rms_sku_num,a.trip_id,a.acp_id,loyalty_status,a.channel order by a.acp_id) as rn
FROM tran_base_ly AS a
	LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw AS d
		ON a.sku_num = d.rms_sku_num
	LEFT JOIN cm_lyl_status_ly c
	    ON c.acp_id =  a.acp_id
	INNER JOIN prd_nap_usr_vws.day_cal_454_dim AS e
		on a.sale_date = e.day_date_last_year_realigned  
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data_ly;

-- full data union 
CREATE MULTISET VOLATILE TABLE full_data AS
(SELECT * FROM full_data_ty WHERE rn = 1
UNION ALL 
SELECT * FROM full_data_ly WHERE rn = 1
)WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data;

-- prefinal
CREATE MULTISET VOLATILE TABLE prefinal AS (
SELECT  year_idnt,
        half_idnt,
		quarter_idnt,
		month_idnt,
		half_label,
		quarter_label,
		month_label,
		CASE WHEN f.channel IN ('FLS','NCOM') THEN 'Nordstrom' ELSE 'Rack' END AS banner,
		f.channel,
		f.ntn_flg,
		f.ty_ly_ind,
		loyalty_status,
		f.trip_id,
		f.acp_id,
		npg_ind,
		sum(f.net_spend) AS net_spend,
		sum(f.gross_spend) AS gross_spend,
		sum(f.net_items) AS net_items,
		sum(f.gross_items) AS gross_items
FROM full_data AS f 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON prefinal;

CREATE MULTISET VOLATILE TABLE npg_scorecard_data AS 
(SELECT 'LOYALTY STATUS' AS product_attribute
,loyalty_status AS dimension
,ty_ly_ind 
,npg_ind
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_spend
,sum(net_spend) AS net_spend 
FROM prefinal
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
UNION ALL 
SELECT 'Total' AS product_attribute 
,'TOTAL' AS dimension
,ty_ly_ind 
,npg_ind
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_spend
,sum(net_spend) AS net_spend 
FROM prefinal 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11)
WITH DATA PRIMARY INDEX(month_idnt, dimension) ON COMMIT PRESERVE ROWS;


DELETE FROM {environment_schema}.npg_scorecard where month_idnt = (SELECT DISTINCT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = CURRENT_DATE()-10);
INSERT INTO {environment_schema}.npg_scorecard
SELECT * FROM npg_scorecard_data;

COLLECT STATISTICS COLUMN(month_idnt, dimension) ON {environment_schema}.npg_scorecard;

