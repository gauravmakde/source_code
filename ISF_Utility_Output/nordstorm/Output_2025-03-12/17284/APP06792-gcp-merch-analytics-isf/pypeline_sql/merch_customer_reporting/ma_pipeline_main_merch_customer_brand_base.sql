/*------------------------------------------------------
 Merch Customer Reporting
 Purpose: Provide customer trends by product hierarchy at a monthly grain
 
 Last Update: 11/29/23 Rasagnya Avala - ETL Pipeline creation
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
    , min(lly_day_date) AS lly_start_dt
    , max(lly_day_date) AS lly_end_dt
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

-- anniv skus
CREATE MULTISET VOLATILE TABLE anniv_skus AS 
(SELECT DISTINCT  
	rms_sku_num AS sku_idnt
	, 'Anniversary' AS event_type
	, EXTRACT(YEAR FROM enticement_start_tmstp) AS event_year
FROM PRD_NAP_USR_VWS.PRODUCT_PROMOTION_TIMELINE_DIM pptd 
WHERE channel_country = 'US'
    AND enticement_tags LIKE '%ANNIVERSARY_SALE%' 
    AND EXTRACT(YEAR FROM enticement_start_tmstp) <= EXTRACT(YEAR FROM CURRENT_DATE) 
    AND EXTRACT(YEAR FROM enticement_start_tmstp) >= EXTRACT(YEAR FROM CURRENT_DATE) - 1)
    WITH DATA PRIMARY INDEX(sku_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(sku_idnt) ON anniv_skus;

-- cyber skus 2023

CREATE VOLATILE MULTISET TABLE cyber_skus_2023 as  
(SELECT
      rnk.sku_idnt
      , banner
      , 'US' AS country
      , CASE
          when banner = 'RACK' and udig_rank = 3 then '1, HOL23_NR_CYBER_SPECIAL PURCHASE'
          when banner = 'NORDSTROM' and udig_rank = 1 then '1, HOL23_N_CYBER_SPECIAL PURCHASE'
          when banner = 'NORDSTROM' and udig_rank = 2 then '2, CYBER 2023_US'
      END AS udig
      , udig_event_type AS event_type
      , 2023 AS cyber_year
    FROM
        (SELECT
            sku_idnt
            , banner
            , 'Cyber' AS udig_event_type
            , MIN(case
                when item_group = '1, HOL23_N_CYBER_SPECIAL PURCHASE' then 1
                when item_group = '2, CYBER 2023_US' then 2
                when item_group = '1, HOL23_NR_CYBER_SPECIAL PURCHASE' then 3
                end) as udig_rank
            FROM
                (
                select distinct
                    sku_idnt,
                    case
                      when udig_colctn_idnt = 50009 then 'RACK'
                      else 'NORDSTROM'
                    end as banner,
                    udig_itm_grp_idnt || ', ' || udig_itm_grp_nm as item_group
                from prd_usr_vws.udig_itm_grp_sku_lkup
                where udig_colctn_idnt in ('50008','50009','25032')
                and udig_itm_grp_nm in ('HOL23_NR_CYBER_SPECIAL PURCHASE', 'HOL23_N_CYBER_SPECIAL PURCHASE', 'CYBER 2023_US')
                ) item_group
            GROUP BY 1,2,3
        ) rnk
) WITH DATA PRIMARY INDEX (sku_idnt, country) ON COMMIT PRESERVE ROWS;  

COLLECT STATISTICS COLUMN(sku_idnt, country) ON cyber_skus_2023;

-- cyber 2022 - 11/04 - 11/29

CREATE VOLATILE MULTISET TABLE cyber_skus_2022 AS ( SELECT 
    rnk.sku_idnt
    , banner
    , country
    ,CASE   
        WHEN udig_rank = 1 THEN '1, HOL22_N_CYBER_SPECIAL PURCHASE'
        WHEN udig_rank = 2 THEN '1, HOL22_NR_CYBER_SPECIAL PURCHASE'
        WHEN udig_rank = 3 THEN '2, CYBER 2022_US'
        WHEN udig_rank = 4 THEN '6, STOCKING STUFFERS 2022_US'
        WHEN udig_rank = 5 THEN '1, GIFTS & GLAM 2022_US'
        WHEN udig_rank = 6 THEN '3, HOLIDAY TREATMENT 2022_US'
        WHEN udig_rank = 7 THEN '4, PALETTES 2022_US'
        WHEN udig_rank = 8 THEN '5, HOLIDAY FRAGRANCE 2022_US'
        WHEN udig_rank = 9 THEN '7, FRAGRANCE STOCKING STUFFERS 2022_US'
    END AS udig
    , udig_event_type AS event_type
    , 2022 AS cyber_year
    
FROM 
    (SELECT 
        sku_idnt
        , country
        , banner
        , 'Cyber' AS udig_event_type
        , MIN(CASE
            WHEN item_group = '1, HOL22_N_CYBER_SPECIAL PURCHASE' THEN 1
            WHEN item_group = '1, HOL22_NR_CYBER_SPECIAL PURCHASE' THEN 2
            WHEN item_group = '2, CYBER 2022_US' THEN 3
            WHEN item_group = '6, STOCKING STUFFERS 2022_US' THEN 4
            WHEN item_group = '1, GIFTS & GLAM 2022_US' THEN 5
            WHEN item_group = '3, HOLIDAY TREATMENT 2022_US' THEN 6
            WHEN item_group = '4, PALETTES 2022_US' THEN 7
            WHEN item_group = '5, HOLIDAY FRAGRANCE 2022_US' THEN 8
            WHEN item_group = '7, FRAGRANCE STOCKING STUFFERS 2022_US' THEN 9
            END) AS udig_rank
        FROM 
            (
            SELECT DISTINCT
                sku_idnt
                ,udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS item_group
                ,'US' AS country
                ,case
                      when udig_colctn_idnt = 50009 then 'RACK'
                      else 'NORDSTROM'
                    end as banner
            FROM PRD_USR_VWS.UDIG_ITM_GRP_SKU_LKUP
            WHERE UDIG_COLCTN_NM IN 
                ('HOLIDAY 2022_RACK', 'HOLIDAY 2022_NORDSTROM', 'US BEAUTY 22 HOLIDAY REPORTING')
            
            UNION ALL 
             
            SELECT DISTINCT
                sku_idnt
                ,udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS item_group
                ,'CA' AS country
                ,case
                      when udig_colctn_idnt = 50009 then 'RACK'
                      else 'NORDSTROM'
                    end as banner
            FROM PRD_USR_VWS.UDIG_ITM_GRP_SKU_LKUP
            WHERE UDIG_COLCTN_NM IN 
                ('HOLIDAY 2022_RACK', 'HOLIDAY 2022_NORDSTROM') -- CA Beauty? 
            ) item_group
        GROUP BY 1,2,3, 4
    ) rnk
) 
WITH DATA 
PRIMARY INDEX ( sku_idnt, country ) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(sku_idnt, country) ON cyber_skus_2022;


CREATE VOLATILE MULTISET TABLE cyber_skus AS
(SELECT * FROM cyber_skus_2023
UNION ALL
SELECT * FROM cyber_skus_2022
)WITH DATA 
PRIMARY INDEX ( sku_idnt, country ) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(sku_idnt, country) ON cyber_skus;

-- anniv + cyber acp_id
CREATE MULTISET VOLATILE TABLE events_data AS (SELECT DISTINCT acp_id
,sale_date
,'Anniversary - '|| event_year AS event_type FROM tran_base_ty 
INNER JOIN anniv_skus ON anniv_skus.sku_idnt = tran_base_ty.sku_num 
AND anniv_skus.event_year = EXTRACT(YEAR FROM CURRENT_DATE) 
WHERE sale_date BETWEEN (
SELECT min(day_dt) FROM t2dl_DAS_SCALED_EVENTS.SCALED_EVENT_DATES 
WHERE ty_ly_lly = 'TY' AND anniv_ind = 1 AND event_type NOT IN ('Non-Event','Post'))
AND (
SELECT max(day_dt) FROM t2dl_DAS_SCALED_EVENTS.SCALED_EVENT_DATES 
WHERE ty_ly_lly = 'TY' AND anniv_ind = 1 AND event_type NOT IN ('Non-Event','Post'))--'2023-07-03' AND date '2023-08-06'
--'2023-07-03' AND date '2023-08-06'
UNION ALL 
SELECT DISTINCT acp_id
,sale_date
,'Anniversary - '|| event_year AS event_type FROM tran_base_ly 
INNER JOIN anniv_skus ON anniv_skus.sku_idnt = tran_base_ly.sku_num 
AND anniv_skus.event_year = EXTRACT(YEAR FROM CURRENT_DATE) - 1
WHERE sale_date BETWEEN (
SELECT min(day_dt) FROM t2dl_DAS_SCALED_EVENTS.SCALED_EVENT_DATES 
WHERE ty_ly_lly = 'LY' AND anniv_ind = 1 AND event_type NOT IN ('Non-Event','Post')
) 
AND (
SELECT max(day_dt) FROM t2dl_DAS_SCALED_EVENTS.SCALED_EVENT_DATES 
WHERE ty_ly_lly = 'LY' AND anniv_ind = 1 AND event_type NOT IN ('Non-Event','Post')
)--'2023-07-03' AND date '2023-08-06'
--'2023-07-03' AND date '2023-08-06'

UNION ALL 
SELECT DISTINCT acp_id
,sale_date
,'Cyber - 2022' AS event_type FROM tran_base_ly 
INNER JOIN cyber_skus ON cyber_skus.sku_idnt = tran_base_ly.sku_num 
AND cyber_skus.country = 'US' AND cyber_skus.cyber_year = 2022--EXTRACT(YEAR FROM CURRENT_DATE) - 1
WHERE sale_date BETWEEN (
SELECT MIN(day_dt) FROM T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DATES 
WHERE yr_idnt = 2022 AND cyber_ind = 1 
) 
AND (
SELECT MAX(day_dt) FROM T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DATES 
WHERE yr_idnt = 2022 AND cyber_ind = 1
)
--'2022-11-04' AND date '2022-11-29'
UNION ALL 
SELECT DISTINCT acp_id
,sale_date
,'Cyber - 2023' AS event_type FROM tran_base_ty 
INNER JOIN cyber_skus ON cyber_skus.sku_idnt = tran_base_ty.sku_num 
AND cyber_skus.country = 'US' AND cyber_skus.cyber_year = 2023--EXTRACT(YEAR FROM CURRENT_DATE) 
WHERE sale_date BETWEEN (
SELECT MIN(day_dt) FROM T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DATES 
WHERE yr_idnt = 2023 AND cyber_ind = 1 
) 
AND (
SELECT MAX(day_dt) FROM T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DATES 
WHERE yr_idnt = 2023 AND cyber_ind = 1
)
--'2023-11-04' AND date '2023-11-29'
) WITH DATA PRIMARY INDEX (acp_id) ON COMMIT PRESERVE ROWS ;

COLLECT STATISTICS COLUMN(acp_id) ON events_data;


-- Loyalty Status TY

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

-- Loyalty Status LY

CREATE MULTISET VOLATILE TABLE cm_lyl_status_ly AS (
 SELECT 'LY' AS ty_ly_ind,
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

-- re-activated customers 

CREATE VOLATILE MULTISET TABLE activated_ty AS (
		SELECT DISTINCT
			 ca.acp_id
			,Coalesce(
				(CASE WHEN ac.ca_dma_code IS NOT NULL THEN 'CA' ELSE NULL end ),
				(CASE WHEN ac.us_dma_code IS NOT NULL THEN 'US' ELSE NULL end )
		 	 ) AS customer_country	
			-- Use country & channel description to identify channel number for future joins 
			,CASE 
				WHEN customer_country = 'US' AND activated_channel = 'FLS' THEN '110'
				WHEN customer_country = 'CA' AND activated_channel = 'FLS' THEN '111'
				WHEN customer_country = 'US' AND activated_channel = 'NCOM' THEN '120'
				WHEN customer_country = 'CA' AND activated_channel = 'NCOM' THEN '121'
				WHEN customer_country = 'US' AND activated_channel = 'RACK' THEN '210'
				WHEN customer_country = 'CA' AND activated_channel = 'RACK' THEN '211'
				WHEN activated_channel = 'NRHL' THEN '250'
			 END AS	activation_channel
			 ,activated_channel
		FROM dl_cma_cmbr.customer_activation ca
		LEFT JOIN prd_nap_usr_vws.analytical_customer ac
			ON ca.acp_id = ac.acp_id
		WHERE 1=1
			AND activated_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup)
	) 
	WITH DATA 
	PRIMARY INDEX(acp_id) 
	ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON activated_ty;

-- Re-activated LY

CREATE VOLATILE MULTISET TABLE activated_ly AS (
		SELECT DISTINCT
			 ca.acp_id
			,Coalesce(
				(CASE WHEN ac.ca_dma_code IS NOT NULL THEN 'CA' ELSE NULL end ),
				(CASE WHEN ac.us_dma_code IS NOT NULL THEN 'US' ELSE NULL end )
		 	 ) AS customer_country	
			-- Use country & channel description to identify channel number for future joins 
			,CASE 
				WHEN customer_country = 'US' AND activated_channel = 'FLS' THEN '110'
				WHEN customer_country = 'CA' AND activated_channel = 'FLS' THEN '111'
				WHEN customer_country = 'US' AND activated_channel = 'NCOM' THEN '120'
				WHEN customer_country = 'CA' AND activated_channel = 'NCOM' THEN '121'
				WHEN customer_country = 'US' AND activated_channel = 'RACK' THEN '210'
				WHEN customer_country = 'CA' AND activated_channel = 'RACK' THEN '211'
				WHEN activated_channel = 'NRHL' THEN '250'
			 END AS	activation_channel
			 ,activated_channel
		FROM dl_cma_cmbr.customer_activation ca
		LEFT JOIN prd_nap_usr_vws.analytical_customer ac
			ON ca.acp_id = ac.acp_id
		WHERE 1=1
			AND activated_date BETWEEN (SELECT ly_start_dt FROM date_lookup) AND (SELECT ly_end_dt FROM date_lookup)
	) 
	WITH DATA 
	PRIMARY INDEX(acp_id) 
	ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON activated_ly;

-- customer age

CREATE MULTISET VOLATILE TABLE cm_customer AS (
SELECT DISTINCT ex.acp_id
	,ex.age_value
	,ex.gender
FROM prd_nap_usr_vws.analytical_customer AS ac
LEFT JOIN prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS ex 
ON ac.acp_id = ex.acp_id	
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON cm_customer;

-- Retained TY

CREATE MULTISET VOLATILE TABLE retained_ty AS (  
SELECT
       DISTINCT  tran.acp_id
       ,'TY' AS ty_ly_ind
      /*, CASE WHEN str.business_unit_desc IN ('FULL LINE','FULL LINE CANADA') THEN 'FLS'
        WHEN str.business_unit_desc IN ('N.CA','N.COM','TRUNK CLUB') THEN 'NCOM'
        WHEN str.business_unit_desc IN ('RACK', 'RACK CANADA') THEN 'RACK'
        WHEN str.business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NRHL'
        END AS channel*/
      /*, tran.sku_num 
      , tran.trip_id  
      , tran.store_num
      , CASE WHEN ntn.acp_id IS NOT NULL THEN 1 ELSE 0 END AS ntn_flg
      , min(sale_date) as sale_date
      , sum(CASE WHEN gc_flag=0 THEN net_amt ELSE 0 END) AS net_spend
      , sum(CASE WHEN gc_flag=0 THEN gross_amt else 0 END) AS gross_spend
      , sum(gross_items) AS gross_items
      , sum(net_items) AS net_items*/
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
  --LEFT JOIN prd_nap_usr_vws.customer_ntn_fact AS ntn
        --left join prd_nap_usr_vws.customer_ntn_status_fact as ntn
   -- ON tran.global_tran_id = ntn.ntn_global_tran_id --ntn.aare_global_tran_id
   -- AND tran.sku_num = ntn.sku_num  --commented out to test the validity of a new NTN TABLE
    WHERE str.store_country_code = 'US' 
--GROUP BY 1,2,3,4,5,6,7
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS; -- USE sku_num AND sale_date

COLLECT STATISTICS COLUMN(acp_id) ON retained_ty;

-- Retained LY

CREATE MULTISET VOLATILE TABLE retained_ly AS (  
SELECT
       DISTINCT  tran.acp_id
       ,'LY' AS ty_ly_ind
      /*, CASE WHEN str.business_unit_desc IN ('FULL LINE','FULL LINE CANADA') THEN 'FLS'
        WHEN str.business_unit_desc IN ('N.CA','N.COM','TRUNK CLUB') THEN 'NCOM'
        WHEN str.business_unit_desc IN ('RACK', 'RACK CANADA') THEN 'RACK'
        WHEN str.business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NRHL'
        END AS channel*/
      /*, tran.sku_num 
      , tran.trip_id  
      , tran.store_num
      , CASE WHEN ntn.acp_id IS NOT NULL THEN 1 ELSE 0 END AS ntn_flg
      , min(sale_date) as sale_date
      , sum(CASE WHEN gc_flag=0 THEN net_amt ELSE 0 END) AS net_spend
      , sum(CASE WHEN gc_flag=0 THEN gross_amt else 0 END) AS gross_spend
      , sum(gross_items) AS gross_items
      , sum(net_items) AS net_items*/
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
WHERE COALESCE(dtl.order_date,dtl.tran_date) >= (SELECT lly_start_dt FROM date_lookup) --TODO: change to between
      AND COALESCE(dtl.order_date,dtl.tran_date) <= (SELECT lly_end_dt FROM date_lookup)
      AND NOT dtl.acp_id IS NULL AND net_items > 0 AND transaction_type = 'retail'
      AND dtl.business_day_date BETWEEN (SELECT lly_start_dt FROM date_lookup) AND (SELECT lly_end_dt FROM date_lookup)
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
  --LEFT JOIN prd_nap_usr_vws.customer_ntn_fact AS ntn
        --left join prd_nap_usr_vws.customer_ntn_status_fact as ntn
   -- ON tran.global_tran_id = ntn.ntn_global_tran_id --ntn.aare_global_tran_id
   -- AND tran.sku_num = ntn.sku_num  --commented out to test the validity of a new NTN TABLE
    WHERE str.store_country_code = 'US' 
--GROUP BY 1,2,3,4,5,6,7
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS; -- USE sku_num AND sale_date

COLLECT STATISTICS COLUMN(acp_id) ON retained_ly;
-- collect stats on month idnt		

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
		events_data.acp_id AS event_acp_id,
		event_type,
		act.acp_id AS activated_acp_id,
		a.ntn_flg,
		a.trip_id,
		a.store_num,
		cASE WHEN c.loyalty_status IS NULL THEN 'non-member' ELSE c.loyalty_status END AS loyalty_status,
		d.brand_name,
		d.div_num,
		d.div_desc,
		d.dept_num,
		d.dept_desc,
		d.grp_num,
		d.grp_desc,
		d.class_num,
		d.class_desc,
		d.sbclass_num,
		d.sbclass_desc,
		catg.category,
		supp.vendor_name AS supplier,
		d.npg_ind,
		cust.age_value,
		a.net_spend AS net_spend,
		a.gross_spend AS gross_spend,
		a.net_items AS net_items,
		a.gross_items AS gross_items,
		ROW_NUMBER() OVER (PARTITION BY npg_ind,supp.vendor_name,a.ty_ly_ind,d.web_style_num,rms_sku_num,catg.category,div_num,d.dept_num,grp_num,d.class_num,d.sbclass_num,a.trip_id,a.acp_id,brand_name,loyalty_status,a.channel order by a.acp_id) as rn
FROM tran_base_ty AS a
	LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw AS d
		ON a.sku_num = d.rms_sku_num AND d.channel_country = 'US'
	LEFT JOIN cm_customer cust
	    ON cust.acp_id = a.acp_id
	LEFT JOIN cm_lyl_status_ty c
	    ON c.acp_id =  a.acp_id
	INNER JOIN prd_nap_usr_vws.day_cal_454_dim AS e
		on a.sale_date = e.day_date  
	LEFT JOIN activated_ty act 
	    ON act.acp_id = a.acp_id AND act.activated_channel = a.channel
	LEFT JOIN events_data ON events_data.acp_id = a.acp_id AND events_data.sale_date = e.day_date
    LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim  catg 
        ON catg.dept_num = d.dept_num 
        AND catg.class_num = d.class_num 
        AND catg.sbclass_num = d.sbclass_num
    left join prd_nap_usr_vws.vendor_dim supp
        on d.prmy_supp_num =supp.vendor_num
   
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data_ty;

-- full_data_ly

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
		events_data.acp_id AS event_acp_id,
		event_type,
		act.acp_id AS activated_acp_id,
		a.ntn_flg,
		a.trip_id,
		a.store_num,
		cASE WHEN c.loyalty_status IS NULL THEN 'non-member' ELSE c.loyalty_status END AS loyalty_status,
		d.brand_name,
		d.div_num,
		d.div_desc,
		d.dept_num,
		d.dept_desc,
		d.grp_num,
		d.grp_desc,
		d.class_num,
		d.class_desc,
		d.sbclass_num,
		d.sbclass_desc,
		catg.category,
		supp.vendor_name AS supplier,
		d.npg_ind,
		cust.age_value,
		a.net_spend AS net_spend,
		a.gross_spend AS gross_spend,
		a.net_items AS net_items,
		a.gross_items AS gross_items,
		ROW_NUMBER() OVER (PARTITION BY npg_ind,supp.vendor_name,a.ty_ly_ind,d.web_style_num,rms_sku_num,catg.category,div_num,d.dept_num,grp_num,d.class_num,d.sbclass_num,a.trip_id,a.acp_id,brand_name,loyalty_status,a.channel order by a.acp_id) as rn
FROM tran_base_ly AS a
	LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw AS d
		ON a.sku_num = d.rms_sku_num AND d.channel_country = 'US'
	LEFT JOIN cm_customer cust
	    ON cust.acp_id = a.acp_id
	LEFT JOIN cm_lyl_status_ly c
	    ON c.acp_id =  a.acp_id
	INNER JOIN prd_nap_usr_vws.day_cal_454_dim AS e
		on a.sale_date = e.day_date_last_year_realigned  
	LEFT JOIN activated_ly act 
	    ON act.acp_id = a.acp_id AND act.activated_channel = a.channel
	LEFT JOIN events_data ON events_data.acp_id = a.acp_id AND events_data.sale_date = e.day_date
    LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim  catg 
        ON catg.dept_num = d.dept_num 
        AND catg.class_num = d.class_num 
        AND catg.sbclass_num = d.sbclass_num
    left join prd_nap_usr_vws.vendor_dim supp
        on d.prmy_supp_num =supp.vendor_num
   
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data_ly;

CREATE MULTISET VOLATILE TABLE full_data AS (
    select * from full_data_ty
    union all 
    SELECT * from full_data_ly
)WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data;

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
		brand_name,
		div_num,
		div_desc,
		dept_num,
		dept_desc,
		grp_num,
		grp_desc,
		class_num,
		class_desc,
		sbclass_num,
		sbclass_desc,
		category,
		supplier,
		f.trip_id,
		f.acp_id,
		event_acp_id,
		event_type,
		f2.acp_id AS retained_acp_id,
		activated_acp_id,
		npg_ind,
		age_value,
		sum(f.net_spend) AS net_spend,
		sum(f.gross_spend) AS gross_spend,
		sum(f.net_items) AS net_items,
		sum(f.gross_items) AS gross_items
FROM full_data AS f 
LEFT JOIN tran_base_ly f2 ON f2.acp_id = f.acp_id
WHERE rn = 1 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON prefinal; 

CREATE MULTISET VOLATILE TABLE customer_brand_base_nordstrom AS 
(SELECT 'Brand                                                               ' AS LEVEL
,brand_name AS DIM 
,'                                   ' AS subdim
,ty_ly_ind
,'     ' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Nordstrom' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Brand x Division' AS LEVEL
,brand_name AS DIM 
,CONCAT(div_num, ', ' , div_desc) AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL
SELECT 'Brand x Department' AS LEVEL
,brand_name AS DIM 
,CONCAT(dept_num, ', ' , dept_desc) AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Brand X Subdivision' AS LEVEL
,brand_name AS DIM 
,CONCAT(grp_num, ', ' , grp_desc) AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
WITH DATA PRIMARY INDEX (month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON customer_brand_base_nordstrom;

CREATE MULTISET VOLATILE TABLE customer_brand_base_nordstrom1 AS 
(SELECT 'Total                                           ' AS LEVEL 
,'TOTAL' AS DIM 
,'                                      ' AS subdim
,ty_ly_ind 
,npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL
SELECT 'Loyalty Status' AS LEVEL
,loyalty_status AS DIM 
,'' AS subdim
,ty_ly_ind 
,npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Division' AS LEVEL 
,CONCAT(div_num, ', ' , div_desc) AS DIM
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Subdivision' AS LEVEL
,CONCAT(grp_num, ', ', grp_desc) AS DIM
,'' AS sub_dim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Department' AS LEVEL
,CONCAT(dept_num, ', ', dept_desc) AS DIM
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Channel' AS LEVEL
,channel AS DIM 
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Class' AS LEVEL 
,CONCAT(class_num, ', ' , class_desc) AS DIM
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) WITH DATA PRIMARY INDEX (month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON customer_brand_base_nordstrom1;

CREATE MULTISET VOLATILE TABLE customer_brand_base_nordstrom2 as 
(SELECT 'Subclass                  ' AS LEVEL 
,CONCAT(sbclass_num, ', ' , sbclass_desc) AS DIM
,'                  ' AS subdim
,ty_ly_ind 
,'  ' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Supplier' AS LEVEL
,supplier AS DIM 
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Category' AS LEVEL
,category AS DIM 
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Nordstrom'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA PRIMARY INDEX (month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON customer_brand_base_nordstrom2;
 
CREATE MULTISET VOLATILE TABLE customer_brand_base_rack as
(SELECT 'Brand                                                               ' AS LEVEL
,brand_name AS DIM 
,'                               ' AS subdim
,ty_ly_ind
,'     ' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Brand x Division' AS LEVEL
,brand_name AS DIM 
,CONCAT(div_num, ', ' , div_desc) AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL
SELECT 'Brand x Department' AS LEVEL
,brand_name AS DIM 
,CONCAT(dept_num, ', ' , dept_desc) AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
WITH DATA PRIMARY INDEX (month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON customer_brand_base_rack;

CREATE MULTISET VOLATILE TABLE customer_brand_base_rack1 AS
(SELECT 'Brand x Subdivision' AS LEVEL
,brand_name AS DIM 
,CONCAT(grp_num, ', ' , grp_desc) AS subdim
,ty_ly_ind 
,' ' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Loyalty Status' AS LEVEL
,loyalty_status AS DIM 
,'' AS subdim
,ty_ly_ind 
,npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Total' AS LEVEL 
,'Total' AS DIM 
,'' AS subdim
,ty_ly_ind 
,npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales
FROM prefinal  WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) 
WITH DATA PRIMARY INDEX (month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON customer_brand_base_rack1;


CREATE MULTISET VOLATILE TABLE customer_brand_base_rack2 AS
(SELECT 'Division    ' AS LEVEL 
,CONCAT(div_num, ', ' , div_desc) AS DIM
,'  ' AS subdim
,ty_ly_ind 
,'  ' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Subdivision' AS LEVEL
,CONCAT(grp_num, ', ', grp_desc) AS DIM
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Department' AS LEVEL
,CONCAT(dept_num, ', ', dept_desc) AS DIM
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
WITH DATA PRIMARY INDEX (month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON customer_brand_base_rack2;


CREATE MULTISET VOLATILE TABLE customer_brand_base_rack3 AS (SELECT 'Class' AS LEVEL 
,CONCAT(class_num, ', ' , class_desc) AS DIM
,'     ' AS subdim
,ty_ly_ind 
,'   ' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Subclass' AS LEVEL 
,CONCAT(sbclass_num, ', ' , sbclass_desc) AS DIM
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Channel' AS LEVEL
,channel AS DIM 
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Supplier' AS LEVEL
,supplier AS DIM 
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL 
SELECT 'Category' AS LEVEL
,category AS DIM 
,'' AS subdim
,ty_ly_ind 
,'' AS npg_ind
,banner
,channel
,event_type
,year_idnt
,half_idnt 
,quarter_idnt
,month_idnt
,half_label
,quarter_label
,month_label
,count(DISTINCT acp_id) AS customers
,count(DISTINCT retained_acp_id) AS retained_customers
,count(DISTINCT activated_acp_id) AS activated_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'non-member' THEN acp_id end) AS loyalty_customers
,count(DISTINCT CASE WHEN loyalty_status = 'member' THEN acp_id end) AS loyalty_member_customers
,count(DISTINCT CASE WHEN loyalty_status <> 'cardmember' THEN acp_id end) AS loyalty_cardmember_customers
,avg(age_value) AS avg_age
,count(DISTINCT trip_id) AS trips
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_customers
,sum(gross_items) AS gross_items
,sum(net_items) AS net_items
,sum(gross_spend) AS gross_sales
,sum(net_spend) AS net_sales 
FROM prefinal WHERE banner = 'Rack'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
WITH DATA PRIMARY INDEX (month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON customer_brand_base_rack3;

CREATE MULTISET VOLATILE TABLE merch_customer_brand_base_N AS 
(SELECT * FROM customer_brand_base_nordstrom
UNION ALL 
SELECT * FROM customer_brand_base_nordstrom1
UNION ALL
SELECT * FROM customer_brand_base_nordstrom2)
WITH DATA PRIMARY INDEX(month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON merch_customer_brand_base_N;

CREATE MULTISET VOLATILE TABLE merch_customer_brand_base_R AS 
(SELECT * FROM customer_brand_base_rack
UNION ALL 
SELECT * FROM customer_brand_base_rack1
UNION ALL
SELECT * FROM customer_brand_base_rack2
UNION ALL 
SELECT * FROM customer_brand_base_rack3)
WITH DATA PRIMARY INDEX(month_idnt,dim) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON merch_customer_brand_base_R;


DELETE FROM t2dl_das_cus.MERCH_CUSTOMER_BRAND_BASE where month_idnt = (SELECT DISTINCT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = CURRENT_DATE()-10);
INSERT INTO t2dl_das_cus.MERCH_CUSTOMER_BRAND_BASE
SELECT * FROM merch_customer_brand_base_R
union all 
select * from merch_customer_brand_base_N;

COLLECT STATISTICS COLUMN(month_idnt,dim) ON t2dl_das_cus.MERCH_CUSTOMER_BRAND_BASE;

/*------------------------------------------------------
 Merch Customer Reporting (Demographic)
 Purpose: Provide demographic trends by product hierarchy at a monthly grain
 
 Last Update: 11/29/23 Rasagnya Avala - ETL Pipeline creation
 Contact for logic/code: Rasagnya Avala (Merch Analytics)
 Contact for ETL: Rasagnya Avala (Merch Analytics)
 --------------------------------------------------------*/
 
-- Customer Age, Income, Gender, Location

DROP TABLE cm_customer;

CREATE MULTISET VOLATILE TABLE cm_customer AS (
SELECT DISTINCT ex.acp_id
	,ex.age_value
	,ex.gender
	,ac.us_dma_code
	,ac.us_dma_desc
	,Coalesce(zip.URBANICITY,'Unknown') AS us_urbanicity
	,ex.household_estimated_income_range
FROM prd_nap_usr_vws.analytical_customer AS ac
LEFT JOIN prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS ex 
ON ac.acp_id = ex.acp_id	
LEFT JOIN T3DL_ACE_CAI.ZIP_CODE_URBANICITY zip
		ON ac.billing_postal_code = zip.ZIP_CODE
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON cm_customer;


DROP TABLE full_data_ty;

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
		d.brand_name,
		d.div_num,
		d.div_desc,
		d.dept_num,
		d.dept_desc,
		d.grp_num,
		d.grp_desc,
		a.net_spend AS net_spend,
		a.gross_spend AS gross_spend,
		a.net_items AS net_items,
		a.gross_items AS gross_items,
		cust.age_value,
		cust.gender,
		cust.household_estimated_income_range,
		CONCAT(cust.us_dma_code,', ',cust.us_dma_desc) AS dma,
		cust.us_urbanicity,
		ROW_NUMBER() OVER (PARTITION BY npg_ind,a.ty_ly_ind,d.web_style_num,rms_sku_num,d.div_num,d.dept_num,d.grp_num,a.trip_id,a.acp_id,a.channel order by a.acp_id) as rn
FROM tran_base_ty AS a
	LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw AS d
		ON a.sku_num = d.rms_sku_num
	LEFT JOIN cm_customer cust 
	    ON cust.acp_id =  a.acp_id
	INNER JOIN prd_nap_usr_vws.day_cal_454_dim AS e
		on a.sale_date = e.day_date  
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data_ty;

DROP TABLE full_data_ly;
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
		d.brand_name,
		d.div_num,
		d.div_desc,
		d.dept_num,
		d.dept_desc,
		d.grp_num,
		d.grp_desc,
		a.net_spend AS net_spend,
		a.gross_spend AS gross_spend,
		a.net_items AS net_items,
		a.gross_items AS gross_items,
		cust.age_value,
		cust.gender,
		cust.household_estimated_income_range,
		CONCAT(cust.us_dma_code,', ',cust.us_dma_desc) AS dma,
		cust.us_urbanicity,
		ROW_NUMBER() OVER (PARTITION BY npg_ind,a.ty_ly_ind,d.web_style_num,rms_sku_num,d.div_num,d.dept_num,d.grp_num,a.trip_id,a.acp_id,a.channel order by a.acp_id) as rn
FROM tran_base_ly AS a
	LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw AS d
		ON a.sku_num = d.rms_sku_num
	LEFT JOIN cm_customer cust 
	    ON cust.acp_id =  a.acp_id
	INNER JOIN prd_nap_usr_vws.day_cal_454_dim AS e
		on a.sale_date = e.day_date_last_year_realigned  
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data_ly;

DROP TABLE full_data;

CREATE MULTISET VOLATILE TABLE full_data AS (
SELECT * FROM full_data_ty 
UNION ALL 
SELECT * FROM full_data_ly
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON full_data;

-- brand_top_10_markets
CREATE MULTISET VOLATILE TABLE brand_top_10_markets AS 
(SELECT 
brand_name
, dma
, ROW_NUMBER() OVER (PARTITION BY brand_name ORDER BY customers DESC) AS market_rk 
FROM 
(SELECT DISTINCT brand_name
,dma
,count(DISTINCT acp_id) AS customers 
FROM full_data 
GROUP BY 1,2 )l 
WHERE dma IS NOT NULL AND dma <> 'Not Defined' )
WITH DATA PRIMARY INDEX(brand_name) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(brand_name) ON brand_top_10_markets;

--div_top_10_markets
CREATE MULTISET VOLATILE TABLE div_top_10_markets AS 
(SELECT div_num
,div_desc
, dma
, ROW_NUMBER() OVER (PARTITION BY div_num,div_desc ORDER BY customers DESC) AS market_rk 
FROM 
(SELECT DISTINCT div_num
,div_desc
,dma
,count(DISTINCT acp_id) AS customers FROM full_data 
GROUP BY 1,2,3 )l 
WHERE dma IS NOT NULL AND dma <> 'Not Defined' )
WITH DATA PRIMARY INDEX(div_num) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(div_num) ON div_top_10_markets;

-- dept_top_10_markets
CREATE MULTISET VOLATILE TABLE dept_top_10_markets AS 
(SELECT dept_num
,dept_desc
, dma
, ROW_NUMBER() OVER (PARTITION BY dept_num,dept_desc ORDER BY customers DESC) AS market_rk 
FROM 
(SELECT DISTINCT dept_num
,dept_desc
,dma
,count(DISTINCT acp_id) AS customers 
FROM full_data 
GROUP BY 1,2,3 )l 
WHERE dma IS NOT NULL AND dma <> 'Not Defined')
WITH DATA PRIMARY INDEX(dept_num) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(dept_num) ON dept_top_10_markets;

-- subdiv_top_10_markets
CREATE MULTISET VOLATILE TABLE subdiv_top_10_markets AS 
(SELECT grp_num
,grp_desc
, dma
, ROW_NUMBER() OVER (PARTITION BY grp_num,grp_desc ORDER BY customers DESC) AS market_rk 
FROM 
(SELECT DISTINCT grp_num
,grp_desc
,dma
,count(DISTINCT acp_id) AS customers 
FROM full_data 
GROUP BY 1,2,3 )l 
WHERE dma IS NOT NULL AND dma <> 'Not Defined' )
WITH DATA PRIMARY INDEX(grp_num) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(grp_num) ON subdiv_top_10_markets;

CREATE MULTISET VOLATILE TABLE prefinal_brands AS (
SELECT  year_idnt,
        half_idnt,
		quarter_idnt,
		month_idnt,
		half_label,
		quarter_label,
		month_label,
		CASE WHEN channel IN ('FLS','NCOM') THEN 'Nordstrom' ELSE 'Rack' END AS banner,
		channel,
		ntn_flg,
		ty_ly_ind,
		f.brand_name,
		/*div_num,
		div_desc,
		dept_num,
		dept_desc,
		grp_num,
		grp_desc,*/
		--trip_id,
		acp_id,
		age_value AS age,
		CASE
			WHEN age_value BETWEEN 14 AND 22 THEN 'Young Adult'
			WHEN age_value BETWEEN 23 AND 29 THEN 'Early Career'
			WHEN age_value BETWEEN 30 AND 44 THEN 'Mid Career'
			WHEN age_value BETWEEN 45 AND 64 THEN 'Late Career'
			WHEN age_value >= 65 THEN 'Retired' 
			ELSE 'Unknown' end AS age_value,
		CASE
			WHEN age_value BETWEEN 18 AND 29 THEN 'Core Customer Target Age'
			WHEN age_value < 18 THEN 'Younger'
			WHEN age_value >= 30 THEN 'Older' 
		ELSE 'Unknown' end AS lifestage_sample,
		gender,
		CASE 
			WHEN household_estimated_income_range = '$1,000-$14,999' THEN 8000
			WHEN household_estimated_income_range = '$15,000-$24,999' THEN 20000
			WHEN household_estimated_income_range = '$25,000-$34,999' THEN 30000
			WHEN household_estimated_income_range = '$35,000-$49,999' THEN 42500
			WHEN household_estimated_income_range = '$50,000-$74,999' THEN 62500
			WHEN household_estimated_income_range = '$75,000-$99,999' THEN 87500
			WHEN household_estimated_income_range = '$100,000-$124,999' THEN 112500
			WHEN household_estimated_income_range = '$125,000-$149,999' THEN 137500
			WHEN household_estimated_income_range = '$150,000-$174,999' THEN 162500
			WHEN household_estimated_income_range = '$175,000-$199,999' THEN 187500
			WHEN household_estimated_income_range = '$200,000-$249,999' THEN 212500
			WHEN household_estimated_income_range = '$250,000+' THEN 350000
		 ELSE NULL end AS estimated_income_range_midpoint,
		bm.dma,
		us_urbanicity,
		CASE WHEN age < 44 THEN 1 END AS age_under_44_flag,
		avg(age) OVER (PARTITION BY f.brand_name,acp_id,banner,channel) AS avg_age,
		avg(estimated_income_range_midpoint) OVER (PARTITION BY f.brand_name,acp_id,banner,channel) AS avg_income,
		sum(net_spend) AS net_spend,
		sum(gross_spend) AS gross_spend,
		sum(net_items) AS net_items,
		sum(gross_items) AS gross_items
FROM full_data AS f 
LEFT JOIN brand_top_10_markets bm ON bm.brand_name = f.brand_name AND bm.dma = f.dma AND bm.market_rk <=10 -- brands
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON prefinal_brands;

CREATE MULTISET VOLATILE TABLE prefinal_div AS (
SELECT  year_idnt,
        half_idnt,
		quarter_idnt,
		month_idnt,
		half_label,
		quarter_label,
		month_label,
		CASE WHEN channel IN ('FLS','NCOM') THEN 'Nordstrom' ELSE 'Rack' END AS banner,
		channel,
		ntn_flg,
		ty_ly_ind,
		f.div_num,
		f.div_desc,
		acp_id,
		age_value AS age,
		CASE
			WHEN age_value BETWEEN 14 AND 22 THEN 'Young Adult'
			WHEN age_value BETWEEN 23 AND 29 THEN 'Early Career'
			WHEN age_value BETWEEN 30 AND 44 THEN 'Mid Career'
			WHEN age_value BETWEEN 45 AND 64 THEN 'Late Career'
			WHEN age_value >= 65 THEN 'Retired' 
			ELSE 'Unknown' end AS age_value,
		CASE
			WHEN age_value BETWEEN 18 AND 29 THEN 'Core Customer Target Age'
			WHEN age_value < 18 THEN 'Younger'
			WHEN age_value >= 30 THEN 'Older' 
		ELSE 'Unknown' end AS lifestage_sample,
		gender,
		CASE 
			WHEN household_estimated_income_range = '$1,000-$14,999' THEN 8000
			WHEN household_estimated_income_range = '$15,000-$24,999' THEN 20000
			WHEN household_estimated_income_range = '$25,000-$34,999' THEN 30000
			WHEN household_estimated_income_range = '$35,000-$49,999' THEN 42500
			WHEN household_estimated_income_range = '$50,000-$74,999' THEN 62500
			WHEN household_estimated_income_range = '$75,000-$99,999' THEN 87500
			WHEN household_estimated_income_range = '$100,000-$124,999' THEN 112500
			WHEN household_estimated_income_range = '$125,000-$149,999' THEN 137500
			WHEN household_estimated_income_range = '$150,000-$174,999' THEN 162500
			WHEN household_estimated_income_range = '$175,000-$199,999' THEN 187500
			WHEN household_estimated_income_range = '$200,000-$249,999' THEN 212500
			WHEN household_estimated_income_range = '$250,000+' THEN 350000
		 ELSE NULL end AS estimated_income_range_midpoint,
		div.dma,
		us_urbanicity,
		CASE WHEN age < 44 THEN 1 END AS age_under_44_flag,
		avg(age) OVER (PARTITION BY f.div_num,acp_id,banner,channel) AS avg_age,
		avg(estimated_income_range_midpoint) OVER (PARTITION BY f.div_num,acp_id,banner,channel) AS avg_income,
		sum(net_spend) AS net_spend,
		sum(gross_spend) AS gross_spend,
		sum(net_items) AS net_items,
		sum(gross_items) AS gross_items
FROM full_data AS f 
LEFT JOIN div_top_10_markets div ON div.div_num = f.div_num AND div.dma = f.dma AND div.market_rk <=10 -- div
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON prefinal_div;

CREATE MULTISET VOLATILE TABLE prefinal_subdiv AS (
SELECT  year_idnt,
        half_idnt,
		quarter_idnt,
		month_idnt,
		half_label,
		quarter_label,
		month_label,
		CASE WHEN channel IN ('FLS','NCOM') THEN 'Nordstrom' ELSE 'Rack' END AS banner,
		channel,
		ntn_flg,
		ty_ly_ind,
		brand_name,
		div_num,
		div_desc,
		dept_num,
		dept_desc,
		f.grp_num,
		f.grp_desc,
		--trip_id,
		acp_id,
		age_value AS age,
		CASE
			WHEN age_value BETWEEN 14 AND 22 THEN 'Young Adult'
			WHEN age_value BETWEEN 23 AND 29 THEN 'Early Career'
			WHEN age_value BETWEEN 30 AND 44 THEN 'Mid Career'
			WHEN age_value BETWEEN 45 AND 64 THEN 'Late Career'
			WHEN age_value >= 65 THEN 'Retired' 
			ELSE 'Unknown' end AS age_value,
		CASE
			WHEN age_value BETWEEN 18 AND 29 THEN 'Core Customer Target Age'
			WHEN age_value < 18 THEN 'Younger'
			WHEN age_value >= 30 THEN 'Older' 
		ELSE 'Unknown' end AS lifestage_sample,
		gender,
		CASE 
			WHEN household_estimated_income_range = '$1,000-$14,999' THEN 8000
			WHEN household_estimated_income_range = '$15,000-$24,999' THEN 20000
			WHEN household_estimated_income_range = '$25,000-$34,999' THEN 30000
			WHEN household_estimated_income_range = '$35,000-$49,999' THEN 42500
			WHEN household_estimated_income_range = '$50,000-$74,999' THEN 62500
			WHEN household_estimated_income_range = '$75,000-$99,999' THEN 87500
			WHEN household_estimated_income_range = '$100,000-$124,999' THEN 112500
			WHEN household_estimated_income_range = '$125,000-$149,999' THEN 137500
			WHEN household_estimated_income_range = '$150,000-$174,999' THEN 162500
			WHEN household_estimated_income_range = '$175,000-$199,999' THEN 187500
			WHEN household_estimated_income_range = '$200,000-$249,999' THEN 212500
			WHEN household_estimated_income_range = '$250,000+' THEN 350000
		 ELSE NULL end AS estimated_income_range_midpoint,
		subdiv.dma,
		us_urbanicity,
		CASE WHEN age < 44 THEN 1 END AS age_under_44_flag,
		avg(age) OVER (PARTITION BY brand_name,acp_id,banner,channel) AS avg_age,
		avg(estimated_income_range_midpoint) OVER (PARTITION BY brand_name,acp_id,banner,channel) AS avg_income,
		sum(net_spend) AS net_spend,
		sum(gross_spend) AS gross_spend,
		sum(net_items) AS net_items,
		sum(gross_items) AS gross_items
FROM full_data AS f 
LEFT JOIN subdiv_top_10_markets subdiv ON subdiv.grp_num = f.grp_num AND subdiv.dma = f.dma AND subdiv.market_rk <=10 -- subdiv
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON prefinal_subdiv;

CREATE MULTISET VOLATILE TABLE prefinal_dept AS (
SELECT  year_idnt,
        half_idnt,
		quarter_idnt,
		month_idnt,
		half_label,
		quarter_label,
		month_label,
		CASE WHEN channel IN ('FLS','NCOM') THEN 'Nordstrom' ELSE 'Rack' END AS banner,
		channel,
		ntn_flg,
		ty_ly_ind,
		brand_name,
		div_num,
		div_desc,
		f.dept_num,
		f.dept_desc,
		grp_num,
		grp_desc,
		--trip_id,
		acp_id,
		age_value AS age,
		CASE
			WHEN age_value BETWEEN 14 AND 22 THEN 'Young Adult'
			WHEN age_value BETWEEN 23 AND 29 THEN 'Early Career'
			WHEN age_value BETWEEN 30 AND 44 THEN 'Mid Career'
			WHEN age_value BETWEEN 45 AND 64 THEN 'Late Career'
			WHEN age_value >= 65 THEN 'Retired' 
			ELSE 'Unknown' end AS age_value,
		CASE
			WHEN age_value BETWEEN 18 AND 29 THEN 'Core Customer Target Age'
			WHEN age_value < 18 THEN 'Younger'
			WHEN age_value >= 30 THEN 'Older' 
		ELSE 'Unknown' end AS lifestage_sample,
		gender,
		CASE 
			WHEN household_estimated_income_range = '$1,000-$14,999' THEN 8000
			WHEN household_estimated_income_range = '$15,000-$24,999' THEN 20000
			WHEN household_estimated_income_range = '$25,000-$34,999' THEN 30000
			WHEN household_estimated_income_range = '$35,000-$49,999' THEN 42500
			WHEN household_estimated_income_range = '$50,000-$74,999' THEN 62500
			WHEN household_estimated_income_range = '$75,000-$99,999' THEN 87500
			WHEN household_estimated_income_range = '$100,000-$124,999' THEN 112500
			WHEN household_estimated_income_range = '$125,000-$149,999' THEN 137500
			WHEN household_estimated_income_range = '$150,000-$174,999' THEN 162500
			WHEN household_estimated_income_range = '$175,000-$199,999' THEN 187500
			WHEN household_estimated_income_range = '$200,000-$249,999' THEN 212500
			WHEN household_estimated_income_range = '$250,000+' THEN 350000
		 ELSE NULL end AS estimated_income_range_midpoint,
		dept.dma,
		us_urbanicity,
		CASE WHEN age < 44 THEN 1 END AS age_under_44_flag,
		avg(age) OVER (PARTITION BY f.brand_name,acp_id,banner,channel) AS avg_age,
		avg(estimated_income_range_midpoint) OVER (PARTITION BY f.brand_name,acp_id,banner,channel) AS avg_income,
		sum(net_spend) AS net_spend,
		sum(gross_spend) AS gross_spend,
		sum(net_items) AS net_items,
		sum(gross_items) AS gross_items
FROM full_data AS f 
LEFT JOIN dept_top_10_markets dept ON dept.dept_num = f.dept_num AND dept.dma = f.dma AND dept.market_rk <=10 -- dept
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id) ON prefinal_dept;

CREATE MULTISET VOLATILE TABLE brand_cte AS 
(
SELECT 'Brand                          ' AS product_attribute
,brand_name AS dimension
,dma
,ty_ly_ind
,banner
,channel
,year_idnt
,half_idnt
,quarter_idnt 
,month_idnt 
,half_label
,quarter_label
,month_label
,avg(avg_age) AS avg_age
,avg(CASE WHEN ntn_flg = 1 THEN avg_age END) AS avg_ntn_age
,avg(avg_income) AS avg_income
,count(DISTINCT acp_id) AS customers
,count(DISTINCT CASE WHEN age_under_44_flag = 1 THEN acp_id end) AS customer_under_44
,count(DISTINCT CASE WHEN gender = 'Male' THEN acp_id end) AS male_customers
,count(DISTINCT CASE WHEN gender = 'Female' THEN acp_id end) AS female_customers
,count(DISTINCT CASE WHEN gender = 'Unknown' THEN acp_id end) AS unknown_gender_customers
,count(distinct CASE WHEN age_value = 'Young Adult' THEN acp_id END) AS young_adult_customers
,count(distinct CASE WHEN age_value = 'Early Career' THEN acp_id END) AS early_career_customers
,count(distinct CASE WHEN age_value = 'Mid Career' THEN acp_id END) AS mid_career_customers
,count(distinct CASE WHEN age_value = 'Late Career' THEN acp_id END) AS late_career_customers
,count(distinct CASE WHEN age_value = 'Retired' THEN acp_id END) AS retired_customers
,count(distinct CASE WHEN age_value = 'Unknown' THEN acp_id END) AS unknown_age_customers
,count(distinct CASE WHEN lifestage_sample = 'Core Customer Target Age' THEN acp_id END) AS core_target_age_customers
,count(distinct CASE WHEN lifestage_sample = 'Higher' THEN acp_id END) AS core_target_higher_customers
,count(distinct CASE WHEN lifestage_sample = 'Lower' THEN acp_id END) AS core_target_lower_customers
,count(distinct CASE WHEN us_urbanicity = 'Rural' THEN acp_id END) AS rural_customers
,count(distinct CASE WHEN us_urbanicity = 'Urban' THEN acp_id END) AS urban_customers
,count(distinct CASE WHEN us_urbanicity = 'Suburban' THEN acp_id END) AS suburban_customers
,sum(gross_items) AS gross_items
,sum(gross_spend) AS gross_spend
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_count
FROM prefinal_brands  
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13)
WITH DATA PRIMARY INDEX(dimension, channel, month_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(dimension, channel, month_idnt) ON brand_cte;

CREATE MULTISET VOLATILE TABLE div_cte AS 
(
SELECT 'Division                          ' AS product_attribute
,CONCAT(div_num, ', ',div_desc) AS dimension
,dma
,ty_ly_ind
,banner
,channel
,year_idnt
,half_idnt
,quarter_idnt 
,month_idnt 
,half_label
,quarter_label
,month_label
,avg(avg_age) AS avg_age
,avg(CASE WHEN ntn_flg = 1 THEN avg_age END) AS avg_ntn_age
,avg(avg_income) AS avg_income
,count(DISTINCT acp_id) AS customers
,count(DISTINCT CASE WHEN age_under_44_flag = 1 THEN acp_id end) AS customer_under_44
,count(DISTINCT CASE WHEN gender = 'Male' THEN acp_id end) AS male_customers
,count(DISTINCT CASE WHEN gender = 'Female' THEN acp_id end) AS female_customers
,count(DISTINCT CASE WHEN gender = 'Unknown' THEN acp_id end) AS unknown_gender_customers
,count(distinct CASE WHEN age_value = 'Young Adult' THEN acp_id END) AS young_adult_customers
,count(distinct CASE WHEN age_value = 'Early Career' THEN acp_id END) AS early_career_customers
,count(distinct CASE WHEN age_value = 'Mid Career' THEN acp_id END) AS mid_career_customers
,count(distinct CASE WHEN age_value = 'Late Career' THEN acp_id END) AS late_career_customers
,count(distinct CASE WHEN age_value = 'Retired' THEN acp_id END) AS retired_customers
,count(distinct CASE WHEN age_value = 'Unknown' THEN acp_id END) AS unknown_age_customers
,count(distinct CASE WHEN lifestage_sample = 'Core Customer Target Age' THEN acp_id END) AS core_target_age_customers
,count(distinct CASE WHEN lifestage_sample = 'Higher' THEN acp_id END) AS core_target_higher_customers
,count(distinct CASE WHEN lifestage_sample = 'Lower' THEN acp_id END) AS core_target_lower_customers
,count(distinct CASE WHEN us_urbanicity = 'Rural' THEN acp_id END) AS rural_customers
,count(distinct CASE WHEN us_urbanicity = 'Urban' THEN acp_id END) AS urban_customers
,count(distinct CASE WHEN us_urbanicity = 'Suburban' THEN acp_id END) AS suburban_customers
,sum(gross_items) AS gross_items
,sum(gross_spend) AS gross_spend
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_count
FROM prefinal_div 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13)
WITH DATA PRIMARY INDEX(dimension, channel, month_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(dimension, channel, month_idnt) ON div_cte;

CREATE MULTISET VOLATILE TABLE subdiv_cte AS 
(
SELECT 'Subdivision                          ' AS product_attribute
,CONCAT(grp_num, ', ',grp_desc) AS dimension
,dma
,ty_ly_ind
,banner
,channel
,year_idnt
,half_idnt
,quarter_idnt 
,month_idnt 
,half_label
,quarter_label
,month_label
,avg(avg_age) AS avg_age
,avg(CASE WHEN ntn_flg = 1 THEN avg_age END) AS avg_ntn_age
,avg(avg_income) AS avg_income
,count(DISTINCT acp_id) AS customers
,count(DISTINCT CASE WHEN age_under_44_flag = 1 THEN acp_id end) AS customer_under_44
,count(DISTINCT CASE WHEN gender = 'Male' THEN acp_id end) AS male_customers
,count(DISTINCT CASE WHEN gender = 'Female' THEN acp_id end) AS female_customers
,count(DISTINCT CASE WHEN gender = 'Unknown' THEN acp_id end) AS unknown_gender_customers
,count(distinct CASE WHEN age_value = 'Young Adult' THEN acp_id END) AS young_adult_customers
,count(distinct CASE WHEN age_value = 'Early Career' THEN acp_id END) AS early_career_customers
,count(distinct CASE WHEN age_value = 'Mid Career' THEN acp_id END) AS mid_career_customers
,count(distinct CASE WHEN age_value = 'Late Career' THEN acp_id END) AS late_career_customers
,count(distinct CASE WHEN age_value = 'Retired' THEN acp_id END) AS retired_customers
,count(distinct CASE WHEN age_value = 'Unknown' THEN acp_id END) AS unknown_age_customers
,count(distinct CASE WHEN lifestage_sample = 'Core Customer Target Age' THEN acp_id END) AS core_target_age_customers
,count(distinct CASE WHEN lifestage_sample = 'Higher' THEN acp_id END) AS core_target_higher_customers
,count(distinct CASE WHEN lifestage_sample = 'Lower' THEN acp_id END) AS core_target_lower_customers
,count(distinct CASE WHEN us_urbanicity = 'Rural' THEN acp_id END) AS rural_customers
,count(distinct CASE WHEN us_urbanicity = 'Urban' THEN acp_id END) AS urban_customers
,count(distinct CASE WHEN us_urbanicity = 'Suburban' THEN acp_id END) AS suburban_customers
,sum(gross_items) AS gross_items
,sum(gross_spend) AS gross_spend
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_count
FROM prefinal_subdiv 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13)
WITH DATA PRIMARY INDEX(dimension, channel, month_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(dimension, channel, month_idnt) ON subdiv_cte;

CREATE MULTISET VOLATILE TABLE dept_cte AS 
( 
SELECT 'Department                          ' AS product_attribute
,CONCAT(dept_num, ', ',dept_desc) AS dimension
,dma
,ty_ly_ind
,banner
,channel
,year_idnt
,half_idnt
,quarter_idnt 
,month_idnt 
,half_label
,quarter_label
,month_label
,avg(avg_age) AS avg_age
,avg(CASE WHEN ntn_flg = 1 THEN avg_age END) AS avg_ntn_age
,avg(avg_income) AS avg_income
,count(DISTINCT acp_id) AS customers
,count(DISTINCT CASE WHEN age_under_44_flag = 1 THEN acp_id end) AS customer_under_44
,count(DISTINCT CASE WHEN gender = 'Male' THEN acp_id end) AS male_customers
,count(DISTINCT CASE WHEN gender = 'Female' THEN acp_id end) AS female_customers
,count(DISTINCT CASE WHEN gender = 'Unknown' THEN acp_id end) AS unknown_gender_customers
,count(distinct CASE WHEN age_value = 'Young Adult' THEN acp_id END) AS young_adult_customers
,count(distinct CASE WHEN age_value = 'Early Career' THEN acp_id END) AS early_career_customers
,count(distinct CASE WHEN age_value = 'Mid Career' THEN acp_id END) AS mid_career_customers
,count(distinct CASE WHEN age_value = 'Late Career' THEN acp_id END) AS late_career_customers
,count(distinct CASE WHEN age_value = 'Retired' THEN acp_id END) AS retired_customers
,count(distinct CASE WHEN age_value = 'Unknown' THEN acp_id END) AS unknown_age_customers
,count(distinct CASE WHEN lifestage_sample = 'Core Customer Target Age' THEN acp_id END) AS core_target_age_customers
,count(distinct CASE WHEN lifestage_sample = 'Higher' THEN acp_id END) AS core_target_higher_customers
,count(distinct CASE WHEN lifestage_sample = 'Lower' THEN acp_id END) AS core_target_lower_customers
,count(distinct CASE WHEN us_urbanicity = 'Rural' THEN acp_id END) AS rural_customers
,count(distinct CASE WHEN us_urbanicity = 'Urban' THEN acp_id END) AS urban_customers
,count(distinct CASE WHEN us_urbanicity = 'Suburban' THEN acp_id END) AS suburban_customers
,sum(gross_items) AS gross_items
,sum(gross_spend) AS gross_spend
,count(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_count
FROM prefinal_dept 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13)
WITH DATA PRIMARY INDEX(dimension, channel, month_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(dimension, channel, month_idnt) ON dept_cte;

DELETE FROM {environment_schema}.merch_customer_demographic WHERE month_idnt = (SELECT DISTINCT month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = CURRENT_DATE()-10) ;
INSERT INTO {environment_schema}.merch_customer_demographic  
SELECT * FROM brand_cte
UNION ALL 
SELECT * FROM div_cte
UNION ALL
SELECT * FROM subdiv_cte
UNION ALL
SELECT * FROM dept_cte; 

COLLECT STATISTICS COLUMN(month_idnt,channel,dimension) ON {environment_schema}.merch_customer_demographic;

