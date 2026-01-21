SET QUERY_BAND = 'App_ID=APP09442;
DAG_ID=nmn_brand_subdiv_customer_metrics_11521_ACE_ENG;
Task_Name=nmn_brand_subdiv_customer_metrics;' 
FOR SESSION VOLATILE;

--drop table date_mapping;
CREATE Multiset VOLATILE TABLE date_mapping
AS
(SELECT
distinct day_date,
month_idnt,
month_idnt-100 as month_start,
case when cast (RIGHT(cast(month_idnt as varchar(10)),2) as INTEGER) =1 then month_idnt-89 else month_idnt-1 end as month_end
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
) WITH data PRIMARY INDEX (day_date) ON COMMIT PRESERVE ROWS;

--DROP TABLE _variables;
CREATE Multiset VOLATILE TABLE _variables
AS
(SELECT
max(month_idnt) as month_idnt,
MIN(day_date) AS start_date,
MAX(day_date) AS end_date
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM 
where month_idnt between (select month_start from date_mapping where day_date= date'2022-08-27' ) and (select month_end from date_mapping where day_date= date'2022-08-27' )
) WITH data PRIMARY INDEX (start_date) ON COMMIT PRESERVE ROWS;

--Drop table shoppers;   
CREATE MULTISET VOLATILE TABLE shoppers AS (
SELECT
    DISTINCT acp_id
FROM T2DL_DAS_SALES_RETURNS.sales_and_returns_fact
WHERE 1=1
    AND BUSINESS_DAY_DATE <= (SELECT end_date FROM _variables)
    AND transaction_type = 'retail'
    AND acp_id IS NOT NULL
    AND shipped_sales > 0
    AND business_unit_desc in ('N.COM', 'FULL LINE')
)
WITH DATA
NO PRIMARY INDEX
ON COMMIT PRESERVE ROWS;

--drop table brand_product;
CREATE MULTISET VOLATILE TABLE brand_product
AS
(SELECT 
		p.rms_sku_num AS sku_num,
		p.grp_desc AS Sub_division,
        v.vendor_brand_name AS brand_name,
        v.vendor_brand_code AS brand_id,
        CAST('Head' AS VARCHAR(10)) AS brand_tier
FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW p
  INNER JOIN PRD_NAP_USR_VWS.PRODUCT_STYLE_DIM q
          ON p.epm_style_num = q.epm_style_num
         AND p.channel_country = q.channel_country
         AND p.channel_country = 'US'
  INNER JOIN PRD_NAP_USR_VWS.vendor_label_dim v
          ON q.vendor_label_code = v.vendor_label_code
         AND (v.vendor_brand_code) IN (SELECT DISTINCT brand_id FROM T2DL_DAS_NMN.nmn_head_brands)
UNION ALL
SELECT 
	   p.rms_sku_num AS sku_num,
	   p.grp_desc AS Sub_division,
       v.vendor_brand_name AS brand_name,
       v.vendor_brand_code AS brand_id,
       CAST('Torso' AS VARCHAR(10)) AS brand_tier
FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW p
  INNER JOIN PRD_NAP_USR_VWS.PRODUCT_STYLE_DIM q
          ON p.epm_style_num = q.epm_style_num
         AND p.channel_country = q.channel_country
         AND p.channel_country = 'US'
  INNER JOIN PRD_NAP_USR_VWS.vendor_label_dim v
          ON q.vendor_label_code = v.vendor_label_code
         AND (v.vendor_brand_code) IN (SELECT DISTINCT brand_id FROM T2DL_DAS_NMN.nmn_torso_brands)
) WITH DATA PRIMARY INDEX (sku_num) ON COMMIT PRESERVE ROWS;


--Drop table brand_visits;
CREATE MULTISET VOLATILE TABLE brand_visits AS (
    SELECT
     x.acp_id
    ,x.brand_name
    ,x.business_day_date
    ,x.brand_tier
    ,x.Sub_division
    ,Max(x.is_brand_day) AS is_brand_day
    ,Rank() Over(PARTITION BY x.acp_id, x.brand_name, x.brand_tier, x.Sub_division ORDER BY x.business_day_date DESC) AS visit_rank
    FROM (
        SELECT
              s.acp_id
             ,np.brand_name
             ,np.brand_tier
             ,np.Sub_division
            ,case when business_day_date between (select start_date from _variables) and (select end_date from _variables) then (select start_date from _variables) else business_day_date end as business_day_date
            ,Max(CASE WHEN np.sku_num IS NOT NULL THEN 1 ELSE 0 end) is_brand_day
        FROM T2DL_DAS_SALES_RETURNS.sales_and_returns_fact r
        INNER JOIN shoppers s
            ON r.acp_id = s.acp_id
        LEFT JOIN brand_product np
            ON r.sku_num = np.sku_num
        WHERE 1=1
            AND r.transaction_type = 'retail'
            AND s.acp_id IS NOT NULL
            AND r.business_day_date <= (SELECT end_date FROM _variables) 
            AND r.business_unit_desc in ('N.COM', 'FULL LINE')
        GROUP BY
            1,2,3,4,5
    ) x
    WHERE 1=1
        AND is_brand_day = 1
    GROUP BY
    1,2,3,4,5
)
WITH DATA
PRIMARY INDEX(acp_id)
ON COMMIT PRESERVE ROWS;

--Drop table new_visits;
CREATE MULTISET VOLATILE TABLE new_visits AS (
SELECT
     acp_id
    ,brand_name
    ,brand_tier
    ,Sub_division
    ,Max(business_day_date) AS latest_brand_visit
    ,Max(CASE WHEN visit_rank = 2 THEN business_day_date ELSE NULL END) AS second_latest_brand_visit
    ,CASE
        WHEN second_latest_brand_visit IS NULL and latest_brand_visit between (select start_date from _variables) and (select end_date from _variables)  THEN 1
     ELSE 0 END AS new_to_brand_flag
    ,CASE
        WHEN second_latest_brand_visit IS NULL THEN 0
        WHEN latest_brand_visit between (select start_date from _variables) and (select end_date from _variables) and (latest_brand_visit - second_latest_brand_visit < 365) THEN 1
     ELSE 0 END AS retained_flag
    ,CASE
        WHEN second_latest_brand_visit IS NULL THEN 0
        WHEN latest_brand_visit between (select start_date from _variables) and (select end_date from _variables) and (latest_brand_visit - second_latest_brand_visit > 365) THEN 1
     ELSE 0 END AS reactivated_flag
    ,CASE        
        WHEN latest_brand_visit < (select start_date from _variables) and ((select start_date from _variables) -latest_brand_visit) between 1 and 365  THEN 1
     ELSE 0 END AS lapsed_flag
FROM brand_visits
GROUP BY 1,2,3,4
)
WITH DATA
PRIMARY INDEX(acp_id)
ON COMMIT PRESERVE ROWS;

--Drop table final_table;
CREATE MULTISET VOLATILE TABLE final_table
AS
(SELECT 
	   brand_name, 
	   brand_tier, 
	   Sub_division, 
	   SUM(active) AS active,
       SUM("new") AS "new",
       SUM(retained) AS retained,
       SUM(reactivated) AS reactivated,
       sum(lapsed) as lapsed
FROM (SELECT 
			 brand_name, 
			 brand_tier, 
			 Sub_division,
			 CAST(0 AS FLOAT) AS active,
             SUM(new_to_brand_flag) AS "new",
             SUM(retained_flag) AS retained,
             SUM(reactivated_flag) AS reactivated,
             sum(lapsed_flag) as lapsed
      FROM new_visits  group by 1,2,3   
      UNION ALL
      SELECT 
			 brand_name, 
			 brand_tier, 
			 Sub_division,
      		 COUNT(DISTINCT acp_id) AS active,
             CAST(0 AS FLOAT) AS "new",
             CAST(0 AS FLOAT) AS retained,
             CAST(0 AS FLOAT) AS reactivated,
             CAST(0 AS FLOAT) AS lapsed
      FROM brand_visits
      WHERE business_day_date BETWEEN (SELECT start_date FROM _variables) AND (SELECT end_date FROM _variables)group by 1,2,3 ) a group by 1,2,3 ) WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;
     
--drop table final_insert;
CREATE MULTISET VOLATILE TABLE final_insert AS (
SELECT
	 case when nv.brand_name='URBAN OUTFITTERS' then 'FREE PEOPLE - URBAN OUTFITTERS' 
	 else nv.brand_name end as brand
	,month_idnt
	,brand_tier
	,Sub_division
    ,active
    ,"new"
    ,retained
    ,reactivated
    ,lapsed
FROM final_table nv, _variables
)WITH DATA
PRIMARY INDEX(brand)
ON COMMIT PRESERVE ROWS;


DELETE from T2DL_DAS_NMN.nmn_brand_subdiv_customer_metrics where month_idnt=(select month_idnt from _variables);
insert into T2DL_DAS_NMN.nmn_brand_subdiv_customer_metrics select a.*,Current_Timestamp AS dw_sys_load_tmstp from final_insert a;

SET QUERY_BAND = NONE FOR SESSION;