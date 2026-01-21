SET QUERY_BAND = 'App_ID=APP08172;
     DAG_ID=nmn_weekly_fact_11521_ACE_ENG;
     Task_Name=nmn_weekly_fact;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2dl_DAS_NMN.nmn_weekly_fact
Team/Owner: NMN
Date Created/Modified:

Note:
-- To Support the NMN Report view 1 
-- Weekly Refresh
*/

CREATE Multiset VOLATILE TABLE _variable
AS
(
  select distinct week_idnt as week_idnt, min(day_date) as day_date from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date=date'2021-01-01'  group by 1
)WITH data PRIMARY INDEX (week_idnt,day_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE _variable_1
AS
(
  select distinct week_idnt as week_idnt, min(day_date) as day_date from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date=date'2024-02-07'  group by 1
)WITH data PRIMARY INDEX (week_idnt,day_date) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE sku_table
AS
(SELECT DISTINCT Trim(sku.rms_sku_num) AS sku_num,
       sku.web_style_num as web_style_num,
       sku.channel_country AS country,
       sku.div_desc AS division_desc,
       sku.div_num AS division_num,
       sku.grp_desc AS subdivision_desc,
       sku.grp_num AS subdivision_num,
       sku.dept_desc AS department_desc,
       sku.dept_num AS department_num,
       sku.class_desc AS class_desc,
       sku.class_num AS class_num,
       sku.brand_name AS brand_name,
       v.npg_flag,
       v.vendor_name AS supplier_name
FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
  LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM v ON sku.prmy_supp_num = v.vendor_num
  LEFT JOIN PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM vr ON sku.prmy_supp_num = vr.order_from_vendor_num
WHERE 1 = 1
AND   v.vendor_status_code = 'A'
AND   vr.payto_vendor_status_code = 'A')
WITH DATA PRIMARY INDEX (sku_num,web_style_num,country) ON COMMIT PRESERVE ROWS;

--- Create Mapping for Last year

CREATE Multiset VOLATILE TABLE last_year_week_mapping
AS
(SELECT DISTINCT trim(a.week_idnt) AS this_year_wk_idnt,
       trim(b.week_idnt) AS last_year_wk_idnt
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM a
  LEFT JOIN (SELECT DISTINCT week_idnt,
                    day_idnt
             FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM) b ON b.day_idnt = a.day_idnt_last_year_realigned
WHERE a.week_idnt BETWEEN 201901 AND 202552)
WITH data PRIMARY INDEX (this_year_wk_idnt) ON COMMIT PRESERVE ROWS;

CREATE multiset volatile TABLE sales_fact_intermediate_table
AS
(SELECT a.channel_num as channel_num,
       a.channel_country AS country,
       trim(dd.week_idnt) AS week_idnt,
       trim(dd.month_idnt) AS month_idnt,
       trim(dd.quarter_idnt) AS quarter_idnt,
       trim(dd.fiscal_year_num) AS fiscal_year_num,
       e.last_year_wk_idnt AS last_year_week_idnt,
       COALESCE (a.mktg_type,'') AS mktg_type,
       COALESCE (a.finance_rollup,'') AS finance_rollup,
       COALESCE (a.finance_detail,'') AS finance_detail,
       COALESCE(CASE WHEN a.utm_channel LIKE '%_ex_%' THEN 1 ELSE 0 END,'') AS nmn_ind,
       COALESCE (b.division_desc,'') AS division_desc,
       COALESCE (b.division_num,'') AS division_num,
       COALESCE (b.subdivision_desc,'') AS subdivision_desc,
       COALESCE (b.subdivision_num,'') AS subdivision_num,
       COALESCE (b.department_desc,'') AS department_desc,
       COALESCE (b.department_num,'') AS department_num,
       COALESCE (b.class_desc,'') AS class_desc,
       COALESCE (b.class_num,'') AS class_num,
       COALESCE (a.brand_name,'') AS brand_name,
       COALESCE (b.npg_flag,'') AS npg_flg,
       COALESCE (a.supplier_name,'') AS supplier_name,
       COALESCE (SUM(0),0) AS product_views,
	   COALESCE (SUM(0),0) AS instock_product_views,
       COALESCE (SUM(0),0) AS orders,
       COALESCE (SUM(a.gross_sales_amt_usd),0) AS gross_sales,
       COALESCE (SUM(a.net_sales_amt_usd),0) AS net_sales,
       COALESCE (SUM(a.net_sales_units),0) AS net_sales_units
FROM T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_SALES_FACT a
  LEFT JOIN sku_table b
         ON a.sku_num = b.sku_num
        AND a.channel_country = b.country
  INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd ON a.business_day_date = dd.DAY_DATE
  LEFT JOIN last_year_week_mapping e ON dd.week_idnt = e.this_year_wk_idnt
  where a.business_day_date between (select day_date from _variable) and (select day_date from _variable_1)
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14,
         15,
         16,
         17,
         18,
         19,
         20,
         21,
		 22) WITH DATA PRIMARY INDEX (country,channel_num,week_idnt,class_num,division_num,subdivision_num,mktg_type,finance_rollup,finance_detail) ON COMMIT PRESERVE ROWS;

CREATE MULTISET Volatile TABLE sales_fact_intermediate_table_final ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      channel_num INTEGER,
      country VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      week_idnt INTEGER,
      month_idnt INTEGER,
      quarter_idnt INTEGER,
      fiscal_year INTEGER,
      last_year_week_idnt VARCHAR(11) CHARACTER SET LATIN NOT CASESPECIFIC,
      mktg_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  nmn_ind INTEGER,
      division_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      division_num INTEGER,
      subdivision_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subdivision_num INTEGER,
      department_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      department_num INTEGER,
      class_desc VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_num INTEGER,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_flg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier_name VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      product_views INTEGER,
	  instock_product_views INTEGER,
      orders INTEGER,
      gross_sales DECIMAL(38,2),
      net_sales DECIMAL(38,2),
      net_sales_units DECIMAL(38,0))
PRIMARY INDEX ( channel_num ,country ,week_idnt,
mktg_type ,finance_rollup ,finance_detail ,division_num ,class_num,subdivision_num ,
brand_name ,supplier_name )ON COMMIT PRESERVE ROWS;
        
Insert into sales_fact_intermediate_table_final select * from sales_fact_intermediate_table;

CREATE MULTISET VOLATILE TABLE sku_table_1
AS
(SELECT DISTINCT trim(web_style_num) AS web_style_num,
       sku.country AS country,
       sku.division_desc AS division_desc,
       sku.division_num AS division_num,
       sku.subdivision_desc AS subdivision_desc,
       sku.subdivision_num AS subdivision_num,
       sku.department_desc AS department_desc,
       sku.department_num AS department_num,
       sku.class_desc AS class_desc,
       sku.class_num AS class_num,
       sku.brand_name AS brand_name,
       sku.npg_flag,
       sku.supplier_name AS supplier_name
FROM sku_table sku)
WITH DATA PRIMARY INDEX (web_style_num,country) ON COMMIT PRESERVE ROWS;

CREATE multiset volatile TABLE funnel_fact_intermediate_table
AS
(SELECT a.channel_num as channel_num,
       a.channel_country AS country,
       trim(dd.week_idnt) AS week_idnt,
       trim(dd.month_idnt) AS month_idnt,
       trim(dd.quarter_idnt) AS quarter_idnt,
       trim(dd.fiscal_year_num) AS fiscal_year_num,
       e.last_year_wk_idnt AS last_year_week_idnt,
	   COALESCE (a.mktg_type,'') AS mktg_type,
       COALESCE (a.finance_rollup,'') AS finance_rollup,
       COALESCE (a.finance_detail,'') AS finance_detail,
       COALESCE(CASE WHEN a.utm_channel LIKE '%_ex_%' THEN 1 ELSE 0 END,'') AS nmn_ind,
       COALESCE (b.division_desc,'') AS division_desc,
       COALESCE (b.division_num,'') AS division_num,
       COALESCE (b.subdivision_desc,'') AS subdivision_desc,
       COALESCE (b.subdivision_num,'') AS subdivision_num,
       COALESCE (b.department_desc,'') AS department_desc,
       COALESCE (b.department_num,'') AS department_num,
       COALESCE (b.class_desc,'') AS class_desc,
       COALESCE (b.class_num,'') AS class_num,
       COALESCE (a.brand_name,'') AS brand_name,
       COALESCE (b.npg_flag,'') AS npg_flg,
       COALESCE (a.supplier_name,'') AS supplier_name,
       COALESCE (SUM(a.product_views),0) AS product_views,
	   COALESCE (SUM(a.instock_product_views),0) AS instock_product_views,
       COALESCE (SUM(a.order_units),0) AS orders,
       COALESCE (SUM(0),0) AS gross_sales,
       COALESCE (SUM(0),0) AS net_sales,
       COALESCE (SUM(0),0) AS net_sales_units
FROM T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT a
  LEFT JOIN sku_table_1 b
         ON trim (a.web_style_num) = trim (b.web_style_num)
        AND a.channel_country = b.country
  INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd ON a.activity_date_pacific = dd.DAY_DATE
  LEFT JOIN last_year_week_mapping e ON dd.week_idnt = e.this_year_wk_idnt
  where a.activity_date_pacific between (select day_date from _variable) and (select day_date from _variable_1)
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14,
         15,
         16,
         17,
         18,
         19,
         20,
         21,
		 22) WITH DATA PRIMARY INDEX (country,channel_num,week_idnt,class_num,division_num,subdivision_num,mktg_type,finance_rollup,finance_detail) ON COMMIT PRESERVE ROWS;

CREATE MULTISET Volatile TABLE funnel_fact_intermediate_table_final ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      channel_num INTEGER,
      country VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      week_idnt INTEGER,
      month_idnt INTEGER,
      quarter_idnt INTEGER,
      fiscal_year INTEGER,
      last_year_week_idnt VARCHAR(11) CHARACTER SET LATIN NOT CASESPECIFIC,
      mktg_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  nmn_ind INTEGER,
      division_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      division_num INTEGER,
      subdivision_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subdivision_num INTEGER,
      department_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      department_num INTEGER,
      class_desc VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_num INTEGER,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_flg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier_name VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      product_views INTEGER,
	  instock_product_views INTEGER,
      orders INTEGER,
      gross_sales DECIMAL(38,2),
      net_sales DECIMAL(38,2),
      net_sales_units DECIMAL(38,0))
PRIMARY INDEX ( channel_num ,country ,week_idnt,
mktg_type ,finance_rollup ,finance_detail ,division_num ,class_num,subdivision_num ,
brand_name ,supplier_name )ON COMMIT PRESERVE ROWS;
        
Insert into funnel_fact_intermediate_table_final select * from funnel_fact_intermediate_table;

CREATE multiset volatile TABLE nmn_reporting_stack_intermediate_table
AS
(SELECT COALESCE(a.channel_num,b.channel_num) AS channel_num,
       COALESCE(a.country,b.country) AS country,
       COALESCE(a.week_idnt,b.week_idnt) AS week_idnt,
       COALESCE(a.month_idnt,b.month_idnt) AS month_idnt,
       COALESCE(a.quarter_idnt,b.quarter_idnt) AS quarter_idnt,
       COALESCE(a.fiscal_year,b.fiscal_year) AS fiscal_year_num,
       COALESCE(a.last_year_week_idnt,b.last_year_week_idnt) AS last_year_week_idnt,
       COALESCE(a.mktg_type,b.mktg_type) AS mktg_type,
       COALESCE(a.finance_rollup,b.finance_rollup) AS finance_rollup,
       COALESCE(a.finance_detail,b.finance_detail) AS finance_detail,
	   COALESCE(a.nmn_ind,b.nmn_ind) AS nmn_ind,
       COALESCE(a.division_desc,b.division_desc) AS division_desc,
       COALESCE(a.division_num,b.division_num) AS division_num,
       COALESCE(a.subdivision_desc,b.subdivision_desc) AS subdivision_desc,
       COALESCE(a.subdivision_num,b.subdivision_num) AS subdivision_num,
       COALESCE(a.department_desc,b.department_desc) AS department_desc,
       COALESCE(a.department_num,b.department_num) AS department_num,
       COALESCE(a.class_desc,b.class_desc) AS class_desc,
       COALESCE(a.class_num,b.class_num) AS class_num,
       COALESCE(a.brand_name,b.brand_name) AS brand_name,
       COALESCE(a.npg_flg,b.npg_flg) AS npg_flg,
       COALESCE(a.supplier_name,b.supplier_name) AS supplier_name,
       COALESCE(SUM(b.product_views),0) AS product_views,
	   COALESCE(SUM(b.instock_product_views),0) AS instock_product_views,
       COALESCE(SUM(b.orders),0) AS orders,
       COALESCE(SUM(a.gross_sales),0) AS gross_sales,
       COALESCE(SUM(a.net_sales),0) AS net_sales,
       COALESCE(SUM(a.net_sales_units),0) AS net_sales_units
FROM sales_fact_intermediate_table_final a
  FULL OUTER JOIN funnel_fact_intermediate_table_final b
               ON a.channel_num = b.channel_num
              AND a.country = b.country
              AND a.week_idnt = b.week_idnt
              AND a.month_idnt = b.month_idnt
              AND a.mktg_type = b.mktg_type
              AND a.finance_rollup = b.finance_rollup
              AND a.finance_detail = b.finance_detail
			  AND a.nmn_ind = b.nmn_ind
              AND a.division_num = b.division_num
              AND a.subdivision_num = b.subdivision_num
              AND a.department_num = b.department_num
              AND a.class_num = b.class_num
              AND a.brand_name = b.brand_name
              AND a.supplier_name = b.supplier_name
              AND a.npg_flg = b.npg_flg
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14,
         15,
         16,
         17,
         18,
         19,
         20,
         21,
		 22) WITH DATA PRIMARY INDEX (country,channel_num,week_idnt,class_num,division_num,subdivision_num,mktg_type,finance_rollup,finance_detail) ON COMMIT PRESERVE ROWS;


CREATE MULTISET Volatile TABLE nmn_reporting_stack_final_table ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      channel_num INTEGER,
      country VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      week_idnt INTEGER,
      month_idnt INTEGER,
      quarter_idnt INTEGER,
      fiscal_year INTEGER,
      last_year_week_idnt VARCHAR(11) CHARACTER SET LATIN NOT CASESPECIFIC,
      mktg_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  nmn_ind INTEGER,
      division_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      division_num INTEGER,
      subdivision_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subdivision_num INTEGER,
      department_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      department_num INTEGER,
      class_desc VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_num INTEGER,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_flg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier_name VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      product_views INTEGER,
	  instock_product_views INTEGER,
      orders INTEGER,
      gross_sales DECIMAL(38,2),
      net_sales DECIMAL(38,2),
      net_sales_units DECIMAL(38,0))
PRIMARY INDEX (channel_num ,country ,week_idnt,
mktg_type ,finance_rollup ,finance_detail ,division_num ,class_num,subdivision_num ,
brand_name ,supplier_name )ON COMMIT PRESERVE ROWS;

Insert into nmn_reporting_stack_final_table select * from nmn_reporting_stack_intermediate_table;
       
delete T2DL_DAS_NMN.nmn_weekly_fact where week_idnt between (select week_idnt from _variable) and (select week_idnt from _variable_1);

INSERT INTO T2DL_DAS_NMN.nmn_weekly_fact
select a.*,CURRENT_TIMESTAMP as dw_sys_load_tmstp from nmn_reporting_stack_final_table a;

COLLECT STATISTICS  
                    COLUMN (channel_num), 
                    COLUMN (channel_country),
                    COLUMN (week_idnt),
                    COLUMN (last_year_week_idnt),
                    COLUMN (mktg_type),
                    COLUMN (finance_rollup),
                    COLUMN (finance_detail),
                    COLUMN (div_num),
                    COLUMN (grp_desc),
                    COLUMN (brand_name),
                    COLUMN (supplier_name)
on T2DL_DAS_NMN.nmn_weekly_fact;

SET QUERY_BAND = NONE FOR SESSION;
