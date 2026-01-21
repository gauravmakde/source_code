SET QUERY_BAND = 'App_ID=APP08172;
     DAG_ID=nmn_weekly_overall_fact_11521_ACE_ENG;
     Task_Name=nmn_weekly_overall_fact;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2dl_DAS_NMN.nmn_weekly_overall_fact
Team/Owner: NMN
Date Created/Modified:

Note:
-- To Support the NMN Report view 1 
-- Weekly Refresh
*/

CREATE Multiset VOLATILE TABLE variable_start_date
AS
(
  select distinct week_idnt as week_idnt, min(day_date) as day_date from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date={start_date}  group by 1
)WITH data PRIMARY INDEX (week_idnt,day_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE variable_end_date
AS
(
  select distinct week_idnt as week_idnt, min(day_date) as day_date from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date={end_date}  group by 1
)WITH data PRIMARY INDEX (week_idnt,day_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE LY_week_mapping
AS
(SELECT	
	   DISTINCT trim(a.week_idnt) AS week_idnt,
       trim(b.week_idnt) AS last_year_wk_idnt,
       trim(a.month_idnt) AS month_idnt,
       trim(a.quarter_idnt) AS quarter_idnt,
       trim(a.fiscal_year_num) AS fiscal_year_num
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM a
  LEFT JOIN (SELECT DISTINCT week_idnt,
                    day_idnt,
                    day_date
             FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM) b ON b.day_idnt = a.day_idnt_last_year
WHERE a.week_idnt BETWEEN 201901 AND 202552)
WITH data PRIMARY INDEX (week_idnt) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE sku_id_map
AS
(SELECT DISTINCT Trim(sku.rms_sku_num) AS rms_sku_num,
       sku.channel_country AS channel_country,
       trim(CONCAT (sku.div_num,', ',sku.div_desc)) AS div_desc,
       sku.div_num AS div_num,
       trim(CONCAT (sku.grp_num,', ',sku.grp_desc)) AS grp_desc,
       sku.grp_num AS grp_num,
       trim(CONCAT (sku.dept_num,', ',sku.dept_desc)) AS dept_desc,
       sku.dept_num AS dept_num,
       trim(CONCAT (sku.class_num,', ',sku.class_desc)) AS class_desc,
       sku.class_num AS class_num,
       sku.brand_name AS brand_name,
       v.npg_flag,
       v.vendor_name as vendor_name
FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
  LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM v ON sku.prmy_supp_num = v.vendor_num
  LEFT JOIN PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM vr ON sku.prmy_supp_num = vr.order_from_vendor_num
WHERE 1 = 1
AND   v.vendor_status_code = 'A'
AND   vr.payto_vendor_status_code = 'A'
AND   sku.channel_country='US')
WITH DATA PRIMARY INDEX (rms_sku_num,channel_country) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE sales_units_week_level
AS
(SELECT a.chnl_idnt as chnl_idnt,
       trim(a.week_idnt) as week_idnt,
       trim(lw.last_year_wk_idnt) AS last_year_wk_idnt,
       trim(lw.month_idnt) AS month_idnt,
       trim(lw.quarter_idnt) AS quarter_idnt,
       trim(lw.fiscal_year_num) AS fiscal_year_num,
       a.country as country,
       a.sku_idnt as sku_idnt,
       SUM(a.sales_dollars) AS sales_dollars,
       SUM(a.return_dollars) AS return_dollars ,
       SUM(a.sales_units) AS sales_units ,
       SUM(a.return_units) AS return_units,
       SUM(a.eoh_units) AS eoh_units
FROM t2dl_das_in_season_management_reporting.wbr_merch_staging a
  LEFT JOIN LY_week_mapping lw ON a.week_idnt = lw.week_idnt
  where a.week_idnt between (select week_idnt from variable_start_date) and (select week_idnt from variable_end_date)
  AND a.country='US'
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8) WITH data PRIMARY INDEX (chnl_idnt,country,WEEK_IDNT,sku_idnt) ON COMMIT PRESERVE ROWS;
  
        
CREATE Multiset VOLATILE TABLE sales_units_final
AS
(SELECT    a.chnl_idnt,
		   a.week_idnt,
		   a.last_year_wk_idnt,
		   a.month_idnt,
		   a.quarter_idnt,
		   a.fiscal_year_num,
           b.channel_country,
           b.div_desc,
           b.div_num,
           b.grp_desc,
           b.grp_num,
           b.dept_desc,
           b.dept_num,
           b.class_desc,
           b.class_num,
           b.brand_name,
           b.npg_flag,
           b.vendor_name,
       SUM(a.sales_dollars) AS sales_dollars,
       SUM(a.return_dollars) AS return_dollars,
       SUM(a.sales_units) AS sales_units,
       SUM(a.return_units) AS return_units,
       SUM(a.eoh_units) AS eoh_units
FROM sales_units_week_level a
  LEFT JOIN sku_id_map b ON a.sku_idnt = b.rms_sku_num and a.country=b.channel_country
  where a.country = 'US' and a.week_idnt between (select week_idnt from variable_start_date) and (select week_idnt from variable_end_date)
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
         18) WITH data PRIMARY INDEX (chnl_idnt,channel_country,week_idnt,div_desc,grp_desc,dept_desc,class_desc,brand_name,vendor_name) ON COMMIT PRESERVE ROWS;
        
create multiset volatile table receipts_week_level
as
(
SELECT
     a.RMS_SKU_NUM,
     a.CHANNEL_NUM,
     a.WEEK_NUM,
     b.last_year_wk_idnt as last_year_wk_idnt,
     COALESCE(SUM(receipts_regular_units + receipts_crossdock_regular_units) ,0) AS receipt_units
FROM prd_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw a
LEFT JOIN LY_week_mapping b ON a.WEEK_NUM = b.week_idnt
WHERE WEEK_NUM between (select week_idnt from variable_start_date) and (select week_idnt from variable_end_date)
GROUP BY 
    1,2,3,4
)WITH data PRIMARY INDEX (rms_sku_num,channel_num,week_num) ON COMMIT PRESERVE ROWS;


CREATE Multiset VOLATILE TABLE receipts_table_final
AS
(
	select a.channel_num as channel_num,
	trim(a.week_num) as week_num,
    trim(a.last_year_wk_idnt) as last_year_wk_idnt,
	trim(dd.month_idnt) as month_idnt,
	trim(dd.quarter_idnt) as quarter_idnt,
	trim(dd.fiscal_year_num) as fiscal_year_num,
	b.channel_country,
	b.div_desc,
	b.div_num,
	b.brand_name,
	b.npg_flag,
	b.grp_desc,
	b.grp_num,
	b.dept_desc,
	b.dept_num,
	b.class_desc,
	b.class_num,
	b.vendor_name,
	sum(a.receipt_units) as receipt_units
	from receipts_week_level a 
	left join (select distinct channel_num,store_country_code from PRD_NAP_USR_VWS.STORE_DIM) c on a.channel_num=c.channel_num
	left Join sku_id_map b on a.rms_sku_num=b.rms_sku_num and c.store_country_code=b.channel_country
	INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
    ON a.week_num  = dd.week_idnt 
    where a.week_num between (select week_idnt from variable_start_date) and (select week_idnt from variable_end_date)
    AND c.store_country_code='US'
    Group by 1,
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
         18
) WITH data PRIMARY INDEX (channel_num, channel_country, week_num,last_year_wk_idnt,div_desc,div_num,grp_desc,grp_num,class_desc,class_num,dept_desc,dept_num,brand_name,npg_flag,vendor_name) ON COMMIT preserve rows;


CREATE multiset volatile TABLE receipts_sales_merged
AS
(SELECT COALESCE(a.chnl_idnt,b.channel_num) AS channel_num,   
       COALESCE(a.channel_country,b.channel_country) AS channel_country,
       COALESCE(a.week_idnt,b.week_num) AS week_idnt,
       COALESCE(a.last_year_wk_idnt,b.last_year_wk_idnt) AS last_year_wk_idnt,
       COALESCE(a.month_idnt,b.month_idnt) AS month_idnt,
       COALESCE(a.quarter_idnt,b.quarter_idnt) AS quarter_idnt,
       COALESCE(a.fiscal_year_num,b.fiscal_year_num) AS fiscal_year_num,
       COALESCE(a.div_desc,b.div_desc) AS div_desc,
       COALESCE(a.div_num,b.div_num) AS div_num,
       COALESCE(a.grp_desc,b.grp_desc) AS grp_desc,
       COALESCE(a.grp_num,b.grp_num) AS grp_num,
       COALESCE(a.dept_desc,b.dept_desc) AS dept_desc,
       COALESCE(a.dept_num,b.dept_num) AS dept_num,
       COALESCE(a.class_desc,b.class_desc) AS class_desc,
       COALESCE(a.Class_num,b.Class_num) AS class_num,
       COALESCE(a.brand_name,b.brand_name) AS brand_name,
       COALESCE(a.npg_flag,b.npg_flag) AS npg_flag,
       COALESCE(a.vendor_name,b.vendor_name) AS vendor_name,
       COALESCE(SUM(a.sales_dollars),0) AS sales_dollars,
       COALESCE(SUM(a.sales_units),0) AS sales_units,
       COALESCE(SUM(a.return_dollars),0) AS return_dollars,
       COALESCE(SUM(a.return_units),0) AS return_units,
       COALESCE(SUM(b.receipt_units),0) AS receipt_units,
       COALESCE(SUM(a.eoh_units),0) AS eoh_units
FROM sales_units_final a
  FULL OUTER JOIN receipts_table_final b
              ON  a.chnl_idnt = b.channel_num
              AND a.channel_country = b.channel_country
              AND a.week_idnt = b.week_num
              AND a.last_year_wk_idnt = b.last_year_wk_idnt
              AND a.month_idnt = b.month_idnt
              AND a.quarter_idnt = b.quarter_idnt
              AND a.fiscal_year_num = b.fiscal_year_num
              AND a.div_num = b.div_num
              AND a.div_desc = b.div_desc
              AND a.grp_desc = b.grp_desc
              AND a.grp_num = b.grp_num
              AND a.dept_desc = b.dept_desc
              AND a.dept_num = b.dept_num
              AND a.class_desc = b.class_desc
              AND a.class_num = b.class_num
              AND a.brand_name = b.brand_name
              AND a.npg_flag = b.npg_flag
              AND a.vendor_name = b.vendor_name
where a.channel_country = 'US'
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
         18
) WITH data PRIMARY INDEX (channel_num, channel_country, week_idnt,last_year_wk_idnt, month_idnt, quarter_idnt, fiscal_year_num,div_desc,div_num,grp_desc,grp_num,class_desc,class_num,dept_desc,dept_num,brand_name,npg_flag,vendor_name) ON COMMIT preserve rows;

create multiset volatile table PV_Order_week_level
AS
(select CASE
         WHEN channel ='FULL_LINE' then '120'
         WHEN channel = 'RACK' then '250'
         ELSE 'Others'
        END AS channel_num,
		trim(a.week_idnt) as week_idnt,
		trim(a.month_idnt) AS month_idnt,
        trim(a.quarter_idnt) AS quarter_idnt,
        trim(a.fiscal_year_num) AS fiscal_year_num,
		b.sku_id,
		b.channelcountry,
		sum(b.product_views) AS product_views,
        sum(b.order_quantity) AS order_quantity
      from T2DL_DAS_PRODUCT_FUNNEL.product_funnel_daily b
      LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM a ON a.day_date = b.event_date_pacifiC
	  where b.event_date_pacifiC between (select day_date from variable_start_date) and (select day_date from variable_end_date)
    AND b.channelcountry='US'
      GROUP BY 1,
        2,
        3,
        4,
        5,
        6,
        7) with data primary index (channel_num,week_idnt, month_idnt, quarter_idnt, sku_ID) ON COMMIT PRESERVE ROWS;  
         
create multiset volatile table PV_Order_final
AS
(select a.channel_num as channel_num,
		a.week_idnt,
		c.last_year_wk_idnt,
		a.month_idnt,
		a.quarter_idnt,
		a.fiscal_year_num,
		b.channel_country,
        b.div_desc,
        b.div_num,
        b.grp_desc,
        b.grp_num,
        b.dept_desc,
        b.dept_num,
        b.class_desc,
        b.class_num,
        b.brand_name,
        b.npg_flag,
        b.vendor_name,
		sum(a.product_views) as product_views,
		sum(a.order_quantity) as order_quantity
		from PV_Order_week_level a
		left join sku_id_map b on b.rms_sku_num = a.sku_id and b.channel_country = a.channelcountry
		left join LY_week_mapping c on c.week_idnt = a.week_idnt	
		where a.channelcountry = 'US' and a.week_idnt between (select week_idnt from variable_start_date) and (select week_idnt from variable_end_date)
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
         18)WITH data PRIMARY INDEX (channel_num,week_idnt,div_desc,grp_desc,dept_desc,class_desc,brand_name,vendor_name) ON COMMIT PRESERVE ROWS;
        
CREATE multiset volatile TABLE merged_table_TY
AS
(SELECT COALESCE(a.channel_num,b.channel_num) AS channel_num,
       COALESCE(a.channel_country,b.channel_country) AS channel_country,
       COALESCE(a.week_idnt,b.week_idnt) AS week_idnt,
       COALESCE(a.last_year_wk_idnt,b.last_year_wk_idnt) AS last_year_wk_idnt,
       COALESCE(a.month_idnt,b.month_idnt) AS month_idnt,
       COALESCE(a.quarter_idnt,b.quarter_idnt) AS quarter_idnt,
       COALESCE(a.fiscal_year_num,b.fiscal_year_num) AS fiscal_year_num,
       COALESCE(a.div_desc,b.div_desc) AS div_desc,
       COALESCE(a.div_num,b.div_num) AS div_num,
       COALESCE(a.grp_desc,b.grp_desc) AS grp_desc,
       COALESCE(a.grp_num,b.grp_num) AS grp_num,
       COALESCE(a.dept_desc,b.dept_desc) AS dept_desc,
       COALESCE(a.dept_num,b.dept_num) AS dept_num,
       COALESCE(a.class_desc,b.class_desc) AS class_desc,
       COALESCE(a.Class_num,b.Class_num) AS class_num,
       COALESCE(a.brand_name,b.brand_name) AS brand_name,
       COALESCE(a.npg_flag,b.npg_flag) AS npg_flag,
       COALESCE(a.vendor_name,b.vendor_name) AS vendor_name,
       COALESCE(SUM(a.sales_dollars),0) AS sales_dollars,
       COALESCE(SUM(a.sales_units),0) AS sales_units,
       COALESCE(SUM(a.return_dollars),0) AS return_dollars,
       COALESCE(SUM(a.return_units),0) AS return_units,
       COALESCE(sum(a.receipt_units),0) as receipt_units,
       COALESCE(SUM(b.product_views),0) AS product_views,
       COALESCE(SUM(b.order_quantity),0) AS order_quantity,
       COALESCE(SUM(a.eoh_units),0) AS eoh_units
FROM receipts_sales_merged a
  FULL OUTER JOIN PV_Order_final b
              ON  a.channel_num = b.channel_num
              AND a.channel_country = b.channel_country
              AND a.week_idnt = b.week_idnt
              AND a.last_year_wk_idnt = b.last_year_wk_idnt
              AND a.month_idnt = b.month_idnt
              AND a.quarter_idnt = b.quarter_idnt
              AND a.fiscal_year_num = b.fiscal_year_num
              AND a.div_num = b.div_num
              AND a.div_desc = b.div_desc
              AND a.grp_desc = b.grp_desc
              AND a.grp_num = b.grp_num
              AND a.dept_desc = b.dept_desc
              AND a.dept_num = b.dept_num
              AND a.class_desc = b.class_desc
              AND a.class_num = b.class_num
              AND a.brand_name = b.brand_name
              AND a.npg_flag = b.npg_flag
              AND a.vendor_name = b.vendor_name
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
         18
) WITH data PRIMARY INDEX (channel_num, channel_country, week_idnt,last_year_wk_idnt, month_idnt, quarter_idnt,div_desc,div_num,grp_desc,grp_num,class_desc,class_num,dept_desc,dept_num,brand_name,npg_flag,vendor_name) ON COMMIT preserve rows;
         
delete {nmn_t2_schema}.nmn_weekly_overall_fact where week_idnt between (select week_idnt from variable_start_date) and (select week_idnt from variable_end_date);

INSERT INTO {nmn_t2_schema}.nmn_weekly_overall_fact
select a.*,CURRENT_TIMESTAMP as dw_sys_load_tmstp from merged_table_TY a;

COLLECT STATISTICS  
                    COLUMN (channel_num), 
                    COLUMN (channel_country),
                    COLUMN (week_idnt),
                    COLUMN (div_num),
                    COLUMN (grp_num),
                    COLUMN (dept_num),
                    COLUMN (class_num),
                    COLUMN (brand_name),
                    COLUMN (supplier_name)
on {nmn_t2_schema}.nmn_weekly_overall_fact;
SET QUERY_BAND = NONE FOR SESSION;
