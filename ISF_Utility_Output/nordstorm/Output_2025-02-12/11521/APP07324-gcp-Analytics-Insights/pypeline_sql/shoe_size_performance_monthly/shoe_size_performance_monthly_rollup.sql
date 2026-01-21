SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=shoe_size_performance_monthly_rollup_11521_ACE_ENG;
     Task_Name=shoe_size_performance_monthly_rollup;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_ccs_categories.shoe_size_performance_monthly_rollup
Team/Owner: Merchandising Insights
Date Created/Modified: 2/13/2024

Note:
-- Purpose: Normalized shoe sizes with monthly performance source from base table
-- Cadence/lookback window: Runs weekly on Monday at 02:30 to pull TY/LY YTD
*/


-- drop table date_lkup_1
create volatile multiset table date_lkup_1 as (
      SELECT DISTINCT
      re.ty_ly_lly_ind AS ty_ly_ind
      ,re.fiscal_year_num AS year_idnt
      ,re.quarter_abrv
      ,re.quarter_label
      ,re.month_idnt
      ,re.month_abrv
      ,re.month_label
      ,re.month_end_day_date AS month_end_date
      ,dcd.week_idnt AS week_idnt_true
      ,re.week_idnt
      ,re.fiscal_week_num
      ,Trim(Both From re.fiscal_year_num) || ' ' || re.week_desc AS week_label
      ,re.month_abrv || ' ' || 'WK' || Trim(Both From re.week_num_of_fiscal_month) AS week_of_month_abrv
      ,Left(re.week_label, 9) || 'WK' || Trim(Both From re.week_num_of_fiscal_month) AS week_of_month_label
     -- ,re.week_start_day_date
      ,re.week_end_day_date
   FROM prd_nap_vws.realigned_date_lkup_vw re
    LEFT JOIN prd_nap_usr_vws.day_cal_454_dim dcd
    ON dcd.day_date = re.day_date
   WHERE 1=1
   AND ytd_ind = 'Y' 
   AND re.week_end_day_date <= Current_Date - 1 
   AND re.ty_ly_lly_ind IN ('TY','LY')
   AND re.day_date = re.week_end_day_date

)
with data primary index(month_idnt) on commit preserve rows
;

-- select distinct month_idnt from date_lkup_1


-- drop table SUPP_ATTR 
CREATE VOLATILE MULTISET TABLE SUPP_ATTR 
AS (
SELECT
 dept_num
,supplier_num
,preferred_partner_desc as Supplier_Attribute
,CASE WHEN supp.banner = 'FP' THEN 'N' ELSE 'NR' END AS BANNER
FROM PRD_NAP_USR_VWS.SUPP_DEPT_MAP_DIM supp

)WITH DATA PRIMARY INDEX(dept_num, supplier_num, banner)ON COMMIT PRESERVE ROWS; 

COLLECT STATS 
		PRIMARY INDEX (dept_num, supplier_num, banner)
		, COLUMN (Supplier_Attribute)
ON SUPP_ATTR;

--drop table sku_lkup_1
CREATE VOLATILE MULTISET TABLE sku_lkup_1
AS (
SELECT
        p.rms_sku_num
        , Trim(p.div_num) || ', ' || p.div_desc AS division
        , Trim(p.grp_num) || ', ' || p.grp_desc AS subdivision
        , CAST(p.dept_num AS varchar(10)) AS dept_idnt
        , Trim(p.dept_num) || ', ' || p.dept_desc AS dept_label
        , Trim(p.class_num) || ', ' || p.class_desc AS class_label
        , Trim(p.sbclass_num) || ', ' || p.sbclass_desc AS sbclass_label
        , p.prmy_supp_num supp_idnt
        , v.vendor_name as supplier
        , p.brand_name as brand
        , CASE p.npg_ind
        	WHEN 'N' THEN 0
        	WHEN 'Y' THEN 1
          END AS npg_ind
        , c2.ccs_subcategory AS ccs_subcategory
        , Coalesce(map1.category, map2.category, 'OTHER') quantrix_category
        , th.merch_themes
        , si.SIZE_1_CLEAN
        , si.LEFT_CORE_RIGHT
        , si.OVERS_UNDERS
        , si.WIDTHS
        , si.WIDE_CALF
--        , ps.dm_recd_last_updt_dt AS record_date
    FROM prd_nap_usr_vws.product_sku_dim_vw p 
    JOIN prd_nap_usr_vws.item_supplier_size_dim sd
    	ON P.rms_sku_num = sd.rms_sku_id
    LEFT JOIN t2dl_das_ccs_categories.ccs_subcategory_sbclass_map c1
        ON p.dept_num = c1.dept_idnt
        AND p.class_num = c1.class_idnt
        AND p.sbclass_num = c1.sbclass_idnt
    LEFT JOIN t2dl_das_ccs_categories.ccs_subcategories c2 
        ON c1.ccs_subcategory = c2.ccs_subcategory
    LEFT JOIN prd_nap_usr_vws.vendor_dim v
		ON p.prmy_supp_num = v.vendor_num 
	left join prd_nap_usr_vws.catg_subclass_map_dim map1 
  		ON p.dept_num = map1.dept_num
 		AND p.class_num = CAST(map1.class_num AS integer)
 		AND p.sbclass_num = CAST(map1.sbclass_num AS integer)
	left JOIN prd_nap_usr_vws.catg_subclass_map_dim map2 
  		ON p.dept_num = map2.dept_num
 		AND p.class_num = CAST(map2.class_num AS integer)
 		AND map2.sbclass_num = '-1'
	LEFT JOIN t2dl_das_ccs_categories.merch_themes_map th
    	ON p.dept_num = th.dept_idnt
    	AND p.class_num  = th.class_idnt
    	AND p.sbclass_num = th.sbclass_idnt
    LEFT JOIN t2dl_das_ccs_categories.shoe_size_map si
        ON Coalesce(p.dept_num,0) = CAST(Coalesce(SI.DEPT_IDNT,0) AS INTEGER)
        AND Coalesce(sd.supplier_size_1,'null') = Coalesce(si.size_1_idnt,'null')
        AND Coalesce(CASE WHEN sd.supplier_size_1_desc = ' ' THEN 'null' ELSE sd.supplier_size_1_desc END, 'null') = Coalesce(si.size_1_desc, 'null')
        AND Coalesce(sd.supplier_size_2,'null') = Coalesce(si.size_2_idnt,'null')
        AND Coalesce(CASE WHEN sd.supplier_size_2_desc = ' ' THEN 'null' ELSE sd.supplier_size_2_desc END, 'null') = Coalesce(si.size_2_desc, 'null')
	WHERE p.dept_num IN (804,805,806,807,800,801,802,803,835,836) 
		AND P.channel_country = 'US'
		AND si.SIZE_1_CLEAN IN ('4',
	                            '4.5',
	                            '5',
	                            '5.5',
	                            '6',
	                            '6.5',
	                            '7',
	                            '7.5',
	                            '8',
	                            '8.5',
	                            '9',
	                            '9.5',
	                            '10',
	                            '10.5',
	                            '11',
	                            '11.5',
	                            '12',
	                            '12.5',
	                            '13',
	                            '13.5',
	                            '14',
	                            '14.5',
	                            '15',
	                            '15.5',
	                            '16')
    )

WITH DATA AND STATS PRIMARY INDEX (rms_sku_num) ON COMMIT PRESERVE ROWS;
COLLECT STATS PRIMARY INDEX (rms_sku_num)
    ,COLUMN (rms_sku_num)
   	,COLUMN (SIZE_1_CLEAN)
	,COLUMN (rms_sku_num,SIZE_1_CLEAN)
    ,COLUMN (DEPT_IDNT,SUPP_IDNT)
  
ON sku_lkup_1;

-- drop table store_lkup_1
create volatile multiset table store_lkup_1 as (
  select distinct
      -- store_num is text in NAP fact table
      store_num as LOC_IDNT
      ,Trim(store_num || ', ' ||store_name) AS LOC_LABEL
      ,channel_num as CHNL_IDNT
      ,trim(both from channel_num) ||', '|| channel_desc as channel
      -- county for map 
      ,store_address_county
      --,selling_channel as channel_type
      ,Trim(region_num  || ', ' ||region_desc) AS region_label
       ,channel_country AS country --LEAVE IN FOR TRANSFERS
    
       ,CASE WHEN channel_brand = 'NORDSTROM' THEN 'N'
        WHEN channel_brand = 'NORDSTROM_RACK' THEN 'NR'
        ELSE NULL END AS banner
   from prd_nap_usr_vws.price_store_dim_vw
   where 1=1
      and store_country_code = 'US'
      -- exclude Trunk Club, Last Chance, DCs, NQC, NPG, Faconnable
     and channel_num IN (110,111,120,121,130,140,210,211,220,221,250,260,261,310,311) --doesn't include DCs, LC, NQC
    AND store_abbrev_name <> 'CLOSED'

      and channel_num is not null

)
with data unique primary index(LOC_IDNT) on commit preserve rows
;


collect statistics 
   unique primary index (LOC_IDNT)
  
on store_lkup_1
;



DELETE FROM {shoe_categories_t2_schema}.shoe_size_performance_monthly_rollup ALL;
INSERT INTO {shoe_categories_t2_schema}.shoe_size_performance_monthly_rollup

SELECT
     dt.ty_ly_ind
    ,dt.month_end_date
	,dt.quarter_abrv
    ,dt.quarter_label
    ,dt.month_idnt
    ,dt.month_abrv
    ,dt.month_label
    ,dt.year_idnt
	, b.rp_idnt
    , s.division
    , s.dept_label
    , s.class_label
    , s.subdivision
    , l.banner  
    , l.CHNL_IDNT
    , l.channel
    , l.LOC_IDNT
    , l.LOC_LABEL
    , l.store_address_county 
    , l.region_label
    , s.ccs_subcategory
    , s.quantrix_category   
    , s.merch_themes
    , s.supplier
    , s.brand
    , sa.Supplier_Attribute
    , s.npg_ind
    , b.price_type
    , b.ds_ind as dropship_ind 
	, s.size_1_clean
    , s.left_core_right
    , s.overs_unders
    , s.widths
    , s.wide_calf
	,Sum(sales_dollars) 
	,Sum(sales_cost)
	,Sum(sales_units)
	,Sum(return_dollars)
	,Sum(return_cost)
	,Sum(return_units)
	,Sum(demand_dollars)
	,Sum(demand_units)
	,Sum(CASE WHEN dt.week_end_day_date = dt.month_end_date THEN eoh_dollars end) AS eoh_dollars
    ,Sum(CASE WHEN dt.week_end_day_date = dt.month_end_date THEN eoh_cost end) AS eoh_cost
    ,Sum(CASE WHEN dt.week_end_day_date = dt.month_end_date THEN eoh_units end) AS eoh_units
	,Sum(receipt_dollars)
	,Sum(receipt_units)
	,Sum(receipt_cost)
	,Sum(transfer_pah_dollars)
	,Sum(transfer_pah_units)
	,Sum(transfer_pah_cost)
	,Sum(oo_dollars)
	,Sum(oo_units)
	,Sum(oo_cost)
	,Sum(oo_4weeks_dollars)
	,Sum(oo_4weeks_units)
	,Sum(oo_4weeks_cost)
	,Sum(oo_12weeks_dollars)
	,Sum(oo_12weeks_units)
	,Sum(oo_12weeks_cost)
	,Sum(net_sales_tot_retl_with_cost)
	,CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
	
	from
	{shoe_categories_t2_schema}.shoe_size_performance_monthly_base b
	join date_lkup_1 dt
	on dt.week_idnt_true= b.week_num
	JOIN sku_lkup_1 s
    ON b.rms_sku_num=s.rms_sku_num
	join store_lkup_1 l
	on b.store_num=l.LOC_IDNT
	LEFT JOIN  SUPP_ATTR sa
    ON s.dept_idnt = sa.dept_num
    AND s.supp_idnt = sa.supplier_num
    AND l.banner = sa.banner

GROUP BY 
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34

;

COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (month_idnt) -- column names used for primary index
on {shoe_categories_t2_schema}.shoe_size_performance_monthly_rollup;


SET QUERY_BAND = NONE FOR SESSION;