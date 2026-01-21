-- Table Name:digital_merch_table_as_is
-- Team/Owner: CIA - Digital Customer Experience Analytics
-- Date Created/Modified: created on 06/11/2024, last modified on 06/11/2024
-- What is the update cadence/lookback window: refresh/overwrite daily
 
DROP TABLE IF EXISTS {hive_schema}.digital_merch_table_as_is;
create table if not exists {hive_schema}.digital_merch_table_as_is (
	web_style_num       string
    ,selling_channel    string
    ,epm_style_num      string
    ,style_group_num      string
    ,style_group_desc    string
    ,style_group_short_desc    string
    ,npg_ind      string        
    ,brand    string
    ,division    string
    ,subdivision    string
    ,department    string
    ,class    string
    ,subclass    string
    ,product_type1    string
    ,product_type2    string
	,prmy_supp_num      string
    ,vender_name    string
    ,genders_code    string
    ,genders_desc    string
    ,age_groups_code    string
    ,age_groups_desc    string
    ,merch_themes    string
    ,parent_group    string
	,quantrix_category string
    ,ccs_category    string
    ,ccs_subcategory    string
    ,NORD_ROLE    string
    ,NORD_ROLE_DESC    string
    ,RACK_ROLE    string
    ,RACK_ROLE_DESC    string
    ,ANNIVERSARY_21    string
    ,ANNIVERSARY_THEME    string
    ,HOLIDAY_21    string
    ,HOLIDAY_THEME_TY    string
    ,GIFT_IND    string
    ,STOCKING_STUFFER    string
    ,ASSORTMENT_GROUPING    string
    ,los_flag   integer
    ,min_selling_price     numeric(20,2)    
    ,max_selling_price     numeric(20,2)    
    ,avg_selling_price     numeric(20,2)    
    ,number_selling_price      integer
    ,min_reg_price     numeric(20,2)    
    ,max_reg_price     numeric(20,2)    
    ,avg_reg_price     numeric(20,2)    
    ,number_reg_price      integer
    ,min_md_discount     numeric(20,2)    
    ,max_md_discount     numeric(20,2)    
    ,avg_md_discount     numeric(20,2)    
    ,number_md_discount      integer
    ,num_md      integer
    ,min_inventory_age     numeric(20,2)    
    ,max_inventory_age     numeric(20,2)    
    ,avg_inventory_age     numeric(20,2)    
    ,sku_count      integer
    ,color_count      integer
    ,sku_count_new      integer
    ,color_count_new      integer
    ,sku_count_not_new      integer
    ,color_count_not_new      integer
    ,sku_count_rp      integer
    ,color_count_rp      integer
    ,sku_count_non_rp      integer
    ,color_count_non_rp      integer
    ,sku_count_R      integer
    ,sku_count_P      integer
    ,sku_count_C      integer
    ,color_count_R      integer
    ,color_count_P      integer
    ,color_count_C      integer
    ,sku_count_DS      integer
    ,sku_count_Unsellable      integer
    ,sku_count_Owned      integer
    ,color_count_DS      integer
    ,color_count_Unsellable      integer
    ,color_count_Owned      integer
    ,flash_sku_count integer
    ,partner_relationship_type  string
    ,mp_flag    integer
    ,sku_count_mp   integer
    ,sku_count_nmp  integer
    ,color_count_mp integer
    ,color_count_nmp    integer
    ,last_date_valid     date
    ,rk      integer
) using PARQUET location 's3://{s3_bucket_root_var}/digital_merch_table_as_is'
;


insert OVERWRITE TABLE {hive_schema}.digital_merch_table_as_is
select *
from (
	select web_style_num, selling_channel, 
		epm_style_num, 
		style_group_num,
		upper(style_group_desc) as style_group_desc,
		upper(style_group_short_desc) as style_group_short_desc,
		cast(npg_ind as varchar(2)) as npg_ind, 
		upper(brand_name) as brand,
		upper(case when concat(cast(div_num as varchar(20)),': ', div_desc) = ': ' then null else concat(cast(div_num as varchar(20)),': ', div_desc) end) as division,
		upper(case when concat(cast(grp_num as varchar(20)),': ', grp_desc) = ': ' then null else concat(cast(grp_num as varchar(20)),': ', grp_desc) end) as subdivision,
		upper(case when concat(cast(dept_num as varchar(20)),': ', dept_desc) = ': ' then null else concat(cast(dept_num as varchar(20)),': ', dept_desc) end) as department,
		upper(case when concat(cast(class_num as varchar(20)),': ', class_desc) = ': ' then null else concat(cast(class_num as varchar(20)),': ', class_desc) end) as class,
		upper(case when concat(cast(sbclass_num as varchar(20)),': ', sbclass_desc) = ': ' then null else concat(cast(sbclass_num as varchar(20)),': ', sbclass_desc) end) as subclass,
		upper(case when concat(cast(type_level_1_num as varchar(20)),': ', type_level_1_desc) = ': ' then null else concat(cast(type_level_1_num as varchar(20)),': ', type_level_1_desc) end) as product_type1,
		upper(case when concat(cast(type_level_2_num as varchar(20)),': ', type_level_2_desc) = ': ' then null else concat(cast(type_level_2_num as varchar(20)),': ', type_level_2_desc) end) as product_type2,
		prmy_supp_num, vender_name,
		genders_code, genders_desc, age_groups_code, age_groups_desc,
		upper(merch_themes) as merch_themes,
		upper(parent_group) as parent_group, 
		upper(quantrix_category) as quantrix_category,
		upper(ccs_category) as ccs_category, 
		upper(ccs_subcategory) as ccs_subcategory, 
		nord_role, nord_role_desc, rack_role, rack_role_desc, 
		anniversary_21, anniversary_theme, holiday_21, holiday_theme_ty, gift_ind, stocking_stuffer, assortment_grouping,
        los_flag,
		min_selling_price, max_selling_price, avg_selling_price, number_selling_price, min_reg_price, max_reg_price, avg_reg_price, number_reg_price, min_md_discount, max_md_discount, avg_md_discount, number_md_discount, num_md, 
        min_inventory_age, max_inventory_age, avg_inventory_age,
		sku_count, color_count, 
		sku_count_new, color_count_new, sku_count_not_new, color_count_not_new, 
		sku_count_rp, color_count_rp, sku_count_non_rp, color_count_non_rp, 
		sku_count_r, sku_count_p, sku_count_c, color_count_r, color_count_p, color_count_c, 
		sku_count_ds, sku_count_unsellable, sku_count_owned, color_count_ds, color_count_unsellable, color_count_owned, 
        flash_sku_count,
        partner_relationship_type, mp_flag, sku_count_mp, sku_count_nmp, color_count_mp, color_count_nmp,
		day_date as last_date_valid,
		row_number() over(partition by selling_channel, web_style_num order by day_date desc, style_group_num desc, div_num desc, grp_num desc, dept_num desc, class_num desc, sbclass_num desc, brand_name DESC, quantrix_category, parent_group, merch_themes) as rk
	from ace_etl.digital_merch_table
)
where rk = 1
;

-- MSCK REPAIR table {hive_schema}.digital_merch_table_as_is;
