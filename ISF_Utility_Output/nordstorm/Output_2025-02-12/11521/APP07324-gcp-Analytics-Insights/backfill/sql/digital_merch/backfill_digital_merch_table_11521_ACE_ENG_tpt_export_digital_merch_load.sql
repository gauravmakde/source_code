create or replace temporary view data_view 
        (
	web_style_num       string
    ,selling_channel    string
    ,epm_style_num      string
    ,partner_relationship_type  string
    ,npg_ind            integer        
    ,brand_name         string
    ,div_num            integer
    ,div_desc           string
    ,grp_num            integer
    ,grp_desc           string
    ,dept_num           integer
    ,dept_desc          string
    ,class_num          integer
    ,class_desc         string
    ,sbclass_num        integer
    ,sbclass_desc       string
    ,prmy_supp_num      string
    ,vender_name        string
    ,style_group_num    string
    ,style_group_desc   string
    ,style_group_short_desc    string
    ,type_level_1_num           string
    ,type_level_1_desc          string
    ,type_level_2_num           string
    ,type_level_2_desc          string
    ,genders_code               string
    ,genders_desc               string
    ,age_groups_code            string
    ,age_groups_desc            string
    ,QUANTRIX_CATEGORY          string
    ,CCS_CATEGORY               string
    ,CCS_SUBCATEGORY            string
    ,NORD_ROLE                  string
    ,NORD_ROLE_DESC             string
    ,RACK_ROLE                  string
    ,RACK_ROLE_DESC             string
    ,PARENT_GROUP               string
    ,MERCH_THEMES               string
    ,ANNIVERSARY_21             string
    ,ANNIVERSARY_THEME          string
    ,HOLIDAY_21                 string
    ,HOLIDAY_THEME_TY           string
    ,GIFT_IND                   string
    ,STOCKING_STUFFER           string
    ,ASSORTMENT_GROUPING        string
    ,product_views_ppfd         numeric(20,2)    
    ,los_flag                   integer
    ,unfiltered_units           integer
    ,unfiltered_orders          integer 
    ,unfiltered_demand          numeric(32,5) 
    ,filtered_units             integer 
    ,filtered_orders            integer 
    ,filtered_demand            numeric(32,5) 
    ,ds_fulfilled_unfiltered_units              integer 
    ,ds_fulfilled_unfiltered_orders             integer 
    ,ds_fulfilled_unfiltered_demand             numeric(32,5) 
    ,ds_fulfilled_filtered_units                integer 
    ,ds_fulfilled_filtered_orders               integer 
    ,ds_fulfilled_filtered_demand               numeric(32,5) 
    ,min_selling_price          numeric(20,2)    
    ,max_selling_price          numeric(20,2)    
    ,avg_selling_price          numeric(20,2)    
    ,number_selling_price       integer
    ,min_reg_price              numeric(20,2)    
    ,max_reg_price              numeric(20,2)    
    ,avg_reg_price              numeric(20,2)    
    ,number_reg_price           integer
    ,min_md_discount            numeric(20,2)    
    ,max_md_discount            numeric(20,2)    
    ,avg_md_discount            numeric(20,2)    
    ,number_md_discount         integer
    ,num_md                     integer
    ,min_inventory_age          numeric(20,2)    
    ,max_inventory_age          numeric(20,2)    
    ,avg_inventory_age          numeric(20,2)    
    ,sku_count                  integer
    ,color_count                integer
    ,boh_units                  integer
    ,eoh_units                  integer
    ,nonsellable_units          integer
    ,instock_views              numeric(20,2)    
    ,total_views                numeric(20,2)    
    ,twist                      numeric(20,2)    
    ,sku_count_new              integer
    ,color_count_new            integer
    ,sku_count_not_new          integer
    ,color_count_not_new        integer
    ,new_boh_units              integer
    ,new_eoh_units              integer
    ,new_nonsellable_units      integer
    ,not_new_boh_units          integer
    ,not_new_eoh_units          integer
    ,not_new_nonsellable_units  integer
    ,new_twist                  numeric(20,2)    
    ,not_new_twist              numeric(20,2)    
    ,sku_count_rp               integer
    ,color_count_rp             integer
    ,sku_count_non_rp           integer
    ,color_count_non_rp         integer
    ,rp_boh_units               integer
    ,rp_eoh_units               integer
    ,rp_nonsellable_units       integer
    ,not_rp_boh_units           integer
    ,not_rp_eoh_units           integer
    ,not_rp_nonsellable_units   integer
    ,rp_twist                   numeric(20,2)    
    ,not_rp_twist               numeric(20,2)    
    ,sku_count_R                integer
    ,sku_count_P                integer
    ,sku_count_C                integer
    ,color_count_R              integer
    ,color_count_P              integer
    ,color_count_C              integer
    ,eoh_reg_units              integer
    ,eoh_clr_units              integer
    ,eoh_pro_units              integer
    ,R_twist                    numeric(20,2)    
    ,P_twist                    numeric(20,2)    
    ,C_twist                    numeric(20,2)    
    ,sku_count_DS               integer
    ,sku_count_Unsellable       integer
    ,sku_count_Owned            integer
    ,color_count_DS             integer
    ,color_count_Unsellable     integer
    ,color_count_Owned          integer
    ,DS_boh_units               integer
    ,Unsellable_boh_units       integer
    ,Owned_boh_units            integer
    ,DS_eoh_units               integer
    ,Unsellable_eoh_units       integer
    ,Owned_eoh_units            integer
    ,DS_twist                   numeric(20,2)    
    ,Unsellable_twist           numeric(20,2)    
    ,Owned_twist                numeric(20,2)    
    ,flash_sku_count            integer
    ,mp_flag                    integer
    ,sku_count_mp               integer
    ,sku_count_nmp              integer
    ,color_count_mp             integer
    ,color_count_nmp            integer
    ,dw_sys_load_tmstp          timestamp
	,day_date                   date
        )
USING csv 
OPTIONS (path "s3://ace-etl/tpt_export/digital_merch_td_to_s3.csv",
        sep ",",
        header "false",
        dateFormat "yy/M/d",
        escape ""); 


-- DROP TABLE IF EXISTS ace_etl.digital_merch_table; 
create table if not exists ace_etl.digital_merch_table
(
	web_style_num       string
    ,selling_channel    string
    ,epm_style_num      string
    ,partner_relationship_type  string
    ,npg_ind            integer        
    ,brand_name         string
    ,div_num            integer
    ,div_desc           string
    ,grp_num            integer
    ,grp_desc           string
    ,dept_num           integer
    ,dept_desc          string
    ,class_num          integer
    ,class_desc         string
    ,sbclass_num        integer
    ,sbclass_desc       string
    ,prmy_supp_num      string
    ,vender_name        string
    ,style_group_num    string
    ,style_group_desc   string
    ,style_group_short_desc     string
    ,type_level_1_num           string
    ,type_level_1_desc          string
    ,type_level_2_num           string
    ,type_level_2_desc          string
    ,genders_code               string
    ,genders_desc               string
    ,age_groups_code            string
    ,age_groups_desc            string
    ,QUANTRIX_CATEGORY          string
    ,CCS_CATEGORY               string
    ,CCS_SUBCATEGORY            string
    ,NORD_ROLE                  string
    ,NORD_ROLE_DESC             string
    ,RACK_ROLE                  string
    ,RACK_ROLE_DESC             string
    ,PARENT_GROUP               string
    ,MERCH_THEMES               string
    ,ANNIVERSARY_21             string
    ,ANNIVERSARY_THEME          string
    ,HOLIDAY_21                 string
    ,HOLIDAY_THEME_TY           string
    ,GIFT_IND                   string
    ,STOCKING_STUFFER           string
    ,ASSORTMENT_GROUPING        string
    ,product_views_ppfd         numeric(20,2)   
    ,los_flag                   integer
    ,unfiltered_units           integer
    ,unfiltered_orders          integer 
    ,unfiltered_demand          numeric(32,5) 
    ,filtered_units             integer 
    ,filtered_orders            integer 
    ,filtered_demand            numeric(32,5) 
    ,ds_fulfilled_unfiltered_units              integer 
    ,ds_fulfilled_unfiltered_orders             integer 
    ,ds_fulfilled_unfiltered_demand             numeric(32,5) 
    ,ds_fulfilled_filtered_units                integer 
    ,ds_fulfilled_filtered_orders               integer 
    ,ds_fulfilled_filtered_demand               numeric(32,5) 
    ,min_selling_price                          numeric(20,2)    
    ,max_selling_price                          numeric(20,2)    
    ,avg_selling_price                          numeric(20,2)    
    ,number_selling_price                       integer
    ,min_reg_price                              numeric(20,2)    
    ,max_reg_price                              numeric(20,2)    
    ,avg_reg_price                              numeric(20,2)    
    ,number_reg_price                           integer
    ,min_md_discount                            numeric(20,2)    
    ,max_md_discount                            numeric(20,2)    
    ,avg_md_discount                            numeric(20,2)    
    ,number_md_discount                         integer
    ,num_md                                     integer
    ,min_inventory_age                          numeric(20,2)    
    ,max_inventory_age                          numeric(20,2)    
    ,avg_inventory_age                          numeric(20,2)    
    ,sku_count                                  integer
    ,color_count                                integer
    ,boh_units                                  integer
    ,eoh_units                                  integer
    ,nonsellable_units                          integer
    ,instock_views                              numeric(20,2)    
    ,total_views                                numeric(20,2)    
    ,twist                                      numeric(20,2)    
    ,sku_count_new                              integer
    ,color_count_new                            integer
    ,sku_count_not_new                          integer
    ,color_count_not_new                        integer
    ,new_boh_units                              integer
    ,new_eoh_units                              integer
    ,new_nonsellable_units                      integer
    ,not_new_boh_units                          integer
    ,not_new_eoh_units                          integer
    ,not_new_nonsellable_units                  integer
    ,new_twist                                  numeric(20,2)    
    ,not_new_twist                              numeric(20,2)    
    ,sku_count_rp                               integer
    ,color_count_rp                             integer
    ,sku_count_non_rp                           integer
    ,color_count_non_rp                         integer
    ,rp_boh_units                               integer
    ,rp_eoh_units                               integer
    ,rp_nonsellable_units                       integer
    ,not_rp_boh_units                           integer
    ,not_rp_eoh_units                           integer
    ,not_rp_nonsellable_units                   integer
    ,rp_twist                                   numeric(20,2)    
    ,not_rp_twist                               numeric(20,2)    
    ,sku_count_R                                integer
    ,sku_count_P                                integer
    ,sku_count_C                                integer
    ,color_count_R                              integer
    ,color_count_P                              integer
    ,color_count_C                              integer
    ,eoh_reg_units                              integer
    ,eoh_clr_units                              integer
    ,eoh_pro_units                              integer
    ,R_twist                                    numeric(20,2)    
    ,P_twist                                    numeric(20,2)    
    ,C_twist                                    numeric(20,2)    
    ,sku_count_DS                               integer
    ,sku_count_Unsellable                       integer
    ,sku_count_Owned                            integer
    ,color_count_DS                             integer
    ,color_count_Unsellable                     integer
    ,color_count_Owned                          integer
    ,DS_boh_units                               integer
    ,Unsellable_boh_units                       integer
    ,Owned_boh_units                            integer
    ,DS_eoh_units                               integer
    ,Unsellable_eoh_units                       integer
    ,Owned_eoh_units                            integer
    ,DS_twist                                   numeric(20,2)    
    ,Unsellable_twist                           numeric(20,2)    
    ,Owned_twist                                numeric(20,2)    
    ,flash_sku_count                            integer
    ,mp_flag                                    integer
    ,sku_count_mp                               integer
    ,sku_count_nmp                              integer
    ,color_count_mp                             integer
    ,color_count_nmp                            integer
    ,dw_sys_load_tmstp                          timestamp
	,day_date                                   date
        )
USING PARQUET
location 's3://ace-etl/digital_merch_table/'
partitioned by (day_date);


--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
msck repair table ace_etl.digital_merch_table;


insert overwrite table ace_etl.digital_merch_table partition (day_date)
select
	trim(web_style_num) as web_style_num
    ,selling_channel
    ,epm_style_num
    ,partner_relationship_type
    ,npg_ind
    ,brand_name
    ,div_num
    ,div_desc
    ,grp_num
    ,grp_desc
    ,dept_num
    ,dept_desc
    ,class_num
    ,class_desc
    ,sbclass_num
    ,sbclass_desc
    ,prmy_supp_num
    ,vender_name
    ,style_group_num
    ,style_group_desc
    ,style_group_short_desc
    ,type_level_1_num
    ,type_level_1_desc
    ,type_level_2_num
    ,type_level_2_desc
    ,genders_code
    ,genders_desc
    ,age_groups_code
    ,age_groups_desc
    ,QUANTRIX_CATEGORY
    ,CCS_CATEGORY
    ,CCS_SUBCATEGORY
    ,NORD_ROLE
    ,NORD_ROLE_DESC
    ,RACK_ROLE
    ,RACK_ROLE_DESC
    ,PARENT_GROUP
    ,MERCH_THEMES
    ,ANNIVERSARY_21
    ,ANNIVERSARY_THEME
    ,HOLIDAY_21
    ,HOLIDAY_THEME_TY
    ,GIFT_IND
    ,STOCKING_STUFFER
    ,ASSORTMENT_GROUPING
    ,product_views_ppfd
    ,los_flag
    ,unfiltered_units
    ,unfiltered_orders
    ,unfiltered_demand
    ,filtered_units
    ,filtered_orders
    ,filtered_demand
    ,ds_fulfilled_unfiltered_units
    ,ds_fulfilled_unfiltered_orders
    ,ds_fulfilled_unfiltered_demand
    ,ds_fulfilled_filtered_units
    ,ds_fulfilled_filtered_orders
    ,ds_fulfilled_filtered_demand
    ,min_selling_price
    ,max_selling_price
    ,avg_selling_price
    ,number_selling_price
    ,min_reg_price
    ,max_reg_price
    ,avg_reg_price
    ,number_reg_price
    ,min_md_discount
    ,max_md_discount
    ,avg_md_discount
    ,number_md_discount
    ,num_md
    ,min_inventory_age
    ,max_inventory_age
    ,avg_inventory_age
    ,sku_count
    ,color_count
    ,boh_units
    ,eoh_units
    ,nonsellable_units
    ,instock_views
    ,total_views
    ,twist
    ,sku_count_new
    ,color_count_new
    ,sku_count_not_new
    ,color_count_not_new
    ,new_boh_units
    ,new_eoh_units
    ,new_nonsellable_units
    ,not_new_boh_units
    ,not_new_eoh_units
    ,not_new_nonsellable_units
    ,new_twist
    ,not_new_twist
    ,sku_count_rp
    ,color_count_rp
    ,sku_count_non_rp
    ,color_count_non_rp
    ,rp_boh_units
    ,rp_eoh_units
    ,rp_nonsellable_units
    ,not_rp_boh_units
    ,not_rp_eoh_units
    ,not_rp_nonsellable_units
    ,rp_twist
    ,not_rp_twist
    ,sku_count_R
    ,sku_count_P
    ,sku_count_C
    ,color_count_R
    ,color_count_P
    ,color_count_C
    ,eoh_reg_units
    ,eoh_clr_units
    ,eoh_pro_units
    ,R_twist
    ,P_twist
    ,C_twist
    ,sku_count_DS
    ,sku_count_Unsellable
    ,sku_count_Owned
    ,color_count_DS
    ,color_count_Unsellable
    ,color_count_Owned
    ,DS_boh_units
    ,Unsellable_boh_units
    ,Owned_boh_units
    ,DS_eoh_units
    ,Unsellable_eoh_units
    ,Owned_eoh_units
    ,DS_twist
    ,Unsellable_twist
    ,Owned_twist
    ,flash_sku_count    
    ,mp_flag              
    ,sku_count_mp         
    ,sku_count_nmp        
    ,color_count_mp       
    ,color_count_nmp            
    ,dw_sys_load_tmstp  
	,day_date
from data_view;

