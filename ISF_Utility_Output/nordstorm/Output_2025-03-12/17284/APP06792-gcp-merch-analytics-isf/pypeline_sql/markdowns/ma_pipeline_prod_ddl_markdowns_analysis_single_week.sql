/*
Name:                markdowns_analysis_single_week
APPID-Name:          APP09527
Purpose:             Markdowns and Clearance analysis and reporting. 
                     Create table t2dl_das_in_season_management_reporting.markdowns_analysis_single_week
Variable(s):         {{environment_schema}} t2dl_das_in_season_management_reporting 
                     {{env_suffix}} dev or prod as appropriate
DAG: 
Author(s):           Jevon Barlas
Date Created:        9/6/2024
Date Last Updated:   9/27/2024
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'markdowns_analysis_single_week{env_suffix}', OUT_RETURN_MSG)
;

create multiset table {environment_schema}.markdowns_analysis_single_week{env_suffix}
,fallback  
,no before journal  
,no after journal  
,checksum = default  
,default mergeblockratio  
,map = td_map1
(  
   snapshot_date                                               date format 'YYYY-MM-DD' not null  
   ,channel_country                                            varchar(10) character set unicode not casespecific not null
   ,channel_brand                                              varchar(25) character set unicode not casespecific not null
   ,banner_code                                                char(1) character set unicode not casespecific
   ,selling_channel                                            varchar(20) character set unicode not casespecific
   ,cc                                                         varchar(20) character set unicode not casespecific
   ,banner_channel_cc                                          varchar(40) character set unicode not casespecific
   ,country_banner_channel_cc                                  varchar(50) character set unicode not casespecific
   ,cc_dsci                                                    varchar(20) character set unicode not casespecific
   ,div_num                                                    integer compress(310,340,345,351,360,365,600,700,800,900)
   ,div_label                                                  varchar(50) character set unicode not casespecific
   ,subdiv_num                                                 integer
   ,subdiv_label                                               varchar(50) character set unicode not casespecific
   ,dept_num                                                   integer
   ,dept_label                                                 varchar(50) character set unicode not casespecific
   ,class_num                                                  integer compress(5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95)
   ,class_label                                                varchar(50) character set unicode not casespecific
   ,subclass_num                                               integer compress(5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95)
   ,subclass_label                                             varchar(50) character set unicode not casespecific
   ,style_group_num                                            varchar(10) character set unicode not casespecific
   ,rms_style_num                                              varchar(10) character set unicode not casespecific
   ,color_num                                                  varchar(10) character set unicode not casespecific
   ,vpn                                                        varchar(30) character set unicode not casespecific
   ,supp_color                                                 varchar(80) character set unicode not casespecific
   ,style_desc                                                 varchar(5000) character set unicode not casespecific
   ,prmy_supp_num                                              varchar(10) character set unicode not casespecific
   ,vendor_name                                                varchar(60) character set unicode not casespecific
   ,vendor_label_name                                          varchar(60) character set unicode not casespecific
   ,npg_ind                                                    char(1) character set unicode not casespecific compress('N','Y')
   ,quantrix_category                                          varchar(40) character set unicode not casespecific
   ,quantrix_category_group                                    varchar(40) character set unicode not casespecific
   ,quantrix_season                                            char(4) character set unicode not casespecific compress ('WARM','COLD','NONE')
   ,anchor_brand_ind                                           char(1) character set unicode not casespecific compress ('N','Y')
   ,promotion_ind                                              char(1) character set unicode not casespecific compress('N','Y')
   ,anniversary_item_ind                                       char(1) character set unicode not casespecific compress('N','Y')
   ,replenishment_eligible_ind                                 char(1) character set unicode not casespecific compress('N','Y')
   ,drop_ship_eligible_ind                                     char(1) character set unicode not casespecific compress('N','Y')  
   ,return_disposition_code                                    varchar(10) character set unicode not casespecific  
   ,first_rack_date                                            date format 'YYYY-MM-DD'  
   ,rack_ind                                                   char(1) character set unicode not casespecific compress('N','Y')  
   ,online_purchasable_ind                                     char(1) character set unicode not casespecific compress('N','Y')  
   ,online_purchasable_eff_begin_date                          date format 'YYYY-MM-DD'  
   ,online_purchasable_eff_end_date                            date format 'YYYY-MM-DD'  
   ,selling_status_code_legacy                                 varchar(10) character set unicode not casespecific compress('S1','C3','T1','NULL')  
   ,selling_status_code                                        varchar(10) character set unicode not casespecific compress('BLOCKED','UNBLOCKED','NULL')  
   ,selling_rights_ind                                         char(1) character set unicode not casespecific compress('N','Y')  
   ,average_unit_cost                                          decimal(18,2)  
   ,price_base                                                 decimal(18,2)  
   ,price_ownership                                            decimal(18,2) compress 0.01  
   ,price_ownership_future                                     decimal(18,2) compress 0.01  
   ,price_selling                                              decimal(18,2) compress 0.01  
   ,price_band_base                                            varchar(20) character set unicode not casespecific  
   ,price_band_ownership                                       varchar(20) character set unicode not casespecific  
   ,price_band_ownership_future                                varchar(20) character set unicode not casespecific  
   ,price_band_selling                                         varchar(20) character set unicode not casespecific  
   ,price_base_drop_ship                                       decimal(18,2)  
   ,price_compare_at                                           decimal(18,2)  
   ,price_current_rack                                         decimal(18,2) compress 0.01  
   ,price_variance_within_cc_ind                               char(1) character set unicode not casespecific compress('N','Y')  
   ,pct_off_ownership_vs_base                                  decimal(18,2)  
   ,pct_off_selling_vs_base                                    decimal(18,2)  
   ,pct_off_ownership_future_vs_base                           decimal(18,2)  
   ,pct_off_ownership_vs_compare_at                            decimal(18,2)  
   ,pct_off_ownership_future_vs_compare_at                     decimal(18,2)  
   ,pct_off_band_ownership_vs_base                             varchar(10) character set unicode not casespecific  
   ,pct_off_band_selling_vs_base                               varchar(10) character set unicode not casespecific  
   ,pct_off_band_ownership_future_vs_base                      varchar(10) character set unicode not casespecific  
   ,pct_off_band_ownership_vs_compare_at                       varchar(10) character set unicode not casespecific  
   ,pct_off_band_ownership_future_vs_compare_at                varchar(10) character set unicode not casespecific  
   ,price_type_code_ownership                                  char(1) character set unicode not casespecific  
   ,price_type_code_ownership_future                           char(1) character set unicode not casespecific  
   ,price_type_code_selling                                    char(1) character set unicode not casespecific  
   ,price_type_ownership                                       varchar(10) character set unicode not casespecific  
   ,price_type_ownership_future                                varchar(10) character set unicode not casespecific  
   ,price_type_selling                                         varchar(10) character set unicode not casespecific  
   ,price_signal                                               char(2) character set unicode not casespecific  
   ,price_signal_future                                        char(2) character set unicode not casespecific  
   ,markdown_type_ownership                                    varchar(20) character set unicode not casespecific  
   ,markdown_type_ownership_future                             varchar(20) character set unicode not casespecific  
   ,lifecycle_phase_name_ownership                             varchar(20) character set unicode not casespecific  
   ,lifecycle_phase_name_ownership_future                      varchar(20) character set unicode not casespecific  
   ,first_clearance_markdown_date                              date format 'YYYY-MM-DD'  
   ,last_clearance_markdown_date                               date format 'YYYY-MM-DD'  
   ,future_clearance_markdown_date                             date format 'YYYY-MM-DD'  
   ,clearance_markdown_version                                 integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,future_markdown_ind                                        char(1) character set unicode not casespecific compress('N','Y')  
   ,weeks_since_last_markdown                                  integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,first_receipt_date                                         date format 'YYYY-MM-DD'  
   ,last_receipt_date                                          date format 'YYYY-MM-DD'  
   ,weeks_since_last_receipt_date                              integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,weeks_since_last_receipt_date_band                         varchar(10) character set unicode not casespecific  
   ,weeks_available_to_sell                                    integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,weeks_available_to_sell_band                               varchar(10) character set unicode not casespecific  
   ,first_sales_date                                           date format 'YYYY-MM-DD'  
   ,weeks_since_first_sale                                     integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,weeks_since_first_sale_band                                varchar(10) character set unicode not casespecific  
   ,intent_plan_week_idnt                                      integer  
   ,intent_exit_month_idnt                                     integer    
   ,intent_season                                              varchar(30) character set unicode not casespecific compress('DESERIALIZATION_DEFAULT_VALUE','FALL','SPRING','SUMMER','WINTER','RESORT','SPRING_SUMMER','FALL_WINTER')
   ,intent_lifecycle_type                                      varchar(30) character set unicode not casespecific compress('DESERIALIZATION_DEFAULT_VALUE','CORE','SEASONAL_CORE','SEASON_FASHION','FASHION')  
   ,intent_scaled_event                                        varchar(30) character set unicode not casespecific compress('DESERIALIZATION_DEFAULT_VALUE','ANNIVERSARY_SALE','LAUNCH','CREATIVE_PROJECTS','HOLIDAY','CYBER','SALE')  
   ,intent_holiday_or_celebration                              varchar(50) character set unicode not casespecific compress('DESERIALIZATION_DEFAULT_VALUE','NEW_YEARS_DAY','LUNAR_NEW_YEAR','VALENTINES_DAY','SAINT_PATRICKS_DAY','MOTHERS_DAY','EASTER','FATHERS_DAY','GRADUATION','PRIDE','FOURTH_OF_JULY','HALLOWEEN','DIWALI','THANKSGIVING','HANUKKAH','CHRISTMAS','KWANZAA')
   ,sales_retail_1wk                                           decimal(18,2) compress 0.00  
   ,sales_units_1wk                                            decimal(18,2) compress 0.00  
   ,sales_retail_2wk                                           decimal(18,2) compress 0.00  
   ,sales_units_2wk                                            decimal(18,2) compress 0.00  
   ,sales_retail_4wk                                           decimal(18,2) compress 0.00  
   ,sales_units_4wk                                            decimal(18,2) compress 0.00  
   ,sales_retail_6mo                                           decimal(18,2) compress 0.00  
   ,sales_units_6mo                                            decimal(18,2) compress 0.00  
   ,sell_thru_4wk                                              decimal(18,2) compress 0.00  
   ,sell_thru_since_last_markdown                              decimal(18,2) compress 0.00  
   ,sell_thru_4wk_avg_wkly                                     decimal(18,4) compress 0.00  
   ,sell_thru_band_4wk_avg_wkly                                varchar(10) character set unicode not casespecific  
   ,sell_thru_since_last_markdown_avg_wkly                     decimal(18,4)  
   ,sell_thru_band_since_last_markdown_avg_wkly                varchar(10) character set unicode not casespecific  
   ,eop_units_selling_locations                                integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,eop_retail_selling_locations                               decimal(18,2) compress 0.00  
   ,eop_cost_selling_locations                                 decimal(18,2) compress 0.00  
   ,eop_retail_future_selling_locations                        decimal(18,2) compress 0.00  
   ,eop_units_reserve_stock                                    integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,eop_retail_reserve_stock                                   decimal(18,2) compress 0.00  
   ,eop_cost_reserve_stock                                     decimal(18,2) compress 0.00  
   ,eop_retail_future_reserve_stock                            decimal(18,2) compress 0.00  
   ,eop_units_pack_and_hold                                    integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,eop_retail_pack_and_hold                                   decimal(18,2) compress 0.00  
   ,eop_cost_pack_and_hold                                     decimal(18,2) compress 0.00  
   ,eop_retail_future_pack_and_hold                            decimal(18,2) compress 0.00  
   ,eop_units_all_locations                                    integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,eop_retail_all_locations                                   decimal(18,2) compress 0.00  
   ,eop_cost_all_locations                                     decimal(18,2) compress 0.00  
   ,eop_retail_future_all_locations                            decimal(18,2) compress 0.00  
   ,on_order_units_selling_locations                           integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,on_order_units_reserve_stock                               integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,on_order_units_pack_and_hold                               integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,on_order_units_all_locations                               integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,markdown_dollars_selling_locations                         decimal(18,2) compress 0.00  
   ,markdown_dollars_reserve_stock                             decimal(18,2) compress 0.00  
   ,markdown_dollars_pack_and_hold                             decimal(18,2) compress 0.00  
   ,markdown_dollars_all_locations                             decimal(18,2) compress 0.00  
   ,markdown_dollars_future_selling_locations                  decimal(18,2) compress 0.00  
   ,markdown_dollars_future_reserve_stock                      decimal(18,2) compress 0.00  
   ,markdown_dollars_future_pack_and_hold                      decimal(18,2) compress 0.00  
   ,markdown_dollars_future_all_locations                      decimal(18,2) compress 0.00  
   ,drop_ship_eligible_ind_other_banner                        char(1) character set unicode not casespecific compress('N','Y')  
   ,replenishment_eligible_ind_other_banner                    char(1) character set unicode not casespecific compress('N','Y')  
   ,price_type_ownership_other_banner                          varchar(10) character set unicode not casespecific  
   ,price_type_ownership_future_other_banner                   varchar(10) character set unicode not casespecific  
   ,price_type_selling_other_banner                            varchar(10) character set unicode not casespecific  
   ,price_base_other_banner                                    decimal(18,2)  
   ,price_ownership_other_banner                               decimal(18,2)  
   ,price_ownership_future_other_banner                        decimal(18,2)  
   ,price_selling_other_banner                                 decimal(18,2)  
   ,future_clearance_markdown_date_other_banner                date format 'YYYY-MM-DD'  
   ,future_markdown_ind_other_banner                           char(1) character set unicode not casespecific compress('N','Y')  
   ,sell_thru_4wk_other_banner                                 decimal(18,2)  
   ,sell_thru_since_last_markdown_other_banner                 decimal(18,2)  
   ,sell_thru_4wk_avg_wkly_other_banner                        decimal(18,4)  
   ,sell_thru_band_4wk_avg_wkly_other_banner                   varchar(10) character set unicode not casespecific  
   ,sell_thru_since_last_markdown_avg_wkly_other_banner        decimal(18,4)  
   ,sell_thru_band_since_last_markdown_avg_wkly_other_banner   varchar(10) character set unicode not casespecific  
   ,eop_units_selling_locations_other_banner                   integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,eop_retail_selling_locations_other_banner                  decimal(18,2) compress 0.00  
   ,eop_cost_selling_locations_other_banner                    decimal(18,2) compress 0.00  
   ,eop_units_all_locations_other_banner                       integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)  
   ,eop_retail_all_locations_other_banner                      decimal(18,2) compress 0.00  
   ,eop_cost_all_locations_other_banner                        decimal(18,2) compress 0.00  
   ,future_clearance_markdown_date_prior_week                  date format 'YYYY-MM-DD'  
   ,price_ownership_future_prior_week                          decimal(18,2)  
   ,price_ownership_prior_week                                 decimal(18,2)  
   ,eop_units_selling_locations_prior_week                     integer compress(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
   ,dw_sys_load_tmstp                                          timestamp(6) with time zone

)
primary index ( snapshot_date, banner_channel_cc )
partition by (
   range_n( snapshot_date between date '2021-01-01' and date '2050-12-31' each interval '1' day )
   ,case_n( selling_channel = 'STORE', selling_channel = 'ONLINE', selling_channel = 'OMNI', no case, unknown)
   ,case_n( banner_code = 'N', banner_code = 'R', no case, unknown)
   )
;

collect stats
   column ( snapshot_date, banner_channel_cc )
   ,column( banner_channel_cc )
   ,column( snapshot_date )
   ,column( channel_brand )
   ,column( selling_channel )
   ,column( snapshot_date, channel_brand )
   ,column( snapshot_date, selling_channel )
   ,column( snapshot_date, cc)
   ,column( snapshot_date, selling_channel, cc )
   ,column( snapshot_date, channel_brand, cc )
   ,column( cc )
   ,column( selling_channel, cc )
   ,column( channel_brand, cc )
   ,column( snapshot_date, selling_channel, channel_brand, cc )
on {environment_schema}.markdowns_analysis_single_week{env_suffix}
;
