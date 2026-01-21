/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09142;
     DAG_ID=ddl_digital_merch_table_11521_ACE_ENG;
     Task_Name=ddl_digital_merch_table;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_digitalmerch_flash
Team/Owner: Nicole Miao, Sean Larkin, Cassie Zhang, Rae Ann Boswell
Date Created/Modified: 01/24/2024, modified on 06/12/2024
*/


-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--  CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'digital_merch_table', OUT_RETURN_MSG);

create multiset table {digital_merch_t2_schema}.digital_merch_table
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
       day_date                     date
        ,web_style_num              varchar(30) 
        ,selling_channel            varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,epm_style_num              varchar(30) compress
        ,partner_relationship_type  varchar(60) compress
        ,npg_ind                    integer compress
        ,brand_name                 varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,div_num                    integer compress
        ,div_desc                   varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,grp_num                    integer compress
        ,grp_desc                   varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,dept_num                   integer compress
        ,dept_desc                  varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,class_num                  integer compress
        ,class_desc                 varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,sbclass_num                integer compress
        ,sbclass_desc               varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,prmy_supp_num              varchar(10) compress
        ,vender_name                varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,style_group_num            varchar(10) compress
        ,style_group_desc           varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,style_group_short_desc     varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,type_level_1_num           varchar(10) compress
        ,type_level_1_desc          varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,type_level_2_num           varchar(10) compress
        ,type_level_2_desc          varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,genders_code               varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,genders_desc               varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,age_groups_code            varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,age_groups_desc            varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,QUANTRIX_CATEGORY          varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,CCS_CATEGORY               varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,CCS_SUBCATEGORY            varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,NORD_ROLE                  varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,NORD_ROLE_DESC             varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,RACK_ROLE                  varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,RACK_ROLE_DESC             varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,PARENT_GROUP               varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,MERCH_THEMES               varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,ANNIVERSARY_21             varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,ANNIVERSARY_THEME          varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,HOLIDAY_21                 varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,HOLIDAY_THEME_TY           varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,GIFT_IND                   varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,STOCKING_STUFFER           varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,ASSORTMENT_GROUPING        varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,product_views_ppfd         numeric(32,2) compress
        ,los_flag                   integer compress
        ,unfiltered_units           integer compress
        ,unfiltered_orders          integer compress
        ,unfiltered_demand          numeric(32,5) compress
        ,filtered_units             integer compress
        ,filtered_orders            integer compress
        ,filtered_demand            numeric(32,5) compress
        ,ds_fulfilled_unfiltered_units              integer compress
        ,ds_fulfilled_unfiltered_orders             integer compress
        ,ds_fulfilled_unfiltered_demand             numeric(32,5) compress
        ,ds_fulfilled_filtered_units                integer compress
        ,ds_fulfilled_filtered_orders               integer compress
        ,ds_fulfilled_filtered_demand               numeric(32,5) compress
        ,min_selling_price          numeric(15,2) compress
        ,max_selling_price          numeric(15,2) compress
        ,avg_selling_price          numeric(15,2) compress
        ,number_selling_price       integer compress
        ,min_reg_price              numeric(15,2) compress
        ,max_reg_price              numeric(15,2) compress
        ,avg_reg_price              numeric(15,2) compress
        ,number_reg_price           integer compress
        ,min_md_discount            numeric(32,2) compress
        ,max_md_discount            numeric(32,2) compress
        ,avg_md_discount            numeric(32,2) compress
        ,number_md_discount         integer compress
        ,num_md                     integer compress
        ,min_inventory_age          integer compress
        ,max_inventory_age          integer compress
        ,avg_inventory_age          numeric(20,2) compress
        ,sku_count                  integer compress
        ,color_count                integer compress
        ,boh_units                  numeric(32,0) compress
        ,eoh_units                  numeric(32,0) compress
        ,nonsellable_units          numeric(32,0) compress
        ,instock_views              numeric(32,3) compress
        ,total_views                numeric(32,3) compress
        ,twist                      numeric(32,7) compress
        ,sku_count_new              integer compress
        ,color_count_new            integer compress
        ,sku_count_not_new          integer compress
        ,color_count_not_new        integer compress
        ,new_boh_units              numeric(32,0) compress
        ,new_eoh_units              numeric(32,0) compress
        ,new_nonsellable_units      numeric(32,0) compress
        ,not_new_boh_units          numeric(32,0) compress
        ,not_new_eoh_units          numeric(32,0) compress
        ,not_new_nonsellable_units  numeric(32,0) compress
        ,new_twist                  numeric(32,7) compress
        ,not_new_twist              numeric(32,7) compress
        ,sku_count_rp               integer compress
        ,color_count_rp             integer compress
        ,sku_count_non_rp           integer compress
        ,color_count_non_rp         integer compress
        ,rp_boh_units               numeric(32,0) compress
        ,rp_eoh_units               numeric(32,0) compress
        ,rp_nonsellable_units       numeric(32,0) compress
        ,not_rp_boh_units           numeric(32,0) compress
        ,not_rp_eoh_units           numeric(32,0) compress
        ,not_rp_nonsellable_units   numeric(32,0) compress
        ,rp_twist                   numeric(32,7) compress
        ,not_rp_twist               numeric(32,7) compress
        ,sku_count_R            integer compress
        ,sku_count_P            integer compress
        ,sku_count_C            integer compress
        ,color_count_R          integer compress
        ,color_count_P          integer compress
        ,color_count_C          integer compress
        ,eoh_reg_units          numeric(32,0) compress
        ,eoh_clr_units          numeric(32,0) compress
        ,eoh_pro_units          numeric(32,0) compress
        ,R_twist                numeric(32,7) compress
        ,P_twist                numeric(32,7) compress
        ,C_twist                numeric(32,7) compress
        ,sku_count_DS           integer compress
        ,sku_count_Unsellable   integer compress
        ,sku_count_Owned        integer compress
        ,color_count_DS         integer compress
        ,color_count_Unsellable integer compress
        ,color_count_Owned      integer compress
        ,DS_boh_units           numeric(32,0) compress
        ,Unsellable_boh_units   numeric(32,0) compress
        ,Owned_boh_units        numeric(32,0) compress
        ,DS_eoh_units           numeric(32,0) compress
        ,Unsellable_eoh_units   integer compress
        ,Owned_eoh_units        integer compress
        ,DS_twist               numeric(32,7) compress
        ,Unsellable_twist       numeric(32,7) compress
        ,Owned_twist            numeric(32,7) compress
        ,flash_sku_count        integer compress
        ,mp_flag                integer compress
        ,sku_count_mp           integer compress
        ,sku_count_nmp          integer compress
        ,color_count_mp         integer compress
        ,color_count_nmp        integer compress
        ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(selling_channel, web_style_num, day_date)
partition by range_n(day_date BETWEEN DATE '2021-01-01' AND DATE '2055-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {digital_merch_t2_schema}.digital_merch_table IS 'A table containing all the merch attributes of digital-sellable web styles';


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

 
 
