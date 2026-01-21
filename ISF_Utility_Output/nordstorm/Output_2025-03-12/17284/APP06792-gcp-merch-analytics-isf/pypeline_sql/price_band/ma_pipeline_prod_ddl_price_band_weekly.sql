/*
Name:                price_band_weekly
APPID-Name:          APP09094
Purpose:             Price band reporting. Create table t2dl_das_in_season_management_reporting.price_band_weekly.
Variable(s):         {{environment_schema}} t2dl_das_in_season_management_reporting 
                     {{env_suffix}} dev or prod as appropriate
DAG: 
Author(s):           Jevon Barlas & Trang Pham
Date Created:        12/5/2023
Date Last Updated:   1/30/2024
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'price_band_weekly{env_suffix}', OUT_RETURN_MSG)
;

create multiset table {environment_schema}.price_band_weekly{env_suffix}
--create multiset table t3dl_ace_pra.price_band_weekly
,fallback
,no before journal
,no after journal
,checksum = default
,default mergeblockratio
,map = td_map1
(
   ty_ly_ind                  char(2) character set unicode not casespecific not null
   ,fiscal_year_num           integer compress (2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030) not null
   ,fiscal_halfyear_num       integer compress (20201,20211,20221,20231,20241,20251,20261,20271,20281,20291,20301,20202,20212,20222,20232,20242,20252,20262,20272,20282,20292,20302) not null
   ,quarter_abrv              char(2) character set unicode not casespecific not null
   ,quarter_label             char(7) character set unicode not casespecific not null
   ,month_idnt                integer not null
   ,month_abrv                char(3) character set unicode not casespecific not null
   ,month_label               char(8) character set unicode not casespecific not null
   ,month_start_day_date      date format 'yyyy/mm/dd' not null
   ,month_end_day_date        date format 'yyyy/mm/dd' not null
   ,fiscal_week_num           integer compress (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53) not null
   ,week_label                varchar(12) character set unicode not casespecific not null
   ,week_of_month_label       varchar(12) character set unicode not casespecific not null
   ,week_start_day_date       date format 'yyyy/mm/dd' not null
   ,week_end_day_date         date format 'yyyy/mm/dd' not null
   ,week_idnt                 integer not null
   ,banner                    varchar(20) character set unicode not casespecific not null
   ,channel_type              varchar(10) character set unicode not casespecific not null
   ,channel_label             varchar(40) character set unicode not casespecific not null
   ,channel_num               integer not null 
   ,div_label                 varchar(40) character set unicode not casespecific not null
   ,subdiv_label              varchar(40) character set unicode not casespecific not null
   ,dept_label                varchar(40) character set unicode not casespecific not null
   ,class_label               varchar(40) character set unicode not casespecific not null
   ,subclass_label            varchar(40) character set unicode not casespecific not null
   ,div_num                   integer compress (310,340,345,351,360,365,600,700,800,900) not null
   ,subdiv_num                integer compress (600,700,705,710,720,725,730,740,745,750,760,765,770,775,780,785,790) not null
   ,dept_num                  integer not null
   ,class_num                 integer not null
   ,subclass_num              integer not null
   ,quantrix_category         varchar(40) character set unicode not casespecific -- no null condition because some products don't have categories
   ,quantrix_category_group   varchar(40) character set unicode not casespecific -- no null condition because some products don't have categories
   ,supplier_number           integer not null
   ,supplier_name             varchar(50) character set unicode not casespecific -- no null condition because product master sometimes has nulls
   ,brand_name                varchar(50) character set unicode not casespecific not null
   ,npg_ind                   char(1) character set unicode not casespecific -- no null condition because product master sometimes has nulls
   ,rp_ind                    char(1) character set unicode not casespecific not null
   ,ds_ind                    char(1) character set unicode not casespecific not null
   ,price_type                char(1) character set unicode not casespecific not null
   ,price_band_one_lower_bound decimal(18,2) not null
   ,price_band_one_upper_bound decimal(18,2) not null
   ,price_band_two_lower_bound decimal(18,2) not null
   ,price_band_two_upper_bound decimal(18,2) not null
   ,sales_u    integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25)
   ,sales_c    decimal(18,2) compress 0.00
   ,sales_r    decimal(18,2) compress 0.00
   ,sales_r_with_cost    decimal(18,2) compress 0.00
   ,demand_u   integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25)
   ,demand_r   decimal(18,2) compress 0.00
   ,eoh_u      integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25)
   ,eoh_c      decimal(18,2) compress 0.00
   ,eoh_r      decimal(18,2) compress 0.00
   ,eop_u      integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25)
   ,eop_c      decimal(18,2) compress 0.00
   ,eop_r      decimal(18,2) compress 0.00
   ,oo_4wk_u       integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25)
   ,oo_4wk_c       decimal(18,2) compress 0.00
   ,oo_4wk_r       decimal(18,2) compress 0.00
   ,oo_5wk_13wk_u  integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25)
   ,oo_5wk_13wk_c  decimal(18,2) compress 0.00
   ,oo_5wk_13wk_r  decimal(18,2) compress 0.00
   ,oo_14wk_u      integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25)
   ,oo_14wk_c      decimal(18,2) compress 0.00
   ,oo_14wk_r      decimal(18,2) compress 0.00      
   ,load_timestamp timestamp format 'yyyy-mm-ddbhh:mi:ss' with time zone
)
primary index (
               week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type, 
               price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
)
partition by (
   range_n( week_idnt between 
      202101 and 202153 each 1
      ,202201 and 202253 each 1
      ,202301 and 202353 each 1
      ,202401 and 202453 each 1
      ,202501 and 202553 each 1
      ,202601 and 202653 each 1
      ,202701 and 202753 each 1
      ,202801 and 202853 each 1
      ,202901 and 202953 each 1
      ,203001 and 203053 each 1
      ,203101 and 203153 each 1
      ,203201 and 203253 each 1
   )
   ,range_n(
      channel_num between 100 and 999 each 10
   )
)
;
