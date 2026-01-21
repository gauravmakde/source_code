/*
Name: Lifecycle Phase CC+Banner
APPID-Name: APP09478
Purpose: Create a table to capture each lifecycle phase (reg price, first mark, remark, etc.) for each CC+Banner combination, and the range of dates the combo is in that phase.
Variable(s):    {{environment_schema}} t2dl_das_in_season_management_reporting 
                {{env_suffix}} dev or prod as appropriate
DAG: 
Author(s): Jevon Barlas
Date Created: 7/31/2024
Date Last Updated: 7/31/2024
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'lifecycle_phase_cc_banner{env_suffix}', OUT_RETURN_MSG)
;

create multiset table {environment_schema}.lifecycle_phase_cc_banner{env_suffix}
,fallback
,no before journal
,no after journal
,checksum = default
,default mergeblockratio
,map = td_map1
(
   cc                                     varchar(20) character set unicode not casespecific not null
   ,rms_style_num                         varchar(10) character set unicode not casespecific 
   ,color_num                             varchar(10) character set unicode not casespecific
   ,banner                                varchar(20) character set unicode not casespecific not null
   ,ownership_price_type_code             char(1) character set unicode not casespecific
   ,last_markdown_version                 integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
   ,regular_price                         decimal(18,2)
   ,ownership_price                       decimal(18,2) compress (0.01)
   ,pst_eff_begin_date                    date format 'yyyy/mm/dd' not null
   ,pst_eff_end_date                      date format 'yyyy/mm/dd' not null
   ,lifecycle_phase_number                integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
   ,lifecycle_phase_name                  varchar(30) character set unicode not casespecific
   ,lifecycle_phase_pst_eff_begin_date    date format 'yyyy/mm/dd'
   ,lifecycle_phase_pst_eff_end_date      date format 'yyyy/mm/dd'
   ,banner_lifecycle_number               integer compress (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
   ,banner_lifecycle_pst_eff_begin_date   date format 'yyyy/mm/dd'
   ,banner_lifecycle_pst_eff_end_date     date format 'yyyy/mm/dd'
   ,ind_reg_price_this_banner_cycle       smallint compress (0,1)
   ,ind_first_mark_this_banner_cycle      smallint compress (0,1)
   ,ind_remark_this_banner_cycle          smallint compress (0,1)
   ,ind_racking_this_banner_cycle         smallint compress (0,1)
   ,ind_last_chance_this_banner_cycle     smallint compress (0,1)
   ,record_load_tmstp                     timestamp(6) with time zone
)
primary index ( cc, banner, pst_eff_begin_date )
partition by (
   case_n(banner = 'NORDSTROM', banner = 'NORDSTROM_RACK')
   ,range_n(pst_eff_begin_date between date '2010-01-01' and date '2050-12-31' each interval '1' day )
)
;

grant select on {environment_schema}.lifecycle_phase_cc_banner{env_suffix} to public
;
