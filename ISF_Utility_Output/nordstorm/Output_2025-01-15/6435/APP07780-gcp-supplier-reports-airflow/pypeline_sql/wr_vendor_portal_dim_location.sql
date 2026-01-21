select 
chain_idnt,
chain_desc,
area_idnt,
area_desc,
distt_idnt,
distt_desc,
loc_idnt,
loc_desc from (
select business_unit_num chain_idnt,
    business_unit_desc chain_desc,
    group_num area_idnt,
    group_desc area_desc,
    region_num distt_idnt,
    region_desc distt_desc,
    trim(store_num) loc_idnt,
    trim(store_short_name) loc_desc from {db_env}_NAP_BASE_VWS.STORE_DIM
where store_num not in (select CONFIG_VALUE from {db_env}_NAP_BASE_VWS.CONFIG_LKUP
where INTERFACE_CODE='SHARED_INV_LOC' 
and config_key='PHYSICAL_LOC_IDNT')

union all

select business_unit_num chain_idnt,
    business_unit_desc chain_desc,
    group_num area_idnt,
    group_desc area_desc,
    region_num distt_idnt,
    region_desc distt_desc, 
    trim(b.loc_idnt1) as loc_idnt,
    trim(b.loc_desc) loc_desc
from {db_env}_NAP_BASE_VWS.STORE_DIM A
join
	(select
	b.config_key as loc_idnt1,
    b.config_value as loc_idnt2,
	a.store_short_name loc_desc
   from {db_env}_NAP_BASE_VWS.STORE_DIM A
	join {db_env}_NAP_BASE_VWS.CONFIG_LKUP b
	on trim(a.store_num)=trim(b.config_key)
	and b.INTERFACE_CODE='SHARED_INV_LOC') B
on A.store_num=B.loc_idnt2)A;