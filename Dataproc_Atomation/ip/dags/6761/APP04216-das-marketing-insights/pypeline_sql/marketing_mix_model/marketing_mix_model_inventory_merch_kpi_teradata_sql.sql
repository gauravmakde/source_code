SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_inventory_merch_kpi_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_inventory_merch_kpi_teradata_job;'
FOR SESSION VOLATILE;

ET;

/* Getting all SKU Metadata  */
create volatile multiset table sku_dim as
(
select
	distinct 
	rms_style_num
	,rms_sku_num
	,color_num
	,grp_long_desc
	,div_long_desc
from
	{db_env}_nap_usr_vws.product_sku_dim psd  
where 
	channel_country = 'US'
) with data primary index(rms_style_num,rms_sku_num,color_num) on commit preserve rows; 

ET;
/* Getting all SKU Avg Age  */

create volatile multiset table avg_sku_age as
(
select
	distinct 
	month_num
	,store_num
	,rms_sku_num
	,inventory_age_days
from
	{db_env}_nap_vws.merch_inventory_age_sku_store_vw miassv  
where 
    month_num >= 202311
	and channel_country = 'US'

) with data primary index(rms_sku_num) on commit preserve rows;

ET;
/* Getting all SKU Live on .COMs and relevant indicators like flash event or dropship */

create volatile multiset table los_sku as
(
select distinct 
	day_date
,case
		when channel_brand = 'NORDSTROM' then 'NORDSTROM'
		else 'NORDSTROM RACK'
	end as banner
,case
		when channel_brand = 'NORDSTROM' then 'N.COM'
		else 'R.COM'
	end as business_unit_desc
,case
		when channel_brand = 'NORDSTROM' then '808'
		else '828'
	end as store_num
,sku_id
,flash_ind as fl_indt --flash event indicator 
,ds_ind as ds_indt -- dropship indicator
from
	t2dl_das_site_merch.live_on_site_daily losd 
where
	day_date between (current_date - 14) and (current_date - 1)
	and channel_country = 'US'
) with data primary index(day_date,business_unit_desc,sku_id) on commit preserve rows;

ET;

create volatile multiset table twist_base as
(
select
	td.day_date
	,case
			when td.banner = 'NORD' then 'NORDSTROM'
			else 'NORDSTROM RACK'
		end as banner
	,case
			when td.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
			when td.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
			when td.business_unit_desc = 'RACK' then 'RACK STORE'
		    else td.business_unit_desc
		end as business_unit_desc
	,td.store_num
	,td.rms_style_num
	,td.rms_sku_num
	,td.current_price_type
	,case
		when td.current_price_amt between 0 and 25 then '0_25'
		when td.current_price_amt between 25 and 50 then '25_50'
		when td.current_price_amt between 50 and 75 then '50_75'
		when td.current_price_amt between 75 and 100 then '75_100'
		when td.current_price_amt between 100 and 250 then '100_250'
		when td.current_price_amt between 250 and 400 then '250_400'
		when td.current_price_amt between 400 and 600 then '400_600'
		when td.current_price_amt >= 600 then '600++'
	end as price_range
	,case
		when td.current_price_type = 'R' then 1
		else 0
	end as reg_indt
	,case
		when td.current_price_type = 'C' then 1
		else 0
	end as cl_indt
	,case
		when td.current_price_type = 'P' then 1
		else 0
	end as pm_indt
	,td.rp_idnt as rp_int
	,td.eoh --stock_on_hand_qty
	,td.asoh --immediately_sellable_qty
	,td.product_views
	,td.current_price_amt
	,td.mc_instock_ind
	,td.allocated_traffic
	,td.traffic
from
	t2dl_das_twist.twist_daily td	

where 
	 td.day_date between (current_date - 14) and (current_date - 1)
	 and td.country = 'US'
) with data primary index(day_date,store_num,rms_sku_num,rms_style_num) 
on commit preserve rows;

ET;


create volatile multiset table twist_base_cal as
(
select
	td.day_date
	,cal.month_num
	,td.banner
	,td.business_unit_desc
	,td.store_num
	,td.rms_style_num
	,td.rms_sku_num
	,td.current_price_type
	,td.price_range
	,td.reg_indt
	,td.cl_indt
	,td.pm_indt
	,td.rp_int
	,td.eoh --stock_on_hand_qty
	,td.asoh --immediately_sellable_qty
	,td.product_views
	,td.current_price_amt
	,td.mc_instock_ind
	,td.allocated_traffic
	,td.traffic
from
	twist_base td	
	
left join (select distinct day_date, month_num from {db_env}_nap_usr_vws.day_cal where day_date between (current_date - 14) and (current_date - 1)) cal
on
	td.day_date = cal.day_date

where 
	td.day_date between (current_date - 14) and (current_date - 1)
) with data primary index(day_date,store_num,rms_sku_num,rms_style_num) on commit preserve rows;

ET;
---CREATING VIEW WITH ALL DESIRED METRICS & DIMENSIONS-----

create volatile multiset table twist_merch as
(
select
	 td.day_date
	,td.month_num
	,td.banner
	,td.business_unit_desc
	,td.store_num
	,td.rms_style_num
	,(td.rms_style_num || '-' || psd.color_num) as cc
	,td.rms_sku_num
	,psd.grp_long_desc
	,psd.div_long_desc
	,td.current_price_type
	,td.price_range
	,td.reg_indt
	,td.cl_indt
	,td.pm_indt
	,td.rp_int
	,td.eoh --stock_on_hand_qty
	,td.asoh --immediately_sellable_qty
	,td.product_views
	,td.current_price_amt
	,td.mc_instock_ind
	,td.allocated_traffic
	,td.traffic
from
	twist_base_cal td	
	
left join sku_dim psd 
on
	td.rms_style_num = psd.rms_style_num
	and td.rms_sku_num = psd.rms_sku_num

where 
     td.day_date between (current_date - 14) and (current_date - 1)
) with data primary index(day_date,store_num,business_unit_desc,rms_sku_num,grp_long_desc,div_long_desc ) on commit preserve rows;

ET;

create volatile multiset table daily_inventory as(
select
	 td.day_date
	,td.banner
	,td.business_unit_desc
	,td.store_num
	,td.rms_style_num
	,td.cc
	,td.rms_sku_num
	,td.grp_long_desc
	,td.div_long_desc
	,td.current_price_type
	,td.price_range
	,td.reg_indt
	,td.cl_indt
	,td.pm_indt
	,td.rp_int
	,los.fl_indt
	,los.ds_indt
	,td.eoh --stock_on_hand_qty
	,td.asoh --immediately_sellable_qty
	,td.product_views
	,td.current_price_amt
	,td.mc_instock_ind
	,td.allocated_traffic
	,td.traffic
	,age.inventory_age_days
from twist_merch td
left join avg_sku_age age
on
	age.month_num = td.month_num
	and td.rms_sku_num = age.rms_sku_num
	and td.store_num = age.store_num
	
left join los_sku los
on
	los.sku_id = td.rms_sku_num
	and td.day_date = los.day_date
	and td.banner = los.banner
	and td.business_unit_desc = los.business_unit_desc
	and td.store_num = los.store_num

   where
   td.day_date between (current_date - 14) and (current_date - 1)
   AND td.business_unit_desc IN ('R.COM','N.COM')

   UNION ALL
   
   select
	 td.day_date
	,td.banner
	,td.business_unit_desc
	,td.store_num
	,td.rms_style_num
	,td.cc
	,td.rms_sku_num
	,td.grp_long_desc
	,td.div_long_desc
	,td.current_price_type
	,td.price_range
	,td.reg_indt
	,td.cl_indt
	,td.pm_indt
	,td.rp_int
	,0 as fl_indt
	,0 as ds_indt
	,td.eoh --stock_on_hand_qty
	,td.asoh --immediately_sellable_qty
	,td.product_views
	,td.current_price_amt
	,td.mc_instock_ind
	,td.allocated_traffic
	,td.traffic
	,age.inventory_age_days
from twist_merch td
left join avg_sku_age age
on
	age.month_num = td.month_num
	and td.rms_sku_num = age.rms_sku_num
	and td.store_num = age.store_num
	
where
   td.day_date between (current_date - 14) and (current_date - 1)
   AND td.business_unit_desc NOT IN ('R.COM','N.COM')
   
) with data primary index(day_date,business_unit_desc,rms_sku_num) on commit preserve rows;

ET;
-------AGGREGATING ABOVE FINAL VIEW-------
DELETE FROM {proto_schema}.MMM_INVENTORY_MERCH_LDG ALL;
ET;

INSERT INTO {proto_schema}.MMM_INVENTORY_MERCH_LDG
select
	day_date
	,banner
	,business_unit_desc
	,store_num
	,grp_long_desc as	subdivision_name
	,div_long_desc as division_name
	,price_range
	,count(distinct rms_style_num) as style_count
	,count(distinct cc) as cc_count
	,count(distinct rms_sku_num) as product_count
	,cast(avg(inventory_age_days) as decimal (38,2)) as avg_inventory_age_days
	,sum(cast(td.eoh as decimal(20,1))) as eoh
	,sum(cast(td.asoh as decimal(20,1))) as asoh
	,sum(td.product_views) product_views
	,sum(td.current_price_amt) current_price_amt
	,sum(case when td.mc_instock_ind = 1 then td.allocated_traffic else 0 end) as instock_traffic
	,sum(td.allocated_traffic) as allocated_traffic
	,sum(td.traffic) as product_traffic
	,sum(rp_int) as rp_indt
	,sum(reg_indt) as reg_indt
	,sum(cl_indt) as cl_indt
	,sum(pm_indt) as pm_indt
	,sum(fl_indt) as fl_indt
	,sum(ds_indt) as ds_indt
	,current_date as dw_batch_date
    ,current_timestamp as dw_sys_load_tmstp
from
	daily_inventory td
where
   td.day_date between (current_date - 14) and (current_date - 1)
group by 1,2,3,4,5,6,7;
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
