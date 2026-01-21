--------------- BEGIN: Sales Units/Dollars Data Health creation and check

-- 2021.10.06 - Sales Units/Dollar validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_sls_01;
create multiset volatile table merch_sku_sls_01
as (
select 	a.day_dt
	,a.sales_units
	,b.sale_units
	,((cast(a.sales_units as decimal(12,2))-cast(b.sale_units as decimal(12,2)))/nullifzero(cast(a.sales_units as decimal(12,2)))) sales_units_percent_diff
	,a.sales_dollars
	,b.sale_retl$
	,((a.sales_dollars-b.sale_retl$)/nullifzero(a.sales_dollars)) sales_dollars_percent_diff
from
	(select day_dt
		,sum(sales_units) sales_units 
		,sum(sales_dollars) sales_dollars 
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and current_date
	group by 1) a join
	(select b.day_date
		,sum(sale_units) sale_units
		,sum(sale_retl$) sale_retl$
		,sum(sale_net_prft$) sale_net_prft$
	from prd_ma_bado.sale_sku_ld_rvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num 
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with data
primary index ( day_dt )
on commit preserve rows;

collect stats 
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_sls_01;

-- 2021.10.08 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff only exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_sls_final;
create multiset volatile table merch_sku_sls_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,SOURCE_KPI_COMPARISON_PERCENT_DIFF
	,current_timestamp as process_timestamp
from (
	select day_dt
		,cast('sales_units' as varchar(50)) as kpi_name
		,cast(sales_units as decimal(15,2)) as source_kpi
		,cast(sale_units as decimal(15,2)) as source_kpi_comparison
		,sales_units_percent_diff as SOURCE_KPI_COMPARISON_PERCENT_DIFF
	from merch_sku_sls_01
	group by 1,2,3,4,5
	union
	select day_dt
		,cast('sales_dollars' as varchar(50)) as kpi_name
		,sales_dollars as source_kpi
		,sale_retl$ as source_kpi_comparison
		,sales_dollars_percent_diff as SOURCE_KPI_COMPARISON_PERCENT_DIFF
	from merch_sku_sls_01
	group by 1,2,3,4,5) a
)
with DATA 
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats 
	primary index ( day_dt, job_name, kpi_name )
	,column (day_dt, job_name, kpi_name )
		on merch_sku_sls_final;

-- 2021.10.08 - Upsert the data needed to the data health table for monitoring
MERGE INTO
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH AS a
USING 
MERCH_SKU_SLS_FINAL AS b
ON 
	a.DAY_DT = b.DAY_DT
	AND a.JOB_NAME = b.JOB_NAME
	AND a.KPI_NAME = b.KPI_NAME
WHEN MATCHED
THEN UPDATE
	SET 
		SOURCE_KPI = b.SOURCE_KPI
		,SOURCE_KPI_COMPARISON = b.SOURCE_KPI_COMPARISON
		,SOURCE_KPI_COMPARISON_PERCENT_DIFF = b.SOURCE_KPI_COMPARISON_PERCENT_DIFF
		,PROCESS_TIMESTAMP = b.PROCESS_TIMESTAMP		
WHEN NOT MATCHED
THEN INSERT
	VALUES 
	(
	b.DAY_DT
	,b.JOB_NAME
	,b.KPI_NAME
	,b.SOURCE_KPI
	,b.SOURCE_KPI_COMPARISON
	,b.SOURCE_KPI_COMPARISON_PERCENT_DIFF
	,b.PROCESS_TIMESTAMP
	);	


/* - How to check individual or chunks of data comparison. This will not be part of the actual pipeline though.
-- 2021.10.06 - Sales Units/Dollars Validation Between MADM and NAP
	-- Here are the records that don't match by day/sku/location
select b.day_dt
	,b.sku_idnt
	,b.loc_idnt
	,b.sales_units
	,a.sale_units
	,b.sales_dollars
	,a.sale_retl$
from 
	(select b.day_date
		,sku_idnt
		,loc_idnt
		,sum(sale_units) sale_units
		,sum(sale_retl$) sale_retl$
	from prd_ma_bado.sale_sku_ld_rvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num 
	where b.day_date between current_date - 10 and current_date
	group by 1,2,3) a JOIN 
	(select day_dt
		,sku_idnt
		,loc_idnt
		,sum(sales_units) sales_units 
		,sum(sales_dollars) sales_dollars 
	 from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY
	 where day_dt between current_date - 10 and current_date
	 group by 1,2,3) b 
	 	on a.day_date = b.day_dt
	 		and a.sku_idnt = b.sku_idnt
	 		and a.loc_idnt = b.loc_idnt
where b.sales_units <> a.sale_units
--Match examples
	--	and b.sku_idnt = 35773314
--Doesn't Match Examples
	and b.sku_idnt = '37804377'
order by 1,3
sample 100;

-- Here are the records that match by day/sku/location
select b.day_dt
	,b.sku_idnt
	,b.loc_idnt
	,b.sales_units
	,a.sale_units
	,b.sales_dollars
	,a.sale_retl$
	,a.sale_net_prft$
from 
	(select b.day_date
		,sku_idnt
		,loc_idnt
		,sum(sale_units) sale_units
		,sum(sale_retl$) sale_retl$
		,sum(sale_net_prft$) sale_net_prft$
	from prd_ma_bado.sale_sku_ld_rvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num 
	where b.day_date between current_date - 10 and current_date
	group by 1,2,3) a JOIN 
	(select day_dt
		,sku_idnt
		,loc_idnt
		,sum(sales_units) sales_units 
		,sum(sales_dollars) sales_dollars 
	 from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY
	 where day_dt between current_date - 10 and current_date
	 group by 1,2,3) b 
	 	on a.day_date = b.day_dt
	 		and a.sku_idnt = b.sku_idnt
	 		and a.loc_idnt = b.loc_idnt
where b.sales_units = a.sale_units
--Match examples
	--	and b.sku_idnt = 35773314
--Doesn't Match Examples
	and b.sku_idnt = '37804377'
order by 1,3
sample 100;
*/
	
--------------- END: Sales Units/Dollars Data Health creation and check

--------------- BEGIN: Return Units/Dollars Data Health creation and check

-- 2021.10.13 - Return Units/Dollar validation between MADM and NAP
	-- In totality This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_return_01;
create multiset volatile table merch_sku_return_01
as (
select a.day_dt
	,a.return_units
	,b.rtrn_units
	,((cast(a.return_units as decimal(12,2))-cast(b.rtrn_units as decimal(12,2)))/nullifzero(cast(a.return_units as decimal(12,2)))) return_units_percent_diff
	,a.return_dollars
	,b.rtrn_retl$
	,((a.return_dollars-b.rtrn_retl$)/nullifzero(a.return_dollars)) return_dollars_percent_diff
from (
	select day_dt
		,sum(return_units) return_units
		,sum(return_dollars) return_dollars
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and current_date
	group by 1) a join
	(select b.day_date
		,sum(rtrn_units) rtrn_units
		,sum(rtrn_retl$) rtrn_retl$
	from prd_ma_bado.sale_sku_ld_rvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num 
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with data 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_return_01;

-- 2021.10.13 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff only exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_return_final;
create multiset volatile table merch_sku_return_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,SOURCE_KPI_COMPARISON_PERCENT_DIFF
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('return_units' as varchar(50)) as kpi_name
		,cast(return_units as decimal(15,2)) as source_kpi
		,cast(rtrn_units as decimal(15,2)) as source_kpi_comparison
		,return_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_return_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('return_dollars' as varchar(50)) as kpi_name
		,return_dollars as source_kpi
		,rtrn_retl$ as source_kpi_comparison
		,return_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_return_01
	group by 1,2,3,4,5) a
)
with data 
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_return_final;
	
-- 2021.10.13 - Upsert the data needed to the data health table for monitoring
merge into 
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using 
merch_sku_return_final as b
on
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when matched
then update
	set
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not matched
then insert
	values
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	);

--------------- END: Return Units/Dollars Data Health creation and check


--------------- BEGIN: Demand Total Units/Dollars Data Health creation and check

-- 2021.10.13 - Demand Total Units/Dollars validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_demand_tot_01;
create multiset volatile table merch_sku_demand_tot_01
as (
select a.day_dt
	,a.demand_units
	,b.co_dmnd_tot_units
	,((cast(a.demand_units as decimal(12,2))-cast(b.co_dmnd_tot_units as decimal(12,2)))/nullifzero(cast(a.demand_units as decimal(12,2)))) demand_units_percent_diff
	,a.demand_dollars
	,b.co_dmnd_tot_retl$
	,((a.demand_dollars-b.co_dmnd_tot_retl$)/nullifzero(a.demand_dollars)) demand_dollars_percent_diff	
from
	(select day_dt
		,sum(demand_units) demand_units
		,sum(demand_dollars) demand_dollars
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and current_date
	group by 1) a join
	(select b.day_date
		,sum(co_dmnd_tot_units) co_dmnd_tot_units
		,sum(co_dmnd_tot_retl$) co_dmnd_tot_retl$
	from prd_ma_bado.co_dmnd_sku_ld_rvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num
	where b.day_date between current_date - 7 and CURRENT_DATE
	group by 1) b 
		on a.day_dt = b.day_date
)
with DATA 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_demand_tot_01;
	
-- 2021.10.13 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff onky exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_demand_tot_final;
create multiset volatile table merch_sku_demand_tot_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,source_kpi_comparison_percent_diff
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('demand_total_units' as varchar(50)) as kpi_name
		,cast(demand_units as decimal(15,2)) as source_kpi
		,cast(co_dmnd_tot_units as decimal(15,2)) as source_kpi_comparison
		,demand_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_demand_tot_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('demand_total_dollars' as varchar(50)) as kpi_name
		,demand_dollars as source_kpi
		,co_dmnd_tot_retl$ as source_kpi_comparison
		,demand_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_demand_tot_01) a 
)
with data
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_demand_tot_final;

-- 2021.10.13 - Upsert the data needed to the data health table for monitoring
merge into
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using
merch_sku_demand_tot_final as b
on 
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when matched
then update
	set 
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	);

--------------- END: Demand Total Units/Dollars Data Health creation and check


--------------- BEGIN: Demand Cancel Units/Dollars Data Health creation and check

-- 2021.10.13 - Demand Cancel Units/Dollars validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_demand_cancel_01;
create multiset volatile table merch_sku_demand_cancel_01
as (
select a.day_dt
	,a.demand_cancel_units
	,b.co_cncl_tot_units
	,((cast(a.demand_cancel_units as decimal(12,2))-cast(b.co_cncl_tot_units as decimal(12,2)))/nullifzero(cast(a.demand_cancel_units as decimal(12,2)))) demand_cancel_units_percent_diff
	,a.demand_cancel_dollars
	,b.co_cncl_tot_retl$
	,((a.demand_cancel_dollars-b.co_cncl_tot_retl$)/nullifzero(a.demand_cancel_dollars)) demand_cancel_dollars_percent_diff
from 
	(select day_dt 
		,sum(demand_cancel_units) demand_cancel_units 
		,sum(demand_cancel_dollars) demand_cancel_dollars 
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and CURRENT_DATE
	group by 1) a JOIN 
	(select b.day_date
		,sum(co_cncl_frd_tot_units)+sum(co_cncl_sd_tot_units) co_cncl_tot_units
		,sum(co_cncl_frd_tot_retl$)+sum(co_cncl_sd_tot_retl$) co_cncl_tot_retl$
	from prd_ma_bado.co_dmnd_sku_ld_rvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with DATA 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_demand_cancel_01;

-- 2021.10.13 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff onky exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_demand_cancel_final;
create multiset volatile table merch_sku_demand_cancel_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,source_kpi_comparison_percent_diff
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('demand_cancel_units' as varchar(50)) as kpi_name
		,cast(demand_cancel_units as decimal(15,2)) as source_kpi
		,cast(co_cncl_tot_units as decimal(15,2)) as source_kpi_comparison
		,demand_cancel_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_demand_cancel_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('demand_cancel_dollars' as varchar(50)) as kpi_name
		,demand_cancel_dollars as source_kpi
		,co_cncl_tot_retl$ as source_kpi_comparison
		,demand_cancel_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_demand_cancel_01) a 
)
with data
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_demand_cancel_final;
		
-- 2021.10.13 - Upsert the data needed to the data health table for monitoring
merge into
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using
merch_sku_demand_cancel_final as b
on
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when MATCHED 
then UPDATE 
	set
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	);

--------------- END: Demand Cancel Units/Dollars Data Health creation and check


--------------- BEGIN: Demand Dropship Units/Dollars Data Health creation and check

-- 2021.10.13 - Demand Dropship Units/Dollars validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_demand_ds_01;
create multiset volatile table merch_sku_demand_ds_01
as (
select a.day_dt
	,a.demand_dropship_units
	,b.co_dmnd_tot_ds_units
	,((cast(a.demand_dropship_units as decimal(12,2))-cast(b.co_dmnd_tot_ds_units as decimal(12,2)))/nullifzero(cast(a.demand_dropship_units as decimal(12,2)))) demand_dropship_units_percent_diff
	,a.demand_dropship_dollars
	,b.co_dmnd_tot_ds_retl$
	,((a.demand_dropship_dollars-b.co_dmnd_tot_ds_retl$)/nullifzero(a.demand_dropship_dollars)) demand_dropship_dollars_percent_diff
from 
	(select day_dt 
		,sum(demand_dropship_units) demand_dropship_units 
		,sum(demand_dropship_dollars) demand_dropship_dollars 
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and CURRENT_DATE
	group by 1) a JOIN 
	(select b.day_date
		,sum(co_dmnd_tot_ds_units) co_dmnd_tot_ds_units
		,sum(co_dmnd_tot_ds_retl$) co_dmnd_tot_ds_retl$
	from prd_ma_bado.co_dmnd_sku_ld_cvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with DATA 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_demand_ds_01;
	
-- 2021.10.13 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff onky exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_demand_dropship_final;
create multiset volatile table merch_sku_demand_dropship_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,source_kpi_comparison_percent_diff
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('demand_dropship_units' as varchar(50)) as kpi_name
		,cast(demand_dropship_units as decimal(15,2)) as source_kpi
		,cast(co_dmnd_tot_ds_units as decimal(15,2)) as source_kpi_comparison
		,demand_dropship_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_demand_ds_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('demand_dropship_dollars' as varchar(50)) as kpi_name
		,demand_dropship_dollars as source_kpi
		,co_dmnd_tot_ds_retl$ as source_kpi_comparison
		,demand_dropship_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_demand_ds_01) a 
)
with data
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_demand_dropship_final;
		
-- 2021.10.13 - Upsert the data needed to the data health table for monitoring
merge into
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using
merch_sku_demand_dropship_final as b
on
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when MATCHED 
then UPDATE 
	set
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	);

--------------- END: Demand Dropship Units/Dollars Data Health creation and check


--------------- BEGIN: Store Fulfill Units/Dollars Data Health creation and check	

--------------- END: Store Fulfill Units/Dollars Data Health creation and check	


--------------- BEGIN: EOH/BOH Fulfill Units/Dollars Data Health creation and check	
	
--------------- END: EOH/BOH Units/Dollars Data Health creation and check	


--------------- BEGIN: Receipt TOT Units/Dollars Data Health creation and check	

-- 2021.10.12 - Receipt Total Units/Dollars validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_rcpt_tot_01;
create multiset volatile table merch_sku_rcpt_tot_01
as (
select a.day_dt
	,a.receipt_units
	,b.rcpt_units
	,((cast(a.receipt_units as decimal(12,2))-cast(b.rcpt_units as decimal(12,2)))/nullifzero(cast(a.receipt_units as decimal(12,2)))) receipt_units_percent_diff
	,a.receipt_dollars
	,b.rcpt_dollars
	,((a.receipt_dollars-b.rcpt_dollars)/nullifzero(a.receipt_dollars)) receipt_dollars_percent_diff
from 
	(select day_dt 
		,sum(receipt_units) receipt_units 
		,sum(receipt_dollars) receipt_dollars 
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and CURRENT_DATE
	group by 1) a JOIN 
	(select b.day_date
		,sum(rcpt_tot_units) rcpt_units
		,sum(rcpt_tot_retl$) rcpt_dollars
	from prd_ma_bado.rcpt_sku_ld_rvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with data 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_rcpt_tot_01;

-- 2021.10.12 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff onky exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_rcpt_tot_final;
create multiset volatile table merch_sku_rcpt_tot_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,source_kpi_comparison_percent_diff
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('receipt_total_units' as varchar(50)) as kpi_name
		,cast(receipt_units as decimal(15,2)) as source_kpi
		,cast(rcpt_units as decimal(15,2)) as source_kpi_comparison
		,receipt_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_tot_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('receipt_total_dollars' as varchar(50)) as kpi_name
		,receipt_dollars as source_kpi
		,rcpt_dollars as source_kpi_comparison
		,receipt_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_tot_01) a 
)
with data
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_rcpt_tot_final;

-- 2021.10.12 - Upsert the data needed to the data health table for monitoring
merge into
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using
merch_sku_rcpt_tot_final as b
on
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when MATCHED 
then UPDATE 
	set
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	);

--------------- END: Receipt TOT Units/Dollars Data Health creation and check	


--------------- BEGIN: Receipt PO Units/Dollars Data Health creation and check	

-- 2021.10.14 - Receipt PO Units/Dollars validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_rcpt_po_01;
create multiset volatile table merch_sku_rcpt_po_01
as (
select a.day_dt
	,a.receipt_po_units
	,b.rcpt_units
	,((cast(a.receipt_po_units as decimal(12,2))-cast(b.rcpt_units as decimal(12,2)))/nullifzero(cast(a.receipt_po_units as decimal(12,2)))) receipt_po_units_percent_diff
	,a.receipt_po_dollars
	,b.rcpt_dollars
	,((a.receipt_po_dollars-b.rcpt_dollars)/nullifzero(a.receipt_po_dollars)) receipt_po_dollars_percent_diff
from 
	(select day_dt 
		,sum(receipt_po_units) receipt_po_units 
		,sum(receipt_po_dollars) receipt_po_dollars 
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and CURRENT_DATE
	group by 1) a JOIN 
	(select b.day_date
		,sum(rcpt_po_tot_units) rcpt_units
		,sum(rcpt_po_tot_retl$) rcpt_dollars
	from prd_ma_bado.rcpt_sku_ld_cvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with data 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_rcpt_po_01;
	
-- 2021.10.14 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff onky exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_rcpt_po_final;
create multiset volatile table merch_sku_rcpt_po_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,source_kpi_comparison_percent_diff
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('receipt_po_units' as varchar(50)) as kpi_name
		,cast(receipt_po_units as decimal(15,2)) as source_kpi
		,cast(rcpt_units as decimal(15,2)) as source_kpi_comparison
		,receipt_po_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_po_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('receipt_po_dollars' as varchar(50)) as kpi_name
		,receipt_po_dollars as source_kpi
		,rcpt_dollars as source_kpi_comparison
		,receipt_po_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_po_01) a 
)
with data
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_rcpt_po_final;

-- 2021.10.14 - Upsert the data needed to the data health table for monitoring
merge into
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using
merch_sku_rcpt_po_final as b
on
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when MATCHED 
then UPDATE 
	set
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	);

--------------- END: Receipt PO Units/Dollars Data Health creation and check	


--------------- BEGIN: Receipt PAH Units/Dollars Data Health creation and check	

-- 2021.10.14 - Receipt PAH Units/Dollars validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_rcpt_pah_01;
create multiset volatile table merch_sku_rcpt_pah_01
as (
select a.day_dt
	,a.receipt_pah_units
	,b.rcpt_units
	,((cast(a.receipt_pah_units as decimal(12,2))-cast(b.rcpt_units as decimal(12,2)))/nullifzero(cast(a.receipt_pah_units as decimal(12,2)))) receipt_pah_units_percent_diff
	,a.receipt_pah_dollars
	,b.rcpt_dollars
	,((a.receipt_pah_dollars-b.rcpt_dollars)/nullifzero(a.receipt_pah_dollars)) receipt_pah_dollars_percent_diff
from 
	(select day_dt 
		,sum(receipt_pah_units) receipt_pah_units 
		,sum(receipt_pah_dollars) receipt_pah_dollars 
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and CURRENT_DATE
	group by 1) a JOIN 
	(select b.day_date
		,sum(rcpt_pah_tot_units) rcpt_units
		,sum(rcpt_pah_tot_retl$) rcpt_dollars
	from prd_ma_bado.rcpt_sku_ld_cvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with data 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_rcpt_pah_01;
	
-- 2021.10.14 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff onky exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_rcpt_pah_final;
create multiset volatile table merch_sku_rcpt_pah_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,source_kpi_comparison_percent_diff
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('receipt_pah_units' as varchar(50)) as kpi_name
		,cast(receipt_pah_units as decimal(15,2)) as source_kpi
		,cast(rcpt_units as decimal(15,2)) as source_kpi_comparison
		,receipt_pah_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_pah_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('receipt_pah_dollars' as varchar(50)) as kpi_name
		,receipt_pah_dollars as source_kpi
		,rcpt_dollars as source_kpi_comparison
		,receipt_pah_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_pah_01) a 
)
with data
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_rcpt_pah_final;

-- 2021.10.14 - Upsert the data needed to the data health table for monitoring
merge into
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using
merch_sku_rcpt_pah_final as b
on
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when MATCHED 
then UPDATE 
	set
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	);

--------------- END: Receipt PAH Units/Dollars Data Health creation and check	


--------------- BEGIN: Receipt Dropship Units/Dollars Data Health creation and check	

-- 2021.10.14 - Receipt Dropship Units/Dollars validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_rcpt_ds_01;
create multiset volatile table merch_sku_rcpt_ds_01
as (
select a.day_dt
	,a.receipt_dropship_units
	,b.rcpt_units
	,((cast(a.receipt_dropship_units as decimal(12,2))-cast(b.rcpt_units as decimal(12,2)))/nullifzero(cast(a.receipt_dropship_units as decimal(12,2)))) receipt_dropship_units_percent_diff
	,a.receipt_dropship_dollars
	,b.rcpt_dollars
	,((a.receipt_dropship_dollars-b.rcpt_dollars)/nullifzero(a.receipt_dropship_dollars)) receipt_dropship_dollars_percent_diff
from 
	(select day_dt 
		,sum(receipt_dropship_units) receipt_dropship_units 
		,sum(receipt_dropship_dollars) receipt_dropship_dollars 
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and CURRENT_DATE
	group by 1) a JOIN 
	(select b.day_date
		,sum(rcpt_dsh_tot_units) rcpt_units
		,sum(rcpt_dsh_tot_retl$) rcpt_dollars
	from prd_ma_bado.rcpt_sku_ld_cvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with data 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_rcpt_ds_01;
	
-- 2021.10.14 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff onky exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_rcpt_ds_final;
create multiset volatile table merch_sku_rcpt_ds_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,source_kpi_comparison_percent_diff
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('receipt_dropship_units' as varchar(50)) as kpi_name
		,cast(receipt_dropship_units as decimal(15,2)) as source_kpi
		,cast(rcpt_units as decimal(15,2)) as source_kpi_comparison
		,receipt_dropship_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_ds_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('receipt_dropship_dollars' as varchar(50)) as kpi_name
		,receipt_dropship_dollars as source_kpi
		,rcpt_dollars as source_kpi_comparison
		,receipt_dropship_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_ds_01) a 
)
with data
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_rcpt_ds_final;

-- 2021.10.14 - Upsert the data needed to the data health table for monitoring
merge into
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using
merch_sku_rcpt_ds_final as b
on
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when MATCHED 
then UPDATE 
	set
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	);

--------------- END: Receipt Dropship Units/Dollars Data Health creation and check	


--------------- BEGIN: Receipt Reserve Stock Units/Dollars Data Health creation and check	

-- 2021.10.14 - Receipt Reserve Stock Units/Dollars validation between MADM and NAP
	-- In totality. This includes skus/locations that may exist in one system and not the other
-- drop table merch_sku_rcpt_rs_01;
create multiset volatile table merch_sku_rcpt_rs_01
as (
select a.day_dt
	,a.receipt_reservestock_units
	,b.rcpt_units
	,((cast(a.receipt_reservestock_units as decimal(12,2))-cast(b.rcpt_units as decimal(12,2)))/nullifzero(cast(a.receipt_reservestock_units as decimal(12,2)))) receipt_reservestock_units_percent_diff
	,a.receipt_reservestock_dollars
	,b.rcpt_dollars
	,((a.receipt_reservestock_dollars-b.rcpt_dollars)/nullifzero(a.receipt_reservestock_dollars)) receipt_reservestock_dollars_percent_diff
from 
	(select day_dt 
		,sum(receipt_reservestock_units) receipt_reservestock_units 
		,sum(receipt_reservestock_dollars) receipt_reservestock_dollars 
	from T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY{env_suffix}
	where day_dt between current_date - 7 and CURRENT_DATE
	group by 1) a JOIN 
	(select b.day_date
		,sum(rcpt_rsk_tot_units) rcpt_units
		,sum(rcpt_rsk_tot_retl$) rcpt_dollars
	from prd_ma_bado.rcpt_sku_ld_cvw a join
		prd_nap_usr_vws.day_cal b 
			on a.day_idnt = b.day_num
	where b.day_date between current_date - 7 and current_date
	group by 1) b
		on a.day_dt = b.day_date
)
with data 
primary index ( day_dt )
on commit preserve rows;

collect stats
	primary index ( day_dt )
	,column ( day_dt )
		on merch_sku_rcpt_rs_01;
	
-- 2021.10.14 - Create final data to be upserted in the proceeding process
	-- Days for average percent off is based on rolling 7 days right now
	-- The comparison percent diff onky exists if there is something to compare against. For this pipeline, it is NAP vs. MADM and it is on a day per day comparison
-- drop table merch_sku_rcpt_rs_final;
create multiset volatile table merch_sku_rcpt_rs_final
as (
select day_dt
	,'ma_pipeline_prod_main_sku_day_loc_price_type_kpi' as job_name
	,kpi_name
	,source_kpi
	,source_kpi_comparison
	,source_kpi_comparison_percent_diff
	,current_timestamp as process_timestamp
from (
	select day_dt 
		,cast('receipt_reservestock_units' as varchar(50)) as kpi_name
		,cast(receipt_reservestock_units as decimal(15,2)) as source_kpi
		,cast(rcpt_units as decimal(15,2)) as source_kpi_comparison
		,receipt_reservestock_units_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_rs_01
	group by 1,2,3,4,5
	UNION 
	select day_dt 
		,cast('receipt_reservestock_dollars' as varchar(50)) as kpi_name
		,receipt_reservestock_dollars as source_kpi
		,rcpt_dollars as source_kpi_comparison
		,receipt_reservestock_dollars_percent_diff as source_kpi_comparison_percent_diff
	from merch_sku_rcpt_rs_01) a 
)
with data
primary index ( day_dt, job_name, kpi_name )
on commit preserve rows;

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
		on merch_sku_rcpt_rs_final;
	
-- 2021.10.14 - Upsert the data needed to the data health table for monitoring
merge into
T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH as a 
using
merch_sku_rcpt_rs_final as b
on
	a.day_dt = b.day_dt
	and a.job_name = b.job_name
	and a.kpi_name = b.kpi_name
when MATCHED 
then UPDATE 
	set
		source_kpi = b.source_kpi
		,source_kpi_comparison = b.source_kpi_comparison
		,source_kpi_comparison_percent_diff = b.source_kpi_comparison_percent_diff
		,process_timestamp = b.process_timestamp
when not MATCHED 
then INSERT 
	VALUES 
	(
	b.day_dt
	,b.job_name
	,b.kpi_name
	,b.source_kpi
	,b.source_kpi_comparison
	,b.source_kpi_comparison_percent_diff
	,b.process_timestamp
	); 

--------------- END: Receipt Reserve Stock Units/Dollars Data Health creation and check	


--------------- BEGIN: Final Stats on permanent output datasets

collect stats
	primary index ( day_dt, job_name, kpi_name )
	,column ( day_dt, job_name, kpi_name )
	,column ( day_dt )
		on T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_HEALTH;

--------------- END: Final Stats on permanent output datasets
