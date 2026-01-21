--------------- BEGIN: WAC Incorporation
-- Entire WAC Design re-designed and completed on 2022.08.01

-- 2022.04.26 - Identification of WAC data for time period of the delta operation
-- DROP TABLE WAC_01;
CREATE MULTISET VOLATILE TABLE WAC_01
AS 
(
	select sku_num 
		,location_num
		,weighted_average_cost_currency_code
		,weighted_average_cost
		,eff_begin_dt
		,eff_end_dt
		,row_number() over (partition by sku_num, location_num order by eff_end_dt desc) rn
	from PRD_NAP_USR_VWS.WEIGHTED_AVERAGE_COST_DATE_DIM
	-- This date will be variable based on the delta operations of the pipeline
	where eff_end_dt >= {start_date}
)
with data
primary index ( sku_num, location_num, eff_begin_dt, eff_end_dt )
on commit preserve rows;

/*
collect stats
	primary index ( sku_num, location_num, eff_begin_dt, eff_end_dt )
	,column (sku_num, location_num, eff_begin_dt, eff_end_dt )
	,column ( rn, eff_end_dt )
		on wac_01;
*/

-- 2022.07.29 - Extract out all row_id's less price type for the time frame being pulled
create multiset volatile table sku_loc_01
as (
	select sku_idnt
		,day_dt
		,loc_idnt
		,row_id
	from {environment_schema}.SKU_LOC_PRICETYPE_DAY_VW slpdv 
	where day_dt between {start_date} and {end_date}
	group by 1,2,3,4
)
with data
primary index ( sku_idnt, day_dt, loc_idnt )
on commit preserve rows;

collect stats
	primary index ( sku_idnt, day_dt, loc_idnt )
	,column ( sku_idnt, day_dt, loc_idnt )
		on sku_loc_01;

-- 2022.07.29 - Identify only CA sku's and prep for CAD/USD conversion
create multiset volatile table wac_02_can
as (
	select a.sku_idnt
		,a.day_dt
		,a.loc_idnt
		,a.row_id
		,b.weighted_average_cost_currency_code
		,c.market_rate
	from sku_loc_01 a 
		inner join 
			(select location_num
				,weighted_average_cost_currency_code
			from wac_01  
			where weighted_average_cost_currency_code = 'CAD'
			group by 1,2) b
			on a.loc_idnt = b.location_num
		inner join 
			(select start_date
				,end_date
				,market_rate
				,from_country_currency_code 
			 from prd_nap_usr_vws.currency_exchg_rate_dim
			 where from_country_currency_code = 'CAD'
			 	and to_country_currency_code = 'USD'
			 group by 1,2,3,4) c 
			 on a.day_dt between c.start_date and c.end_date
			 	and b.weighted_average_cost_currency_code = c.from_country_currency_code
)
with DATA 
primary index ( sku_idnt, day_dt, loc_idnt )
on commit preserve rows;

collect stats
	primary index ( sku_idnt, day_dt, loc_idnt )
	,column ( sku_idnt, day_dt, loc_idnt )
	,column ( row_id )
		on wac_02_can;
	
-- 2022.07.29 - From the data that is to be updated, associate specifically Canadian locations with the currency market rate for final execution
create multiset volatile table sku_loc_final
as (
select sku_idnt
	,day_dt
	,loc_idnt
	,row_id
	,market_rate
from
	(select a.sku_idnt
		,a.day_dt
		,a.loc_idnt
		,a.row_id
		,cast(NULL as DECIMAL(8,4)) market_rate
	from sku_loc_01 a
		left join wac_02_can b 
			on a.row_id = b.row_id
	where b.row_id is null
	union all
	select sku_idnt
		,day_dt
		,loc_idnt
		,row_id
		,market_rate
	from wac_02_can) a
group by 1,2,3,4,5
)
with data
primary index ( sku_idnt, day_dt, loc_idnt )
on commit preserve rows;

collect stats
	primary index ( sku_idnt, day_dt, loc_idnt )
	,column ( sku_idnt, day_dt, loc_idnt )
		on sku_loc_final;
	
-- 2022.07.29 - Identify the WAC for each row_id and convert CAD to USD where relevant	
create multiset volatile table wac_final
as (
	select row_id
		,sku_idnt
		,day_dt
		,loc_idnt
		,market_rate
		,weighted_average_cost
		,weighted_average_cost * coalesce(market_rate,1) weighted_average_cost_new
	from sku_loc_final a 
		inner join wac_01 b 
			on a.sku_idnt = b.sku_num
				and a.loc_idnt = b.location_num
	where a.day_dt between b.eff_begin_dt and eff_end_dt - 1
	group by 1,2,3,4,5,6,7
)
with DATA 
primary index ( row_id )
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '{partition_start_date}' AND DATE '{partition_end_date}' EACH INTERVAL '1' DAY , NO RANGE)
on commit preserve rows;

collect stats
	primary index ( row_id )
	,column ( row_id )
	,column ( day_dt )
		on wac_final;

-- 2022.05.02 - Update the final dataset with WAC values where matched
	-- 2022.07.29 - Adjust for adjusted build to accommodate CAD/US conversion in transit
UPDATE {environment_schema}.SKU_LOC_PRICETYPE_DAY 
FROM wac_final b
	SET 
		weighted_average_cost = b.weighted_average_cost_new
		,update_timestamp = current_timestamp
WHERE SKU_LOC_PRICETYPE_DAY.DAY_DT BETWEEN {start_date} AND {end_date}
	and SKU_LOC_PRICETYPE_DAY.ROW_ID = b.ROW_ID;
	
	
-------------------- WAC THAT ENDS
	-- Overview: The WAC logic above is with the premise that sku/loc/day continually persists over time, i.e. whether with an arbitrary end date or new rows representing differing states of WAC
	-- Currently, there haven't been any sku/loc/day's that end without a current/future state WAC presented. While this may not exist today, we are building this in in the chance it does exist in the future

-- 2022.05.02 - Identify if any of the records that have a WAC value ending doesn't have a new WAC value starting
	-- This is a safeguard statement. While this doesn't exist today, if a WAC value ends without a new value with new timelines begins on tha same day
		-- we want to capture that with the actual effective end date. Otherwise, we end up with a gap situation
CREATE MULTISET VOLATILE TABLE WAC_ENDING
AS 
(
	SELECT *
	FROM WAC_01
	WHERE rn = 1
		and eff_end_dt <> '9999-12-31'
)
WITH DATA 
PRIMARY INDEX ( SKU_NUM, LOCATION_NUM, EFF_BEGIN_DT, EFF_END_DT )
ON COMMIT PRESERVE ROWS;

COLLECT stats
	primary index ( sku_num, location_num, eff_begin_dt, eff_end_dt )
	,column (sku_num, location_num, eff_begin_dt, eff_end_dt )
		on WAC_ENDING;
	
-- 2022.07.29 - Identify the WAC for each row_id and convert CAD to USD where relevant	
create multiset volatile table wac_ending_final
as (
	select row_id
		,sku_idnt
		,day_dt
		,loc_idnt
		,market_rate
		,weighted_average_cost
		,weighted_average_cost * coalesce(market_rate,1) weighted_average_cost_new
	from sku_loc_final a 
		inner join wac_ending b 
			on a.sku_idnt = b.sku_num
				and a.loc_idnt = b.location_num
	where a.day_dt between b.eff_begin_dt and eff_end_dt
	group by 1,2,3,4,5,6,7
)
with DATA 
primary index ( row_id )
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '{partition_start_date}' AND DATE '{partition_end_date}' EACH INTERVAL '1' DAY , NO RANGE)
on commit preserve rows;

collect stats
	primary index ( row_id )
	,column ( row_id )
	,column ( day_dt )
		on wac_ending_final;

-- 2022.05.02 - Update the WAC value with any gap scenario's if exists
UPDATE {environment_schema}.SKU_LOC_PRICETYPE_DAY 
FROM WAC_ENDING_FINAL b
	SET 
		weighted_average_cost = b.weighted_average_cost
		,update_timestamp = current_timestamp
WHERE SKU_LOC_PRICETYPE_DAY.DAY_DT BETWEEN {start_date} AND {end_date}
	and SKU_LOC_PRICETYPE_DAY.ROW_ID = b.ROW_ID;

--------------- END: WAC Incorporation

--------------- BEGIN: Gross Margin

-- 2022.08.02 - Implementation of product_margin
	-- We are not changing the field name from gross_margin to product_margin as the alter statement would be too much cpu for this.
	-- Rather, we will handle this through the table's view
update {environment_schema}.sku_loc_pricetype_day
	set gross_margin = sku_loc_pricetype_day.sales_dollars - sku_loc_pricetype_day.cost_of_goods_sold
where sku_loc_pricetype_day.day_dt between {start_date} and {end_date}
	and sku_loc_pricetype_day.sales_dollars is not null 
	and sku_loc_pricetype_day.cost_of_goods_sold is not null;

--------------- END: Gross Margin

--------------- BEGIN: Final Stats on permanent output datasets
/*
COLLECT stats
	PRIMARY INDEX ( row_id )
	,COLUMN ( row_id )
	,COLUMN ( day_dt )
	,COLUMN ( sku_idnt, loc_idnt )
	,COLUMN ( PARTITION )
		ON {environment_schema}.SKU_LOC_PRICETYPE_DAY;
*/

--------------- END: Final Stats on permanent output datasets
