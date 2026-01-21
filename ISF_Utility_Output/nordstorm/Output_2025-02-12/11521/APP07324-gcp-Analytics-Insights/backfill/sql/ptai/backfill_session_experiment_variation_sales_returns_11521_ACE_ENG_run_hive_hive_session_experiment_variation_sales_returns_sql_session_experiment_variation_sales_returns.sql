create table if not exists ace_etl.session_experiment_variation_sales_returns (
    channel string,
	channelcountry string,
	experience string,
	projectname string,
	variationname string,
    first_activity_date_pst date,
    last_activity_date_pst date,
	total_unique_sessions bigint,
	total_sales_units DECIMAL (38,2),
    total_sales_quantity bigint,
	total_returns_units DECIMAL (38,2),
	total_returns_quantity bigint,
	dollar_return_rate DECIMAL (6,4),
    return_rate_results string,
    total_sessions_srm_flag string,
    purchasing_sessions_srm_flag string,
    bonferroni_flag int,
    unique_purchasing_session_ids bigint,
    unique_returning_session_ids bigint,
    experimentname string
)
using PARQUET
location "s3://ace-etl/session_experiment_variation_sales_returns"
partitioned by (experimentname);

MSCK REPAIR table ace_etl.session_experiment_variation_sales_returns;

-- Pull experiments that are still running or have ended less than 90 days from the end_date
-- 90 days is the max return window, thats when we stop running
create or replace temporary view last_90_days as 
select
	experimentname,
	min(first_activity_date_pst) as min_first_activity_date_pst,
	max(last_activity_date_pst) as max_last_activity_date_pst
from ace_etl.session_experiment_variation_metadata
where last_activity_date_pst >= date'2024-04-08' - interval '90' day
group by 1 
;

-- Create a date array to help with partition of session_sales_returns table later 
create or replace temporary view date_array as 
SELECT distinct explode(date_array) as day
from (
	select
	SEQUENCE(
	        cast(min(min_first_activity_date_pst) AS date), 
	        cast(max(max_last_activity_date_pst) AS date), 
	        INTERVAL '1' DAY
	      ) AS date_array
	from last_90_days
)
lateral view outer explode(date_array) as day
;

-- Need to pull the date the PdM stopped the experiment and moved 100% of population to one arm
create or replace temporary view stop_date as
select
    experimentname,
    channel,
    channelcountry,
    experience,
    projectname,
    min(last_activity_date_pst) as stop_date
from ace_etl.session_experiment_variation_metadata
group by 1,2,3,4,5
;

-- Now pull all the orders that occured before or during the day the PdM turned the test off 
create or replace temporary view stop_date_orders as 
select
    ord.session_id,
	ord.projectname,
	ord.channel, 
	ord.channelcountry,
	ord.experience, 
	ord.experimentname, 
	ord.variationname, 
	ord.orderlineid,
	stop.stop_date
from ace_etl.session_experiment_variation_orders ord
left join stop_date stop
on ord.projectname = stop.projectname
and ord.channel = stop.channel
and ord.channelcountry = stop.channelcountry
and ord.experience = stop.experience
and ord. experimentname = stop.experimentname
where ord.experimentname in (select experimentname from last_90_days)
and ord.activity_date_pst <= stop.stop_date
;

-- Join sales and returns data in and use the date array from before to limit the amount of sales/returns data we pull 
create or replace temporary view sales_returns as 
select
	ord.projectname,
	ord.channel, 
	ord.channelcountry,
	ord.experience, 
	ord.experimentname, 
	ord.variationname,
	sum(sale_order_line_current_units) as total_sales_units,
	sum(sale_quantity) as total_sales_quantity,
	sum(return_order_line_current_units) as total_returns_units,
	sum(return_quantity) as total_returns_quantity,
	count(distinct case when sale_order_line_current_units is not null then ord.session_id end) as unique_purchasing_session_ids,
	count(distinct case when return_order_line_current_units is not null then ord.session_id end) as unique_returning_session_ids
from  stop_date_orders ord
inner join ace_etl.session_sales_returns_transactions sale
on ord.orderlineid = sale.orderlineid
where sale.activity_date_partition IN (SELECT * FROM date_array)
and ord.experimentname in (select experimentname from last_90_days)
and ord.stop_date >= sale.activity_date_partition 
group by 1,2,3,4,5,6
;

-- Count the number of variations in the experiment starting with default, then pull the max of the row_number to determine the number of variations
-- If the number of variations is greater than 2 then we apply the bonferroni correction
create or replace temporary view bonferroni_check as 
select  
    channel,
	channelcountry,
	experience,
	projectname,
	experimentname,
	variationname,
    first_activity_date_pst,
    last_activity_date_pst,
	total_unique_sessions,
	variation_rk,
	max(variation_rk) over(partition by channel, experience, projectname, experimentname) as total_variations
from 
	(
	select     
	    channel,
		channelcountry,
		experience,
		projectname,
		experimentname,
		variationname,
	    first_activity_date_pst,
	    last_activity_date_pst,
		total_unique_sessions,
		row_number() over(partition by channel, experience, projectname, experimentname order by case when variationname like 'default' then 0 else 1 end, variationname desc) variation_rk
	from ace_etl.session_experiment_variation_metadata
	WHERE experimentname IN (SELECT DISTINCT experimentname from last_90_days)
	)
group by 1,2,3,4,5,6,7,8,9,10
;

-- Perform two-tailed z-test for proportions with 90% confidence and apply bonferroni correction where variation count is > 2
create or replace temporary view stats_testing as 
select
	channel,
	channelcountry,
	experience,
	projectname,
	experimentname,
	variationname,
	variation_rk,
	total_variations,
	first_activity_date_pst,
   	last_activity_date_pst,
	total_unique_sessions,
	total_sales_units,
	total_sales_quantity,
	total_returns_units,
	total_returns_quantity,
	total_returns_units*1.0000/total_sales_units*1.0000 as dollar_return_rate,
    case when variationname <> 'default' then RR_result_query else 'control arm' end as return_rate_results,
    unique_purchasing_session_ids,
    unique_returning_session_ids
from (
    select
        projectname,
		channel,
		channelcountry,
		experience,
		experimentname,
		variationname,
		total_sales_units,
		total_sales_quantity,
		total_returns_units,
		total_returns_quantity,
        unique_purchasing_session_ids,
        unique_returning_session_ids,
		variation_rk,
		total_variations,
		first_activity_date_pst,
		last_activity_date_pst,
		total_unique_sessions,
		control_sales,
		test_sales,
		control_returns,
		test_returns,
		RR_pool_p_query,
		RR_q_query,
		RR_test_sample_p_query,
		RR_control_sample_p_query,
		z,
		CTR_z_query,
        case
        	when CTR_z_query >= z then 'losing'
            when CTR_z_query <= -z then 'winning'
            else 'not stat sig'
        end as RR_result_query
    from
    (
        select
            projectname,
			channel,
			channelcountry,
			experience,
			experimentname,
			variationname,
			total_sales_units,
			total_sales_quantity,
			total_returns_units,
			total_returns_quantity,
            unique_purchasing_session_ids,
            unique_returning_session_ids,
			variation_rk,
			total_variations,
			first_activity_date_pst,
    		last_activity_date_pst,			
			total_unique_sessions,
			control_sales,
			test_sales,
			control_returns,
			test_returns,
			RR_pool_p_query,
			RR_q_query,
			RR_test_sample_p_query,
			RR_control_sample_p_query,
            case 
            	when total_variations = 2 then 1.645
            	when total_variations = 3 then 1.96
            	else 2.13
            end as z,
            (RR_test_sample_p_query-RR_control_sample_p_query)/sqrt(RR_pool_p_query * RR_q_query / test_sales + RR_pool_p_query * RR_q_query / control_sales) as CTR_z_query
        from
        (
            select
				projectname,
				channel,
				channelcountry,
				experience,
				experimentname,
				variationname,
				total_sales_units,
				total_sales_quantity,
				total_returns_units,
				total_returns_quantity,
                unique_purchasing_session_ids,
				unique_returning_session_ids,
				variation_rk,
				total_variations,
				first_activity_date_pst,
				last_activity_date_pst,
				total_unique_sessions,
				control_sales,
				test_sales,
				control_returns,
				test_returns,
                case when variationname <> 'default' then (control_returns+test_returns)*1.00000000/(control_sales+test_sales)*1.00000000 end as RR_pool_p_query,
                1-case when variationname <> 'default' then (control_returns+test_returns)*1.00000000/(control_sales+test_sales)*1.00000000 end as RR_q_query,
                test_returns*1.00000000/test_sales*1.00000000 as RR_test_sample_p_query,
                control_returns*1.00000000/control_sales*1.00000000 as RR_control_sample_p_query
            from
            (
                select
                    base.projectname,
					base.channel,
					base.channelcountry,
					base.experience,
					base.experimentname,
					base.variationname,
					base.total_sales_units,
					base.total_sales_quantity,
					base.total_returns_units,
					base.total_returns_quantity,
                    base.unique_purchasing_session_ids,
                    base.unique_returning_session_ids,
					rk.variation_rk,
					rk.total_variations,
					rk.first_activity_date_pst,
    				rk.last_activity_date_pst,
					rk.total_unique_sessions,
                    sum(case when base.variationname = 'default' then base.total_sales_units end) over (partition by  base.channel,base.experience,base.projectname,base.experimentname) as control_sales,
                    case when base.variationname <>'default' then base.total_sales_units else null end test_sales,
                    sum(case when base.variationname = 'default' then  base.total_returns_units end) over (partition by  base.channel,base.experience,base.projectname,base.experimentname) as control_returns,
                    case when base.variationname <>'default' then base.total_returns_units else null end test_returns
                from sales_returns base 
                inner join bonferroni_check rk
                on base.channel = rk.channel
				and base.experience = rk.experience
				and base.projectname = rk.projectname
				and base.experimentname = rk.experimentname
				and base.variationname = rk.variationname
                WHERE base.experimentname IN (SELECT DISTINCT experimentname from last_90_days)
                and total_variations <> 1
            )
        )
    )
)
;

-- For experiments with 2 arms, check for SRM 
create or replace temporary view two_arm_srm as
select 
	*,
	case 
		when control_total_sessions > .55
		or control_total_sessions < .45
		or control_total_sessions is null 
		or test_total_sessions is null 
		then 'srm'
		else 'okay'
	end as total_sessions_srm_flag,
	case 
		when control_total_purchasing_sessions > .55
		or control_total_purchasing_sessions < .45
		or control_total_purchasing_sessions is null 
		or test_total_purchasing_sessions is null 
		then 'srm'
		else 'okay'
	end as purchasing_sessions_srm_flag
from
	(
	select
	    channel,
	    channelcountry,
		experience,
		projectname,
		experimentname,
	    sum(case when variationname = 'default' then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as control_total_sessions,
	    sum(case when variationname <> 'default' then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as test_total_sessions,
	    sum(case when variationname = 'default' then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as control_total_purchasing_sessions,
	    sum(case when variationname <> 'default' then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as test_total_purchasing_sessions
	from stats_testing 
	where total_variations = 2 
	group by 1,2,3,4,5
	)
    ;

-- For experiments with 3 arms, check for SRM 
create or replace temporary view three_arm_srm as 
select 
	*,
	case 
		when control_total_sessions > .383
		or control_total_sessions < .283
		or control_total_sessions is null 
		or test_two_total_sessions is null 
		or test_three_total_sessions is null 
		then 'srm'
		else 'okay'
	end as total_sessions_srm_flag,
	case 
		when control_total_purchasing_sessions > .383
		or control_total_purchasing_sessions < .283
		or control_total_purchasing_sessions is null 
		or test_two_total_purchasing_sessions is null 
		or test_three_total_purchasing_sessions is null 
		then 'srm'
		else 'okay'
	end as purchasing_sessions_srm_flag
from
	(
	select
	    channel,
	    channelcountry,
		experience,
		projectname,
		experimentname,
	    sum(case when variation_rk = 1 then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as control_total_sessions,
	    sum(case when variation_rk = 2 then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as test_two_total_sessions,
	    sum(case when variation_rk = 3 then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as test_three_total_sessions,
	    sum(case when variation_rk = 1 then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as control_total_purchasing_sessions,
	    sum(case when variation_rk = 2 then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as test_two_total_purchasing_sessions,
	    sum(case when variation_rk = 3 then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as test_three_total_purchasing_sessions
	from stats_testing 
	where total_variations = 3
	group by 1,2,3,4,5
	)
    ;

-- For experiments with 4 arms, check for SRM. I have never seen 4 arms but maybe they will occur in the future. 
create or replace temporary view four_arm_srm as 
select 
	*,
	case 
		when control_total_sessions > .30
		or control_total_sessions < .20
		or control_total_sessions is null 
		or test_two_total_sessions is null 
		or test_three_total_sessions is null
		or test_four_total_sessions is null 
		then 'srm'
		else 'okay'
	end as total_sessions_srm_flag,
	case 
		when control_total_purchasing_sessions > .30
		or control_total_purchasing_sessions < .20
		or control_total_purchasing_sessions is null 
		or test_two_total_purchasing_sessions is null 
		or test_three_total_purchasing_sessions is null 
		or test_four_total_purchasing_sessions is null
		then 'srm'
		else 'okay'
	end as purchasing_sessions_srm_flag
from
	(
	select
	    channel,
	    channelcountry,
		experience,
		projectname,
		experimentname,
	    sum(case when variation_rk = 1 then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as control_total_sessions,
	    sum(case when variation_rk = 2 then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as test_two_total_sessions,
	    sum(case when variation_rk = 3 then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as test_three_total_sessions,
	    sum(case when variation_rk = 4 then total_unique_sessions end)*1.000/sum(total_unique_sessions)*1.000 as test_four_total_sessions,
	    sum(case when variation_rk = 1 then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as control_total_purchasing_sessions,
	    sum(case when variation_rk = 2 then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as test_two_total_purchasing_sessions,
	    sum(case when variation_rk = 3 then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as test_three_total_purchasing_sessions,
	    sum(case when variation_rk = 4 then unique_purchasing_session_ids end)*1.000/sum(unique_purchasing_session_ids)*1.000 as test_four_total_purchasing_sessions
	from stats_testing 
	where total_variations = 4
	group by 1,2,3,4,5
	)
    ;

-- Join everything together for output
create or replace temporary view output as
select 
    stats.channel,
	stats.channelcountry,
	stats.experience,
	stats.projectname,
	stats.variationname,
    stats.first_activity_date_pst,
    stats.last_activity_date_pst,
	stats.total_unique_sessions,
	stats.total_sales_units,
	stats.total_sales_quantity,
	stats.total_returns_units,
	stats.total_returns_quantity,
	round(stats.dollar_return_rate,4) as dollar_return_rate,
    stats.return_rate_results,
    coalesce(two.total_sessions_srm_flag, three.total_sessions_srm_flag, four.total_sessions_srm_flag, 'MISSING') as total_sessions_srm_flag,
    coalesce(two.purchasing_sessions_srm_flag, three.purchasing_sessions_srm_flag, four.purchasing_sessions_srm_flag, 'MISSING') as purchasing_sessions_srm_flag,
    case when total_variations > 2 then 1 else 0 end as bonferroni_flag,
    stats.unique_purchasing_session_ids,
    stats.unique_returning_session_ids,
    stats.experimentname
from stats_testing stats
left join two_arm_srm two 
on  stats.channel = two.channel
and	stats.experience = two.experience
and	stats.projectname = two.projectname
and	stats.experimentname = two.experimentname
left join three_arm_srm three
on  stats.channel = three.channel
and	stats.experience = three.experience
and	stats.projectname = three.projectname
and	stats.experimentname = three.experimentname
left join four_arm_srm four 
on  stats.channel = four.channel
and	stats.experience = four.experience
and	stats.projectname = four.projectname
and	stats.experimentname = four.experimentname
;

insert
    OVERWRITE TABLE ace_etl.session_experiment_variation_sales_returns PARTITION (experimentname)
select
    *
from
    output;