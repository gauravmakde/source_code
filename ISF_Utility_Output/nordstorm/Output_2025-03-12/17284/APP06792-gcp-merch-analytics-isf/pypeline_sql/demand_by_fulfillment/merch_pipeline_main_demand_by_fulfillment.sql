/*
Name:Demand by Fulfillment Method script
Project:Demand by Fulfillment Method Dash
Purpose:  In the demand by fulfillment dashboard, there is a custom sql query and the query was using mdam data source, This script is replacing mdam data source to NAP data source(JWN_DEMAND_METRIC_VW)
Variable(s):    {{environment_schema}} T2DL_DAS_ACE_MFP
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
DAG:
Author(s):Xiao Tong
Date Created:1/23/23
Date Last Updated:4/19/24
*/

------------------------------------------------------------- end TEMPORARY TABLES -----------------------------------------------------------------------

-- need to reviste fiscal week alignment logic in the future
CREATE MULTISET VOLATILE TABLE drvr_wk as
(
	select
				day_date
			, fiscal_year_num
			, week_idnt as wk_idnt
			, fiscal_week_num as wk_of_fyr
			, week_454_label as fiscal_week
			, month_454_label as fiscal_month
			, quarter_desc as fiscal_quarter
			, left(half_label,6) as fiscal_half
			, case
				when fiscal_year_num = (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < current_date)
					then CAST('TY' AS varchar(3))
				when fiscal_year_num = (SELECT MAX(fiscal_year_num) -1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < current_date)
					then CAST('LY' AS varchar(3))
			--	when fiscal_year_num = (SELECT MAX(fiscal_year_num) -2 FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
			--		then CAST('LLY' AS varchar(3))
				else 'uh?'
			 end as ty_ly_lly_ind
	from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dcm
	where ty_ly_lly_ind in ('TY', 'LY') and ytd_last_full_week_ind = 'Y'
)
with DATA
PRIMARY INDEX (day_date)
on commit PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (day_date)
	,COLUMN (day_date)
	on drvr_wk;

CREATE MULTISET VOLATILE TABLE demand_by_fulfillment as
(
    select
				case
					when fulfilled_from_location_type = 'Vendor (Drop Ship)'
						then 'DROPSHIP'
    			when line_item_fulfillment_type = 'StorePickup'
    			  then 'STORE PICKUP'
					when fulfilled_from_location_type = 'Store'
						then 'STORE FULFILL'
					when fulfilled_from_location_type in ('Trunk Club','DC','FC')
						then 'FC'
					else 'UNKNOWN'
				end as fulfill_method
		    , trim(DIVISION_NUM || ', ' || DIVISION_NAME) as division
		    , trim(SUBDIVISION_NUM || ', ' || SUBDIVISION_NAME) as subdivision
		    , trim(DEPT_NUM || ', ' || DEPT_NAME) as department
		    , trim(CLASS_NUM || ', ' || CLASS_NAME) as "class"
		    , 'N/A' as subclass -- trim(sbclass_num || ', ' || SBCLASS_NAME) as subclass
		    , SUPPLIER_NAME as supplier
			, COALESCE(price_type, 'Unknown') AS price_type
	        , npg_ind
			, rp_ind
		--  location dimensions
		    , business_unit_desc as channel
		    , fulfilled_from_location as location_index
		--  fiscal dimensions
        , d.wk_of_fyr as fiscal_week_number
		--  data source info
		    , 'NAP' as data_source
			, ty_ly_lly_ind
			, sum(JWN_GROSS_DEMAND_AMT) as demand
			, sum(demand_units) as demand_units
			, sum(case when jwn_fulfilled_demand_ind = 'Y' then JWN_GROSS_DEMAND_AMT else 0 end) as demand_shipped
			, sum(case when jwn_fulfilled_demand_ind = 'Y' then demand_units else 0 end) as demand_shipped_units
	FROM
	PRD_NAP_USR_VWS.JWN_DEMAND_METRIC_VW base
	INNER JOIN drvr_wk d
		ON base.demand_date = d.day_date
	WHERE gift_with_purchase_ind = 'N'
		AND smart_sample_ind = 'N'
		-- New 11/14/23 - exclude same day fraud and customer cancels
		AND same_day_fraud_cancel_ind = 'N'
		AND same_day_cust_cancel_ind = 'N'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
with DATA
PRIMARY INDEX (fulfill_method, department, "class", supplier, location_index, fiscal_week_number, ty_ly_lly_ind)
on commit PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (fulfill_method, department, "class", supplier, location_index, fiscal_week_number, ty_ly_lly_ind)
	,COLUMN (fulfill_method, department, "class", supplier, location_index, fiscal_week_number, ty_ly_lly_ind)
		ON demand_by_fulfillment;
------------------------------------------------------------- END TEMPORARY TABLES -----------------------------------------------------------------------



---------------------------------------------------------------- START MAIN QUERY -------------------------------------------------------------------------
delete from {environment_schema}.demand_by_fulfillment;
insert into {environment_schema}.demand_by_fulfillment
-- get TY, LY, LLY demand/units based on year ind
	select
    --demand dimensions
    fulfill_method
--  product dimensions
    , division
    , subdivision
    , department
    , "class"
    , subclass
    , supplier
	, price_type
	, npg_ind
--  location dimensions
    , case when channel = 'FULL LINE' then '110, FULL LINE STORES'
		  when channel = 'N.COM' then '120, N.COM'
		  when channel = 'RACK' then '210, RACK STORES'
			when channel = 'OFFPRICE ONLINE' then '250, OFFPRICE ONLINE'
			else channel end as channel
    , trim(location_index || ', ' || store_short_name) as location
--  fiscal dimensions
    , wk.fiscal_week_number
    , wk.fiscal_week
    , wk.fiscal_month
    , wk.fiscal_quarter
    , wk.fiscal_half
--  data source info
    , data_source
--  Let users of the data source know which weeks are available
    , (select trim(fiscal_year_num  || ' Week ' || trim(wk_of_fyr))  from drvr_wk where day_date = (select max(day_date) from drvr_wk)) as data_through_week
--ty metrics
    , (case when sub.ty_ly_lly_ind = 'TY' then demand else 0 end) as demand_ty
    , (case when sub.ty_ly_lly_ind = 'TY' then demand_units else 0 end) as demand_units_ty
    , (case when sub.ty_ly_lly_ind = 'TY' then demand_shipped else 0 end) as demand_shipped_ty
    , (case when sub.ty_ly_lly_ind = 'TY' then demand_shipped_units else 0 end) as demand_shipped_units_ty
-- LY metrics
    , (case when sub.ty_ly_lly_ind = 'LY' then demand else 0 end) as demand_ly
    , (case when sub.ty_ly_lly_ind = 'LY' then demand_units else 0 end) as demand_units_ly
    , (case when sub.ty_ly_lly_ind = 'LY' then demand_shipped else 0 end) as demand_shipped_ly
    , (case when sub.ty_ly_lly_ind = 'LY' then demand_shipped_units else 0 end) as demand_shipped_units_ly
-- LLY metrics
    -- , (case when sub.ty_ly_lly_ind = 'LLY' then demand else 0 end) as demand_lly
    -- , (case when sub.ty_ly_lly_ind = 'LLY' then demand_units else 0 end) as demand_units_lly
    -- , (case when sub.ty_ly_lly_ind = 'LLY' then demand_shipped else 0 end) as demand_shipped_lly
    -- , (case when sub.ty_ly_lly_ind = 'LLY' then demand_shipped_units else 0 end) as demand_shipped_units_lly
		, current_timestamp as process_tmstp
		, rp_ind
  from demand_by_fulfillment sub
		left join PRD_NAP_USR_VWS.STORE_DIM sd
		on sd.store_num = sub.location_index
		inner join (
				select
		        distinct fiscal_week_num as fiscal_week_number
      		, week_454_label as fiscal_week
      		, month_454_label as fiscal_month
      		, quarter_desc as fiscal_quarter
      		, left(half_label,6) as fiscal_half
		    from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW
		    where ty_ly_lly_ind = 'TY' and ytd_last_full_week_ind = 'Y'
		) wk
		on wk.fiscal_week_number = sub.fiscal_week_number
		where
    demand_units_ty <> 0
    or demand_units_ly <> 0
    --or demand_units_lly <> 0
    or demand_shipped_units_ty <> 0
    or demand_shipped_units_ly <> 0
    -- or demand_shipped_units_lly <> 0
;
COLLECT STATISTICS
    PRIMARY INDEX (fulfill_method, department, "class", supplier, location, fiscal_week_number)
   on {environment_schema}.demand_by_fulfillment;

