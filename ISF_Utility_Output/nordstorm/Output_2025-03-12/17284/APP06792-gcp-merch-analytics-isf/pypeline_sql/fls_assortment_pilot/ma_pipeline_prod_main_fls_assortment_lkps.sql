-- Sku Lookup
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'pilot_sku_dim', OUT_RETURN_MSG);
create multiset table {environment_schema}.pilot_sku_dim as (
	select
		p.rms_sku_num
		, p.rms_style_num
		, p.style_desc
		, p.style_group_num
		, p.web_style_num
		, p.supp_part_num

		, p.color_num
		, p.color_desc
		, p.size_1_num
		, p.size_1_desc
		, p.size_2_num
		, p.size_2_desc

	    , trim(p.dept_num) || ':' || trim(p.class_num) || ':' || trim(p.sbclass_num) ||':' || trim(p.prmy_supp_num) || ':' || trim(p.supp_part_num) || ':' || trim(p.color_num) || ':' || trim(p.color_desc) as cc_num
		, p.div_num
		, trim(p.div_num) || ', ' || p.div_desc as div_label
		, p.grp_num as sdiv_num
		, trim(p.grp_num) || ', ' || p.grp_desc as sdiv_label
		, p.dept_num as dept_num
		, trim(p.dept_num) || ', ' || p.dept_desc as dept_label
		, p.class_num as class_num
		, trim(p.class_num) || ', ' || p.class_desc as class_label
		, p.sbclass_num as sbclass_num
		, trim(p.sbclass_num) || ', ' || p.sbclass_desc as sbclass_label

		, p.prmy_supp_num as supplier_num
		, v.vendor_name as supplier_name
		, upper(p.brand_name) as brand_name
		, p.npg_ind

		, coalesce(map1.category, map2.category, 'OTHER') as quantrix_category

	from prd_nap_usr_vws.product_sku_dim_vw p
	left join prd_nap_usr_vws.catg_subclass_map_dim map1 on 1=1
		and p.dept_num = map1.dept_num
		and p.class_num = cast(map1.class_num as integer)
		and p.sbclass_num = cast(map1.sbclass_num as integer)
	left join prd_nap_usr_vws.catg_subclass_map_dim map2 on 1=1
		and p.dept_num = map2.dept_num
		and p.class_num = cast(map2.class_num as integer)
		and map2.sbclass_num = '-1'
	left join prd_nap_usr_vws.vendor_dim v on 1=1
		and p.prmy_supp_num = v.vendor_num
	where 1=1
		and p.channel_country = 'US'
)
with data
primary index (rms_sku_num);

collect stats
	primary index (rms_sku_num)
	on {environment_schema}.pilot_sku_dim;

grant select on {environment_schema}.pilot_sku_dim to public;

-- Store Lookup
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'pilot_store_dim', OUT_RETURN_MSG);
create multiset table {environment_schema}.pilot_store_dim as (
	with store_dim_base as (
		select
			s.store_num
			, trim(s.store_num) || ', ' || s.store_short_name as store_label

			, case when s.store_num in (209, 210, 212) then 210 else s.store_num end as store_num_nyc
			, case when s.store_num in (209, 210, 212) then '210+, NYC FLAGSHIP+' else store_label end as store_label_nyc

			, s.channel_num
			, trim(s.channel_num) || ', ' || s.channel_desc as channel_label
			, s.selling_channel
			, s.banner

			, s.store_type_code
			, s.store_type_desc

			, s.store_address_state
			, s.store_address_state_name
			, s.store_postal_code
			, s.store_country_code
			, s.store_dma_desc
			, s.store_region

			, s.store_location_latitude
			, s.store_location_longitude
			, s.comp_status_code
			, s.eligibility_types
			, case
				when s.channel_num = 110 and s.store_type_code = 'FL' and s.store_close_date is null and s.store_num not in (387, 922, 923, 1443, 1446, 8889, 427 /*SAN FRANCISCO*/) then 1
				when s.channel_num = 210 and s.store_close_date is null and s.store_name <> 'UNASSIGNED RK' then 1
				when s.channel_num in (111, 121) then 0
				else 0
			end as active_store_flag

			, s2.gross_square_footage
		from prd_nap_usr_vws.jwn_store_dim_vw s
		left join prd_nap_usr_vws.price_store_dim_vw s2 on 1=1
			and s.store_num = s2.store_num
	)

	select *
	from store_dim_base
	where 1=1
		and channel_num = 110
		and (active_store_flag = 1 or store_num = 209)
)
with data
primary index (store_num);

collect stats
	primary index(store_num)
	on {environment_schema}.pilot_store_dim;


grant select on {environment_schema}.pilot_store_dim to public;

-- Day Lookup
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'pilot_day_dim', OUT_RETURN_MSG);
create multiset table {environment_schema}.pilot_day_dim as (
	WITH ty_ly	AS (
	SELECT
	 CAST('TY' AS VARCHAR(3)) as ty_ly_lly_ind
	 ----- day
	 , a.day_date
	 , a.day_idnt as day_num
	 , a.day_abrv
	 ----- week
	 , a.week_idnt as week_num
	 , a.week_num_of_fiscal_month
	 , a.week_end_day_date as week_end_date
	 , a.fiscal_week_num
	 ---- month
	 , a.month_idnt as month_num
	 , a.month_abrv
	 , a.month_end_day_date as month_end_date
	 ---- quarter
	 , a.quarter_idnt as quarter_num
	 , a.fiscal_quarter_num
	 ---- half
	 , a.fiscal_halfyear_num as half_num
	 ---- year
	 , a.fiscal_year_num as year_num
	FROM PRD_NAP_USR_VWS.day_cal_454_dim a
	WHERE a.fiscal_year_num = (SELECT max(fiscal_year_num) FROM PRD_NAP_USR_VWS.day_cal_454_dim WHERE month_end_day_date <= CURRENT_DATE - 1)

	UNION ALL

	SELECT
	 'LY' as ty_ly_lly_ind
	 ----- day
	 , a.day_date_last_year_realigned as day_date
	 , a.day_idnt - 1000 AS day_num
	 , b.day_abrv
	 ----- week
	 , a.week_idnt - 100 AS week_num
	 , a.week_num_of_fiscal_month
	 , b.week_end_day_date as week_end_date
	 , a.fiscal_week_num
	 ---- month
	 , a.month_idnt - 100 AS month_num
	 , a.month_abrv
	 , b.month_end_day_date as month_end_date
	 ---- quarter
	 , CAST(TRIM(TRIM(a.fiscal_year_num - 1) || TRIM(a.fiscal_quarter_num)) AS INTEGER) AS quarter_idnt
	 , a.fiscal_quarter_num
	 ---- half
	 , a.fiscal_halfyear_num - 10 AS fiscal_halfyear_num
	 ---- year
	 , a.fiscal_year_num - 1 AS fiscal_year_num
	FROM PRD_NAP_USR_VWS.day_cal_454_dim a
	INNER JOIN PRD_NAP_USR_VWS.day_cal_454_dim b
	 ON a.day_date_last_year_realigned = b.day_date
	WHERE a.fiscal_year_num = (SELECT max(fiscal_year_num) FROM PRD_NAP_USR_VWS.day_cal_454_dim WHERE month_end_day_date <= CURRENT_DATE - 1)

	UNION ALL

	SELECT
	 'LLY' AS ty_ly_lly_ind
	 ----- day
	 , b.day_date_last_year_realigned as day_date
	 , a.day_idnt - 2000 AS day_num
	 , c.day_abrv
	 ----- week
	 , a.week_idnt - 200 AS week_num
	 , a.week_num_of_fiscal_month
	 , c.week_end_day_date as week_end_date
	 , a.fiscal_week_num
	 ---- month
	 , a.month_idnt - 200 AS month_idnt
	 , a.month_abrv
	 , c.month_end_day_date as month_end_date
	 ---- quarter
	 , CAST(TRIM(TRIM(a.fiscal_year_num - 2) || TRIM(a.fiscal_quarter_num)) AS INTEGER) AS quarter_idnt
	 , a.fiscal_quarter_num
	 ---- half
	 , a.fiscal_halfyear_num - 20 AS fiscal_halfyear_num
	 ---- year
	 , a.fiscal_year_num - 2 AS fiscal_year_num
	FROM PRD_NAP_USR_VWS.day_cal_454_dim a
	INNER JOIN PRD_NAP_USR_VWS.day_cal_454_dim b
	 ON a.day_date_last_year_realigned = b.day_date
	INNER JOIN PRD_NAP_USR_VWS.day_cal_454_dim c
	 ON b.day_date_last_year_realigned = c.day_date
	WHERE a.fiscal_year_num = (SELECT max(fiscal_year_num) FROM PRD_NAP_USR_VWS.day_cal_454_dim WHERE month_end_day_date <= CURRENT_DATE - 1)
	)
	SELECT
	   b.day_date
	   , b.day_num
	   , left(b.day_abrv, 1) || lower(right(b.day_abrv, 2)) as day_abrv
	   , b.day_index
	   , b.first_day_of_week_flag
	   , b.last_day_of_week_flag
	   ----- week
	   , b.week_num
	   , b.week_end_date
	   , 'Week ' || cast(b.fiscal_week_num as varchar(2)) || ' ' || year_label as week_label
	   , b.week_index
	   , b.first_week_of_month_flag
	   , b.last_week_of_month_flag
	   ---- month
	   , b.month_num
	   , left(b.month_abrv, 1) || lower(right(b.month_abrv, 2)) as month_abrv
	   , left(b.month_abrv, 1) || lower(right(b.month_abrv, 2)) || ' ' || year_label as month_label
	   , b.month_index
	   ---- quarter
	   , b.quarter_num
	   , 'Q' || cast(b.fiscal_quarter_num as varchar(1)) || ' ' || year_label as quarter_label
	   ---- half
	   , b.half_num
	   , 'H' || cast(b.half_num mod 10 as varchar(1)) || ' ' || year_label as half_label
	   ---- year
	   , b.year_num
	   , 'FY''' || right(cast(b.year_num as varchar(4)), 2) as year_label
		 , c.week_idnt as true_week_num
	FROM (
	    SELECT
	        a.*
	    		, case when day_num = min(day_num) over (partition by week_num) then 1 else 0 end as first_day_of_week_flag
	    		, case when day_num = max(day_num) over (partition by week_num) then 1 else 0 end as last_day_of_week_flag
	    		, case when week_num = min(week_num) over (partition by month_num) then 1 else 0 end as first_week_of_month_flag
	    		, case when week_num = max(week_num) over (partition by month_num) then 1 else 0 end as last_week_of_month_flag
	    		, case when day_date < current_date then 1 else 0 end as day_complete_flag
	    		, case when week_end_date < current_date then 1 else 0 end as week_complete_flag
	    		, case when month_end_date < current_date then 1 else 0 end as month_complete_flag
	    		, day_complete_flag * dense_rank() over (partition by day_complete_flag order by day_num desc) as day_index
	    		, week_complete_flag * dense_rank() over (partition by week_complete_flag order by week_num desc) as week_index
	    		, month_complete_flag * dense_rank() over (partition by month_complete_flag order by month_num desc) as month_index
	    FROM ty_ly a
	) b
	LEFT JOIN prd_nap_usr_vws.day_cal_454_dim c
  ON b.day_date = c.day_date

	WHERE month_index >= 1
		AND month_num >= 202201
)
with data
primary index (day_num);


grant select on {environment_schema}.pilot_day_dim to public;
