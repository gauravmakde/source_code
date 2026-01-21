create volatile multiset table rp as (
	select
		rms_sku_num
		, location_num
		, on_sale_date
		, off_sale_date
		, lead(on_sale_date, 1) over (partition by rms_sku_num, location_num order by on_sale_date, off_sale_date) as next_on_sale_date
		, case
			when off_sale_date - next_on_sale_date >= 0
				then next_on_sale_date - 1
			else off_sale_date
		end as off_sale_date_adj
	from (
		select
			rp.rms_sku_num
			, rp.location_num
			, rp.on_sale_date
			, rp.off_sale_date
		from prd_nap_usr_vws.rp_sku_loc_dim_hist rp
		qualify row_number() over (partition by rms_sku_num, location_num, on_sale_date order by change_date desc) = 1
	) x
)
with data
primary index (rms_sku_num, location_num)
on commit preserve rows;

collect stats
	primary index (rms_sku_num, location_num)
	on rp;


create volatile multiset table trans as (
	select
		h.acp_id

	      -- change bopus intent_store_num to ringing_store_num 808 or 867
		, case
			when line_item_order_type = 'CustInitWebOrder'
				and line_item_fulfillment_type = 'StorePickup'
				and line_item_net_amt_currency_code = 'USD'
		      then 808
		    when line_item_order_type = 'CustInitWebOrder'
				and line_item_fulfillment_type = 'StorePickup'
				and line_item_net_amt_currency_code = 'CAD'
				then 867
		    else intent_store_num
		end as store_num

        , h.ringing_store_num
        , h.fulfilling_store_num
        , t.intent_store_num
        , coalesce(h.fulfilling_store_num, t.intent_store_num) as wac_store_num

	    -- use order date is there is one
	    , case
	    	when t.line_net_usd_amt > 0 and t.tran_type_code in ('SALE', 'EXCH') then coalesce(h.order_date, t.tran_date)
	    	when t.line_net_usd_amt < 0 and t.tran_type_code in ('RETN', 'EXCH') then t.tran_date
	    end as sale_date

	    -- define trip based on store_num + date
	    , cast(trim(h.acp_id) || ':' || trim(store_num) || ':' || trim(sale_date) as varchar(150)) as trip_id

	    , t.global_tran_id
	    , t.business_day_date
	    , h.order_num

	    , case
	    	when t.line_net_usd_amt > 0 and t.tran_type_code in ('SALE', 'EXCH') then 1
	    	else 0
	    end as positive_sale_flag

	    , case
	    	when t.line_net_usd_amt < 0 and t.tran_type_code in ('RETN', 'EXCH') then 1
	    	else 0
	    end as negative_sale_flag

	    , coalesce(t.sku_num, t.hl_sku_num) as sku_num

	    , case
	    	when positive_sale_flag = 1 then t.line_item_quantity
	    	when negative_sale_flag = 1 then -t.line_item_quantity
	    end as units

	    , t.line_net_usd_amt as sale_amt

	    , t.tran_type_code
	    , t.tran_line_id
	    , t.line_item_seq_num
	    , t.line_item_activity_type_desc
	    , t.line_item_fulfillment_type
	    , t.line_item_order_type
	    , t.employee_discount_flag
	    , case when t.line_item_promo_usd_amt < 0 then 1 else 0 end as promo_flag

	from prd_nap_usr_vws.retail_tran_detail_fact t
	join prd_nap_usr_vws.retail_tran_hdr_fact h on 1=1
		and t.global_tran_id = h.global_tran_id
		and t.business_day_date = h.business_day_date
	where 1=1
		and t.line_item_merch_nonmerch_ind = 'MERCH'
		and h.acp_id is not null
		and sale_date between (select min(day_date) from {environment_schema}.pilot_day_dim) and (select max(day_date) from {environment_schema}.pilot_day_dim)
		and store_num in (select store_num from {environment_schema}.pilot_store_dim)
)
with data
primary index(trip_id, acp_id, sku_num, store_num)
on commit preserve rows;


collect stats
	primary index (trip_id, acp_id, sku_num, store_num)
	, column (acp_id)
	, column (sale_date)
	, column (trip_id, acp_id, sale_date)
	, column (store_num)
	, column (sku_num)
	, column (business_day_date)
	, column (acp_id, global_tran_id)
	, column (order_num, sku_num, tran_line_id)
	, column (wac_store_num)
	on trans;


create volatile multiset table loyalty_base as (
	select
		a.acp_id
		, a.mktg_profile_type
		, a.acp_loyalty_id as loyalty_id
		, initcap(l.rewards_level) as nordy_level
		, l.start_day_date
		, case
			when l.end_day_date = lead(start_day_date, 1) over (partition by a.acp_id, a.acp_loyalty_id order by start_day_date)
				then l.end_day_date - interval '1' day
			else l.end_day_date
		end as end_day_date
	from prd_nap_usr_vws.analytical_customer a
	left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw l on 1=1
		and a.acp_loyalty_id = l.loyalty_id
	where 1=1
		and l.start_day_date <= (select max(day_date) from {environment_schema}.pilot_day_dim)
		and l.end_day_date >= (select min(day_date) from {environment_schema}.pilot_day_dim)
		--and a.acp_id in (select acp_id from trans)
)
with data
primary index (acp_id)
on commit preserve rows;


collect stats
	primary index (acp_id)
	, column (start_day_date, end_day_date)
	on loyalty_base;


create volatile multiset table trans_trips as (
	select
		trip_id
		, acp_id
		, sale_date
	from trans
	group by 1, 2, 3
)
with data
primary index (acp_id, sale_date)
on commit preserve rows;

create volatile multiset table loyalty as (
	select
		t.trip_id
		, l.nordy_level
		, row_number() over (partition by t.trip_id order by
			case
				when nordy_level = 'Member' then 1
				when nordy_level = 'Influencer' then 2
				when nordy_level = 'Ambassador' then 3
				when nordy_level = 'Icon' then 4
			end desc) as rn
	from trans_trips t
	join loyalty_base l on 1=1
		and t.acp_id = l.acp_id
		and t.sale_date between l.start_day_date and l.end_day_date
	qualify rn = 1
)
with data
primary index (trip_id)
on commit preserve rows;


create volatile multiset table customer_ntn as (
	select
		trip_id
		, max(case when n.acp_id is not null then 1 else 0 end) as ntn_flag
	from trans t
	join prd_nap_usr_vws.customer_ntn_fact n on 1=1
		and t.acp_id = n.acp_id
		and t.global_tran_id = n.ntn_global_tran_id
	group by 1
)
with data
primary index (trip_id)
on commit preserve rows;



create volatile multiset table shipped_orders as (
	select
		o.order_num
		, o.rms_sku_num
		, o.order_line_num
		, o.destination_country_code as country_code

		--for online orders, use ship-to dma; otherwise, use store dma
		, case
			when o.destination_country_code = 'US' then d2.dma_desc
			when o.destination_country_code = 'CA' then case
				when d3.ca_dma_code = 405 then 'Riviere-du-Loup, QC'
				when d3.ca_dma_code = 412 then 'Sept-Iles, QC'
				when d3.ca_dma_code = 421 then 'Quebec, QC'
				when d3.ca_dma_code = 442 then 'Trois-Rivieres, QC'
				when d3.ca_dma_code = 462 then 'Montreal, QC'
				else d3.ca_dma_desc
			end
		end as dma

		, case
			when o.destination_country_code = 'US' then z.state_name
			when o.destination_country_code = 'CA' then d3.prov_desc
		end as state_province

		, case
			when o.destination_country_code = 'US' then z.state_code
			when o.destination_country_code = 'CA' then d3.prov_code
		end as state_province_code
	from prd_nap_usr_vws.order_line_detail_fact o
	--dma for orders shipped to customers
	left join prd_nap_usr_vws.org_us_zip_dma d1 on 1=1
		and o.destination_country_code = 'US'
		and o.destination_zip_code = d1.us_zip_code
	left join prd_nap_usr_vws.org_dma d2 on 1=1
		and d1.us_dma_code = d2.dma_code
	left join prd_nap_usr_vws.org_ca_zip_dma d3 on 1=1
		and o.destination_country_code = 'CA'
		and left(o.destination_zip_code, 3) = d3.postal_code
	--state/province for orders shipped to u.s. customers
	left join (
		select distinct
			country_code
			, zip_code
			, case
				when state_code = 'DC' then 'District of Columbia'
				else initcap(state_name)
			end as state_name
			, state_code
		from prd_nap_usr_vws.zip_codes_dim
	) z on 1=1
		and o.destination_country_code = z.country_code
		and o.destination_zip_code = z.zip_code
	where 1=1
		and o.order_num in (select order_num from trans where order_num is not null)
		--and o.rms_sku_num in (select sku_idnt from skus)
)
with data
primary index (order_num, rms_sku_num, order_line_num)
on commit preserve rows;


create volatile multiset table wac as (
	select
		sku_num
		, location_num
		, weighted_average_cost
		, weighted_average_cost_currency_code
		, eff_begin_dt
		--, eff_end_dt
		, case
			when eff_end_dt = lead(eff_begin_dt, 1) over (partition by sku_num, location_num order by eff_begin_dt, eff_end_dt)
				then eff_end_dt - interval '1' day
			else eff_end_dt
		end as eff_end_dt_mod
	from prd_nap_usr_vws.weighted_average_cost_date_dim c
	where 1=1
		and eff_begin_dt <= (select max(day_date) from {environment_schema}.pilot_day_dim)
	qualify 1=1
		and eff_end_dt_mod >= (select min(day_date) from {environment_schema}.pilot_day_dim)
)
with data
primary index (sku_num, location_num)
on commit preserve rows;


create multiset volatile table trans_base as (
	select
		t.*
		, p.cc_num
		, t.sale_amt / nullif(t.units, 0) as unit_price



		/*--for online orders, use ship-to state/province; otherwise, use store state/province
		, case
			when o.order_num is not null then o.state_province
			else s.state_province
		end as state_province*/

		--for online orders, use ship-to dma; otherwise, use store dma
		, case
			when o.order_num is not null then o.dma
			else s.store_dma_desc
		end as dma

		, coalesce(n.ntn_flag, 0) as ntn_flag

		, coalesce(l.nordy_level, 'Nonmember') as nordy_level

		, seg.predicted_segment as customer_segment

		, coalesce(w.weighted_average_cost, w2.weighted_average_cost) as weighted_average_cost
		--, coalesce(w.weighted_average_cost, w2.weighted_average_cost) * coalesce(e.market_rate, 1), 2) as weighted_average_cost

		, case when rp.rms_sku_num is not null then 1 else 0 end as rp_flag

	from trans t
	join {environment_schema}.pilot_sku_dim p on 1=1
		and t.sku_num = p.rms_sku_num
	join {environment_schema}.pilot_store_dim s on 1=1
		and t.store_num = s.store_num

	--customer acquisition
	left join customer_ntn n on 1=1
		and t.trip_id = n.trip_id

	--customer loyalty
	left join loyalty l on 1=1
		and t.trip_id = l.trip_id

	--customer segment (note: leaving T3 dependency for prod)
	left join t3dl_ace_ma.core_target_current seg on 1=1
		and t.acp_id = seg.acp_id

	left join shipped_orders o on 1=1
		and t.order_num = o.order_num
		and t.sku_num = o.rms_sku_num
		and t.tran_line_id = o.order_line_num

	--weighted average cost: use business day date; if wac data not available for specific location/date, use current location wac
	left join wac w on 1=1
		and t.sku_num = w.sku_num
		and t.wac_store_num = w.location_num
		and t.business_day_date between w.eff_begin_dt and w.eff_end_dt_mod
	left join wac w2 on 1=1
		and t.sku_num = w2.sku_num
		and t.wac_store_num = w2.location_num
		and w2.eff_end_dt_mod = date'9999-12-31'

	left join rp on 1=1
		and t.sku_num = rp.rms_sku_num
		and t.intent_store_num = rp.location_num
		and t.sale_date between rp.on_sale_date and rp.off_sale_date_adj
)
with data
primary index(trip_id, sku_num, sale_date, store_num)
on commit preserve rows;

collect stats
	primary index (trip_id, acp_id, sku_num, store_num)
	, column (acp_id)
	, column (sale_date)
	, column (trip_id, acp_id, sale_date)
	, column (store_num)
	, column (sku_num)
	, column (business_day_date)
	, column (acp_id, global_tran_id)
	, column (order_num, sku_num, tran_line_id)
	, column (wac_store_num)
	on trans;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'fls_assortment_pilot_trans_base', OUT_RETURN_MSG);
create multiset table {environment_schema}.fls_assortment_pilot_trans_base
as (
select
	t.*
	, current_timestamp as data_update_tmstp
from trans_base t
) with data
primary index (trip_id, sku_num, store_num, sale_date);

collect stats
	primary index (trip_id, sku_num, sale_date, store_num)
	, column (sku_num)
	, column (store_num)
	, column (sale_date)
	, column (sku_num, sale_date)
	, column (acp_id)
	, column (trip_id)
	, column (sku_num, store_num, sale_date)
	on {environment_schema}.fls_assortment_pilot_trans_base;

grant select on {environment_schema}.fls_assortment_pilot_trans_base to public;
