
-- Table Name:f2dd_product_cvr
-- Team/Owner: Digital and Customer Experience/Cassie Zhang
-- Date Created/Modified: created on 12/14/2023, last modified on 12/14/2023
-- What is the update cadence/lookback window: refresh daily and 3 days lookback window 

create table if not exists {hive_schema}.F2DD_product_CVR (
    activity_date date, 
    channel string,
    experience string, 
    channelcountry string, 
	session_id string,
	with_acp_id int, 
	acp_id string, 
	pageinstance_id string, 
	context_pagetype string,
	page_indicator string, 
	PDV_timestamp timestamp, 
	pdv_productstyle_id string,
	f2dd_enticement_shown_flag int, 
	f2dd_enticement_flag string, 
	zip_changed_flag string,
	assigned_zip string,
	market string,
	conv_flag int,
	brand string,
	department string, 
	class string,
	subclass string,
	product_type1 string, 
	product_type2 string, 
	division string, 
	sub_division string, 
	activity_date_partition date
) 
using PARQUET 
location 's3://{s3_bucket_root_var}/F2DD_product_CVR' partitioned by (activity_date_partition);



create or replace temp view f2dd_pv as 

select activity_date_partition, channel, experience, channelcountry, session_id, 
	pageinstance_id, context_pagetype, page_indicator, 
	pdv_productstyle_id, 
	max(case when element_id in ('ProductDetail/FreeShipping/Enticement') and event_name in ('com.nordstrom.event.customer.Impressed') then 1 else 0 end) as F2DD_Enticement_shown_flag,
	max(case when element_id in ('ProductDetail/FreeShipping/Enticement') and event_name in ('com.nordstrom.event.customer.Impressed') then 
			(case when element_value like ('%Cardmembers%') then 'Cardmember' 
				when element_value like ('%Free%') then 'Non Cardmember' 
			end)
		end) as F2DD_Enticement_flag,
	max(case when element_id in ('ProductDetail/StoreModal/ZipCode') and event_name in ('com.nordstrom.event.customer.Engaged') and element_value != '' then 1 else 0 end) as zip_changed_flag,
	count(distinct case when element_id in ('ProductDetail/FreeShipping/ZipCode') and event_name in ('com.nordstrom.event.customer.Impressed') then element_value else null end)as unique_zip_shown,
	min(case when event_name = 'com.nordstrom.customer.ProductDetailViewed' then event_time_pst else null end) as PDV_timestamp
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where activity_date_partition between {start_date} and {end_date}  
		and channel = 'NORDSTROM'
		and context_pagetype = 'PRODUCT_DETAIL'
		and page_indicator = 'PRODUCT_DETAIL'
		and (
			(event_name in ('com.nordstrom.event.customer.Impressed') and element_id in ('ProductDetail/FreeShipping/Enticement') and element_value not like '%testing%' and digitalcontents_type = 'STYLE')
			or
			(event_name in ('com.nordstrom.event.customer.Impressed') and element_id in ('ProductDetail/FreeShipping/ZipCode') and digitalcontents_type = 'STYLE')
			or 
			(event_name in ('com.nordstrom.event.customer.Engaged') and element_id in ('ProductDetail/StoreModal/ZipCode') and digitalcontents_type = 'STYLE')
			or 
			(event_name = 'com.nordstrom.customer.ProductDetailViewed')
			)
group by 1,2,3,4,5,6,7,8,9
;



create or replace temp view detail as 

select a.activity_date_partition, channel, experience, channelcountry, a.session_id, a.pageinstance_id, a.context_pagetype, a.page_indicator, a.pdv_productstyle_id,
		a.F2DD_Enticement_shown_flag, a.F2DD_Enticement_flag, a.zip_changed_flag, a.unique_zip_shown, a.PDV_timestamp,
		b.last_zip_shown, b.last_zip_changed
from f2dd_pv a 
join
(select distinct activity_date_partition, session_id, pageinstance_id,  
	last_value(case when element_id in ('ProductDetail/FreeShipping/ZipCode') and event_name in ('com.nordstrom.event.customer.Impressed') then element_value else null end,true) 
		over(partition by activity_date_partition, session_id, pageinstance_id order by event_time_pst rows between unbounded preceding and unbounded following) as last_zip_shown,
	last_value(case when element_id in ('ProductDetail/StoreModal/ZipCode') and event_name in ('com.nordstrom.event.customer.Engaged') then element_value else null end, true) 
		over(partition by activity_date_partition, session_id, pageinstance_id order by event_time_pst rows between unbounded preceding and unbounded following) as last_zip_changed
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where activity_date_partition between {start_date} and {end_date}  
		and channel = 'NORDSTROM'
		and digitalcontents_type = 'STYLE'
		and context_pagetype = 'PRODUCT_DETAIL'
		and page_indicator = 'PRODUCT_DETAIL'
		and (
			(element_id in ('ProductDetail/FreeShipping/ZipCode') and event_name in ('com.nordstrom.event.customer.Impressed'))
			or 
			(element_id in ('ProductDetail/StoreModal/ZipCode') and event_name in ('com.nordstrom.event.customer.Engaged'))
			)
) b
on a.activity_date_partition = b.activity_date_partition
	and a.session_id = b.session_id
	and a.pageinstance_id = b.pageinstance_id
;



create or replace temp view tmp as 

select activity_date_partition, channel, experience, channelcountry, session_id, pageinstance_id, context_pagetype, page_indicator, PDV_timestamp, pdv_productstyle_id, 
		F2DD_enticement_shown_flag, 
		F2DD_Enticement_flag,
		case when unique_zip_shown = 1 and zip_changed_flag = 0 then 'no zip change' else 'zip change' end as zip_changed_flag,
		case when zip_changed_flag = 0 then last_zip_shown else last_zip_changed end as assigned_zip
from detail
where unique_zip_shown > 0
;




create or replace temp view no_div as 

select distinct 
		a.activity_date_partition, channel, experience, channelcountry, a.session_id, pageinstance_id, context_pagetype, page_indicator, PDV_timestamp, pdv_productstyle_id, 
		f2dd_enticement_shown_flag, f2dd_enticement_flag, zip_changed_flag, assigned_zip,
		case when b.order_style_id is not null then 1 else 0 end as conv_flag
from tmp a
left join
	(select distinct activity_date_partition, session_id, order_style_id, event_time_pst
	from acp_event_intermediate.session_evt_expanded_attributes_parquet
	where activity_date_partition between {start_date} and {end_date}  
			and channel = 'NORDSTROM'
			and event_name = 'com.nordstrom.customer.OrderSubmitted'
	) b
on a.activity_date_partition = b.activity_date_partition
	and a.session_id = b.session_id
	and a.pdv_productstyle_id = b.order_style_id
	and a.pdv_timestamp < b.event_time_pst
;




create or replace temp view final_view as 

with find_single_empsytleid as (
select webstyleid, marketcode, parentepmstyleid, brandlabelid, brandlabeldisplayname, sourcepublishtimestamp
from
    (select *,
        row_number() over(partition by webstyleid, marketcode order by sourcepublishtimestamp desc, parentepmstyleid, rmsstylegroupid, brandlabelid, brandlabeldisplayname) rk
    from
    	(select distinct webstyleid, marketcode, rmsstylegroupid, cast(parentepmstyleid as integer) as parentepmstyleid, brandlabelid, brandlabeldisplayname, from_utc_timestamp(sourcepublishtimestamp,'PST') as sourcepublishtimestamp
    	from nap_product.sku_digital_webstyleid_bucketed
		where webstyleid is not null
			and  cast(from_utc_timestamp(sourcepublishtimestamp,'PST') as date) <= {start_date}
		)
    )
where rk = 1
),


find_single_attribute as (
select marketcode, empstyleid, division_num, division_short_name, subdivision_num, subdivision_short_name, 
		departmentdescription, departmentnumber, subclassdescription, subclassnumber, classdescription, classnumber, typelevel1description, typelevel2description
from 
	(select *,
		row_number() over(partition by empstyleid, marketcode order by sourcepublishtimestamp desc, division_num, subdivision_num, departmentnumber, classnumber, typelevel1description, typelevel2description, gender, agegroup) rk
	from
		(select distinct marketcode, empstyleid, 
				genderdescription as gender, 
				agedescription as agegroup, 
				division_num, division_short_name, subdivision_num, subdivision_short_name,
				departmentdescription, departmentnumber, classdescription, classnumber, subclassdescription, subclassnumber, typelevel1description, typelevel2description, 
				from_utc_timestamp(sourcepublishtimestamp,'PST') as sourcepublishtimestamp
		from nap_product.style_digital_epmStyleId_bucketed_vw
		where empstyleid is not null
			and  cast(from_utc_timestamp(sourcepublishtimestamp,'PST') as date)  <= {start_date}
		)
	)
where rk = 1
)



select distinct 
		a.activity_date_partition as activity_date, channel, experience, channelcountry, 
		a.session_id, case when c.session_id is not null then 1 else 0 end as with_acp_id, c.acp_id, 
		pageinstance_id, context_pagetype, page_indicator, PDV_timestamp, pdv_productstyle_id, 
		f2dd_enticement_shown_flag, f2dd_enticement_flag, zip_changed_flag, assigned_zip, b.market, conv_flag,
		upper(case when concat(brandlabelid,': ', brandlabeldisplayname) = ': ' then 'Unknown' else concat(brandlabelid,': ', brandlabeldisplayname) end) as brand,
		upper(concat(departmentnumber,': ', departmentdescription)) as department, 
		upper(concat(classnumber,': ', classdescription)) as class,
		upper(concat(subclassnumber,': ', subclassdescription)) as subclass,
		upper(typelevel1description) as product_type1, 
		upper(typelevel2description) as product_type2, 
		upper(concat(c.division_num,': ', c.division_short_name)) as division, 
		upper(concat(c.subdivision_num,': ', c.subdivision_short_name)) as sub_division, 
		a.activity_date_partition
from no_div a
left join find_single_empsytleid b
on a.pdv_productstyle_id = b.webstyleid
	and a.channelcountry = b.marketcode
left join find_single_attribute c
on b.parentepmstyleid = c.empstyleid
	and b.marketcode = c.marketcode
left join digital_optimization_sandbox.hqky_F2DD_NMS_market_zip_mapping b
on a.assigned_zip = substr(b.zip, 2, length(b.zip)-2)
left join 
	(select activity_date_partition, session_id, max(acp_id) as acp_id
	from acp_vector.customer_session_xref
	where activity_date_partition between {start_date} and {end_date}  
		and acp_id != '' and  acp_id is not null
	group by 1,2
	) c 
on a.session_id = c.session_id
	and a.activity_date_partition = c.activity_date_partition
;


insert OVERWRITE TABLE {hive_schema}.F2DD_product_CVR PARTITION (activity_date_partition) 
select *
from final_view;

MSCK REPAIR table {hive_schema}.F2DD_product_CVR;  




