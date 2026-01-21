SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_delivery_speed_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_delivery_speed_teradata_job;'
FOR SESSION VOLATILE;

ET;

CREATE VOLATILE multiset TABLE delivery_speed (  
        business_unit_desc                      VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        order_date                              DATE FORMAT 'YYYY-MM-DD' NOT NULL ,
        avg_delivered_vs_promised_days	        DECIMAL(20,12),
        avg_order_promised_days	                DECIMAL(20,12),
        avg_order_delivered_days                DECIMAL(20,12)
) ON COMMIT PRESERVE ROWS; 

ET;

insert into delivery_speed
   select 
     business_unit_desc
    ,order_date_pacific as order_date
    ,avg(delivered_vs_promised) as avg_delivered_vs_promised_days
    ,avg(order_promise_days) as avg_order_promised_days
    ,avg(order_delivery_days) as avg_order_delivered_days
from
	(
	select
		case
			when source_channel_code = 'RACK' then 'R.COM'
			else 'N.COM'
		end as business_unit_desc
        ,order_date_pacific
        ,requested_max_promise_date_pacific - cast(carrier_delivered_tmstp_pacific as date) as delivered_vs_promised
        ,cast(carrier_delivered_tmstp_pacific as date) - order_date_pacific as order_delivery_days
        ,requested_max_promise_date_pacific - order_date_pacific as order_promise_days
        ,1 items
        ,order_line_amount_usd as shipped_sales
	from
		{db_env}_nap_usr_vws.order_line_detail_fact oldf
	left join {db_env}_nap_usr_vws.day_cal dc on
		oldf.order_date_pacific = dc.day_date
	where
		1 = 1
		and year_num >= 2021
		and source_channel_code in ('FULL_LINE', 'RACK')
		and source_channel_country_code = 'US'
		and cancel_reason_code is null
		and coalesce(gift_with_purchase_ind,
		'N') = 'N'
		and coalesce(beauty_sample_ind,
		'N') = 'N'
		and order_date_pacific between (current_date - 14) and (current_date - 1)
    ) a
group by 1,2;

ET;
--DELETING AND INSERTING DATA IN THE LANDING TABLE
DELETE FROM {proto_schema}.MMM_DELIVERY_SPEED_LDG ALL;
ET;

INSERT INTO {proto_schema}.MMM_DELIVERY_SPEED_LDG
select
business_unit_desc,
order_date,
avg_delivered_vs_promised_days,
avg_order_promised_days,
avg_order_delivered_days,
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from delivery_speed;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;