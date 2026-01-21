SET QUERY_BAND = 'App_ID=APP08718;
DAG_ID=paid_marketability_11521_ACE_ENG;
Task_Name=paid_marketability;'
FOR SESSION VOLATILE;


/*
Table: T2DL_DAS_MKTG_AUDIENCE.paid_marketability
Owner: Sophie Wang, Amanda Van Orsdale, Sushmitha Palleti, Nicole Miao
Modification Date: 2023-05-05
SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/
-- collect stats on ldg table
COLLECT STATISTICS column (acp_id)
		 , column (event_date_pacific)
		 , column (finance_detail)
on {mktg_audience_t2_schema}.paid_marketability_ldg
;

delete
from {mktg_audience_t2_schema}.paid_marketability
where event_date_pacific between {start_date} and {end_date}
;

insert into {mktg_audience_t2_schema}.paid_marketability
select
			  	acp_id
				, loyaltyid
				, iscardmember
				, nordylevel
				, channelcountry
				, channel
				, platform
				, utm_source
				, finance_detail
				, converted
				, order_channelcountry
				, order_channel
				, order_platform
				, order_date_pacific
				, demand
				, event_date_pacific
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from {mktg_audience_t2_schema}.paid_marketability_ldg
where event_date_pacific between {start_date} and {end_date}
;

collect statistics column (acp_id)
		 , column (event_date_pacific)
		 , column (finance_detail)
on {mktg_audience_t2_schema}.paid_marketability
;

SET QUERY_BAND = NONE FOR SESSION;
