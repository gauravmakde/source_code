INSERT OVERWRITE TABLE mta_last_touch_ldg_output
SELECT
 distinct
      mta_tb.order_number
    , mta_tb.mktg_type
    , mta_tb.utm_channel
    , mta_tb.order_date_pacific
	, mta_tb.sp_campaign
	, mta_tb.utm_campaign
	, mta_tb.utm_content
	, mta_tb.utm_source
	, mta_tb.utm_term
FROM (

    SELECT distinct
          order_number
        , order_date_pacific
        , mktg_type
		, utm_channel
		, sp_campaign
		, utm_campaign
		, utm_content
		, utm_source
		, utm_term
        , arrived_timestamp_utc
        , sum(demand) AS demand
        , rank() OVER (PARTITION BY order_number ORDER BY arrived_timestamp_utc DESC, utm_channel desc) AS touch_rank

    FROM ace_etl.mta_touches

    WHERE 1=1
        AND order_date_pacific between {start_date} and {end_date}

    GROUP BY
          order_number
        , order_date_pacific
        , mktg_type
        , utm_channel
		, sp_campaign
		, utm_campaign
		, utm_content
		, utm_source
		, utm_term
        , arrived_timestamp_utc

    ) mta_tb

WHERE 1=1
    AND mta_tb.touch_rank = 1
    AND mta_tb.mktg_type <> 'BASE'

