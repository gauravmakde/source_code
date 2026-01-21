-- pipeline job for digital_wbr_daily_events table 
-- code owner Sara Scott
-- refer readme.md file for more details

--- Importing data from S3
CREATE OR REPLACE TEMPORARY VIEW customer_browse_search_results_source USING avro OPTIONS(path 's3://tf-nap-prod-styling-event-landing/event/proton-sink/customer-browse-search-results-generated-avro/year={year}/');
CREATE OR REPLACE TEMPORARY VIEW customer_activity_engaged_source USING parquet OPTIONS(path 's3://tf-nap-prod-styling-presentation/event/proton-sink/customer-activity-engaged-avro/year={year}/');
CREATE OR REPLACE TEMPORARY VIEW customer_activity_arrived_source USING parquet OPTIONS(path 's3://tf-nap-prod-styling-presentation/event/proton-sink/customer-activity-arrived-avro/year={year}/');
CREATE OR REPLACE TEMPORARY VIEW customer_activity_PSCV_source USING parquet OPTIONS(path 's3://tf-nap-prod-styling-presentation/event/proton-sink/customer-activity-productsummarycollectionviewed-avro/year={year}/');
CREATE OR REPLACE TEMPORARY VIEW customer_activity_PSS_source USING parquet OPTIONS(path 's3://tf-nap-prod-styling-presentation/event/proton-sink/customer-activity-productsummaryselected-avro/year={year}/');
CREATE OR REPLACE TEMPORARY VIEW filter_lookup USING orc OPTIONS(path 's3://ace-etl/digital_wbr_filter_lookup/');

---- Creating view with date filter
CREATE OR REPLACE TEMPORARY VIEW customer_browse_search_results as 
select * from customer_browse_search_results_source
where to_date(concat("{year}", '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date};
CREATE OR REPLACE TEMPORARY VIEW customer_activity_engaged as
select * from customer_activity_engaged_source
where to_date(concat("{year}", '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date};
CREATE OR REPLACE TEMPORARY VIEW customer_activity_arrived as
select * from customer_activity_arrived_source
where to_date(concat("{year}", '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date};
CREATE OR REPLACE TEMPORARY VIEW customer_activity_PSCV as
select * from customer_activity_PSCV_source
where to_date(concat("{year}", '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date};
CREATE OR REPLACE TEMPORARY VIEW customer_activity_PSS as
select * from customer_activity_PSS_source
where to_date(concat("{year}", '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date};

--- step1
CREATE OR replace TEMPORARY VIEW browse_URL_view AS
SELECT 
    cust_id_type
    , cust_id
    , channel
    , channel_country
    , platform
    , url_path
    , page_instance_id
    , page_type
    , feature
    , context_id
    , day_dt_pacific
    , first_page_view_time_pacific
    , total_result_count
    , LEAD(first_page_view_time_pacific) over (partition by day_dt_pacific, cust_id order by first_page_view_time_pacific) as next_page_time_pacific
    FROM (
        SELECT
           pscv.value.customer.idtype AS cust_id_type
           , cbsr.value.customer.id AS cust_id
           , pscv.value.source.channel AS channel
           , pscv.value.source.channelCountry AS channel_country
           , cbsr.value.source.platform AS platform
           , cbsr.value.path AS url_path
           , pscv.value.context.pageinstanceid AS page_instance_id
           , pscv.value.context.pageType AS page_type
           , pscv.value.source.feature AS feature
           , pscv.value.context.id AS context_id
           , DATE (from_utc_timestamp(cbsr.value.eventtime, 'US/Pacific')) AS day_dt_pacific
           , MIN(from_utc_timestamp(pscv.value.eventtime, 'US/Pacific')) AS first_page_view_time_pacific
           , MIN(cbsr.value.totalresultcount) AS total_result_count
        FROM customer_activity_pscv pscv
        INNER JOIN customer_browse_search_results cbsr ON pscv.value.customer.id = cbsr.value.customer.id
            AND pscv.value.context.id = cbsr.value.contextid
            AND cbsr.value.offset = 0
            AND DATE (from_utc_timestamp(cbsr.value.eventtime, 'US/Pacific')) = DATE (from_utc_timestamp(pscv.value.eventtime, 'US/Pacific'))
        WHERE pscv.value.context.pageType IN ('CATEGORY_RESULTS','BRAND_RESULTS', 'FLASH_EVENT_RESULTS', 'FLASH_EVENTS')
        	AND pscv.value.source.feature IN ('ProductResults', 'SEARCH_BROWSE')
        	AND pscv.value.context.id <> ""
          	AND pscv.value.source.platform NOT IN ('ANDROID')
        	AND coalesce(lower(cast(pscv.headers['identified-bot'] as string)),'false') = 'false'
        	AND pscv.headers['Nord-Load'] IS NULL
        	AND pscv.headers['nord-load'] IS NULL
        	AND pscv.headers['nord-test'] IS NULL
        	AND pscv.headers['Nord-Test'] IS NULL
        	AND pscv.headers['Sretest'] IS NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
);

-- creating a view to limit the data to only customers that had product summary collection viewed event
CREATE
    OR replace TEMPORARY VIEW pscv_cust_view AS

SELECT DISTINCT
    url_view.cust_id_type AS cust_id_type
    , url_view.cust_id AS cust_id
    , url_view.channel AS channel
    , url_view.channel_country AS channel_country
    , url_view.platform AS platform
    , url_view.day_dt_pacific AS day_dt_pacific
FROM browse_URL_view url_view;

--- step2
CREATE OR replace TEMPORARY VIEW events_view AS

SELECT eng.value.customer.idType AS cust_id_type
    , eng.value.customer.id AS cust_id
    , eng.value.source.channel AS channel
    , eng.value.source.channelCountry AS channel_country
    , eng.value.source.platform AS platform
    , case when char_length(cast(eng.value.eventtime as bigint)) < 13 THEN from_utc_timestamp(cast( cast(eng.value.eventtime as bigint) as timestamp), 'US/Pacific')
else from_utc_timestamp(cast( cast(eng.value.eventtime as bigint)/1000 as timestamp), 'US/Pacific') END AS event_time_pacific
    , CASE
        WHEN eng.value.element.id LIKE '%Filter%/%Select%'
            THEN 'Filter'
        WHEN eng.value.element.id LIKE '%Filter%/%Deselect%'
            THEN 'Deselect'
        WHEN eng.value.element.id LIKE '%Filter%/%ClearAll%'
            THEN 'ClearAll'
        WHEN eng.value.element.id LIKE '%Pagination%'
            THEN 'Pagination'
        WHEN eng.value.element.id LIKE '%Global/Navigation%'
            THEN 'TopNav'
        WHEN eng.value.element.id LIKE '%LeftNav%'
            THEN 'LeftNav'
        WHEN eng.value.element.id = 'ProductResults/Navigation/Link'
            THEN 'ProductNav'
        WHEN eng.value.element.id LIKE '%KeywordSearch%'
            THEN 'KeywordSearch'
        WHEN eng.value.element.id LIKE '%Sort%'
            THEN 'Sort'
        WHEN eng.value.element.id LIKE 'Global/AccountNav%'
            THEN 'Account'
        ELSE 'Other'
        END AS event
    , Coalesce(CASE
        WHEN cardinality(eng.value.element.valueHierarchy) > 1
            THEN eng.value.element.valueHierarchy [0]
        END, case when  eng.value.element.id like '%Filters/Select%' then filters.event_detail_header_1 else null end)  AS event_detail_header_1
    , coalesce(case 
        when cardinality(eng.value.element.valueHierarchy) > 2 
            THEN eng.value.element.valueHierarchy[1]
        END, case when  eng.value.element.id like '%Filters/Select%' then filters.event_detail_header_1 else null end) AS event_detail_header_2
    , eng.value.element.value AS event_detail
    , eng.value.context.pageType AS page_type
    , eng.value.context.id AS context_id
    , eng.value.context.pageinstanceid as page_instance_id
    , eng.headers ['Id'] AS event_Id
    , CASE
        WHEN eng.value.element.id LIKE '%KeywordSearch%'
            THEN rank() OVER (PARTITION BY eng.value.customer.id, eng.value.context.pageinstanceid ORDER BY eng.value.eventtime DESC)
        ELSE 1
        END AS search_val
    , CASE
        WHEN eng.value.element.id LIKE '%Pagination%'
            THEN count(1) OVER (PARTITION BY eng.value.customer.id, eng.value.context.id)
        ELSE 0
        END pagination_count
    , CASE
        WHEN eng.value.element.id LIKE '%Pagination%'
            THEN rank() OVER (PARTITION BY eng.value.customer.id, eng.value.context.id ORDER BY eng.value.eventtime)
        ELSE 1
        END pagination_val
    , case when eng.value.element.id like '%Sort%' then 1 else 0 end as sort_flag
    , case when eng.value.element.id like '%Filter%/%Select%' then 1 else 0 end as filter_flag
    , 0 AS PSS_count
    , 1 AS PSS_val
FROM customer_activity_engaged eng
JOIN pscv_cust_view pscv ON pscv.cust_id_type = eng.value.customer.idType
    AND pscv.cust_id = eng.value.customer.id
    AND pscv.channel = eng.value.source.channel
    AND pscv.channel_country = eng.value.source.channelCountry
    AND pscv.platform = eng.value.source.platform
    AND pscv.day_dt_pacific = DATE (case when char_length(cast(eng.value.eventtime as bigint)) < 13 THEN from_utc_timestamp(cast( cast(eng.value.eventtime as bigint) as timestamp), 'US/Pacific')
else from_utc_timestamp(cast( cast(eng.value.eventtime as bigint)/1000 as timestamp), 'US/Pacific') END)
LEFT JOIN filter_lookup filters ON eng.value.source.channel = filters.channel
    AND eng.value.source.platform = filters.platform
    and eng.value.element.value = filters.event_detail
    
WHERE eng.value.element.id NOT LIKE '%Filters%/%Show%'
    AND eng.value.element.id NOT LIKE '%Filters%/%Hide%'
    AND eng.value.element.id NOT LIKE '%Filter%/%SearchBox%'
    AND eng.value.element.id NOT LIKE '%ProductDetail%'
    AND eng.value.element.id NOT LIKE 'MiniPDP%'
    AND eng.value.context.pageType IN ('CATEGORY_RESULTS', 'BRAND_RESULTS', 'UNKNOWN', 'UNDETERMINED', 'FLASH_EVENT_RESULTS', 'FLASH_EVENTS')
    AND coalesce(lower(cast(eng.headers['identified-bot'] as string)),'false') = 'false'
    AND eng.headers['Nord-Load'] IS NULL
    AND eng.headers['nord-load'] IS NULL
    AND eng.headers['nord-test'] IS NULL
    AND eng.headers['Nord-Test'] IS NULL
    AND eng.headers['Sretest'] IS NULL

UNION ALL

SELECT pss.value.customer.idType AS cust_id_type
    , pss.value.customer.id AS cust_id
    , pss.value.source.channel AS channel
    , pss.value.source.channelCountry AS channel_country
    , pss.value.source.platform AS platform
    , from_utc_timestamp(from_unixtime(cast(cast(headers.EventTime as string) as double)/1000), 'US/Pacific') AS event_time_pacific
    , 'Product' AS event
    , NULL AS event_detail_header_1
    , NULL AS event_detail_header_2
    , NULL AS event_detail
    , NULL AS pageType
    , pss.value.context.id AS context_id
    , pss.value.context.pageinstanceid AS page_instance_id
    , pss.headers ['Id'] AS event_Id
    , 1 AS search_val
    , 0 AS pagination_count
    , 1 AS pagination_val
    , 0 as sort_flag
    , 0 as filter_flag
    , count(1) OVER (PARTITION BY pss.value.customer.id, pss.value.context.id) AS pss_count
    , rank() OVER (PARTITION BY pss.value.customer.id, pss.value.context.id ORDER BY pss.value.eventtime) AS pss_val
FROM customer_activity_PSS pss
JOIN pscv_cust_view pscv ON pscv.cust_id_type = pss.value.customer.idType
    AND pscv.cust_id = pss.value.customer.id
    AND pscv.channel = pss.value.source.channel
    AND pscv.channel_country = pss.value.source.channelCountry
    AND pscv.platform = pss.value.source.platform
    AND pscv.day_dt_pacific = DATE (from_utc_timestamp(from_unixtime(cast(cast(headers.EventTime as string) as double)/1000), 'US/Pacific'))
WHERE pss.value.context.pageType IN ('CATEGORY_RESULTS', 'BRAND_RESULTS', 'FLASH_EVENT_RESULTS', 'FLASH_EVENTS')
	AND pss.value.context.id <> ""
	AND coalesce(lower(cast(pss.headers['identified-bot'] as string)),'false') = 'false'
	AND pss.headers['Nord-Load'] IS NULL
	AND pss.headers['nord-load'] IS NULL
	AND pss.headers['nord-test'] IS NULL
	AND pss.headers['Nord-Test'] IS NULL
	AND pss.headers['Sretest'] IS NULL;

--- step3

CREATE OR replace TEMPORARY VIEW events_on_page_view AS

SELECT cust_id_type
    , cust_id
    , channel
    , channel_country
    , platform
    , first_page_view_time_pacific
    , event_time_pacific_clean AS event_time_pacific
    , day_dt_pacific
    , url_path
    , total_result_count
    , case when event = 'unknown' and last_event_ind = 1 then 'No Event' else event end as event
    , event_detail_header_1
    , event_detail_header_2
    , event_detail
    , page_type
    , feature
    , page_instance_id_eng
    , context_id
    , event_rank
    , pss_count
    , pagination_count
    , sort_count 
    , filter_count
    , case when event = 'unknown' then rank () over (partition by date(first_page_view_time_pacific), cust_id, url_path, page_instance_id_eng order by first_page_view_time_pacific  asc) else 1 end as back_button_rank
    , CASE
        WHEN event = 'unknown'
            THEN row_number() OVER (PARTITION BY cust_id, url_path, event ORDER BY event_time_pacific_clean desc)
        ELSE 1
        END AS dedupe_ind
    , CASE 
        WHEN lead(url_path, 1, 'test') over (partition by day_dt_pacific, cust_id order by first_page_view_time_pacific desc) = url_path and event = 'unknown' and last_event_ind = 0
            THEN 0 
        ELSE 1 
        END as event_test
FROM (
	SELECT url_view.cust_id_type
		, url_view.cust_id
		, url_view.channel
		, url_view.channel_country
		, url_view.platform
		, url_view.first_page_view_time_pacific
		, ev.event_time_pacific
		, url_view.day_dt_pacific
		, url_view.url_path
		, url_view.total_result_count
		, coalesce(ev.event, 'unknown') AS event
		, ev.event_detail_header_1
		, ev.event_detail_header_2
		, ev.event_detail
		, ev.page_instance_id AS page_instance_id_eng
		, url_view.page_instance_id
		, ev.event_ID
		, url_view.page_type
		, url_view.feature
		, ev.context_id AS context_id_eng
		, url_view.context_id
		, ev.pagination_count AS page_count_inv
		, ev.pss_count AS pss_count_inv
		, ev.sort_flag
		, ev.filter_flag
		, CASE
			WHEN ISNULL(ev.event_time_pacific) = False
				THEN MAX(first_page_view_time_pacific) OVER (PARTITION BY DATE (url_view.first_page_view_time_pacific), url_view.cust_id, ev.event_ID)
			WHEN ISNULL(ev.event_time_pacific) = True
				THEN MAX(first_page_view_time_pacific) OVER (PARTITION BY DATE (url_view.first_page_view_time_pacific), url_view.cust_id, url_view.page_instance_id)
			ELSE url_view.first_page_view_time_pacific
			END AS times
		, CASE WHEN MAX(first_page_view_time_pacific) over (partition by DATE(url_view.first_page_view_time_pacific), url_view.cust_id) = url_view.first_page_view_time_pacific then 1 else 0 end as last_event_ind
		, MAX(ev.event_time_pacific) OVER (PARTITION BY DATE (url_view.first_page_view_time_pacific), url_view.cust_id, url_view.url_path, url_view.context_id) AS event_time_pacific_clean
		, SUM(coalesce(ev.pagination_count, 0)) OVER (PARTITION BY url_view.cust_id, url_view.url_path ORDER BY first_page_view_time_pacific ASC) AS pagination_count
		, SUM(sort_flag) over (partition by DATE(url_view.first_page_view_time_pacific), url_view.cust_id, url_view.url_path ORDER by url_view.first_page_view_time_pacific ) as sort_count
		, SUM(filter_flag) over (partition by DATE(url_view.first_page_view_time_pacific), url_view.cust_id, url_view.url_path ORDER by url_view.first_page_view_time_pacific ) as filter_count
		, SUM(coalesce(ev.pss_count, 0)) OVER (PARTITION BY url_view.cust_id, url_view.url_path ORDER BY url_view.first_page_view_time_pacific ASC) AS pss_count
		, rank() OVER (PARTITION BY DATE (url_view.first_page_view_time_pacific), url_view.cust_id, url_view.context_id ORDER BY ev.event_time_pacific DESC) AS event_rank
	FROM browse_url_view url_view
	LEFT JOIN events_view ev ON url_view.page_instance_id = ev.page_instance_id
		AND CASE
			WHEN ev.context_id <> ""
				AND ev.context_id = url_view.context_id
				THEN 1
			WHEN ev.context_id = ""
				THEN 1
			ELSE 0
			END = 1
		AND url_view.cust_id = ev.cust_id
		AND unix_timestamp(ev.event_time_pacific) - unix_timestamp(url_view.first_page_view_time_pacific) >= 0
		AND unix_timestamp(url_view.next_page_time_pacific) - unix_timestamp(ev.event_time_pacific) >= 0
	WHERE url_view.url_path IS NOT NULL
		AND url_view.url_path <> ""
		AND url_view.context_id <> ""
		AND (ev.search_val = 1 OR ev.search_val IS NULL)
		AND (ev.pagination_val = 1 OR ev.pagination_val IS NULL)
		AND (ev.pss_val = 1 OR ev.pss_val IS NULL)
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,25
	) a
WHERE  event <> 'Deselect'
    AND event <> 'ClearAll'
    AND event_rank = 1;


-- step4

CREATE OR replace TEMPORARY VIEW arrived_events_view AS

SELECT cust_id_type
	, cust_id
	, channel_country
	, channel
	, platform
	, CASE
		WHEN referrer = ""
			AND marketing_type IS NULL
			THEN 'direct load'
		WHEN hi.marketing_type IS NOT NULL
			THEN 'external'
		END AS referrer
	, hi.marketing_type
	, hi.finance_rollup
	, hi.marketing_channel_detailed
	, event_time_pacific
	, day_dt_pacific
FROM (
    SELECT event_time_pacific
        , cust_id_type
        , cust_id
        , channel_country
        , channel
        , platform
        , utm_channel
        , referrer
        , CASE
            WHEN utm_channel IS NULL
                AND referrer IN (
                    'google'
                    , 'bing'
                    , 'yahoo'
                    )
                THEN 'SEO_SEARCH'
            WHEN utm_channel IS NULL
                AND referrer IN (
                    'facebook'
                    , 'instagram'
                    , 'pinterest'
                    )
                THEN 'arr_organic_social'
            ELSE 'paid'
            END AS mktg_type
        , day_dt_pacific
    FROM (
        SELECT from_utc_timestamp(from_unixtime(cast(cast(arr.headers.EventTime as string) as double)/1000), 'US/Pacific') AS event_time_pacific
            , arr.value.customer.idType AS cust_id_type
            , arr.value.customer.Id AS cust_id
            , arr.value.source.channelCountry AS channel_country
            , arr.value.source.channel AS channel
            , arr.value.source.platform AS platform
            , coalesce(arr.value.requestedDestination, arr.value.actualDestination) AS url_destination
            , parse_url(coalesce(arr.value.requestedDestination, arr.value.actualDestination), 'QUERY', 'utm_channel') AS utm_channel
            , CASE
                WHEN lower(arr.value.referrer) LIKE '%google%'
                    THEN 'google'
                WHEN lower(arr.value.referrer) LIKE '%bing%'
                    THEN 'bing'
                WHEN lower(arr.value.referrer) LIKE '%yahoo%'
                    THEN 'yahoo'
                WHEN lower(arr.value.referrer) LIKE '%facebook%'
                    THEN 'facebook'
                WHEN lower(arr.value.referrer) LIKE '%instagram%'
                    THEN 'instagram'
                WHEN lower(arr.value.referrer) LIKE '%pinterest%'
                    THEN 'pinterest'
                ELSE arr.value.referrer
                END AS referrer
            , DATE (from_utc_timestamp(from_unixtime(cast(cast(arr.headers.EventTime as string) as double)/1000), 'US/Pacific')) day_dt_pacific
        FROM customer_activity_arrived arr
        WHERE arr.value.source.platform NOT IN ('ANDROID')
            AND coalesce(lower(cast(arr.headers['identified-bot'] as string)),'false') = 'false'
            AND arr.headers['Nord-Load'] IS NULL
            AND arr.headers['nord-load'] IS NULL
            AND arr.headers['nord-test'] IS NULL
            AND arr.headers['Nord-Test'] IS NULL
            AND arr.headers['Sretest'] IS NULL
        ) AS b
    ) AS a
LEFT JOIN ace_etl.utm_channel_lookup lk ON lower(a.utm_channel) = lower(lk.utm_mkt_chnl)
LEFT JOIN ace_etl.marketing_channel_hierarchy hi ON coalesce(lk.bi_channel, a.mktg_type) = hi.join_channel;


-- step5

CREATE OR replace TEMPORARY VIEW full_events AS

SELECT ev.cust_id_type
    , ev.cust_id
    , ev.channel
    , ev.channel_country
    , ev.platform
    , NULL AS arrived_flag
    , coalesce(ev.event_time_pacific, ev.first_page_view_time_pacific) AS event_time_pacific
    , ev.day_dt_pacific
    , ev.url_path
    , ev.total_result_count
    , CASE WHEN event = 'unknown' and lead(ev.back_button_rank, 1, 1) over (partition by ev.day_dt_pacific, ev.cust_id order by ev.first_page_view_time_pacific) <> 1 then 'Back Button' else ev.event end as event
    , ev.event_detail_header_1
    , ev.event_detail_header_2
    , ev.event_detail
    , ev.sort_count
    , ev.filter_count
    , ev.pagination_count
    , ev.pss_count
    , ev.page_instance_id_eng AS page_instance_id
    , ev.context_id AS context_id
    , ev.back_button_rank
    , NULL AS referrer
FROM events_on_page_view ev
WHERE ev.dedupe_ind = 1
AND event_test = 1

UNION ALL

SELECT arr.cust_id_type AS cust_id_type
    , arr.cust_id AS cust_id
    , arr.channel AS channel
    , arr.channel_country AS channel_country
    , arr.platform AS platform
    , 1 AS arrived_flag
    , arr.event_time_pacific AS event_time_pacific
    , arr.day_dt_pacific AS day_dt_pacific
    , NULL AS url_path
    , NULL AS total_result_count
    , arr.referrer AS event
    , arr.marketing_type AS event_detail_header_1
    , arr.finance_rollup AS event_detail_header_2
    , arr.marketing_channel_detailed AS event_detail
    , 0 as sort_count
    , 0 as filter_count
    , 0 AS pagination_count
    , 0 AS pss_count
    , NULL AS page_instance_id
    , NULL AS context_id
    , 1 as back_button_rank
    , arr.referrer AS referrer
FROM arrived_events_view arr

UNION ALL

SELECT eng.value.customer.idType AS cust_id_type
    , eng.value.customer.id AS cust_id
    , eng.value.source.channel AS channel
    , eng.value.source.channelCountry AS channel_country
    , eng.value.source.platform AS platform
    , NULL AS arrived_flag
    , case when char_length(cast(eng.value.eventtime as bigint)) < 13 THEN from_utc_timestamp(cast( cast(eng.value.eventtime as bigint) as timestamp), 'US/Pacific')
        else from_utc_timestamp(cast( cast(eng.value.eventtime as bigint)/1000 as timestamp), 'US/Pacific') END AS event_time_pacific
    , DATE (case when char_length(cast(eng.value.eventtime as bigint)) < 13 THEN from_utc_timestamp(cast( cast(eng.value.eventtime as bigint) as timestamp), 'US/Pacific')
        else from_utc_timestamp(cast( cast(eng.value.eventtime as bigint)/1000 as timestamp), 'US/Pacific') END) AS day_dt_pacific
    , NULL AS url_path
    , NULL AS total_result_count
    , CASE
        WHEN eng.value.element.id = 'Global/Navigation/Section/Link'
            THEN 'TopNav'
        WHEN eng.value.element.id = 'ProductResults/LeftNav/Accordion/Category'
            THEN 'LeftNav'
        WHEN eng.value.element.id = 'ProductResults/Navigation/Link'
            THEN 'ProductNav'
        WHEN eng.value.element.id LIKE 'Content%'
            THEN 'Content'
        END AS event
    , CASE
        WHEN cardinality(eng.value.element.valueHierarchy) > 1
            THEN eng.value.element.valueHierarchy [0]
        END AS event_detail_header_1
    , CASE
        WHEN cardinality(eng.value.element.valueHierarchy) > 2
            THEN eng.value.element.valueHierarchy [1]
        END AS event_detail_header_2
    , eng.value.element.value AS event_detail
    , 0 as sort_count
    , 0 as filter_count
    , 0 AS pagination_count
    , 0 AS pss_count
    , eng.value.context.pageinstanceid AS page_instance_id
    , eng.value.context.id AS context_id
    , 1 as back_button_rank
    , eng.value.context.pageType AS referrer
FROM customer_activity_engaged eng
JOIN pscv_cust_view pscv ON pscv.cust_id_type = eng.value.customer.idType
    AND pscv.cust_id = eng.value.customer.id
    AND pscv.channel = eng.value.source.channel
    AND pscv.channel_country = eng.value.source.channelcountry
    AND pscv.platform = eng.value.source.platform
    AND pscv.day_dt_pacific = DATE (case when char_length(cast(eng.value.eventtime as bigint)) < 13 THEN from_utc_timestamp(cast( cast(eng.value.eventtime as bigint) as timestamp), 'US/Pacific')
    else from_utc_timestamp(cast( cast(eng.value.eventtime as bigint)/1000 as timestamp), 'US/Pacific') END)
    AND (eng.value.element.id = 'Global/Navigation/Section/Link' OR eng.value.element.id = 'ProductResults/LeftNav/Accordion/Category' OR eng.value.element.id = 'ProductResults/Navigation/Link'
    OR (eng.value.element.id LIKE 'Content%' AND eng.value.element.type = 'HYPERLINK' OR eng.value.element.type = 'PHOTO')
    );

CREATE OR replace TEMPORARY VIEW orders_demand_view AS

SELECT ord.event_date_pacific as day_dt_pacific
    , from_utc_timestamp(ord.event_timestamp_utc, 'US/Pacific') as order_tstamp_pacific
    , ord.cust_id_type
    , ord.cust_id
    , ord.channelCountry as channel_country
    , ord.channel
    , ord.platform
    , 1 as ordering_visitor
    , count(ord.orderLineId) AS order_quantity
    , sum(ord.current_price + ord.employee_discount) AS order_demand
    , count(distinct order_number) as orders
FROM acp_event_intermediate.funnel_events_order_submitted ord
LEFT JOIN (
    SELECT orderLineId
    FROM acp_event_intermediate.funnel_events_order_cancelled
    WHERE event_date_pacific BETWEEN {start_date}
            AND {end_date}
        AND cancel_reason LIKE '%FRAUD%'
    ) c ON ord.orderLineId = c.orderLineId
WHERE ord.event_date_pacific BETWEEN {start_date}
        AND {end_date}
    AND coalesce(lower(ord.identified_bot), 'false') = 'false'
    AND NOT arrays_overlap(ord.header_keys, array('Nord-Load', 'nord-load', 'nord-test', 'Nord-Test', 'sretest'))
    AND c.orderLineId IS NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- final clean up to remove duplicated events for the same page instance id
CREATE OR replace TEMPORARY VIEW cleanup_view AS

SELECT  cust_id_type
        , cust_id
        , channel
        , channel_country
        , platform
        , arrived_flag
        , event_time_pacific
        , day_dt_pacific
        , REPLACE(url_path, "'", "''") as url_path
        , total_result_count
        , event
        , REPLACE(event_detail_header_1, "'", "''") as event_detail_header_1
        , REPLACE(event_detail_header_2, "'", "''") as event_detail_header_2
        , REPLACE(event_detail, "'", "''") as event_detail
        , sort_count
        , filter_count
        , pagination_count
        , pss_count
        , page_instance_id
        , context_id
        , back_button_rank
        , REPLACE(referrer, "'", "''") as referrer
FROM (
    SELECT row_number() OVER (PARTITION BY cust_id, page_instance_id, event_time_pacific ORDER BY url_path desc) AS dedupe
        , cust_id_type
        , cust_id
        , channel
        , channel_country
        , platform
        , arrived_flag
        , event_time_pacific
        , day_dt_pacific
        , url_path
        , total_result_count
        , event
        , COALESCE(event_detail_header_1, 'N/A') as event_detail_header_1
        , COALESCE(event_detail_header_2, 'N/A') as event_detail_header_2
        , COALESCE(event_detail, 'N/A') as event_detail
        , COALESCE(sort_count, 0) as sort_count
        , COALESCE(filter_count, 0) as filter_count
        , coalesce(pagination_count, 0) as pagination_count
        , coalesce(pss_count, 0) as pss_count
        , page_instance_id
        , context_id
        , back_button_rank
        , referrer

    FROM full_events
    )
WHERE dedupe = 1;


-- step6
CREATE OR replace TEMPORARY VIEW referral_event_view AS
SELECT 
    cust_id_type
    , cust_id
    , channel
    , channel_country
    , platform
    , event_time_pacific
    , day_dt_pacific
    , referrer
    , referral_event
    , referral_detail_header_1
    , referral_detail_header_2
    , referral_detail
    , url_path
    , sort_ind
    , sort_count
    , filter_ind
    , filter_count
    , pagination_ind
    , pagination_count
    , pss_count
    , page_interaction_ind
    , last_event_ind
    , next_event
    , next_event_detail_header_1
    , next_event_detail_header_2
    , next_event_detail
    , page_views
    , cust_order_demand
    , cust_order_quantity
    , cust_orders
    , ordering_visitor
    , page_view_ind
    , SUM(case when referral_event is null then 0 else 1 end) over (partition by cust_id, channel, channel_country, platform, day_dt_pacific order by event_time_pacific) as value_partition

FROM (
    SELECT c.cust_id_type
        , c.cust_id
        , c.channel
        , c.channel_country
        , c.platform
        , c.event_time_pacific
        , c.day_dt_pacific
        , CASE WHEN c.event = 'unknown' and back_button_rank = 2 then 'Category Page' ELSE 
                LAG(CASE
                    WHEN event <> 'Pagination'
                        AND event <> 'Product'
                        AND event <> 'Sort'
                        AND event <> 'Filter'
                        AND event <> 'KeywordSearch'
                        and event <> 'Account'
                        and event <> 'FooterNav'
                        and event <> 'unknown'
                        THEN referrer
                    END , 1, NULL) OVER (PARTITION BY c.cust_id, c.day_dt_pacific ORDER BY event_time_pacific
            ) END AS referrer
        ,CASE WHEN c.event = 'unknown' and back_button_rank = 2 then 'Back Button' ELSE
                LAG(CASE
                    WHEN event <> 'Pagination'
                        AND event <> 'Product'
                        AND event <> 'Sort'
                        AND event <> 'Filter'
                        AND event <> 'KeywordSearch'
                        and event <> 'Account'
                        and event <> 'FooterNav'
                        and event <> 'unknown'
                        THEN event
                    END, 1, NULL) OVER (PARTITION BY c.cust_id, c.day_dt_pacific ORDER BY event_time_pacific
            ) END as referral_event
        , CASE WHEN c.event = 'unknown' and back_button_rank = 2 then 'N/A' ELSE 
                LAG(CASE
                    WHEN event <> 'Pagination'
                        AND event <> 'Product'
                        AND event <> 'Sort'
                        AND event <> 'Filter'
                        AND event <> 'KeywordSearch'
                        and event <> 'Account'
                        and event <> 'FooterNav'
                        and event <> 'unknown'
                        THEN event_detail_header_1
                    END, 1, NULL) OVER (PARTITION BY c.cust_id, c.day_dt_pacific ORDER BY event_time_pacific
            ) END as referral_detail_header_1
        , CASE WHEN c.event = 'unknown' and back_button_rank = 2 then 'N/A' ELSE
                LAG(CASE
                    WHEN event <> 'Pagination'
                        AND event <> 'Product'
                        AND event <> 'Sort'
                        AND event <> 'Filter'
                        AND event <> 'KeywordSearch'
                        and event <> 'Account'
                        and event <> 'FooterNav'
                        and event <> 'unknown'
                        THEN event_detail_header_2
                    END, 1, NULL) OVER (PARTITION BY c.cust_id, c.day_dt_pacific ORDER BY event_time_pacific
            ) END as referral_detail_header_2
        , CASE WHEN c.event = 'unknown' and back_button_rank = 2 then 'N/A' ELSE
                LAG(CASE
                    WHEN event <> 'Pagination'
                        AND event <> 'Product'
                        AND event <> 'Sort'
                        AND event <> 'Filter'
                        AND event <> 'KeywordSearch'
                        and event <> 'Account'
                        and event <> 'FooterNav'
                        and event <> 'unknown'
                        THEN event_detail
                    END, 1, NULL) OVER (PARTITION BY c.cust_id, c.day_dt_pacific ORDER BY event_time_pacific
            ) END as referral_detail
        , url_path
        , total_result_count
        , case when sort_count = 0 then 0 else 1 end as sort_ind
        , sort_count
        , case when filter_count = 0 then 0 else 1 end as filter_ind
        , filter_count
        , case when pagination_count = 0 then 0 else 1 end as pagination_ind
        , pagination_count
        , pss_count
        , CASE
            WHEN event = 'Pagination'
                OR event = 'Sort'
                OR event = 'Filter'
                THEN 1
            ELSE 0
            END AS page_interaction_ind
        , CASE
            WHEN MAX(event_time_pacific) OVER (PARTITION BY c.day_dt_pacific, c.cust_id) = event_time_pacific
                THEN 1
            ELSE 0
            END AS last_event_ind
        , event AS next_event
        , event_detail_header_1 AS next_event_detail_header_1
        , event_detail_header_2 AS next_event_detail_header_2
        , event_detail AS next_event_detail
        , 1 as page_views
        , cast(coalesce(order_demand,0) as double) AS cust_order_demand
        , coalesce(order_quantity,0) AS cust_order_quantity
        , coalesce(orders, 0) AS cust_orders
        , coalesce(ordering_visitor, 0) as ordering_visitor
        , row_number() OVER (PARTITION BY c.day_dt_pacific, c.cust_id, c.url_path ORDER BY event_time_pacific desc) AS page_view_ind
    FROM cleanup_view c
    LEFT JOIN orders_demand_view o ON c.day_dt_pacific = o.day_dt_pacific
        AND c.cust_id = o.cust_id
        AND c.channel = o.channel
        AND c.platform = o.platform
        AND c.event_time_pacific <= o.order_tstamp_pacific
    )
WHERE url_path IS NOT NULL;


------- final view
Create or replace TEMPORARY VIEW final_view as 
SELECT cast(cust_id_type as string) cust_id_type
	, cast(cust_id as string) as cust_id
	, cast(channel as string) as channel
	, cast(channel_country as string) as channel_country
	, cast(platform as string) as platform
	, cast(day_dt_pacific as date) as day_dt_pacific
	, cast(coalesce(case when referral_event is null and referrer is null then first_value(referrer) over (partition by day_dt_pacific, channel, platform, cust_id, value_partition, url_path order by event_time_pacific) else referrer end, 'unknown') as string) as referrer
	, cast(coalesce(case when referral_event is null then first_value(referral_event) over (partition by day_dt_pacific, channel, platform, cust_id, value_partition, url_path order by event_time_pacific) else referral_event end, 'unknown') as string) as referral_event
	, cast(coalesce(case when referral_event is null and referral_detail_header_1 is null then first_value(referral_detail_header_1) over (partition by day_dt_pacific, channel, platform, cust_id, value_partition, url_path order by event_time_pacific) else referral_detail_header_1 end, 'unknown')  as string) as referral_detail_header_1
	, cast(coalesce(case when referral_event is null and referral_detail_header_2 is null then first_value(referral_detail_header_2) over (partition by day_dt_pacific, channel, platform, cust_id, value_partition, url_path order by event_time_pacific) else referral_detail_header_2 end, 'unknown')  as string) as referral_detail_header_2
	, cast(coalesce(case when referral_event is null and referral_detail is null then first_value(referral_detail) over (partition by day_dt_pacific, channel, platform, cust_id, value_partition, url_path order by event_time_pacific) else referral_detail end, 'unknown')  as string) as referral_detail
 	, cast(url_path as string) as url_path
	, pagination_count
	, pss_count
	, sort_count
	, filter_count
	, page_interaction_ind
	, last_event_ind
	, sort_ind
	, filter_ind
	, pagination_ind
	, cast(case when next_event = 'unknown' and last_event_ind = 1 then 'No Event' else next_event end as string) as next_event
	, cast(next_event_detail_header_1 as string) as next_event_detail_header_1
	, cast(next_event_detail_header_2 as string) as next_event_detail_header_2
	, cast(next_event_detail as string) as next_event_detail
	, cust_order_demand as cust_order_demand
	, cust_order_quantity as cust_order_quantity
	, cust_orders as cust_orders
	, ordering_visitor as ordering_visitor
	, page_views as page_views
	, SUM(case when next_event = 'unknown' and last_event_ind = 1 then 0 when next_event = 'No Event' then 0 else page_views end) over (partition by day_dt_pacific, cust_id, url_path) as page_clicks
	, case when page_view_ind = 1 then 1 else 0 end as visitors
    from referral_event_view;


-- final select to get aggregated data to load into Teradata
Create or replace TEMPORARY view final_summary_view as
SELECT 
cust_id_type
, channel
, channel_country
, platform
, referrer
, referral_event
, referral_detail_header_1
, referral_detail_header_2
, referral_detail
, url_path
, next_event
, next_event_detail_header_1
, next_event_detail_header_2
, next_event_detail
, page_interaction_ind
, sort_ind
, filter_ind
, pagination_ind
, last_event_ind
, SUM(visitors) as visitors
, SUM(cust_order_demand) as cust_order_demand
, SUM(cust_order_quantity) as cust_order_quantity
, SUM(cust_orders) as cust_orders
, SUM(ordering_visitor) as ordering_visitors
, SUM(page_views) as page_views
, SUM(page_clicks) as page_clicks
, SUM(sort_count) as sort_clicks
, SUM(filter_count) as filter_clicks
, SUM(pagination_count) as pagination_clicks
, SUM(pss_count) as pss_clicks
, day_dt_pacific
FROM final_view  
GROUP BY 
cust_id_type
, channel
, channel_country
, platform
, referrer
, referral_event
, referral_detail_header_1
, referral_detail_header_2
, referral_detail
, url_path
, next_event
, next_event_detail_header_1
, next_event_detail_header_2
, next_event_detail
, page_interaction_ind
, sort_ind
, filter_ind
, pagination_ind
, last_event_ind
, day_dt_pacific
;

-------- insert into TPT CSV
INSERT OVERWRITE TABLE digital_wbr_daily_events_ldg_output
select  
cust_id_type
, channel
, channel_country
, platform
, day_dt_pacific
, referrer
, referral_event
, referral_detail_header_1
, referral_detail_header_2
, regexp_replace(referral_detail, "\\r\\n|\\n|\\r", '') as  referral_detail
, url_path
, next_event
, next_event_detail_header_1
, next_event_detail_header_2
, regexp_replace(next_event_detail, "\\r\\n|\\n|\\r", '') as next_event_detail
, page_interaction_ind
, sort_ind
, filter_ind
, pagination_ind
, last_event_ind
, visitors
, cust_order_demand
, cust_order_quantity
, cust_orders
, ordering_visitors
, page_views
, page_clicks
, sort_clicks
, filter_clicks
, pagination_clicks
, pss_clicks
from final_summary_view WHERE day_dt_pacific BETWEEN {start_date} and {end_date}
; 
