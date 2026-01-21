-- Table Name:search_session_analytical
-- Team/Owner: Digital Optimization/Jin Liu
-- Date Created/Modified: created on 11/2/2022, last modified on 11/2/2022

-- Note:
-- What is the the purpose of the table: to enable analyst and engineer to do query level analysis
-- What is the update cadence/lookback window: refresh daily and 2 days lookback window


create table if not exists {hive_schema}.search_session_analytical
(      	 
                
                activity_week_start date
				,event_time_utc timestamp
				,event_time_pst timestamp
                ,cust_id string
				,channelcountry string
       			,channel string
				,experience string 
                ,session_id string
       			,session_end_page string
       			,session_start_page string
       			,session_starttime_utc timestamp
				,session_endtime_utc timestamp
				,session_starttime_pst timestamp
				,session_endtime_pst timestamp
                ,event_id string 
       			,event_name string
       			,feature string
				,element_id string
                            ,element_subid string
       			,context_id string 
       			,context_pagetype string
       			,pageinstance_id string
       			,pagecontainer string
				,source_feature string
				,source_page_id string
       			,source_page_type string
				,next_page_id string
       			,next_page_type string
				,arrival_referrer string
				,suggestion_value string
                ,arrival_rank integer
			    ,keyword string
		        ,prior_keyword string
				,parent_query_rank integer
				,parent_query_id  string
       			,parent_query_sorted integer
       			,repeat_keyword_rank integer
       			,repeat_keyword_count integer
       			,result_query_rank integer
       			,result_context_id string
       			,result_query_id string
       			,matchstrategy string
			    ,filters_str string
				,sortorder string
				,searchresult_count integer
				,requested_count integer
				,offset integer
				,product_id string
				,element_index string
                            ,element_subindex string
       		    ,atb_product_id string
       		    ,activity_date_partition date
)
using ORC
location 's3://{s3_bucket_root_var}/search/search_session_analytical/'
partitioned by (activity_date_partition);


MSCK REPAIR table {hive_schema}.search_session_analytical;



CREATE OR replace TEMPORARY VIEW all_sessions AS 
select
                                            activity_date_partition, 
                                            (date_trunc('week',(activity_date_partition + interval '1' day))) - interval '1' day as activity_week_start,
                                            e.session_id,
                                            s.session_starttime_utc,
                                            s.session_endtime_utc,
                                            s.session_starttime_pst,
                                            s.session_endtime_pst,
                                            e.event_id,
                                            e.event_name,
                                            cast(e.event_time_utc as timestamp) as event_time_utc,
                                            from_utc_timestamp(e.event_time_utc, 'US/Pacific') as event_time_pst,
                                            e.channelcountry, 
                                            e.channel,
                                            e.experience,
                                            e.cust_id,
                                            e.feature,
                                            e.context_id,
                                            e.context_pagetype,
                                            e.pageinstance_id,
                                            e.pagecontainer,
						  e.referrer
                                        from
                                            acp_vector.customer_digital_session_evt_fact e 
                                            inner join 
                                            (
                                                select
                                                    session_id,
                                                    session_starttime_utc, 
                                                    session_endtime_utc,
                                                    from_utc_timestamp(session_starttime_utc, 'US/Pacific') session_starttime_pst,
                                                    from_utc_timestamp(session_endtime_utc, 'US/Pacific') session_endtime_pst
                                                from
                                                    acp_vector.customer_session_fact
                                                    where activity_date_partition between {start_date} and {end_date} 
                                                    and session_id <> 'UNKNOWN'
                                                    and experience in ('MOBILE_WEB', 'DESKTOP_WEB') 
                                            ) s on s.session_id = e.session_id
                                            where e.selling_channel = 'ONLINE' 
                                            and activity_date_partition  between {start_date}  and {end_date}
                                            and experience in ('MOBILE_WEB', 'DESKTOP_WEB') 
                                           
;

CREATE
OR replace TEMPORARY VIEW search_sessions AS
SELECT
       *
FROM
       (
              SELECT
                     activity_week_start,
                     session_id,
                     session_end_page,
                     session_start_page,
                     session_endtime_utc ,
                     session_starttime_utc ,
                     session_endtime_pst ,
                     session_starttime_pst,
                     event_id,
                     event_name,
                     event_time_utc ,
                     event_time_pst ,
                     channelcountry,
                     channel,
                     experience,
                     cust_id,
                     feature,
                     context_id,
                     context_pagetype,
                     pageinstance_id,
                     pagecontainer,
                     INT(kw_gen_rank) AS kw_gen_rank 
,
                     last_value(
                            case
                                   when feature <> '' then feature
                                   else null
                            end,
                            true
                     ) over (
                            partition by session_id
                            ORDER BY
                                   event_time_utc asc rows between unbounded preceding
                                   and 1 preceding
                     ) AS source_feature 
,
                     last_value(
                            case
                                   when pageinstance_id <> '' then pageinstance_id
                                   else null
                            end,
                            true
                     ) over (
                            partition by session_id
                            ORDER BY
                                   event_time_utc asc rows between unbounded preceding
                                   and 1 preceding
                     ) AS source_page_id 
,
                     last_value(
                            case
                                   when context_pagetype <> '' then context_pagetype
                                   else null
                            end,
                            true
                     ) over(
                            partition by session_id
                            ORDER BY
                                   event_time_utc asc rows between unbounded preceding
                                   and 1 preceding
                     ) AS source_page_type 
,
                     first_value(
                            case
                                   when pageinstance_id <> '' then pageinstance_id
                                   else null
                            end,
                            true
                     ) over (
                            partition by session_id
                            ORDER BY
                                   event_time_utc asc rows between 1 following
                                   and unbounded following
                     ) AS next_page_id 
,
                     first_value(
                            case
                                   when context_pagetype <> '' then context_pagetype
                                   else null
                            end,
                            true
                     ) over (
                            partition by session_id
                            ORDER BY
                                   event_time_utc asc rows between 1 following
                                   and unbounded following
                     ) AS next_page_type 
,
                     activity_date_partition 
              FROM
                     (
                            SELECT
                                   activity_date_partition,
                                   activity_week_start,
                                   session_id,
                                   session_starttime_utc,
                                   session_endtime_utc,
                                   session_starttime_pst,
                                   session_endtime_pst,
                                   event_id,
                                   event_name,
                                   event_time_utc,
                                   event_time_pst,
                                   channelcountry,
                                   channel,
                                   experience,
                                   cust_id,
                                   feature,
                                   context_id,
                                   context_pagetype,
                                   pageinstance_id,
                                   pagecontainer,
                                   session_id,
                                   COUNT(
                                          case
                                                 WHEN event_name = 'com.nordstrom.customer.event.KeywordSearchResultsGenerated' THEN event_id
                                                 else null
                                          end
                                   ) over(
                                          partition by session_id
                                          ORDER BY
                                                 event_time_utc asc
                                   ) AS kw_gen_rank 
,
                                   MAX(
                                          case
                                                 WHEN event_name = 'com.nordstrom.customer.event.KeywordSearchResultsGenerated' THEN 1
                                                 else 0
                                          end
                                   ) over (partition by session_id) AS search_session_flag 
,
                                   last_value(
                                          case
                                                 WHEN pageinstance_id <> '' THEN pageinstance_id
                                                 else null
                                          end,
                                          true
                                   ) over (
                                          partition by session_id
                                          ORDER BY
                                                 event_time_utc asc range BETWEEN unbounded preceding
                                                 AND unbounded following
                                   ) AS session_end_page 
,
                                   first_value(
                                          case
                                                 WHEN pageinstance_id <> '' THEN pageinstance_id
                                                 else null
                                          end,
                                          true
                                   ) over (
                                          partition by session_id
                                          ORDER BY
                                                 event_time_utc asc range BETWEEN unbounded preceding
                                                 AND unbounded following
                                   ) AS session_start_page 
,
                                   COUNT(event_name) over (partition by session_id) AS event_count 
                            FROM
                                   (
                                          SELECT
                                                 activity_date_partition,
                                                 activity_week_start,
                                                 session_id,
                                                 session_starttime_utc 
,
                                                 session_endtime_utc 
,
                                                 session_starttime_pst,
                                                 session_endtime_pst,
                                                 event_id,
                                                 event_name,
                                                 event_time_utc,
                                                 event_time_pst,
                                                 channelcountry,
                                                 channel,
                                                 experience,
                                                 cust_id,
                                                 feature,
                                                 context_id,
                                                 context_pagetype,
                                                 pageinstance_id,
                                                 pagecontainer,
                                                 session_id
                                          FROM
                                                 all_sessions
                                   ) a
                     ) b
              WHERE
                     search_session_flag = 1 
                     AND event_count >= 2 
                     AND event_name IN (
                            'com.nordstrom.customer.event.KeywordSearchResultsGenerated',
                            'com.nordstrom.event.customer.Engaged',
                            'com.nordstrom.event.customer.Impressed',
                            'com.nordstrom.event.customer.ProductSummarySelected',
                            'com.nordstrom.event.customer.ProductSummaryCollectionViewed'
                     )
       ) base
WHERE
       kw_gen_rank <> 0
       AND event_name = 'com.nordstrom.customer.event.KeywordSearchResultsGenerated'
       OR (
              kw_gen_rank <> 0
              AND event_name IN (
                     'com.nordstrom.event.customer.ProductSummarySelected',
                     'com.nordstrom.event.customer.ProductSummaryCollectionViewed'
              ) 
              AND context_pagetype = 'SEARCH_RESULTS'
       );






CREATE OR replace TEMPORARY VIEW pscv AS 
(
   SELECT
     eventtime
   , explode_outer(productsummarycollection) AS psc_array
   , psc_array.element.id                         AS element_id
   , psc_array.element.subid                      AS element_subid
   , psc_array.productstyle.id                    AS product_id
   , psc_array.element.index                      AS element_index   
   , psc_array.element.subindex                   AS element_subindex
   , context.pagetype
   , cast(headers ['Id'] AS string) as event_id
   , year
   , month
   , day
   , hour
   FROM
    acp_event_view.customer_activity_product_summary_collection_viewed
    where 
    to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} -1 and {end_date} + 1
);

CREATE OR replace TEMPORARY VIEW pss AS 
(
SELECT
      eventtime
   , productsummary.element.id      AS element_id
   , productsummary.element.subid   as element_subid
   , productsummary.productstyle.id AS product_id
   , productsummary.element.index   AS element_index
   , productsummary.element.subindex as element_subindex
   , context.pagetype
   , cast(headers ['Id'] AS string) as event_id 
   , year
   , month
   , day
   , hour
   FROM
    acp_event_view.customer_activity_product_summary_selected
      WHERE 
    to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} -1 and {end_date} + 1
);

CREATE OR replace TEMPORARY VIEW search_pscv_pss AS 
SELECT  pscv_pss_event_id
       ,element_id
       ,element_subid
       ,product_id
       ,element_index
       ,element_subindex
       ,agg
FROM
(
	SELECT  pscv_pss_event_id
	       ,element_id
	       ,element_subid
	       ,product_id
	       ,element_index
	       ,element_subindex
	       ,COUNT(*) AS agg
	FROM
	( 
		SELECT  event_id               AS pscv_pss_event_id
		       ,element_id
		       ,element_subid
		       ,product_id
		       ,element_index
		       ,element_subindex
		FROM pscv 
        WHERE pagetype = 'SEARCH_RESULTS'
	)
	GROUP BY  1,2,3,4,5,6
	UNION ALL
	SELECT  event_id AS pscv_pss_event_id
	       ,element_id
	       ,element_subid
	       ,product_id
	       ,element_index
	       ,element_subindex
	       ,COUNT(*)                       AS agg
	FROM pss 
	WHERE 
           pagetype = 'SEARCH_RESULTS' 
    GROUP BY  1,2,3,4,5,6
);

CREATE OR replace TEMPORARY VIEW atb AS 
SELECT      session_id
	       ,product_id
FROM 
(
	(
	SELECT 
	event_id
	,session_id
	FROM
    all_sessions e
    WHERE e.event_name = 'com.nordstrom.customer.AddedToBag' 
	) e
	
	INNER JOIN
	
	( 
		SELECT  cast(headers ['Id'] AS string) AS u_event_id
		       ,productstyle.id AS product_id
		FROM acp_event_view.customer_activity_added_to_bag atb
		WHERE 
    to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} -1 and {end_date} + 1
        and bagtype = 'DEFAULT'
		AND bagactiontype = 'ADD'
	) atb_d
	ON (e.event_id = atb_d.u_event_id)
)
GROUP BY  1,2;	


CREATE OR replace TEMPORARY VIEW search_pss_pscv_atb AS 
SELECT  
        SS.activity_week_start
       ,SS.session_id
       ,SS.session_end_page
       ,SS.session_start_page
       ,SS.session_endtime_utc
       ,SS.session_starttime_utc
       ,SS.session_endtime_pst
       ,SS.session_starttime_pst
       ,SS.event_id
       ,SS.event_name
       ,SS.event_time_utc
       ,SS.event_time_pst
       ,SS.channelcountry
       ,SS.channel
       ,SS.experience
       ,SS.cust_id
       ,SS.feature
       ,SS.context_id
       ,SS.context_pagetype
       ,SS.pageinstance_id
       ,SS.pagecontainer
       ,SS.kw_gen_rank
       ,SS.source_feature
       ,SS.source_page_id
       ,SS.source_page_type
       ,SS.next_page_id
       ,SS.next_page_type 
       ,P.pscv_pss_event_id
       ,P.element_id
       ,P.element_subid
       ,P.product_id
       ,P.element_index
       ,P.element_subindex
       ,atb.product_id AS atb_product_id
       ,SS.activity_date_partition
FROM search_sessions SS
LEFT JOIN search_pscv_pss P ON (SS.event_id = P.pscv_pss_event_id)
LEFT JOIN atb ON (SS.session_id = atb.session_id AND p.product_id = atb.product_id);



CREATE OR replace TEMPORARY VIEW kwgen_session_detail  AS 
SELECT
            kwgen_event_id
	       ,kwgen_context_id
	       ,keyword
	       ,filters_str
	       ,sortorder
	       ,matchstrategy
	       ,searchresult_count
	       ,requested_count
	       ,offset
	       ,lead(event_time_utc) over (partition by session_id ORDER BY event_time_utc asc) AS next_kwgen_event_time_utc
	       ,from_utc_timestamp(lead(event_time_utc) over (partition by session_id ORDER BY event_time_utc asc), 'US/Pacific') AS next_kwgen_event_time_pst
           ,COUNT(kwgen_event_id) over(partition by session_id ORDER BY event_time_utc asc) AS kw_gen_rank
           ,session_id
           ,event_time_utc
           ,from_utc_timestamp(event_time_utc, 'US/Pacific') AS event_time_pst
		   ,date(from_utc_timestamp(event_time_utc, 'US/Pacific')) as activity_date_partition
FROM
(
select
            cast(headers ['Id'] AS string)                      AS kwgen_event_id
	       ,nullif(lower(kw.contextid),'')               AS kwgen_context_id
	       ,lower(kw.phrase)                             AS keyword
	       ,array_join(flatten(map_values(filters)),'|') AS filters_str
	       ,sortorder                                    AS sortorder
	       ,matchstrategy                                AS matchstrategy
	       ,totalresultcount                             AS searchresult_count
	       ,resultcountrequested                         AS requested_count
	       ,offset                                       AS offset
	       ,S.session_id
	       ,S.event_time_utc
	       , COUNT(*)                                          AS record_ct
	FROM  acp_event.customer_keyword_search_results_generated kw 
	INNER JOIN search_sessions S 
	ON S.event_id = cast(kw.headers ['Id'] AS string)
	WHERE S.event_name = 'com.nordstrom.customer.event.KeywordSearchResultsGenerated' 
       and nullif(lower(kw.contextid),'')  <>''
   and to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} -1 and {end_date} + 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) kwgen_session_detail;


CREATE OR replace TEMPORARY VIEW engaged  AS 
SELECT  context.pageinstanceid AS pageinstanceid,
		MAX(element.value) AS suggestion_value
		FROM acp_event_view.customer_activity_engaged 
		WHERE source.feature = 'KeywordSearch'
		AND element.id = 'Global/KeywordSearch/Suggestion'
		AND source.platform in ('MOW', 'WEB')
		
   and to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} -1 and {end_date} + 1
		GROUP BY 1;


CREATE OR replace TEMPORARY VIEW customer_activity_arrived AS
SELECT  session_id 
       ,pageinstance_id
       ,MAX(referrer)     AS arrival_referrer
       ,MIN(arrival_rank) AS arrival_rank
FROM
(
	SELECT  e.session_id
	       ,e.event_id
	       ,e.event_time_utc 
	       ,e.context_pagetype
	       ,e.pageinstance_id
	       ,e.referrer
	       ,rank() over (partition by session_id ORDER BY event_time_utc asc) AS arrival_rank
	FROM all_sessions e
	WHERE 
	e.event_name = 'com.nordstrom.customer.Arrived' 
)
WHERE pageinstance_id is not null
AND pageinstance_id <> ''
GROUP BY  1,2;     


CREATE OR replace TEMPORARY VIEW kwgen_session_detail_expanded AS
SELECT      	 search_exp.activity_week_start
				,search_exp.event_time_utc
				,search_exp.event_time_pst
                ,search_exp.cust_id
				,search_exp.channelcountry
       			,search_exp.channel
				,search_exp.experience
                ,search_exp.session_id
       			,search_exp.session_end_page
       			,search_exp.session_start_page
       			,cast(substr(search_exp.session_starttime_utc,1,22) AS timestamp) AS session_starttime_utc
				,cast(substr(search_exp.session_endtime_utc,1,22) AS timestamp) AS session_endtime_utc
				,cast(substr(search_exp.session_starttime_pst,1,22) AS timestamp) AS session_starttime_pst
				,cast(substr(search_exp.session_endtime_pst,1,22) AS timestamp) AS session_endtime_pst
                ,search_exp.event_id
       			,search_exp.event_name
       			,search_exp.feature
				,search_exp.element_id
                            ,search_exp.element_subid
       			,search_exp.context_id
       			,search_exp.context_pagetype
       			,search_exp.pageinstance_id
       			,search_exp.pagecontainer
				,search_exp.source_feature
				,search_exp.source_page_id
       			,search_exp.source_page_type
				,search_exp.next_page_id
       			,search_exp.next_page_type
				,search_exp.arrival_referrer
				,CASE WHEN event_name = 'com.nordstrom.customer.event.KeywordSearchResultsGenerated' THEN suggestion_value  ELSE null END AS suggestion_value 
                ,CASE WHEN event_name = 'com.nordstrom.customer.event.KeywordSearchResultsGenerated' THEN arrival_rank  ELSE null END     AS arrival_rank    
			    ,search_exp.keyword
		        ,search_exp.prior_keyword
				,search_exp.parent_query_rank
				,concat(session_id,cast(parent_query_rank AS string))                                                       AS parent_query_id    
				
       			,MAX(case WHEN sortorder <> 'FEATURED' THEN 1 else 0 end) over (partition by session_id,parent_query_rank) AS parent_query_sorted 
       			,MAX(parent_query_rank) over (partition by session_id,keyword)                                             AS repeat_keyword_rank 
       			,(MAX(parent_query_rank) over (partition by session_id,keyword))-1                                         AS repeat_keyword_count 
       			,INT(kw_result_rank)                                                                                       AS result_query_rank  
       			,kw_result_context                                                                                     AS result_context_id
       			,concat(session_id,cast(kw_result_rank AS string))                                                         AS result_query_id 
       			,matchstrategy
			    ,search_exp.filters_str
				,search_exp.sortorder
				,INT(search_exp.searchresult_count) AS searchresult_count 
				,INT(search_exp.requested_count) AS requested_count  
				,INT(search_exp.offset) AS offset  
				,search_exp.product_id 
				,search_exp.element_index 
                            ,search_exp.element_subindex 
       		    ,search_exp.atb_product_id
       		    ,search_exp.activity_date_partition
				FROM
(
SELECT  search_2.*
       ,INT(COUNT(parent_id) over(partition by session_id ORDER BY event_time_utc asc)) AS parent_query_rank 
	   FROM
(
SELECT search_1.*
       ,CASE WHEN event_name = 'com.nordstrom.customer.event.KeywordSearchResultsGenerated' AND (prior_keyword <> keyword or kw_result_rank = 1 or source_feature = 'KeywordSearch' or arrival_rank is not null) THEN event_id  ELSE null END AS parent_id
	  FROM     
(
SELECT          
        		 SS.activity_week_start
       			,SS.activity_date_partition
       			,SS.session_id
       			,SS.session_end_page
       			,SS.session_start_page
       			,SS.session_endtime_utc
       			,SS.session_starttime_utc
       			,SS.session_endtime_pst
       			,SS.session_starttime_pst
       			,SS.event_id
       			,SS.event_name
       			,SS.event_time_utc
       			,SS.event_time_pst
       			,SS.channelcountry
       			,SS.channel
       			,SS.experience
       			,SS.cust_id
       			,SS.feature
       			,SS.context_id
       			,SS.context_pagetype
       			,SS.pageinstance_id
       			,SS.pagecontainer
       			,SS.kw_gen_rank
       			,SS.source_feature
       			,SS.source_page_id
       			,SS.source_page_type
       			,SS.next_page_id
       			,SS.next_page_type 
       			,SS.pscv_pss_event_id
       		    ,SS.element_id
       		    ,SS.element_subid
       		    ,SS.product_id
       		    ,SS.element_index
       		    ,SS.element_subindex
       		    ,SS.atb_product_id
		        ,filters_str
		        ,kw.kwgen_event_id    AS kw_result_id
		        ,kw.event_time_utc  AS kw_result_start_time_utc
		        ,kw.next_kwgen_event_time_utc AS kw_result_end_time_utc
		        ,kw.event_time_pst  AS kw_result_start_time_pst
		        ,kw.next_kwgen_event_time_pst AS kw_result_end_time_pst
		        ,kw.kw_gen_rank  AS kw_result_rank
		        ,kw.kwgen_context_id AS kw_result_context
		        ,kw.keyword
		        ,last_value(case WHEN SS.event_name = 'com.nordstrom.customer.event.KeywordSearchResultsGenerated' THEN kw.keyword else null end,true ) over (partition by SS.session_id ORDER BY SS.event_time_utc asc  rows between  unbounded preceding and 1 preceding) AS prior_keyword
		        ,kw.sortorder
		        ,kw.matchstrategy
		        ,kw.searchresult_count
		        ,kw.requested_count
		        ,kw.offset
			    ,sugg.pageinstanceid
			    ,sugg.suggestion_value
			    ,arr.arrival_referrer
	            ,cast(arr.arrival_rank AS integer) AS arrival_rank
		FROM search_pss_pscv_atb SS
		LEFT JOIN kwgen_session_detail kw ON (SS.session_id = kw.session_id AND SS.kw_gen_rank = kw.kw_gen_rank)
		LEFT JOIN customer_activity_arrived arr ON (arr.pageinstance_id = SS.next_page_id AND arr.session_id = SS.session_id)
        LEFT JOIN engaged sugg ON (sugg.pageinstanceid = SS.source_page_id)
) search_1
) search_2
)search_exp
; 








insert overwrite table {hive_schema}.search_session_analytical partition (activity_date_partition)
select *
from kwgen_session_detail_expanded;
