SET QUERY_BAND = 'App_ID=APP09330;
     DAG_ID=customer_session_data_11521_ACE_ENG;
     Task_Name=customer_session_data;'
     FOR SESSION VOLATILE;


--T2/Table Name: {sessions_t2_schema}.customer_session_data
--Team/Owner: MOA
--Date Created/Modified: 2024-04-29
--Note:
-- This table supports SESSION X CUSTOMER Dashboard-Consolidated Version.


CREATE Multiset VOLATILE TABLE start_varibale AS
(SELECT min(day_date) As start_date
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE week_idnt = (select distinct week_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date={start_date}))
WITH data PRIMARY INDEX (start_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE end_varibale AS
(SELECT max(day_date) AS end_date
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE week_idnt = (select distinct week_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date={end_date}))
WITH data PRIMARY INDEX (end_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE _variables
AS
( select start_date , end_date from start_varibale, end_varibale)
WITH data PRIMARY INDEX (start_date) ON COMMIT PRESERVE ROWS;

CREATE volatile multiset TABLE date_lookup
AS
(SELECT DISTINCT day_date AS DAY_DT,
	   last_year_day_date as ly_day_dt,
       MIN(CASE WHEN last_year_day_date_realigned IS NULL THEN (day_date -364) ELSE last_year_day_date_realigned END) -1 AS LY_start_dt,
       MIN(day_date) -1 AS LY_end_dt,
       MAX(day_date) AS TY_end_dt
FROM prd_nap_usr_vws.day_cal,
     _variables
WHERE day_date BETWEEN start_date AND end_date
GROUP BY 1,2
	  ) WITH data PRIMARY INDEX (DAY_DT) ON COMMIT preserve rows;

CREATE multiset volatile TABLE customer_session_data_temp
AS
(SELECT 			a.acp_id,
					a.activity_date_pacific AS DAY_DT,
                    channel,
                    mrkt_type,
                    finance_rollup,
                    finance_detail,
                    count(distinct session_id) AS total_sessions,
                    sum(bounce_flag) AS bounced_sessions,
                    sum(product_views) AS product_views,
                    sum(cart_adds) AS cart_adds,
                    sum(web_orders) AS web_orders,
                    sum(web_demand) AS web_demands,
                    sum(a.web_ordered_units) as web_ordered_units,
       				sum(a.web_demand_usd) as web_demand_usd
FROM T2DL_DAS_SESSIONS.dior_session_fact a
WHERE CAST(a.activity_date_pacific AS DATE)  between (SELECT start_date FROM _variables) and (SELECT end_date FROM _variables)
AND   a.finance_rollup IN ('affiliates', 'PAID_SEARCH', 'EMAIL', 'SEO', 'DISPLAY', 'SOCIAL', 'SHOPPING')
AND   1 = 1
AND   a.channelcountry = 'US'
AND   a.channel IN ('NORDSTROM_RACK','NORDSTROM')
         GROUP BY 1,
                      2,
                      3,
                      4,
                      5,
                     6) WITH data PRIMARY INDEX (acp_id,channel,mrkt_type,finance_rollup) ON COMMIT preserve rows;

CREATE multiset volatile TABLE customer_session_data_final
AS
(SELECT 			a.acp_id,
					a.DAY_DT,
                    channel,
                    mrkt_type,
                    finance_rollup,
                    finance_detail,
                    c.engagement_cohort,
                    b.nordy_level AS LOYALTY_STATUS,
                    a.total_sessions,
                    sum(a.bounced_sessions) AS bounced_sessions,
                    sum(a.product_views) AS product_views,
                    sum(a.cart_adds) AS cart_adds,
                    sum(a.web_orders) AS web_orders,
                    sum(a.web_demands) AS web_demands,
                    sum(a.web_ordered_units) as web_ordered_units,
       				sum(a.web_demand_usd) as web_demand_usd
FROM customer_session_data_temp a
LEFT JOIN (SELECT DISTINCT acp_id,
                    nordy_level
             FROM prd_nap_usr_vws.analytical_customer
             GROUP BY 1,
                      2) AS b ON a.acp_id = b.acp_id
LEFT JOIN (SELECT DISTINCT acp_id,
       execution_qtr_start_dt,
       execution_qtr_end_dt,
       engagement_cohort
FROM T2DL_DAS_AEC.audience_engagement_cohorts
GROUP BY 1,
         2,
         3,
         4) AS c ON a.acp_id = c.acp_id and a.DAY_DT between  c.execution_qtr_start_dt  and c.execution_qtr_end_dt
         GROUP BY 1,
                      2,
                      3,
                      4,
                      5,
                     6,
                    7,
                   8,
                   9) WITH data PRIMARY INDEX (acp_id,channel,mrkt_type,finance_rollup,finance_detail, LOYALTY_STATUS,engagement_cohort) ON COMMIT preserve rows;

                  
DELETE 
FROM    {sessions_t2_schema}.customer_session_data
where day_dt between (select start_date from _variables) and (select end_date from _variables);

INSERT INTO {sessions_t2_schema}.customer_session_data
select m.*,CURRENT_TIMESTAMP as dw_sys_load_tmstp from customer_session_data_final m;


SET QUERY_BAND = NONE FOR SESSION;
