SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=funnel_cost_fact_vw_11521_ACE_ENG;
     Task_Name=funnel_cost_fact_vw;'
     FOR SESSION VOLATILE;

/*

T2/View Name: funnel_cost_fact_vw
Team/Owner: AE
Date Created/Modified: 2023-02-09

Note:
A combined view of the rack_funnel_cost_fact and fp_funnel_cost_fact tables.

*/

CREATE VIEW {funnel_io_t2_schema}.funnel_cost_fact_vw
AS
LOCK ROW FOR ACCESS

SELECT *
FROM    {funnel_io_t2_schema}.rack_funnel_cost_fact
UNION ALL
SELECT *
FROM    {funnel_io_t2_schema}.fp_funnel_cost_fact
;




SET QUERY_BAND = NONE FOR SESSION;