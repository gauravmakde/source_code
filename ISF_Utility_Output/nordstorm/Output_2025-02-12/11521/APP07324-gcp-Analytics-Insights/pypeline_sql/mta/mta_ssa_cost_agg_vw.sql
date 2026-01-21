SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_ssa_cost_11521_ACE_ENG;
     Task_Name=mta_ssa_cost_agg_vw;'
     FOR SESSION VOLATILE;

replace view {mta_t2_schema}.MTA_SSA_COST_AGG_VW
/************************************************************************************/
/* View Name   : MTA_SSA_COST_AGG_VW                                                */
/* Description : Daily aggregate table with TY/LY for Tableau MTA reports           */
/*                                                                                  */
/* Created By  : AWYA                                                               */
/*                                                                                  */
/*  Version     Change Description                     Updated Date                 */
/*  1.0         Initial Version                         04-May-2023                 */
/************************************************************************************/
AS
LOCKING ROW FOR ACCESS
SELECT
    activity_date_pacific
    , year_num
    , month_short_desc
    , week_454_num
    , week_of_fyr
    , week_num
    , week_desc
    , month_num
    , month_desc
    , order_channel
    , channelcountry
    , arrived_channel
    , marketing_type
    , finance_rollup
    , finance_detail
    , funnel_type
    , nmn_flag
     -- TY
    , sum(attributed_demand) as attributed_demand
    , sum(attributed_units) as attributed_units
    , sum(attributed_orders) as attributed_orders
    , sum(attributed_pred_net) as attributed_pred_net
    , sum(sessions) as sessions
    , sum(bounced_sessions) as bounced_sessions
    , sum(session_orders) as session_orders
	, sum(session_demand) as session_demand
    , sum(cost) as cost
     -- LY
    , sum(ly_attributed_demand) as ly_attributed_demand
    , sum(ly_attributed_units) as ly_attributed_units
    , sum(ly_attributed_orders) as ly_attributed_orders
    , sum(ly_attributed_pred_net) as ly_attributed_pred_net
    , sum(ly_sessions) as ly_sessions
    , sum(ly_bounced_sessions) as ly_bounced_sessions
    , sum(ly_session_orders) as ly_session_orders
	, sum(ly_session_demand) as ly_session_demand
    , sum(ly_cost) as ly_cost
    , dw_sys_load_tmstp
FROM (
    --TY
    SELECT
        d.day_date AS activity_date_pacific
        , d.year_num
        , d.month_short_desc
        , d.week_454_num
        , d.week_of_fyr
	    , d.week_num
        , d.week_desc
	    , d.month_num
	    , d.month_desc
        , mpcs_ty.order_channel
        , mpcs_ty.channelcountry
        , mpcs_ty.arrived_channel
        , mpcs_ty.marketing_type
        , mpcs_ty.finance_rollup
        , mpcs_ty.finance_detail
        , mpcs_ty.funnel_type
        , mpcs_ty.nmn_flag
        -- TY
        , mpcs_ty.attributed_demand
        , mpcs_ty.attributed_units
        , mpcs_ty.attributed_orders
        , mpcs_ty.attributed_pred_net
        , mpcs_ty.sessions
        , mpcs_ty.bounced_sessions
        , mpcs_ty.session_orders
		, mpcs_ty.session_demand
        , mpcs_ty.cost
        -- LY
        , CAST(0 AS FLOAT) as ly_attributed_demand
        , CAST(0 AS FLOAT) as ly_attributed_units
        , CAST(0 AS FLOAT) as ly_attributed_orders
        , CAST(0 AS FLOAT) as ly_attributed_pred_net
        , CAST(0 AS FLOAT) as ly_sessions
        , CAST(0 AS FLOAT) as ly_bounced_sessions
        , CAST(0 AS FLOAT) as ly_session_orders
		, CAST(0 AS FLOAT) as ly_session_demand
        , CAST(0 AS FLOAT) as ly_cost
        , mpcs_ty.dw_sys_load_tmstp
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {mta_t2_schema}.MTA_SSA_COST_AGG mpcs_ty
            on d.day_date =  mpcs_ty.activity_date_pacific
    UNION ALL
    --LY
    SELECT
        d.day_date AS activity_date_pacific
        , d.year_num
        , d.month_short_desc
        , d.week_454_num
        , d.week_of_fyr
	    , d.week_num
        , d.week_desc
	    , d.month_num
	    , d.month_desc
        , mpcs_ly.order_channel
        , mpcs_ly.channelcountry
        , mpcs_ly.arrived_channel
        , mpcs_ly.marketing_type
        , mpcs_ly.finance_rollup
        , mpcs_ly.finance_detail
        , mpcs_ly.funnel_type
        , mpcs_ly.nmn_flag
        -- TY
        , CAST(0 AS FLOAT) AS ty_attributed_demand
        , CAST(0 AS FLOAT) AS ty_attributed_units
        , CAST(0 AS FLOAT) AS ty_attributed_orders
        , CAST(0 AS FLOAT) AS ty_attributed_pred_net
        , CAST(0 AS FLOAT) AS ty_sessions
        , CAST(0 AS FLOAT) AS ty_bounced_sessions
        , CAST(0 AS FLOAT) AS ty_session_orders
		, CAST(0 AS FLOAT) AS ty_session_demand
        , CAST(0 AS FLOAT) AS ty_cost
        -- LY
        , mpcs_ly.attributed_demand     AS ly_attributed_demand
        , mpcs_ly.attributed_units      AS ly_attributed_units
        , mpcs_ly.attributed_orders     AS ly_attributed_orders
        , mpcs_ly.attributed_pred_net   AS ly_attributed_pred_net
        , mpcs_ly.sessions              AS ly_sessions
        , mpcs_ly.bounced_sessions      AS ly_bounced_sessions
        , mpcs_ly.session_orders        AS ly_session_orders
		, mpcs_ly.session_demand        AS ly_session_demand
        , mpcs_ly.cost                  AS ly_cost
        , mpcs_ly.dw_sys_load_tmstp
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {mta_t2_schema}.MTA_SSA_COST_AGG mpcs_ly
            on d.last_year_day_date_realigned =  mpcs_ly.activity_date_pacific
) mpcs
WHERE mpcs.activity_date_pacific <= (select max(activity_date_pacific ) from {mta_t2_schema}.MTA_SSA_COST_AGG)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, dw_sys_load_tmstp
;

SET QUERY_BAND = NONE FOR SESSION;

