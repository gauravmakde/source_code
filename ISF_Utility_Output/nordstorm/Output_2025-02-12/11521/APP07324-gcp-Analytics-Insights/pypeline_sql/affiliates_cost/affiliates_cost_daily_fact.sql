SET QUERY_BAND = 'App_ID=APP08823;
     DAG_ID=affiliates_cost_11521_ACE_ENG;
     Task_Name=affiliates_cost_daily_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_FUNNEL_IO.affiliates_cost_daily_fact
Team/Owner: AE
Date Created/Modified: 3/10/23

Note:
-10 day lookback window to match the upstream funnel_io jobs.

*/



-- prototype based on code here: https://gitlab.nordstrom.com/nordace/napbi/-/blob/master/sql/teradata/nrhl_affiliates_cost.sql
-- removing all ly comps, as the end table will be presumably different
-- need to pull in dimensions, low spend, mid spend

-- starting with US
-- start with sum of low funnel and sum of mid at a daily level
-- no need to do network fees or return rates for Rack
-- drop table publisher_costs;


-- update to only rack/CA?
-- Union in FP logic


-- drop table publisher_network
CREATE MULTISET VOLATILE TABLE publisher_network AS (
-- FP Logic
SELECT	'Nordstrom' AS banner
        , CASE WHEN publisher_cost.currency = 'USD' THEN 'US'
                WHEN publisher_cost.currency = 'CAD' THEN 'CA'
        END AS country
        , publisher_cost.stats_date
        , publisher_cost.low_funnel_publisher_cost
        , network_fees.low_funnel_network_fees
        , publisher_cost.mid_funnel_publisher_cost
        , network_fees.mid_funnel_network_fees
FROM -- publisher cost
        (SELECT	fp.file_name
                , fp.currency
                , fp.stats_date
                , sum(CASE WHEN fp.campaign_name NOT LIKE 'MF%' then fp.gross_commissions*(1-rr.return_rate) END) AS low_funnel_publisher_cost
                , sum(CASE WHEN fp.campaign_name LIKE 'MF%' then fp.gross_commissions*(1-rr.return_rate) END) AS mid_funnel_publisher_cost
        FROM    T2DL_DAS_FUNNEL_IO.fp_funnel_cost_fact fp
        LEFT JOIN  T2DL_DAS_FUNNEL_IO.affiliates_return_rate rr ON fp.stats_date BETWEEN rr.start_day_date AND rr.end_day_date
                                                                AND rr.banner = 'Nordstrom'
                                                                AND rr.country = 'US'
        WHERE   fp.stats_date BETWEEN {start_date} AND {end_date}
        AND     fp.file_name = 'fp_rakuten'
        AND     fp.currency = 'USD'
        GROUP BY 1,2,3 ) publisher_cost
LEFT JOIN -- network fees
                (SELECT fp.file_name
                        , fp.currency
                        , fp.stats_date
                        , sum(CASE WHEN fp.campaign_name NOT LIKE 'MF%' then fp.gross_sales*(1-rr.return_rate)*0.0023 END) AS low_funnel_network_fees
                        , sum(CASE WHEN fp.campaign_name LIKE 'MF%' then fp.gross_sales*(1-rr.return_rate)*0.0023 END) AS mid_funnel_network_fees
                FROM    T2DL_DAS_FUNNEL_IO.fp_funnel_cost_fact fp
                LEFT JOIN  T2DL_DAS_FUNNEL_IO.affiliates_return_rate rr ON fp.stats_date BETWEEN rr.start_day_date AND rr.end_day_date
                                                                        AND rr.banner = 'Nordstrom'
                                                                        AND rr.country = 'US'
                WHERE   stats_date BETWEEN {start_date} AND {end_date}
                AND     fp.file_name = 'fp_rakuten'
                AND     fp.currency = 'USD'
                GROUP BY 1,2,3) network_fees ON publisher_cost.stats_date = network_fees.stats_date
                                        AND publisher_cost.file_name = network_fees.file_name
                                        AND publisher_cost.currency = network_fees.currency
UNION ALL
-- RACK Logic
SELECT	'Rack' AS banner
        , CASE WHEN rfcf.currency = 'USD' THEN 'US'
                        WHEN rfcf.currency = 'CAD' THEN 'CA'
        END AS country
        , rfcf.stats_date
        , sum(CASE WHEN rfcf.campaign_name NOT LIKE 'MF%' THEN rfcf.estimated_net_total_cost END) AS low_funnel_publisher_cost
        , NULL AS low_funnel_network_fees
        , sum(CASE WHEN rfcf.campaign_name LIKE 'MF%' THEN rfcf.estimated_net_total_cost END) AS mid_funnel_publisher_cost
        , NULL AS mid_funnel_network_fees
FROM	T2DL_DAS_FUNNEL_IO.rack_funnel_cost_fact rfcf
WHERE 	rfcf.stats_date BETWEEN {start_date} AND {end_date}
AND 	rfcf.file_name = 'rack_rakuten'
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX(stats_date, banner, country) ON COMMIT PRESERVE ROWS;
;


-- drop table  daily_cost_split
CREATE MULTISET VOLATILE TABLE daily_cost_split AS (
SELECT  banner
        , country
        , encrypted_id
        , publisher
        , start_day_date
        , end_day_date
	, case when end_day_date <> start_day_date then mf_total_spend/(end_day_date - start_day_date)
		else mf_total_spend/1 end as mf_daily_cost
	, case when end_day_date <> start_day_date then lf_commission/(end_day_date - start_day_date)
		else lf_commission/1 end as lf_daily_commission
	, case when end_day_date <> start_day_date then lf_paid_placement/(end_day_date - start_day_date)
		else lf_paid_placement/1 end as lf_daily_paid_placement
FROM    T2DL_DAS_FUNNEL_IO.affiliates_campaign_cost tl
) WITH DATA PRIMARY INDEX(start_day_date) ON COMMIT PRESERVE ROWS
;


-- expands to full table
CREATE MULTISET VOLATILE TABLE daily_cost_rollup AS (
SELECT	splt.banner
        , splt.country
        , cal.day_date
        , sum(splt.mf_daily_cost) AS mf_daily_cost
        , sum(splt.lf_daily_commission) AS lf_daily_commission
        , sum(splt.lf_daily_paid_placement) AS lf_paid_placement
FROM 	prd_nap_usr_vws.day_cal cal
INNER JOIN daily_cost_split splt ON cal.day_date BETWEEN splt.start_day_date AND splt.end_day_date
WHERE cal.day_date BETWEEN {start_date} AND {end_date}
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX(day_date) ON COMMIT PRESERVE ROWS
;


-- bring all costs together
CREATE MULTISET VOLATILE TABLE all_costs AS (
SELECT	pn.stats_date
        , pn.banner
        , pn.country
        , pn.low_funnel_publisher_cost
        , pn.low_funnel_network_fees
        , dcr.lf_daily_commission
        , dcr.lf_paid_placement
        , pn.mid_funnel_publisher_cost
        , pn.mid_funnel_network_fees
        , dcr.mf_daily_cost
FROM    publisher_network pn
LEFT JOIN daily_cost_rollup dcr ON pn.stats_date = dcr.day_date
                                AND pn.banner = dcr.banner
                                AND pn.country = dcr.country
) WITH DATA PRIMARY INDEX(stats_date, banner, country) ON COMMIT PRESERVE ROWS
;



CREATE MULTISET VOLATILE TABLE lf_mf_totals AS (
SELECT  stats_date
        , banner
        , country
        , coalesce(low_funnel_publisher_cost,0) as low_funnel_publisher_cost
        , coalesce(low_funnel_network_fees,0) as low_funnel_network_fees
        , coalesce(lf_daily_commission,0) as lf_daily_commission
        , coalesce(lf_paid_placement,0) as lf_paid_placement
        , coalesce(low_funnel_publisher_cost,0) + coalesce(low_funnel_network_fees,0) + coalesce(lf_daily_commission,0) + coalesce(lf_paid_placement,0) AS low_funnel_daily_cost
        , coalesce(mid_funnel_publisher_cost,0) as mid_funnel_publisher_cost
        , coalesce(mid_funnel_network_fees,0) as mid_funnel_network_fees
        , coalesce(mf_daily_cost,0) as mf_daily_cost
        , coalesce(mid_funnel_publisher_cost,0) + coalesce(mid_funnel_network_fees,0) + coalesce(mf_daily_cost,0) AS mid_funnel_daily_cost
FROM    all_costs
) WITH DATA PRIMARY INDEX(stats_date, banner, country) ON COMMIT PRESERVE ROWS
;

-- add in override
CREATE MULTISET VOLATILE TABLE override_daily_total AS (
SELECT	lm.stats_date
        , lm.banner
        , lm.country
        , coalesce(o.lf_cost, lm.low_funnel_daily_cost) AS low_funnel_daily_cost
        , coalesce(NULLIFZERO(o.total_cost - o.lf_cost), lm.mid_funnel_daily_cost) AS mid_funnel_daily_cost
FROM	lf_mf_totals lm
LEFT JOIN T2DL_DAS_FUNNEL_IO.affiliates_cost_override o ON o.day_date = lm.stats_date
                                                        AND o.banner = lm.banner
                                                        AND o.country = lm.country
) WITH DATA PRIMARY INDEX(stats_date, banner, country) ON COMMIT PRESERVE ROWS
;


DELETE
FROM    {funnel_io_t2_schema}.affiliates_cost_daily_fact
WHERE stats_date BETWEEN {start_date} AND {end_date}
;


INSERT INTO {funnel_io_t2_schema}.affiliates_cost_daily_fact
SELECT	stats_date
        , banner
        , country
        , low_funnel_daily_cost
        , mid_funnel_daily_cost
        , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM 	override_daily_total
WHERE 	stats_date BETWEEN {start_date} AND {end_date}
;

COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (stats_date, banner, country), -- column names used for primary index
                    COLUMN (stats_date)  -- column names used for partition
on {funnel_io_t2_schema}.affiliates_cost_daily_fact;

SET QUERY_BAND = NONE FOR SESSION;