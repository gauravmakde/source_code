SET QUERY_BAND = 'App_ID=APP08629;
     DAG_ID=rakuten_publisher_daily_11521_ACE_ENG;
     Task_Name=rakuten_publisher_daily;'
     FOR SESSION VOLATILE;


--T2/Table Name: {apd_t2_schema}.rakuten_publisher_daily
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2023-09-27
--Note:
-- This table supports the 4 BOX Affiliates Performance Dashboard-Consolidated Version.

CREATE Multiset VOLATILE TABLE start_varibale
AS
(SELECT MIN(day_date) AS start_date
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE week_idnt = (SELECT DISTINCT week_idnt
                   FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
                   WHERE day_date = {start_date}))
WITH data PRIMARY INDEX (start_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE end_varibale
AS
(SELECT max(day_date) AS end_date
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE week_idnt = (SELECT DISTINCT week_idnt
                   FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
                   WHERE day_date = {end_date}))
WITH data PRIMARY INDEX (end_date) ON COMMIT PRESERVE ROWS;



CREATE Multiset VOLATILE TABLE _variables2
AS
( select start_date , end_date from start_varibale, end_varibale)
WITH data PRIMARY INDEX (start_date) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE apd_4_box_mta_temp
AS
(SELECT DAY_DT,
       UTM_CAMPAIGN,
       PUBLISHER,
       PUBLISHER_GROUP,
       PUBLISHER_SUBGROUP,
       BANNER,
       SUM(ATTRIBUTED_DEMAND) AS ATTRIBUTED_DEMAND,
       SUM(ATTRIBUTED_ORDERS) AS ATTRIBUTED_ORDERS,
       SUM(ATTRIBUTED_NET_SALES) ATTRIBUTED_NET_SALES,
	SUM(ACQUIRED_ATTRIBUTED_ORDERS) AS ACQUIRED_ATTRIBUTED_ORDERS,
	sum(RETAINED_ATTRIBUTED_ORDERS) AS RETAINED_ATTRIBUTED_ORDERS,	
       SUM(ACQUIRED_ATTRIBUTED_DEMAND) AS ACQUIRED_ATTRIBUTED_DEMAND
FROM {apd_t2_schema}.mta_publisher_daily, _variables2
where day_dt between start_date and end_date
GROUP BY 1,
         2,
         3,
         4,
         5,
         6
	  ) WITH data PRIMARY INDEX (DAY_DT,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP,BANNER) ON COMMIT PRESERVE ROWS;
	 
CREATE MULTISET VOLATILE TABLE AFF_SESSIONS
AS
(SELECT activity_date_pacific AS DAY_DT,
       CASE
         WHEN channel = 'NORDSTROM' THEN 'NORDSTROM'
         WHEN channel = 'NORDSTROM_RACK' THEN 'NORDSTROM RACK'
         ELSE NULL
       END AS BANNER,
       UPPER(UTM_CAMPAIGN) AS UTM_CAMPAIGN,
       SUM(CAST(total_sessions AS DECIMAL(13,5))) AS SESSIONS
FROM T2DL_DAS_MTA.MTA_UTM_SESSION_AGG,
     _variables2
WHERE activity_date_pacific BETWEEN start_date AND end_date
AND   channel IN ('NORDSTROM','NORDSTROM_RACK')
AND   channelcountry = 'US'
AND   finance_rollup = 'AFFILIATES'
AND   session_type = 'ARRIVED'
GROUP BY 1,
         2,
         3) WITH data PRIMARY INDEX (DAY_DT,BANNER,UTM_CAMPAIGN) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE daily_cost_split AS (
SELECT CASE
         WHEN banner = 'Rack' THEN 'NORDSTROM RACK'
         ELSE 'NORDSTROM'
       END AS BANNER,
        UPPER(encrypted_id) as UTM_CAMPAIGN,
        start_day_date,
        end_day_date,
	  case when end_day_date <> start_day_date then mf_total_spend/(end_day_date - start_day_date)
		else mf_total_spend/1 end as DAILY_COST,
	  case when end_day_date <> start_day_date then lf_paid_placement/(end_day_date - start_day_date)
		else lf_paid_placement/1 end as PAID_PLACEMENT
FROM    T2DL_DAS_FUNNEL_IO.affiliates_campaign_cost tl
where country = 'US'
) WITH DATA PRIMARY INDEX(start_day_date) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE PAID_PLACEMENT AS (
SELECT	cal.day_date as DAY_DT,
		splt.BANNER,
        splt.UTM_CAMPAIGN,
        sum(splt.DAILY_COST) AS DAILY_COST,
        sum(splt.PAID_PLACEMENT) AS PAID_PLACEMENT
FROM 	prd_nap_usr_vws.day_cal cal
INNER JOIN daily_cost_split splt ON cal.day_date BETWEEN splt.start_day_date AND splt.end_day_date,_variables2
WHERE cal.day_date BETWEEN start_date AND end_date
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX(DAY_DT) ON COMMIT PRESERVE ROWS;


CREATE multiset volatile TABLE RAKUTEN_TBL
AS
(SELECT stats_date AS DAY_DT,
       UPPER(platform_id) AS UTM_CAMPAIGN,
       'NORDSTROM RACK' AS BANNER,
       aff.return_rate as aff_return_rate,
       COALESCE(SUM(clicks),0) AS clicks,
       COALESCE(SUM(gross_commissions),0) AS RAKUTEN_GROSS_COMMISSIONS,
	   COALESCE(SUM(ESTIMATED_NET_TOTAL_COST),0) as ESTIMATED_NET_TOTAL_COST,
       COALESCE(SUM(gross_sales),0) AS RAKUTEN_GROSS_SALES,
       COALESCE(SUM(sales),0) AS RAKUTEN_NET_SALES
FROM T2DL_DAS_FUNNEL_IO.rack_funnel_cost_fact r
left join T2DL_DAS_FUNNEL_IO.affiliates_return_rate aff
ON r.stats_date BETWEEN aff.start_day_date AND aff.end_day_date ,_variables2
WHERE currency = 'USD'
AND   sourcetype LIKE 'rakuten%'
AND   file_name='rack_rakuten'
AND   stats_date BETWEEN start_date AND end_date
AND   1 = 1
GROUP BY 1,
         2,
         3,
         4
UNION ALL
SELECT stats_date AS DAY_DT,
       UPPER(platform_id) AS UTM_CAMPAIGN,
       'NORDSTROM' AS BANNER,
       aff_1.return_rate as aff_return_rate,
       COALESCE(SUM(clicks),0) AS clicks,
       COALESCE(SUM(gross_commissions),0) AS RAKUTEN_GROSS_COMMISSIONS,
	   COALESCE(SUM(ESTIMATED_NET_TOTAL_COST),0) as ESTIMATED_NET_TOTAL_COST,
       COALESCE(SUM(gross_sales),0) AS RAKUTEN_GROSS_SALES,
       COALESCE(SUM(sales),0) AS RAKUTEN_NET_SALES
FROM T2DL_DAS_FUNNEL_IO.fp_funnel_cost_fact s
left join T2DL_DAS_FUNNEL_IO.affiliates_return_rate aff_1
ON s.stats_date BETWEEN aff_1.start_day_date AND aff_1.end_day_date , _variables2
WHERE currency = 'USD'
AND   sourcetype LIKE 'rakuten%'
AND   file_name='fp_rakuten'
AND   stats_date BETWEEN start_date AND end_date
AND   1 = 1
GROUP BY 1,
         2,
         3,
         4) WITH DATA PRIMARY INDEX (DAY_DT,UTM_CAMPAIGN,BANNER) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE RAKUTEN_TBL_2
AS
(select COALESCE (a.DAY_DT,c.day_dt,d.day_dt,e.day_dt) as DAY_dt,
		COALESCE (a.UTM_CAMPAIGN,c.UTM_CAMPAIGN,d.UTM_CAMPAIGN,e.UTM_CAMPAIGN) as UTM_CAMPAIGN,
		COALESCE (a.PUBLISHER,g.publisher_name,'Unknown') as PUBLISHER,
		COALESCE (a.PUBLISHER_GROUP,g.publisher_group,'Unknown') as PUBLISHER_GROUP,
		COALESCE (a.PUBLISHER_SUBGROUP,g.publisher_subgroup,'Unknown') as PUBLISHER_SUBGROUP,
		COALESCE (a.BANNER,c.BANNER,d.BANNER,e.BANNER) as BANNER,
		COALESCE (aff_return_rate,0) AS aff_return_rate,
		SUM(ATTRIBUTED_DEMAND) AS ATTRIBUTED_DEMAND,
		SUM(ATTRIBUTED_NET_SALES) AS ATTRIBUTED_NET_SALES,		
       	SUM(ATTRIBUTED_ORDERS) AS ATTRIBUTED_ORDERS,
	   	SUM(ACQUIRED_ATTRIBUTED_ORDERS) AS ACQUIRED_ATTRIBUTED_ORDERS,
	   	sum(RETAINED_ATTRIBUTED_ORDERS) AS RETAINED_ATTRIBUTED_ORDERS,	
       	SUM(ACQUIRED_ATTRIBUTED_DEMAND) AS ACQUIRED_ATTRIBUTED_DEMAND,
		COALESCE (SUM(e.CLICKS),0) AS CLICKS,
		COALESCE (SUM(SESSIONS),0) AS SESSIONS,
		COALESCE (SUM(PAID_PLACEMENT),0) AS PAID_PLACEMENT,
		COALESCE (SUM(DAILY_COST),0) AS DAILY_COST,
		COALESCE (SUM(RAKUTEN_GROSS_COMMISSIONS),0) AS RAKUTEN_GROSS_COMMISSIONS,
		COALESCE (SUM(ESTIMATED_NET_TOTAL_COST),0) AS ESTIMATED_NET_TOTAL_COST,
		COALESCE (SUM(RAKUTEN_GROSS_SALES),0) AS RAKUTEN_GROSS_SALES,
		COALESCE (SUM(CASE WHEN a.DAY_DT = e.DAY_DT AND a.UTM_CAMPAIGN = e.UTM_CAMPAIGN AND a.BANNER = e.BANNER THEN a.ATTRIBUTED_ORDERS ELSE 0 END),0) AS RAKUTEN_ORDERS,
		COALESCE (SUM(RAKUTEN_NET_SALES),0) AS RAKUTEN_NET_SALES,
		COALESCE (SUM(ACQUIRED_ATTRIBUTED_ORDERS),0) AS RAKUTEN_ACQUIRED_ATTRIBUTED_ORDERS,
		COALESCE (SUM(ACQUIRED_ATTRIBUTED_DEMAND),0) AS RAKUTEN_ACQUIRED_ATTRIBUTED_DEMAND
from apd_4_box_mta_temp a	
full outer join 
(SELECT * from AFF_SESSIONS) c
       ON a.DAY_DT = c.DAY_DT
        AND a.UTM_CAMPAIGN = c.UTM_CAMPAIGN
        AND a.BANNER = c.BANNER    
full outer join 
(SELECT * from PAID_PLACEMENT, _variables2 where day_dt<=end_date) d
       ON a.DAY_DT = d.DAY_DT
        AND a.UTM_CAMPAIGN = d.UTM_CAMPAIGN
        AND a.BANNER = d.BANNER	
full outer join 
(SELECT * from RAKUTEN_TBL) e
       ON a.DAY_DT = e.DAY_DT
        AND a.UTM_CAMPAIGN = e.UTM_CAMPAIGN
        AND a.BANNER = e.BANNER	
left join t2dl_das_funnel_io.affiliate_publisher_mapping g
on COALESCE(c.UTM_CAMPAIGN,d.UTM_CAMPAIGN,e.UTM_CAMPAIGN)=g.encrypted_id                
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7
)WITH data PRIMARY INDEX (DAY_DT,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP,BANNER) ON COMMIT PRESERVE ROWS;	


CREATE MULTISET VOLATILE TABLE apd_4_box_rakuten
AS
(SELECT a.DAY_DT,	
	a.BANNER,
       a.UTM_CAMPAIGN,
       a.PUBLISHER,
       a.PUBLISHER_GROUP,
       a.PUBLISHER_SUBGROUP,
       max(a.aff_return_rate) as aff_return_rate,
       SUM(a.ATTRIBUTED_DEMAND) AS ATTRIBUTED_DEMAND,
	   SUM(a.ATTRIBUTED_ORDERS) AS ATTRIBUTED_ORDERS,
       SUM(a.ATTRIBUTED_NET_SALES) AS ATTRIBUTED_NET_SALES,
	   SUM(a.ACQUIRED_ATTRIBUTED_DEMAND) AS ACQUIRED_ATTRIBUTED_DEMAND,
	   SUM(a.ACQUIRED_ATTRIBUTED_ORDERS) AS ACQUIRED_ATTRIBUTED_ORDERS,
	   SUM(a.RETAINED_ATTRIBUTED_ORDERS) AS RETAINED_ATTRIBUTED_ORDERS,
       SUM(a.CLICKS) AS CLICKS,
       SUM(a.SESSIONS) AS SESSIONS,
       SUM(a.PAID_PLACEMENT) AS PAID_PLACEMENT,
       SUM(a.DAILY_COST) AS DAILY_COST,       
       SUM(a.RAKUTEN_GROSS_COMMISSIONS) AS RAKUTEN_GROSS_COMMISSIONS,
	   SUM(ESTIMATED_NET_TOTAL_COST) AS ESTIMATED_NET_TOTAL_COST,
       SUM(a.RAKUTEN_GROSS_SALES) AS RAKUTEN_GROSS_SALES,
       SUM(a.RAKUTEN_ORDERS) AS RAKUTEN_ORDERS,
       SUM(a.RAKUTEN_NET_SALES) AS RAKUTEN_NET_SALES,
       SUM(a.RAKUTEN_ACQUIRED_ATTRIBUTED_ORDERS) AS RAKUTEN_ACQUIRED_ATTRIBUTED_ORDERS,
       SUM(a.RAKUTEN_ACQUIRED_ATTRIBUTED_DEMAND) AS RAKUTEN_ACQUIRED_ATTRIBUTED_DEMAND
FROM RAKUTEN_TBL_2 AS a
GROUP BY 1,
         2,
         3,
         4,
         5,
         6         
        ) WITH DATA PRIMARY INDEX (DAY_DT,BANNER,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP) ON COMMIT PRESERVE ROWS;

DELETE 
FROM    {apd_t2_schema}.rakuten_publisher_daily
where day_dt between (select start_date from _variables2) and (select end_date from _variables2);


INSERT INTO {apd_t2_schema}.rakuten_publisher_daily
select a.*,CURRENT_TIMESTAMP as dw_sys_load_tmstp from apd_4_box_rakuten a;


COLLECT STATISTICS COLUMN (DAY_DT,BANNER,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP)                   
on {apd_t2_schema}.rakuten_publisher_daily;


SET QUERY_BAND = NONE FOR SESSION;	
