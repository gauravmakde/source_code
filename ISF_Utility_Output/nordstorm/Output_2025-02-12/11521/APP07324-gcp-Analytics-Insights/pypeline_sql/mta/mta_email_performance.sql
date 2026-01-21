SET QUERY_BAND = 'App_ID=APP08300;
     DAG_ID=mta_email_performance_11521_ACE_ENG;
     Task_Name=mta_email_performance;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_mta.mta_email_performance
Team/Owner: MTA and MOA
Date Created/Modified: 6/2023

Note:
-- This table joins SalesForce email engagement data and MTA transactional data for the Email Performance Dashboard
-- The data rruns daily for all data existing since May 2022
*/


CREATE Multiset VOLATILE TABLE bottom_funnel
AS
(
WITH base AS
(
SELECT activity_date_pacific,
	   case when LENGTH(STRTOK (cast(utm_content as char(20)),'_',1)) = 8 
	   	and STRTOK (cast(utm_content as char(20)),'_',1) like '202%' then
	   		cast(STRTOK (cast(utm_content as char(20)),'_',1) as date format 'YYYYMMDD')
	   else null end
	   as deploy_date,
	   CASE WHEN utm_source in ('NR_planned','NR_Transactional','NR_triggered','NR_triggered_bc') 
	   				THEN 'Nordstrom Rack'
	   		WHEN utm_source  in ('N_transactional','N_triggered','N_triggered_bc','N_planned') 
					THEN 'Nordstrom'
	   	END as Brand,
       CASE
         WHEN (utm_source in ('N_planned', 'NR_planned') or (utm_source in ('N_transactional', 'NR_transactional') and utm_term like '%_%'))
		 					AND STRTOK (cast(utm_term as char(20)),'_',1) IS NOT NULL 
		 					AND STRTOK (cast(utm_term as char(20)),'_',2) IS NOT NULL 
							THEN coalesce(STRTOK (cast(utm_term as char(20)),'_',1),0)
         ELSE utm_campaign
        END AS campaign_id_version,
       CASE
         WHEN cast(utm_term as char(20)) IS NOT NULL 
         	AND STRTOK (cast(utm_term as char(20)),'_',2) IS NOT NULL 
         	AND STRTOK (cast(utm_term as char(20)),'_',2) <> '0' 
         	THEN coalesce(TRIM(STRTOK (cast(utm_term as char(20)),'_',2)),0)
         ELSE coalesce(TRIM(STRTOK (cast(utm_term as char(20)),'_',1)),0)
       END AS job_id,
	   utm_source,	   
	   channel as order_channel,
	   case when arrived_channel = 'R.COM' then 'Nordstrom Rack'
	   		when arrived_channel = 'N.COM' then 'Nordstrom' end as arrived_channel,
       SUM(gross) AS gross_demand,
       SUM(net_sales) AS net_demand,
       SUM(units) AS units,
       SUM(orders) AS orders,
       sum(sessions) as sessions,
       sum(session_orders) as session_orders,
	   sum(session_demand) as session_demand,
       sum(bounced_sessions) as bounced_sessions
FROM t2dl_das_mta.mkt_utm_agg 
WHERE finance_detail in ('EMAIL_MARKETING','EMAIL_TRANSACT','EMAIL_TRIGGER')
	AND   utm_source in ('NR_planned','NR_Transactional','NR_triggered','NR_triggered_bc',
							'N_transactional','N_triggered','N_triggered_bc','N_planned')
  AND channelcountry = 'US'
	AND   activity_date_pacific BETWEEN {start_date} AND {end_date}
	AND UPPER(job_id)=LOWER(job_id)(CASESPECIFIC) 
	AND LENGTH(job_id) <= 6
GROUP BY 1,2,3,4,5,6,7,8
)

select base.*
, (activity_date_pacific - deploy_date) as measurement_days
from base
where activity_date_pacific >= deploy_date
)
WITH data PRIMARY INDEX (activity_date_pacific) 
ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE top_funnel_1
AS
(
SELECT message_id as message_id,
		case when message_id in (655030, 655031) then 600827
        	 when message_id in (655152, 655153) then 600830
        	 when campaign_name = '07_750_DAM_Credit' then 'tdamsendcomm'
        	 when version>0 then version 
        	 else campaign_id end as campaign_id_version,
        brand,
        campaign_type,
        last_updated_date as deploy_date,
        CASE WHEN campaign_type like '%Planned' then (engagement_date - last_updated_date) 
        	 WHEN campaign_type like '%Triggered' and last_updated_date <= engagement_date THEN (engagement_date - last_updated_date) 
        	 end as measurement_days,
    	case when campaign_id='RACKwindowshop' then 'RACKwindowshop'
        	 when message_id in (655030, 655031) then 'CTR-reminder-womens'
        	 when message_id in (655152, 655153) then 'CTR-last-day-womens'
        	 else campaign_name end as campaign_name,
	   min(subject_line) as subject_line,
       SUM(total_sent_count) as total_sent_count,
       SUM(total_delivered_count) as total_delivered_count,
       SUM(total_clicked_count) as total_clicked_count,
       SUM(total_opened_count) as total_opened_count,
       SUM(total_bounced_count) as total_bounced_count,
       SUM(cpp_total_unsubscribed_count) + SUM(ced_total_unsubscribed_count) as total_unsubscribed_count,
       SUM(unique_clicked_count) as unique_clicked_count,
       SUM(unique_opened_count) as unique_opened_count,
       SUM(ced_unique_unsubscribed_count) + SUM(cpp_total_unsubscribed_count) as unique_unsubscribed_count
FROM PRD_NAP_USR_VWS.EMAIL_CONTACT_EVENT_HDR_FCT a
WHERE last_updated_date IS NOT NULL
	AND   campaign_type <> ''
  	AND   campaign_segment <> 'CA'
	AND   a.last_updated_date BETWEEN {start_date} AND {end_date}
group  by 1,2,3,4,5,6,7
) 
WITH data PRIMARY INDEX (campaign_id_version, deploy_date) 
ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE impacted_message_id
AS
(SELECT DISTINCT campaign_id_version,message_id
FROM (SELECT campaign_id_version,message_id,
             SUM(total_delivered_count) AS total_delivered_count,
             SUM(unique_opened_count) AS unique_opened_count,
			 SUM(unique_clicked_count) AS unique_clicked_count,
			 SUM(total_bounced_count) AS total_bounced_count
      FROM top_funnel_1
      GROUP BY 1,2) a
WHERE total_delivered_count < 0
OR    unique_opened_count > total_delivered_count
OR    unique_clicked_count > unique_opened_count
OR    unique_clicked_count > total_delivered_count
OR    total_bounced_count > total_delivered_count)
WITH data PRIMARY INDEX (campaign_id_version,message_id) 
ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE top_funnel
AS
(
select a.*,
case when b.campaign_id_version is not null and b.message_id is not null then 1 else 0 end as impacted_flag
from top_funnel_1 a
left join impacted_message_id b on a.campaign_id_version=b.campaign_id_version and a.message_id=b.message_id
) 
WITH data PRIMARY INDEX (campaign_id_version, deploy_date) 
ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE top_funnel_1_day
AS
(select 
  a.message_id as message_id,
  trim(a.campaign_id_version) as campaign_id_version,		
  a.brand as brand,
  trim(a.campaign_type) as campaign_type,
  a.deploy_date as deploy_date,
  a.campaign_name as campaign_name,
  max(a.impacted_flag) as impacted_flag,
  min(a.subject_line) as subject_line,
  sum(a.total_sent_count) as total_sent_count,
  sum(a.total_delivered_count) as total_delivered_count,
  sum(a.total_clicked_count) as total_clicked_count,
  sum(a.total_opened_count) as total_opened_count,
  sum(a.total_bounced_count) as total_bounced_count,
  sum(a.total_unsubscribed_count) as total_unsubscribed_count,
  sum(a.unique_clicked_count) as unique_clicked_count,
  sum(a.unique_opened_count) as unique_opened_count,
  sum(a.unique_unsubscribed_count) as unique_unsubscribed_count
from top_funnel a
where measurement_days BETWEEN 0 AND 1
group by 1,2,3,4,5,6
) 
WITH data PRIMARY INDEX (message_id,campaign_id_version, deploy_date) 
ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE top_funnel_10_day
AS
(select 
  a.message_id as message_id,
  trim(a.campaign_id_version) as campaign_id_version,		
  a.brand as brand,
  trim(a.campaign_type) as campaign_type,
  a.deploy_date as deploy_date,
  a.campaign_name as campaign_name,
  max(a.impacted_flag) as impacted_flag,
  min(a.subject_line) as subject_line,
  sum(a.total_sent_count) as total_sent_count,
  sum(a.total_delivered_count) as total_delivered_count,
  sum(a.total_clicked_count) as total_clicked_count,
  sum(a.total_opened_count) as total_opened_count,
  sum(a.total_bounced_count) as total_bounced_count,
  sum(a.total_unsubscribed_count) as total_unsubscribed_count,
  sum(a.unique_clicked_count) as unique_clicked_count,
  sum(a.unique_opened_count) as unique_opened_count,
  sum(a.unique_unsubscribed_count) as unique_unsubscribed_count
from top_funnel a
where measurement_days BETWEEN 0 AND 10
group by 1,2,3,4,5,6
) 
WITH data PRIMARY INDEX (message_id,campaign_id_version, deploy_date) 
ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE top_funnel_30_day
AS
(select 
  message_id as message_id,
  trim(campaign_id_version) as campaign_id_version,		
  brand as brand,
  trim(campaign_type) as campaign_type,
  deploy_date as deploy_date,
  campaign_name as campaign_name,
  max(impacted_flag) as impacted_flag,
  min(subject_line) as subject_line,
  sum(total_sent_count) as total_sent_count,
  sum(total_delivered_count) as total_delivered_count,
  sum(total_clicked_count) as total_clicked_count,
  sum(total_opened_count) as total_opened_count,
  sum(total_bounced_count) as total_bounced_count,
  sum(total_unsubscribed_count) as total_unsubscribed_count,
  sum(unique_clicked_count) as unique_clicked_count,
  sum(unique_opened_count) as unique_opened_count,
  sum(unique_unsubscribed_count) as unique_unsubscribed_count
from top_funnel
where measurement_days BETWEEN 0 AND 30
group by 1,2,3,4,5,6
) 
WITH data PRIMARY INDEX (message_id,campaign_id_version, deploy_date) 
ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE complete_funnel_1_day
AS
(SELECT trim(a.campaign_id_version) as campaign_id_version,		
		a.message_id as job_id,
		a.deploy_date as deploy_date,
		a.brand as box_type,
		case when a.campaign_type in ('Marketing Planned','Marketing Triggered') then 'Marketing' else 'Transactional' end as program_type,
		trim(a.campaign_type) as email_type,
		a.campaign_name as campaign_name,
		arrived_channel,
		case 
when a.brand = 'Nordstrom' and a.campaign_type = 'Transactional Triggered' then 'N Transactional Triggers'
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Triggered' and utm_source = 'N_triggered_bc' then 'N Bluecore Triggers'
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Triggered' then 'N Marketing Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Transactional Triggered'  then 'NR Transactional Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Triggered' and b.utm_source = 'NR_triggered_bc' then 'NR Bluecore Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Triggered' and (b.utm_source <> 'NR_triggered_bc' or b.utm_source is NULL) then 'NR Marketing Triggers'
-- corrected campaigns
when a.campaign_type = 'Marketing Planned' and c.campaign_category_corrected is not NULL then c.campaign_category_corrected
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Planned' and d.campaign_category_new is not NULL then d.campaign_category_new
-- both N and NR campaigns (including AEC/cohorts)
when a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Sale%' then 'Sale'
when a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Loyalty%' then 'Loyalty'
when a.campaign_name LIKE '%lcactive%' then 'LCActive'
when a.campaign_name LIKE '%lclapsed%' then 'LCLapsed'
when a.campaign_name LIKE '%welcome%' then 'Welcome'
when a.campaign_name LIKE '%lcreengagement%' then 'LCReengagement'
when a.campaign_name LIKE '%squad%' then 'Squad'
-- Nordstrom campaigns
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Pop In%' then 'Pop-In'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack Loyalty%' then 'Rack Loyalty'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Holiday%' then 'Holiday'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack Credit%' then 'Rack Credit'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens YA Apparel%' then 'Womens YA Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Contemporary Apparel%' then 'Womens Contemporary Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Active Shoes%' then 'Womens Active Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Contemporary Shoes%' then 'Womens Contemporary Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Active Shoes%' then 'Mens Active Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Core Apparel%' then 'Womens Core Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Core Shoes%' then 'Womens Core Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Active Apparel%' then 'Womens Active Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Designer%' then 'Womens Designer'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Accessories%' then 'Womens Accessories'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Specialized%'  then 'Womens Specialized' -- or ( a.campaign_name like '%Womens%' and (a.campaign_name like '%Lingerie%' or a.campaign_name like '%Swim%' or a.campaign_name like '%Sleepwear%'))
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Core Apparel%' then 'Mens Core Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Core Shoes%' then 'Mens Core Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Active Apparel%' then 'Mens Active Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Designer%' then 'Mens Designer'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Accessories%' then 'Mens Accessories'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Specialized%' then 'Mens Specialized' -- or ( a.campaign_name like '%Mens%' and (a.campaign_name like '%Socks%' or a.campaign_name like '%Swim%' or a.campaign_name like '%Underwear%')) 
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%XDIV%' then 'XDIV'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Multi DIV%' then 'XDIV'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Home%' then 'Home'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Kids%' then 'Kids'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Beauty%' then 'Beauty'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack%' then 'Rack'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Cyber%' then 'Cyber'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Hallmark Holidays%' then 'Hallmark Holidays'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%CRM%' then 'CRM'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%CRI%' then 'CRI'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%NPG%' then 'NPG'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%New Concepts%' then 'New Concepts'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Promo%' then 'Promo'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%Services' or a.campaign_name like '%Styling' or a.campaign_name like '%NMS' or a.campaign_name like '%Events') then 'Services'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary EA%' then 'Anniversary EA'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary Preview%' then 'Anniversary Preview'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary Public%' then 'Anniversary Public'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%Anni Awareness%' or a.campaign_name like '%Anniversary Awareness%') then 'Anniversary Awareness'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%DIB/CSR%' or a.campaign_name like '%DBI%' or a.campaign_name like '%DBI/CSR%')then 'DIB/CSR'
when a.brand = 'Nordstrom' and  a.campaign_name like '%Credit%' then 'Credit'
when a.brand = 'Nordstrom' and  a.campaign_name like '%GC%' then 'Gift Card'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' then 'N Marketing Planned - Unknown'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' and (a.campaign_name like '%Corporate%' or a.campaign_name like '%Service%')  then 'Corporate'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' and a.campaign_name like '%Loyalty%' then 'Loyalty-Transactional Planned'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' then 'N Transactional Planned'
-- rack campaigns
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%CTR%' then 'CTR'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and (a.campaign_name LIKE '%merch-store%' or a.campaign_name LIKE '%store-driving%' or a.campaign_name LIKE '%store%') then 'Store'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%blowout%' then 'Blowouts'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Survey%' then 'Survey'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Dedicated%' then 'Dedicated'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Markdown%' then 'Markdowns'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%new-arrivals%' then 'New Arrivals'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Division-Affinity%' then 'Division Affinity'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch%' then 'Merch'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch-dedicated%' then 'Dedicated'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch-survey%' then 'Survey'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%flash%' then 'Flash'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE 'sundaypm%' then 'SundayPM'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE 'clearance-promo%' then 'Clearance Promo'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' then 'NR Marketing Planned - Unknown'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Transactional Planned'  then 'NR Transactional Planned'
else a.campaign_type
        end as campaign_category,
		1 as Measurement_days,
		impacted_flag,
		subject_line,
		total_sent_count,
		total_delivered_count,
		total_clicked_count,
		total_opened_count,
		total_bounced_count,
		total_unsubscribed_count,
		unique_clicked_count,
		unique_opened_count,
		unique_unsubscribed_count,
       COALESCE(sum(b.gross_demand),0) AS gross_Sales,
       COALESCE(sum(b.net_demand),0) AS net_sales,
       COALESCE(sum(b.orders),0) AS orders,
       COALESCE(sum(b.units),0) AS units,
       COALESCE(sum(b.sessions),0) AS sessions,
       COALESCE(sum(b.session_orders),0) AS session_orders,
	   COALESCE(sum(b.session_demand),0) AS session_demand,
       COALESCE(sum(b.bounced_sessions),0) AS bounced_sessions
FROM top_funnel_1_day a 
left join (select deploy_date,brand, utm_source,campaign_id_version,job_id,arrived_channel,measurement_days,
			sum(gross_demand) as gross_demand,sum(net_demand) as net_demand, sum(units) as units, sum(orders) as orders, 
			sum(sessions) as sessions, sum(session_orders) as session_orders, sum(session_demand) as session_demand,  sum(bounced_sessions) as bounced_sessions
			from bottom_funnel where measurement_days between 0 and 1 group by 1,2,3,4,5,6,7) b 
	on cast(a.campaign_id_version as char(70))=CAST(b.campaign_id_version as char(70)) 
		and a.message_id =b.job_id and a.deploy_date = b.deploy_date
left join {mta_t2_schema}.mta_email_campaign_category_correction c 
	on a.campaign_name = c.campaign_name
left join {mta_t2_schema}.mta_email_campaign_id_correction d
    on a.campaign_id_version = d.campaign_id_version
WHERE (CURRENT_DATE-a.deploy_date)>=1 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21) 
WITH data PRIMARY INDEX (campaign_id_version, deploy_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE complete_funnel_10_day
AS
(SELECT trim(a.campaign_id_version) as campaign_id_version,			
		a.message_id as job_id,
		a.deploy_date as deploy_date,
		a.brand as box_type,
		case when a.campaign_type in ('Marketing Planned','Marketing Triggered') then 'Marketing' else 'Transactional' end as program_type,
		trim(a.campaign_type) as email_type,
		a.campaign_name as campaign_name,
		arrived_channel,
		case 
when a.brand = 'Nordstrom' and a.campaign_type = 'Transactional Triggered' then 'N Transactional Triggers'
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Triggered' and utm_source = 'N_triggered_bc' then 'N Bluecore Triggers'
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Triggered' then 'N Marketing Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Transactional Triggered'  then 'NR Transactional Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Triggered' and b.utm_source = 'NR_triggered_bc' then 'NR Bluecore Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Triggered' and (b.utm_source <> 'NR_triggered_bc' or b.utm_source is NULL) then 'NR Marketing Triggers'
-- corrected campaigns
when a.campaign_type = 'Marketing Planned' and c.campaign_category_corrected is not NULL then c.campaign_category_corrected
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Planned' and d.campaign_category_new is not NULL then d.campaign_category_new
-- both N and NR campaigns (including AEC/cohorts)
when a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Sale%' then 'Sale'
when a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Loyalty%' then 'Loyalty'
when a.campaign_name LIKE '%lcactive%' then 'LCActive'
when a.campaign_name LIKE '%lclapsed%' then 'LCLapsed'
when a.campaign_name LIKE '%welcome%' then 'Welcome'
when a.campaign_name LIKE '%lcreengagement%' then 'LCReengagement'
when a.campaign_name LIKE '%squad%' then 'Squad'
-- Nordstrom campaigns
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Pop In%' then 'Pop-In'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack Loyalty%' then 'Rack Loyalty'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Holiday%' then 'Holiday'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack Credit%' then 'Rack Credit'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens YA Apparel%' then 'Womens YA Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Contemporary Apparel%' then 'Womens Contemporary Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Active Shoes%' then 'Womens Active Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Contemporary Shoes%' then 'Womens Contemporary Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Active Shoes%' then 'Mens Active Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Core Apparel%' then 'Womens Core Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Core Shoes%' then 'Womens Core Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Active Apparel%' then 'Womens Active Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Designer%' then 'Womens Designer'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Accessories%' then 'Womens Accessories'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Specialized%'  then 'Womens Specialized' -- or ( a.campaign_name like '%Womens%' and (a.campaign_name like '%Lingerie%' or a.campaign_name like '%Swim%' or a.campaign_name like '%Sleepwear%'))
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Core Apparel%' then 'Mens Core Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Core Shoes%' then 'Mens Core Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Active Apparel%' then 'Mens Active Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Designer%' then 'Mens Designer'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Accessories%' then 'Mens Accessories'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Specialized%' then 'Mens Specialized' -- or ( a.campaign_name like '%Mens%' and (a.campaign_name like '%Socks%' or a.campaign_name like '%Swim%' or a.campaign_name like '%Underwear%')) 
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%XDIV%' then 'XDIV'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Multi DIV%' then 'XDIV'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Home%' then 'Home'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Kids%' then 'Kids'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Beauty%' then 'Beauty'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack%' then 'Rack'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Cyber%' then 'Cyber'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Hallmark Holidays%' then 'Hallmark Holidays'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%CRM%' then 'CRM'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%CRI%' then 'CRI'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%NPG%' then 'NPG'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%New Concepts%' then 'New Concepts'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Promo%' then 'Promo'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%Services' or a.campaign_name like '%Styling' or a.campaign_name like '%NMS' or a.campaign_name like '%Events') then 'Services'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary EA%' then 'Anniversary EA'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary Preview%' then 'Anniversary Preview'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary Public%' then 'Anniversary Public'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%Anni Awareness%' or a.campaign_name like '%Anniversary Awareness%') then 'Anniversary Awareness'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%DIB/CSR%' or a.campaign_name like '%DBI%' or a.campaign_name like '%DBI/CSR%')then 'DIB/CSR'
when a.brand = 'Nordstrom' and  a.campaign_name like '%Credit%' then 'Credit'
when a.brand = 'Nordstrom' and  a.campaign_name like '%GC%' then 'Gift Card'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' then 'N Marketing Planned - Unknown'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' and (a.campaign_name like '%Corporate%' or a.campaign_name like '%Service%')  then 'Corporate'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' and a.campaign_name like '%Loyalty%' then 'Loyalty-Transactional Planned'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' then 'N Transactional Planned'
-- rack campaigns
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%CTR%' then 'CTR'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and (a.campaign_name LIKE '%merch-store%' or a.campaign_name LIKE '%store-driving%' or a.campaign_name LIKE '%store%') then 'Store'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%blowout%' then 'Blowouts'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Survey%' then 'Survey'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Dedicated%' then 'Dedicated'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Markdown%' then 'Markdowns'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%new-arrivals%' then 'New Arrivals'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Division-Affinity%' then 'Division Affinity'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch%' then 'Merch'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch-dedicated%' then 'Dedicated'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch-survey%' then 'Survey'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%flash%' then 'Flash'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE 'sundaypm%' then 'SundayPM'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE 'clearance-promo%' then 'Clearance Promo'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' then 'NR Marketing Planned - Unknown'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Transactional Planned'  then 'NR Transactional Planned'
else a.campaign_type
        end as campaign_category,
		10 as Measurement_days,
		impacted_flag,
		subject_line,
		total_sent_count,
		total_delivered_count,
		total_clicked_count,
		total_opened_count,
		total_bounced_count,
		total_unsubscribed_count,
		unique_clicked_count,
		unique_opened_count,
		unique_unsubscribed_count,
       COALESCE(sum(b.gross_demand),0) AS gross_Sales,
       COALESCE(sum(b.net_demand),0) AS net_sales,
       COALESCE(sum(b.orders),0) AS orders,
       COALESCE(sum(b.units),0) AS units,
       COALESCE(sum(b.sessions),0) AS sessions,
       COALESCE(sum(b.session_orders),0) AS session_orders,
	   COALESCE(sum(b.session_demand),0) AS session_demand,
       COALESCE(sum(b.bounced_sessions),0) AS bounced_sessions
FROM top_funnel_10_day a 
left join (select deploy_date,campaign_id_version,job_id,utm_source,arrived_channel,measurement_days,
			sum(gross_demand) as gross_demand,sum(net_demand) as net_demand, sum(units) as units, sum(orders) as orders, 
			sum(sessions) as sessions, sum(session_orders) as session_orders, sum(session_demand) as session_demand, sum(bounced_sessions) as bounced_sessions
			from bottom_funnel where measurement_days between 0 and 10 group by 1,2,3,4,5,6) b 
	on cast(a.campaign_id_version as char(70))=CAST(b.campaign_id_version as char(70)) and a.message_id =b.job_id and a.deploy_date = b.deploy_date
left join {mta_t2_schema}.mta_email_campaign_category_correction c 
	on a.campaign_name = c.campaign_name
left join {mta_t2_schema}.mta_email_campaign_id_correction d
	on a.campaign_id_version = d.campaign_id_version
WHERE (CURRENT_DATE-a.deploy_date)>=10
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21) 
WITH data PRIMARY INDEX (campaign_id_version, deploy_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE complete_funnel_30_day
AS
(SELECT trim(a.campaign_id_version) as campaign_id_version,			
		a.message_id as job_id,
		a.deploy_date as deploy_date,
		a.brand as box_type,
		case when a.campaign_type in ('Marketing Planned','Marketing Triggered') then 'Marketing' else 'Transactional' end as program_type,
		trim(a.campaign_type) as email_type,
		a.campaign_name as campaign_name,
		arrived_channel,
		case 
when a.brand = 'Nordstrom' and a.campaign_type = 'Transactional Triggered' then 'N Transactional Triggers'
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Triggered' and utm_source = 'N_triggered_bc' then 'N Bluecore Triggers'
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Triggered' then 'N Marketing Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Transactional Triggered'  then 'NR Transactional Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Triggered' and b.utm_source = 'NR_triggered_bc' then 'NR Bluecore Triggers'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Triggered' and (b.utm_source <> 'NR_triggered_bc' or b.utm_source is NULL) then 'NR Marketing Triggers'
-- corrected campaigns
when a.campaign_type = 'Marketing Planned' and c.campaign_category_corrected is not NULL then c.campaign_category_corrected
when a.brand = 'Nordstrom' and a.campaign_type = 'Marketing Planned' and d.campaign_category_new is not NULL then d.campaign_category_new
-- both N and NR campaigns (including AEC/cohorts)
when a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Sale%' then 'Sale'
when a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Loyalty%' then 'Loyalty'
when a.campaign_name LIKE '%lcactive%' then 'LCActive'
when a.campaign_name LIKE '%lclapsed%' then 'LCLapsed'
when a.campaign_name LIKE '%welcome%' then 'Welcome'
when a.campaign_name LIKE '%lcreengagement%' then 'LCReengagement'
when a.campaign_name LIKE '%squad%' then 'Squad'
-- Nordstrom campaigns
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Pop In%' then 'Pop-In'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack Loyalty%' then 'Rack Loyalty'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Holiday%' then 'Holiday'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack Credit%' then 'Rack Credit'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens YA Apparel%' then 'Womens YA Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Contemporary Apparel%' then 'Womens Contemporary Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Active Shoes%' then 'Womens Active Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Contemporary Shoes%' then 'Womens Contemporary Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Active Shoes%' then 'Mens Active Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Core Apparel%' then 'Womens Core Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Core Shoes%' then 'Womens Core Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Active Apparel%' then 'Womens Active Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Designer%' then 'Womens Designer'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Accessories%' then 'Womens Accessories'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Womens Specialized%'  then 'Womens Specialized' -- or ( a.campaign_name like '%Womens%' and (a.campaign_name like '%Lingerie%' or a.campaign_name like '%Swim%' or a.campaign_name like '%Sleepwear%'))
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Core Apparel%' then 'Mens Core Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Core Shoes%' then 'Mens Core Shoes'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Active Apparel%' then 'Mens Active Apparel'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Designer%' then 'Mens Designer'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Accessories%' then 'Mens Accessories'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Mens Specialized%' then 'Mens Specialized' -- or ( a.campaign_name like '%Mens%' and (a.campaign_name like '%Socks%' or a.campaign_name like '%Swim%' or a.campaign_name like '%Underwear%')) 
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%XDIV%' then 'XDIV'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Multi DIV%' then 'XDIV'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Home%' then 'Home'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Kids%' then 'Kids'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Beauty%' then 'Beauty'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Rack%' then 'Rack'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Cyber%' then 'Cyber'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Hallmark Holidays%' then 'Hallmark Holidays'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%CRM%' then 'CRM'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%CRI%' then 'CRI'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%NPG%' then 'NPG'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%New Concepts%' then 'New Concepts'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like '%Promo%' then 'Promo'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%Services' or a.campaign_name like '%Styling' or a.campaign_name like '%NMS' or a.campaign_name like '%Events') then 'Services'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary EA%' then 'Anniversary EA'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary Preview%' then 'Anniversary Preview'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and a.campaign_name like 'Anniversary Public%' then 'Anniversary Public'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%Anni Awareness%' or a.campaign_name like '%Anniversary Awareness%') then 'Anniversary Awareness'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' and (a.campaign_name like '%DIB/CSR%' or a.campaign_name like '%DBI%' or a.campaign_name like '%DBI/CSR%')then 'DIB/CSR'
when a.brand = 'Nordstrom' and  a.campaign_name like '%Credit%' then 'Credit'
when a.brand = 'Nordstrom' and  a.campaign_name like '%GC%' then 'Gift Card'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Marketing Planned' then 'N Marketing Planned - Unknown'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' and (a.campaign_name like '%Corporate%' or a.campaign_name like '%Service%')  then 'Corporate'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' and a.campaign_name like '%Loyalty%' then 'Loyalty-Transactional Planned'
when a.brand = 'Nordstrom' and  a.campaign_type = 'Transactional Planned' then 'N Transactional Planned'
-- rack campaigns
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%CTR%' then 'CTR'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and (a.campaign_name LIKE '%merch-store%' or a.campaign_name LIKE '%store-driving%' or a.campaign_name LIKE '%store%') then 'Store'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%blowout%' then 'Blowouts'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Survey%' then 'Survey'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Dedicated%' then 'Dedicated'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Markdown%' then 'Markdowns'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%new-arrivals%' then 'New Arrivals'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%Division-Affinity%' then 'Division Affinity'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch%' then 'Merch'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch-dedicated%' then 'Dedicated'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%merch-survey%' then 'Survey'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE '%flash%' then 'Flash'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE 'sundaypm%' then 'SundayPM'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' and a.campaign_name LIKE 'clearance-promo%' then 'Clearance Promo'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Marketing Planned' then 'NR Marketing Planned - Unknown'
when a.brand = 'Nordstrom Rack' and a.campaign_type = 'Transactional Planned'  then 'NR Transactional Planned'
else a.campaign_type 
        end as campaign_category,
		30 as Measurement_days,
		impacted_flag,
		subject_line,
		total_sent_count,
		total_delivered_count,
		total_clicked_count,
		total_opened_count,
		total_bounced_count,
		total_unsubscribed_count,
		unique_clicked_count,
		unique_opened_count,
		unique_unsubscribed_count,
       COALESCE(sum(b.gross_demand),0) AS gross_Sales,
       COALESCE(sum(b.net_demand),0) AS net_sales,
       COALESCE(sum(b.orders),0) AS orders,
       COALESCE(sum(b.units),0) AS units,
       COALESCE(sum(b.sessions),0) AS sessions,
       COALESCE(sum(b.session_orders),0) AS session_orders,
	   COALESCE(sum(b.session_demand),0) AS session_demand,
       COALESCE(sum(b.bounced_sessions),0) AS bounced_sessions
FROM top_funnel_30_day a 
left join (select deploy_date,campaign_id_version,job_id,utm_source,arrived_channel,measurement_days,
			sum(gross_demand) as gross_demand,sum(net_demand) as net_demand, sum(units) as units, sum(orders) as orders, 
			sum(sessions) as sessions, sum(session_orders) as session_orders, sum(session_demand) as session_demand, sum(bounced_sessions) as bounced_sessions
			from bottom_funnel where measurement_days between 0 and 30 group by 1,2,3,4,5,6) b 
	on cast(a.campaign_id_version as char(70))=CAST(b.campaign_id_version as char(70)) and a.message_id =b.job_id and a.deploy_date = b.deploy_date
left join {mta_t2_schema}.mta_email_campaign_category_correction c 
	on a.campaign_name = c.campaign_name
left join {mta_t2_schema}.mta_email_campaign_id_correction d
	on a.campaign_id_version = d.campaign_id_version
WHERE (CURRENT_DATE-a.deploy_date)>=30
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21) 
WITH data PRIMARY INDEX (campaign_id_version, deploy_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE email_campaign_deep_dive
AS
(
select campaign_id_version,		
		job_id,
		deploy_date,
		box_type,
		program_type,
		email_type,
		campaign_name,
		arrived_channel,
		campaign_category,
		Measurement_days,
		impacted_flag,
		subject_line,
		total_sent_count,
		total_delivered_count,
		total_clicked_count,
		total_opened_count,
		total_bounced_count,
		total_unsubscribed_count,
		unique_clicked_count,
		unique_opened_count,
		unique_unsubscribed_count,
        gross_sales,
        net_sales,
        orders,
        units,
        sessions,
        session_orders,
		session_demand,
        bounced_sessions
from
(
select complete_funnel_1_day.*, 
case when total_sent_count<10 and (email_type='Marketing Planned' or campaign_name like '%test%') then 1 else 0 end as dropflag 
from complete_funnel_1_day
union all
select complete_funnel_10_day.*, 
case when total_sent_count<10 and (email_type='Marketing Planned' or campaign_name like '%test%') then 1 else 0 end as dropflag  
from complete_funnel_10_day
union all
select complete_funnel_30_day.*, 
case when total_sent_count<10 and (email_type='Marketing Planned' or campaign_name like '%test%') then 1 else 0 end as dropflag  
from complete_funnel_30_day
) a
where dropflag = 0
)
WITH data PRIMARY INDEX ( campaign_id_version, job_id, deploy_date, subject_line, measurement_days, gross_sales)
ON COMMIT PRESERVE ROWS;
--


/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM    {mta_t2_schema}.mta_email_performance
WHERE deploy_date between {start_date} and {end_date} 
;


INSERT INTO {mta_t2_schema}.mta_email_performance
SELECT  email_campaign_deep_dive.*                      
, CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM	email_campaign_deep_dive
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (campaign_id_version, job_id, deploy_date, subject_line, measurement_days), -- column names used for primary index
                    COLUMN (deploy_date)  -- column names used for partition
on {mta_t2_schema}.mta_email_performance;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
