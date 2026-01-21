SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=optimine_processed_data_box_level_vw_11521_ACE_ENG;
     Task_Name=optimine_processed_data_box_level_vw;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.optimine_processed_data_box_level_vw
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-11-28
--Note:
-- This view supports the MBR Dashboard.


REPLACE VIEW {kpi_scorecard_t2_schema}.optimine_processed_data_box_level_vw
AS
LOCK ROW FOR ACCESS 
select 
COALESCE(a.TimePeriod,b.TimePeriod) as TimePeriod,
COALESCE (a.Influencing_Level1,b.Level1) as Analysis_Category_Name,
COALESCE (a.Influencing_Level2,b.Level2) as NMNFlag,
COALESCE (a.Influencing_Level3,b.Level3) as PlatformType,
COALESCE (a.Influencing_Level4,b.Level4) as AcqRetType,
COALESCE (a.Influencing_Level5,b.Level5) as AdFormat,
COALESCE (a.Influencing_Level6,b.Level6) as LoyaltyStatus,
COALESCE (a.Influencing_Level7,b.Level7) as Category,
COALESCE (a.Influencing_Level8,b.Level8) as Banner,
COALESCE (a.flagship_flag,b.flagship_flag) as flagship_flag,
COALESCE (a.Box,b.Box) as Box,
sum(COALESCE (Net_Revenue,0)) as Net_Revenue,
sum(COALESCE (Gross_Demand,0)) as Gross_Demand,
sum(COALESCE (Units,0)) as Units,
sum(COALESCE (Trips,0)) as Trips,
sum(COALESCE (Existing_Customer,0)) as Existing_Customer,
sum(COALESCE (New_Customer,0)) as New_Customer,
sum(COALESCE (Traffic,0)) as Traffic,
sum(COALESCE (ReportedCost,0)) as ReportedCost,
sum(COALESCE(BASELINE_NET_REVENUE,0)) as BASELINE_NET_REVENUE,
sum(COALESCE(BASELINE_GROSS_DEMAND,0)) as BASELINE_GROSS_DEMAND,
sum(COALESCE(BASELINE_Units,0)) as BASELINE_Units,
sum(COALESCE(BASELINE_Trips,0)) as BASELINE_Trips,
sum(COALESCE(BASELINE_Traffic,0)) as BASELINE_Traffic,
sum(COALESCE(BASELINE_New_Customer,0)) as BASELINE_New_Customer,
sum(COALESCE(BASELINE_Existing_Customer,0)) as BASELINE_Existing_Customer
from (
SELECT 
TimePeriod,
Influencing_Level0,
Influencing_Level1,
Influencing_Level2,
Influencing_Level3,
Influencing_Level4,
Influencing_Level5,
Influencing_Level6,
Influencing_Level7,
Influencing_Level8,
case when Converting_Level2 like '210%' or Converting_Level2 like '212%' or Converting_Level2 like '220%' or Converting_Level2 like '360%' or Converting_Level2 like '380%'
 or Converting_Level2 like '723%' or Converting_Level2 like '1%' or Converting_Level2 like '4%' then 'Y' else 'N' end as flagship_flag,
case when Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Instore' 
OR Converting_Level7 = 'BANNER: Nordstrom' and Converting_Level8 = 'ST: Instore' then 'FLS'
WHEN Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Online' 
OR Converting_Level7 = 'BANNER: Nordstrom' and Converting_Level8 = 'ST: Online'then 'NCOM'
when Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Instore' 
OR Converting_Level7 = 'BANNER: Rack' and Converting_Level8 = 'ST: Instore'then 'RACK'
WHEN Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Online' 
OR Converting_Level7 = 'BANNER: Rack' and Converting_Level8 = 'ST: Online'then 'RCOM'
END AS Box,
sum(case when Influencing_Level0 like '%SalesTrips%'  then XChannelValue1 end ) as Net_Revenue ,
sum(case when Influencing_Level0 like '%SalesTrips%'  then XChannelValue2 end ) as Gross_Demand ,
sum(case when Influencing_Level0 like '%SalesTrips%'  then XChannelCount1 end ) as Units ,
sum(case when Influencing_Level0 like '%SalesTrips%'  then XChannelCount2 end ) as Trips ,
sum(case when Influencing_Level0 like '%Customer%'  then XChannelCount2 end ) as Existing_Customer,
sum(case when Influencing_Level0 like '%Customer%'  then XChannelCount3 end ) as New_Customer,
sum(case when Influencing_Level0 like '%Traffic%' then XChannelCount1 end ) as Traffic
from {kpi_scorecard_t2_schema}.optimine_channel 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) a
full join 
(select b.week_start_day_date as TimePeriod, 
Level0,
Level1,
Level2,
Level3,
Level4,
Level5,
Level6,
Level7,
Level8,
case when Level2 like '210%' or Level2 like '212%' or Level2 like '220%' or Level2 like '360%' or Level2 like '380%'
 or Level2 like '723%' or Level2 like '1%' or Level2 like '4%' then 'Y' else 'N' end as flagship_flag,
case when Level8 = 'BANNER: Nordstrom' and Level9 = 'ST: Instore' 
OR Level7 = 'BANNER: Nordstrom' and Level8 = 'ST: Instore' then 'FLS'
WHEN Level8 = 'BANNER: Nordstrom' and Level9 = 'ST: Online' 
OR Level7 = 'BANNER: Nordstrom' and Level8 = 'ST: Online'then 'NCOM'
when Level8 = 'BANNER: Rack' and Level9 = 'ST: Instore' 
OR Level7 = 'BANNER: Rack' and Level8 = 'ST: Instore'then 'RACK'
WHEN Level8 = 'BANNER: Rack' and Level9 = 'ST: Online' 
OR Level7 = 'BANNER: Rack' and Level8 = 'ST: Online'then 'RCOM'
END AS Box,
sum(case when Level0 LIKE ('Nordstrom_SalesTrips%') OR Level0 LIKE ('Rack_SalesTrips%') then ReportedCost else 0 end ) as ReportedCost,
sum(case when Level1 LIKE '%Nordstrom - Sales (In-Store)%'or Level1 LIKE '%Nordstrom - Sales (Online)%' or Level1 LIKE '%Rack - Sales (In-Store)%'
or Level1 LIKE '%Rack - Sales (Online)%' then ModeledValue1 else 0 end ) as BASELINE_NET_REVENUE,
sum(case when Level1 LIKE '%Nordstrom - Sales (In-Store)%' or Level1 LIKE '%Nordstrom - Sales (Online)%' or Level1 LIKE '%Rack - Sales (In-Store)%'
or Level1 LIKE '%Rack - Sales (Online)%' then ModeledValue2 else 0 end ) as BASELINE_GROSS_DEMAND,
sum(case when Level1 LIKE '%Nordstrom - Sales (In-Store)%' or Level1 LIKE '%Nordstrom - Sales (Online)%' or Level1 LIKE '%Rack - Sales (In-Store)%'
or Level1 LIKE '%Rack - Sales (Online)%' then ModeledCount1 else 0 end ) as BASELINE_Units,
sum(case when Level1 LIKE '%Nordstrom - Trips (In-Store)%' or Level1 LIKE '%Nordstrom - Trips (Online)%' or Level1 LIKE '%Rack - Trips (In-Store)%'
or Level1 LIKE '%Rack - Trips (Online)%' then ModeledCount2 else 0 end ) as BASELINE_Trips,
sum(case when Level1 LIKE '%Nordstrom - Traffic (In-Store)%' or Level1 LIKE '%Nordstrom - Traffic (Online)%' or Level1 LIKE '%Rack - Traffic (In-Store)%'
or Level1 LIKE '%Rack - Traffic (Online)%' then ModeledCount1 else 0 end ) as BASELINE_Traffic,
sum(case when Level1 LIKE '%Nordstrom - Customer (In-Store)%' or Level1 LIKE '%Nordstrom - Customer (Online)%' or Level1 LIKE '%Rack - Customer (In-Store)%'
or Level1 LIKE '%Rack - Customer (Online)%' then ModeledCount3 else 0 end ) as BASELINE_New_Customer,
sum(case when Level1 LIKE '%Nordstrom - Customer (In-Store)%' or Level1 LIKE '%Nordstrom - Customer (Online)%' or Level1 LIKE '%Rack - Customer (In-Store)%' 
or Level1 LIKE '%Rack - Customer (Online)%' then ModeledCount2 else 0 end ) as BASELINE_Existing_Customer
from  {kpi_scorecard_t2_schema}.optimine_summary a left join prd_nap_usr_vws.DAY_CAL_454_DIM b on a.TimePeriod = b.day_date group by 1,2,3,4,5,6,7,8,9,10,11,12) b
on a.TimePeriod = b.TimePeriod and
 a.Influencing_Level0 = b.Level0 and 
a.Influencing_Level1 = b.Level1 and 
a.Influencing_Level2 = b.Level2 AND 
a.Influencing_Level3 = b.Level3 AND 
a.Influencing_Level4 = b.Level4 and 
a.Influencing_Level5 = b.Level5 and 
a.Influencing_Level6 = b.Level6 AND 
a.Influencing_Level7 = b.Level7 AND 
a.Influencing_Level8 = b.Level8 and 
a.flagship_flag = b.flagship_flag and
a.Box=b.Box
group by 1,2,3,4,5,6,7,8,9,10,11;


SET QUERY_BAND = NONE FOR SESSION;

