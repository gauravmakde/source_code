SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=optimine_processed_data_final_11521_ACE_ENG;
     Task_Name=optimine_processed_data_final;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.optimine_processed_data_final
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-08-21
--Note:
-- This view supports the MMM Insights Dashboard.


REPLACE VIEW {kpi_scorecard_t2_schema}.optimine_processed_data_final
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
sum(COALESCE (FULL_LINE_Net_Revenue,0)) as FULL_LINE_Net_Revenue,
sum(COALESCE (NCOM_Net_Revenue,0)) as NCOM_Net_Revenue,
sum(COALESCE (RACK_Net_Revenue,0)) as RACK_Net_Revenue,
sum(COALESCE (RCOM_Net_Revenue,0))as RCOM_Net_Revenue ,
sum(COALESCE (FULL_LINE_Gross_Demand,0)) as FULL_LINE_Gross_Demand,
sum(COALESCE (NCOM_Gross_Demand,0)) as NCOM_Gross_Demand,
sum(COALESCE (RACK_Gross_Demand,0)) as RACK_Gross_Demand,
sum(COALESCE (RCOM_Gross_Demand,0)) as RCOM_Gross_Demand,
sum(COALESCE (FULL_LINE_Units,0)) as FULL_LINE_Units,
sum(COALESCE (NCOM_Units,0)) as NCOM_Units,
sum(COALESCE (RACK_Units,0)) as RACK_Units,
sum(COALESCE (RCOM_Units,0)) as RCOM_Units,
sum(COALESCE (FULL_LINE_Trips,0)) as FULL_LINE_Trips,
sum(COALESCE (NCOM_Trips,0)) as NCOM_Trips, 
sum(COALESCE (RACK_Trips,0)) as RACK_Trips,
sum(COALESCE (RCOM_Trips,0)) as RCOM_Trips,
sum(COALESCE (FULL_LINE_Existing_Customer,0)) as FULL_LINE_Existing_Customer,
sum(COALESCE (NCOM_Existing_Customer,0)) as NCOM_Existing_Customer,
sum(COALESCE (Rack_Existing_Customer,0)) as Rack_Existing_Customer,
sum(COALESCE (RCOM_Existing_Customer,0)) as RCOM_Existing_Customer,
sum(COALESCE (FULL_LINE_New_Customer,0)) as FULL_LINE_New_Customer,
sum(COALESCE (NCOM_New_Customer,0)) as NCOM_New_Customer,
sum(COALESCE (Rack_New_Customer,0)) as Rack_New_Customer,
sum(COALESCE (RCOM_New_Customer,0)) as RCOM_New_Customer,
sum(COALESCE (FULL_LINE_Traffic,0)) as FULL_LINE_Traffic,
sum(COALESCE (NCOM_Traffic,0)) as NCOM_Traffic,
sum(COALESCE (Rack_Traffic,0)) as Rack_Traffic,
sum(COALESCE (RCOM_Traffic,0)) as RCOM_Traffic,
sum(COALESCE (ReportedCost,0)) as ReportedCost,
sum(COALESCE(FULL_LINE_BASELINE_NET_REVENUE,0)) as FULL_LINE_BASELINE_NET_REVENUE,
sum(COALESCE(NCOM_BASELINE_NET_REVENUE,0)) as NCOM_BASELINE_NET_REVENUE,
sum(COALESCE(RACK_BASELINE_NET_REVENUE,0)) as RACK_BASELINE_NET_REVENUE,
sum(COALESCE(RCOM_BASELINE_NET_REVENUE,0)) as RCOM_BASELINE_NET_REVENUE,
sum(COALESCE(FULL_LINE_BASELINE_GROSS_DEMAND,0)) as FULL_LINE_BASELINE_GROSS_DEMAND,
sum(COALESCE(NCOM_BASELINE_GROSS_DEMAND,0)) as NCOM_BASELINE_GROSS_DEMAND,
sum(COALESCE(RACK_BASELINE_GROSS_DEMAND,0)) as RACK_BASELINE_GROSS_DEMAND,
sum(COALESCE(RCOM_BASELINE_GROSS_DEMAND,0)) as RCOM_BASELINE_GROSS_DEMAND,
sum(COALESCE(FULL_LINE_BASELINE_Units,0)) as FULL_LINE_BASELINE_Units,
sum(COALESCE(NCOM_BASELINE_Units,0)) as NCOM_BASELINE_Units,
sum(COALESCE(RACK_BASELINE_Units,0)) as RACK_BASELINE_Units,
sum(COALESCE(RCOM_BASELINE_Units,0)) as RCOM_BASELINE_Units,
sum(COALESCE(FULL_LINE_BASELINE_Trips,0)) as FULL_LINE_BASELINE_Trips,
sum(COALESCE(NCOM_BASELINE_Trips,0)) as NCOM_BASELINE_Trips,
sum(COALESCE(RACK_BASELINE_Trips,0)) as RACK_BASELINE_Trips,
sum(COALESCE(RCOM_BASELINE_Trips,0)) as RCOM_BASELINE_Trips,
sum(COALESCE(FULL_LINE_BASELINE_Traffic,0)) as FULL_LINE_BASELINE_Traffic,
sum(COALESCE(NCOM_BASELINE_Traffic,0)) as NCOM_BASELINE_Traffic,
sum(COALESCE(RACK_BASELINE_Traffic,0)) as RACK_BASELINE_Traffic,
sum(COALESCE(RCOM_BASELINE_Traffic,0)) as RCOM_BASELINE_Traffic,
sum(COALESCE(FULL_LINE_BASELINE_New_Customer,0)) as FULL_LINE_BASELINE_New_Customer,
sum(COALESCE(NCOM_BASELINE_New_Customer,0)) as NCOM_BASELINE_New_Customer,
sum(COALESCE(RACK_BASELINE_New_Customer,0)) as RACK_BASELINE_New_Customer,
sum(COALESCE(RCOM_BASELINE_New_Customer,0)) as RCOM_BASELINE_New_Customer,
sum(COALESCE(FULL_LINE_BASELINE_Existing_Customer,0)) as FULL_LINE_BASELINE_Existing_Customer,
sum(COALESCE(NCOM_BASELINE_Existing_Customer,0)) as NCOM_BASELINE_Existing_Customer,
sum(COALESCE(RACK_BASELINE_Existing_Customer,0)) as RACK_BASELINE_Existing_Customer,
sum(COALESCE(RCOM_BASELINE_Existing_Customer,0)) as RCOM_BASELINE_Existing_Customer
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
sum(case when Influencing_Level0 like 'Nordstrom_SalesTrips%' and Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Instore' then XChannelValue1 end ) as FULL_LINE_Net_Revenue ,
sum(case when Influencing_Level0 like 'Nordstrom_SalesTrips%' and Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Online' then XChannelValue1 end ) as NCOM_Net_Revenue ,
sum(case when Influencing_Level0 like 'Rack_SalesTrips%' and Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Instore' then XChannelValue1 end ) as RACK_Net_Revenue ,
sum(case when Influencing_Level0 like 'Rack_SalesTrips%' and Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Online' then XChannelValue1 end ) as RCOM_Net_Revenue ,
sum(case when Influencing_Level0 like 'Nordstrom_SalesTrips%' and Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Instore' then XChannelValue2 end ) as FULL_LINE_Gross_Demand ,
sum(case when Influencing_Level0 like 'Nordstrom_SalesTrips%' and Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Online' then XChannelValue2 end ) as NCOM_Gross_Demand ,
sum(case when Influencing_Level0 like 'Rack_SalesTrips%' and Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Instore' then XChannelValue2 end ) as RACK_Gross_Demand ,
sum(case when Influencing_Level0 like 'Rack_SalesTrips%' and Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Online' then XChannelValue2 end ) as RCOM_Gross_Demand ,
sum(case when Influencing_Level0 like 'Nordstrom_SalesTrips%' and Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Instore' then XChannelCount1 end ) as FULL_LINE_Units ,
sum(case when Influencing_Level0 like 'Nordstrom_SalesTrips%' and Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Online' then XChannelCount1 end ) as NCOM_Units ,
sum(case when Influencing_Level0 like 'Rack_SalesTrips%' and Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Instore' then XChannelCount1 end ) as RACK_Units ,
sum(case when Influencing_Level0 like 'Rack_SalesTrips%' and Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Online' then XChannelCount1 end ) as RCOM_Units ,
sum(case when Influencing_Level0 like 'Nordstrom_SalesTrips%' and Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Instore' then XChannelCount2 end ) as FULL_LINE_Trips ,
sum(case when Influencing_Level0 like 'Nordstrom_SalesTrips%' and Converting_Level8 = 'BANNER: Nordstrom' and Converting_Level9 = 'ST: Online' then XChannelCount2 end ) as NCOM_Trips ,
sum(case when Influencing_Level0 like 'Rack_SalesTrips%' and Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Instore' then XChannelCount2 end ) as RACK_Trips ,
sum(case when Influencing_Level0 like 'Rack_SalesTrips%' and Converting_Level8 = 'BANNER: Rack' and Converting_Level9 = 'ST: Online' then XChannelCount2 end ) as RCOM_Trips ,
sum(case when Influencing_Level0 like 'Nordstrom_Customer%' and Converting_Level7 = 'BANNER: Nordstrom' and Converting_Level8 = 'ST: Instore' then XChannelCount2 end ) as FULL_LINE_Existing_Customer,
sum(case when Influencing_Level0 like 'Nordstrom_Customer%' and Converting_Level7 = 'BANNER: Nordstrom' and Converting_Level8 = 'ST: Online' then XChannelCount2 end ) as NCOM_Existing_Customer,
sum(case when Influencing_Level0 like 'Rack_Customer%' and Converting_Level7 = 'BANNER: Rack' and Converting_Level8 = 'ST: Instore' then XChannelCount2 end ) as Rack_Existing_Customer,
sum(case when Influencing_Level0 like 'Rack_Customer%' and Converting_Level7 = 'BANNER: Rack' and Converting_Level8 = 'ST: Online' then XChannelCount2 end ) as RCOM_Existing_Customer,
sum(case when Influencing_Level0 like 'Nordstrom_Customer%' and Converting_Level7 = 'BANNER: Nordstrom' and Converting_Level8 = 'ST: Instore' then XChannelCount3 end ) as FULL_LINE_New_Customer,
sum(case when Influencing_Level0 like 'Nordstrom_Customer%' and Converting_Level7 = 'BANNER: Nordstrom' and Converting_Level8 = 'ST: Online' then XChannelCount3 end ) as NCOM_New_Customer,
sum(case when Influencing_Level0 like 'Rack_Customer%' and Converting_Level7 = 'BANNER: Rack' and Converting_Level8 = 'ST: Instore' then XChannelCount3 end ) as Rack_New_Customer,
sum(case when Influencing_Level0 like 'Rack_Customer%' and Converting_Level7 = 'BANNER: Rack' and Converting_Level8 = 'ST: Online' then XChannelCount3 end ) as RCOM_New_Customer,
sum(case when Influencing_Level0 like 'Nordstrom_Traffic%' and Converting_Level7 = 'BANNER: Nordstrom' and Converting_Level8 = 'ST: Instore' then XChannelCount1 end ) as FULL_LINE_Traffic,
sum(case when Influencing_Level0 like 'Nordstrom_Traffic%' and Converting_Level7 = 'BANNER: Nordstrom' and Converting_Level8 = 'ST: Online' then XChannelCount1 end ) as NCOM_Traffic,
sum(case when Influencing_Level0 like 'Rack_Traffic%' and Converting_Level7 = 'BANNER: Rack' and Converting_Level8 = 'ST: Instore' then XChannelCount1 end ) as Rack_Traffic,
sum(case when Influencing_Level0 like 'Rack_Traffic%' and Converting_Level7 = 'BANNER: Rack' and Converting_Level8 = 'ST: Online' then XChannelCount1 end ) as RCOM_Traffic
from {kpi_scorecard_t2_schema}.optimine_channel 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11) a
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
sum(case when Level0 LIKE ('Nordstrom_SalesTrips%') OR Level0 LIKE ('Rack_SalesTrips%') then ReportedCost else 0 end ) as ReportedCost,
sum(case when Level1 like ('%Nordstrom - Sales (In-Store)%') then ModeledValue1 else 0 end ) as FULL_LINE_BASELINE_NET_REVENUE,
sum(case when Level1 like ('%Nordstrom - Sales (Online)%') then ModeledValue1 else 0 end ) as NCOM_BASELINE_NET_REVENUE,
sum(case when Level1 like ('%Rack - Sales (In-Store)%') then ModeledValue1 else 0 end ) as RACK_BASELINE_NET_REVENUE,
sum(case when Level1 like ('%Rack - Sales (Online)%') then ModeledValue1 else 0 end ) as RCOM_BASELINE_NET_REVENUE,
sum(case when Level1 like ('%Nordstrom - Sales (In-Store)%') then ModeledValue2 else 0 end ) as FULL_LINE_BASELINE_GROSS_DEMAND,
sum(case when Level1 like ('%Nordstrom - Sales (Online)%') then ModeledValue2 else 0 end ) as NCOM_BASELINE_GROSS_DEMAND,
sum(case when Level1 like ('%Rack - Sales (In-Store)%') then ModeledValue2 else 0 end ) as RACK_BASELINE_GROSS_DEMAND,
sum(case when Level1 like ('%Rack - Sales (Online)%') then ModeledValue2 else 0 end ) as RCOM_BASELINE_GROSS_DEMAND,
sum(case when Level1 like ('%Nordstrom - Sales (In-Store)%') then ModeledCount1 else 0 end ) as FULL_LINE_BASELINE_Units,
sum(case when Level1 like ('%Nordstrom - Sales (Online)%') then ModeledCount1 else 0 end ) as NCOM_BASELINE_Units,
sum(case when Level1 like ('%Rack - Sales (In-Store)%') then ModeledCount1 else 0 end ) as RACK_BASELINE_Units,
sum(case when Level1 like ('%Rack - Sales (Online)%') then ModeledCount1 else 0 end ) as RCOM_BASELINE_Units,
sum(case when Level1 like ('%Nordstrom - Trips (In-Store)%') then ModeledCount2 else 0 end ) as FULL_LINE_BASELINE_Trips,
sum(case when Level1 like ('%Nordstrom - Trips (Online)%') then ModeledCount2 else 0 end ) as NCOM_BASELINE_Trips,
sum(case when Level1 like ('%Rack - Trips (In-Store)%') then ModeledCount2 else 0 end ) as RACK_BASELINE_Trips,
sum(case when Level1 like ('%Rack - Trips (Online)%') then ModeledCount2 else 0 end ) as RCOM_BASELINE_Trips,
sum(case when Level1 like ('%Nordstrom - Traffic (In-Store)%') then ModeledCount1 else 0 end ) as FULL_LINE_BASELINE_Traffic,
sum(case when Level1 like ('%Nordstrom - Traffic (Online)%') then ModeledCount1 else 0 end ) as NCOM_BASELINE_Traffic,
sum(case when Level1 like ('%Rack - Traffic (In-Store)%') then ModeledCount1 else 0 end ) as RACK_BASELINE_Traffic,
sum(case when Level1 like ('%Rack - Traffic (Online)%') then ModeledCount1 else 0 end ) as RCOM_BASELINE_Traffic,
sum(case when Level1 like ('%Nordstrom - Customer (In-Store)%') then ModeledCount3 else 0 end ) as FULL_LINE_BASELINE_New_Customer,
sum(case when Level1 like('%Nordstrom - Customer (Online)%') then ModeledCount3 else 0 end ) as NCOM_BASELINE_New_Customer,
sum(case when Level1 like ('%Rack - Customer (In-Store)%') then ModeledCount3 else 0 end ) as RACK_BASELINE_New_Customer,
sum(case when Level1 like ('%Rack - Customer (Online)%') then ModeledCount3 else 0 end ) as RCOM_BASELINE_New_Customer,
sum(case when Level1 like ('%Nordstrom - Customer (In-Store)%') then ModeledCount2 else 0 end ) as FULL_LINE_BASELINE_Existing_Customer,
sum(case when Level1 like ('%Nordstrom - Customer (Online)%') then ModeledCount2 else 0 end ) as NCOM_BASELINE_Existing_Customer,
sum(case when Level1 like ('%Rack - Customer (In-Store)%') then ModeledCount2 else 0 end ) as RACK_BASELINE_Existing_Customer,
sum(case when Level1 like ('%Rack - Customer (Online)%') then ModeledCount2 else 0 end ) as RCOM_BASELINE_Existing_Customer
from  {kpi_scorecard_t2_schema}.optimine_summary a left join prd_nap_usr_vws.DAY_CAL_454_DIM b on a.TimePeriod = b.day_date group by 1,2,3,4,5,6,7,8,9,10,11) b
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
a.flagship_flag = b.flagship_flag
group by 1,2,3,4,5,6,7,8,9,10;


SET QUERY_BAND = NONE FOR SESSION;

