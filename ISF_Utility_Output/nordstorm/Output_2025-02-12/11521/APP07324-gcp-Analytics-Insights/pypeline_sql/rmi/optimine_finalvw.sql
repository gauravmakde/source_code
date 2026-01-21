SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=optimine_finalvw_11521_ACE_ENG;
     Task_Name=optimine_finalvw;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.optimine_finalvw
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-08-21
--Note:
-- This view supports the MMM Insights Dashboard.


CREATE VIEW {kpi_scorecard_t2_schema}.optimine_finalvw
AS
LOCK ROW FOR ACCESS 
 SELECT
    day_dt
    , DAY_NUM
    , FISCAL_WEEK
    , Analysis_Category_Name
    , NMNFlag
    , PlatformType
    , AcqRetType
    , AdFormat
    , LoyaltyStatus
    , Category
    , Banner
    , FlagshipFlag
     -- TY
    , sum(TY_FULL_LINE_Net_Revenue) as TY_FULL_LINE_Net_Revenue
    , sum(TY_NCOM_Net_Revenue) as TY_NCOM_Net_Revenue
    , sum(TY_RACK_Net_Revenue) as TY_RACK_Net_Revenue
    , sum(TY_RCOM_Net_Revenue) as TY_RCOM_Net_Revenue
    , sum(TY_FULL_LINE_Gross_Demand) as TY_FULL_LINE_Gross_Demand
    , sum(TY_NCOM_Gross_Demand) as TY_NCOM_Gross_Demand
    , sum(TY_RACK_Gross_Demand) as TY_RACK_Gross_Demand
    , sum(TY_RCOM_Gross_Demand) as TY_RCOM_Gross_Demand
    , sum(TY_FULL_LINE_Units) as TY_FULL_LINE_Units
    , sum(TY_NCOM_Units) as TY_NCOM_Units
    , sum(TY_RACK_Units) as TY_RACK_Units
    , sum(TY_RCOM_Units) as TY_RCOM_Units
    , sum(TY_FULL_LINE_Trips) as TY_FULL_LINE_Trips
    , sum(TY_NCOM_Trips) as TY_NCOM_Trips
    , sum(TY_RACK_Trips) as TY_RACK_Trips
    , sum(TY_RCOM_Trips) as TY_RCOM_Trips
    , sum(TY_FULL_LINE_Existing_Customer) as TY_FULL_LINE_Existing_Customer
    , sum(TY_NCOM_Existing_Customer) as TY_NCOM_Existing_Customer
    , sum(TY_Rack_Existing_Customer) as TY_Rack_Existing_Customer
    , sum(TY_RCOM_Existing_Customer) as TY_RCOM_Existing_Customer
    , sum(TY_FULL_LINE_New_Customer) as TY_FULL_LINE_New_Customer
    , sum(TY_NCOM_New_Customer) as TY_NCOM_New_Customer
    , sum(TY_Rack_New_Customer) as TY_Rack_New_Customer
    , sum(TY_RCOM_New_Customer) as TY_RCOM_New_Customer
    , sum(TY_FULL_LINE_Traffic) as TY_FULL_LINE_Traffic
    , sum(TY_NCOM_Traffic) as TY_NCOM_Traffic
    , sum(TY_Rack_Traffic) as TY_Rack_Traffic
    , sum(TY_RCOM_Traffic) as TY_RCOM_Traffic
    , sum(TY_Spend) as TY_Spend
    , sum(TY_FULL_LINE_BASELINE_NET_REVENUE) as TY_FULL_LINE_BASELINE_NET_REVENUE
    , sum(TY_NCOM_BASELINE_NET_REVENUE) as TY_NCOM_BASELINE_NET_REVENUE
    , sum(TY_RACK_BASELINE_NET_REVENUE) as TY_RACK_BASELINE_NET_REVENUE
    , sum(TY_RCOM_BASELINE_NET_REVENUE) as TY_RCOM_BASELINE_NET_REVENUE
    , sum(TY_FULL_LINE_BASELINE_GROSS_DEMAND) as TY_FULL_LINE_BASELINE_GROSS_DEMAND
    , sum(TY_NCOM_BASELINE_GROSS_DEMAND) as TY_NCOM_BASELINE_GROSS_DEMAND
    , sum(TY_RACK_BASELINE_GROSS_DEMAND) as TY_RACK_BASELINE_GROSS_DEMAND
    , sum(TY_RCOM_BASELINE_GROSS_DEMAND) as TY_RCOM_BASELINE_GROSS_DEMAND
    , sum(TY_FULL_LINE_BASELINE_Units) as TY_FULL_LINE_BASELINE_Units
    , sum(TY_NCOM_BASELINE_Units) as TY_NCOM_BASELINE_Units
    , sum(TY_RACK_BASELINE_Units) as TY_RACK_BASELINE_Units
    , sum(TY_RCOM_BASELINE_Units) as TY_RCOM_BASELINE_Units
    , sum(TY_FULL_LINE_BASELINE_Trips) as TY_FULL_LINE_BASELINE_Trips
    , sum(TY_NCOM_BASELINE_Trips) as TY_NCOM_BASELINE_Trips
    , sum(TY_RACK_BASELINE_Trips) as TY_RACK_BASELINE_Trips
    , sum(TY_RCOM_BASELINE_Trips) as TY_RCOM_BASELINE_Trips
    , sum(TY_FULL_LINE_BASELINE_Traffic) as TY_FULL_LINE_BASELINE_Traffic
    , sum(TY_NCOM_BASELINE_Traffic) as TY_NCOM_BASELINE_Traffic
    , sum(TY_RACK_BASELINE_Traffic) as TY_RACK_BASELINE_Traffic
    , sum(TY_RCOM_BASELINE_Traffic) as TY_RCOM_BASELINE_Traffic
    , sum(TY_FULL_LINE_BASELINE_New_Customer) as TY_FULL_LINE_BASELINE_New_Customer
    , sum(TY_NCOM_BASELINE_New_Customer) as TY_NCOM_BASELINE_New_Customer
    , sum(TY_RACK_BASELINE_New_Customer) as TY_RACK_BASELINE_New_Customer
    , sum(TY_RCOM_BASELINE_New_Customer) as TY_RCOM_BASELINE_New_Customer
    , sum(TY_FULL_LINE_BASELINE_Existing_Customer) as TY_FULL_LINE_BASELINE_Existing_Customer
    , sum(TY_NCOM_BASELINE_Existing_Customer) as TY_NCOM_BASELINE_Existing_Customer
    , sum(TY_RACK_BASELINE_Existing_Customer) as TY_RACK_BASELINE_Existing_Customer
    , sum(TY_RCOM_BASELINE_Existing_Customer) as TY_RCOM_BASELINE_Existing_Customer
     -- LY
    , sum(LY_FULL_LINE_Net_Revenue) as LY_FULL_LINE_Net_Revenue
    , sum(LY_NCOM_Net_Revenue) as LY_NCOM_Net_Revenue
    , sum(LY_RACK_Net_Revenue) as LY_RACK_Net_Revenue
    , sum(LY_RCOM_Net_Revenue) as LY_RCOM_Net_Revenue
    , sum(LY_FULL_LINE_Gross_Demand) as LY_FULL_LINE_Gross_Demand
    , sum(LY_NCOM_Gross_Demand) as LY_NCOM_Gross_Demand
    , sum(LY_RACK_Gross_Demand) as LY_RACK_Gross_Demand
    , sum(LY_RCOM_Gross_Demand) as LY_RCOM_Gross_Demand
    , sum(LY_FULL_LINE_Units) as LY_FULL_LINE_Units
    , sum(LY_NCOM_Units) as LY_NCOM_Units
    , sum(LY_RACK_Units) as LY_RACK_Units
    , sum(LY_RCOM_Units) as LY_RCOM_Units
    , sum(LY_FULL_LINE_Trips) as LY_FULL_LINE_Trips
    , sum(LY_NCOM_Trips) as LY_NCOM_Trips
    , sum(LY_RACK_Trips) as LY_RACK_Trips
    , sum(LY_RCOM_Trips) as LY_RCOM_Trips
    , sum(LY_FULL_LINE_Existing_Customer) as LY_FULL_LINE_Existing_Customer
    , sum(LY_NCOM_Existing_Customer) as LY_NCOM_Existing_Customer
    , sum(LY_Rack_Existing_Customer) as LY_Rack_Existing_Customer
    , sum(LY_RCOM_Existing_Customer) as LY_RCOM_Existing_Customer
    , sum(LY_FULL_LINE_New_Customer) as LY_FULL_LINE_New_Customer
    , sum(LY_NCOM_New_Customer) as LY_NCOM_New_Customer
    , sum(LY_Rack_New_Customer) as LY_Rack_New_Customer
    , sum(LY_RCOM_New_Customer) as LY_RCOM_New_Customer
    , sum(LY_FULL_LINE_Traffic) as LY_FULL_LINE_Traffic
    , sum(LY_NCOM_Traffic) as LY_NCOM_Traffic
    , sum(LY_Rack_Traffic) as LY_Rack_Traffic
    , sum(LY_RCOM_Traffic) as LY_RCOM_Traffic
    , sum(LY_Spend) as LY_Spend
    , sum(LY_FULL_LINE_BASELINE_NET_REVENUE) as LY_FULL_LINE_BASELINE_NET_REVENUE
    , sum(LY_NCOM_BASELINE_NET_REVENUE) as LY_NCOM_BASELINE_NET_REVENUE
    , sum(LY_RACK_BASELINE_NET_REVENUE) as LY_RACK_BASELINE_NET_REVENUE
    , sum(LY_RCOM_BASELINE_NET_REVENUE) as LY_RCOM_BASELINE_NET_REVENUE
    , sum(LY_FULL_LINE_BASELINE_GROSS_DEMAND) as LY_FULL_LINE_BASELINE_GROSS_DEMAND
    , sum(LY_NCOM_BASELINE_GROSS_DEMAND) as LY_NCOM_BASELINE_GROSS_DEMAND
    , sum(LY_RACK_BASELINE_GROSS_DEMAND) as LY_RACK_BASELINE_GROSS_DEMAND
    , sum(LY_RCOM_BASELINE_GROSS_DEMAND) as LY_RCOM_BASELINE_GROSS_DEMAND
    , sum(LY_FULL_LINE_BASELINE_Units) as LY_FULL_LINE_BASELINE_Units
    , sum(LY_NCOM_BASELINE_Units) as LY_NCOM_BASELINE_Units
    , sum(LY_RACK_BASELINE_Units) as LY_RACK_BASELINE_Units
    , sum(LY_RCOM_BASELINE_Units) as LY_RCOM_BASELINE_Units
    , sum(LY_FULL_LINE_BASELINE_Trips) as LY_FULL_LINE_BASELINE_Trips
    , sum(LY_NCOM_BASELINE_Trips) as LY_NCOM_BASELINE_Trips
    , sum(LY_RACK_BASELINE_Trips) as LY_RACK_BASELINE_Trips
    , sum(LY_RCOM_BASELINE_Trips) as LY_RCOM_BASELINE_Trips
    , sum(LY_FULL_LINE_BASELINE_Traffic) as LY_FULL_LINE_BASELINE_Traffic
    , sum(LY_NCOM_BASELINE_Traffic) as LY_NCOM_BASELINE_Traffic
    , sum(LY_RACK_BASELINE_Traffic) as LY_RACK_BASELINE_Traffic
    , sum(LY_RCOM_BASELINE_Traffic) as LY_RCOM_BASELINE_Traffic
    , sum(LY_FULL_LINE_BASELINE_New_Customer) as LY_FULL_LINE_BASELINE_New_Customer
    , sum(LY_NCOM_BASELINE_New_Customer) as LY_NCOM_BASELINE_New_Customer
    , sum(LY_RACK_BASELINE_New_Customer) as LY_RACK_BASELINE_New_Customer
    , sum(LY_RCOM_BASELINE_New_Customer) as LY_RCOM_BASELINE_New_Customer
    , sum(LY_FULL_LINE_BASELINE_Existing_Customer) as LY_FULL_LINE_BASELINE_Existing_Customer
    , sum(LY_NCOM_BASELINE_Existing_Customer) as LY_NCOM_BASELINE_Existing_Customer
    , sum(LY_RACK_BASELINE_Existing_Customer) as LY_RACK_BASELINE_Existing_Customer
    , sum(LY_RCOM_BASELINE_Existing_Customer) as LY_RCOM_BASELINE_Existing_Customer
FROM (
    --TY
    SELECT
        d.day_date AS day_dt
       , d.DAY_NUM
        ,CONCAT(d.month_short_desc,' ',d.week_desc) AS FISCAL_WEEK
        , of_ty.Analysis_Category_Name AS Analysis_Category_Name
        , of_ty.NMNFlag AS NMNFlag
        , of_ty.PlatformType AS PlatformType
        , of_ty.AcqRetType AS AcqRetType
        , of_ty.AdFormat AS AdFormat
        , of_ty.LoyaltyStatus AS LoyaltyStatus
        , of_ty.Category AS Category
        , of_ty.Banner AS Banner
        , of_ty.flagship_flag AS FlagshipFlag
        -- TY
        , of_ty.FULL_LINE_Net_Revenue AS TY_FULL_LINE_Net_Revenue
        , of_ty.NCOM_Net_Revenue AS TY_NCOM_Net_Revenue
        , of_ty.RACK_Net_Revenue AS TY_RACK_Net_Revenue
        , of_ty.RCOM_Net_Revenue AS TY_RCOM_Net_Revenue
        , of_ty.FULL_LINE_Gross_Demand AS TY_FULL_LINE_Gross_Demand
        , of_ty.NCOM_Gross_Demand AS TY_NCOM_Gross_Demand
        , of_ty.RACK_Gross_Demand AS TY_RACK_Gross_Demand
        , of_ty.RCOM_Gross_Demand AS TY_RCOM_Gross_Demand
        , of_ty.FULL_LINE_Units AS TY_FULL_LINE_Units
        , of_ty.NCOM_Units AS TY_NCOM_Units
        , of_ty.RACK_Units AS TY_RACK_Units
        , of_ty.RCOM_Units AS TY_RCOM_Units
        , of_ty.FULL_LINE_Trips AS TY_FULL_LINE_Trips
        , of_ty.NCOM_Trips AS TY_NCOM_Trips
        , of_ty.RACK_Trips AS TY_RACK_Trips
        , of_ty.RCOM_Trips AS TY_RCOM_Trips
        , of_ty.FULL_LINE_Existing_Customer AS TY_FULL_LINE_Existing_Customer
        , of_ty.NCOM_Existing_Customer AS TY_NCOM_Existing_Customer
        , of_ty.Rack_Existing_Customer AS TY_Rack_Existing_Customer
        , of_ty.RCOM_Existing_Customer AS TY_RCOM_Existing_Customer
        , of_ty.FULL_LINE_New_Customer AS TY_FULL_LINE_New_Customer
        , of_ty.NCOM_New_Customer AS TY_NCOM_New_Customer
        , of_ty.Rack_New_Customer AS TY_Rack_New_Customer
        , of_ty.RCOM_New_Customer AS TY_RCOM_New_Customer
        , of_ty.FULL_LINE_Traffic AS TY_FULL_LINE_Traffic
        , of_ty.NCOM_Traffic AS TY_NCOM_Traffic
        , of_ty.Rack_Traffic AS TY_Rack_Traffic
        , of_ty.RCOM_Traffic AS TY_RCOM_Traffic
        , of_ty.ReportedCost AS TY_Spend
        , of_ty.FULL_LINE_BASELINE_NET_REVENUE AS TY_FULL_LINE_BASELINE_NET_REVENUE
        , of_ty.NCOM_BASELINE_NET_REVENUE AS TY_NCOM_BASELINE_NET_REVENUE
        , of_ty.RACK_BASELINE_NET_REVENUE AS TY_RACK_BASELINE_NET_REVENUE
        , of_ty.RCOM_BASELINE_NET_REVENUE AS TY_RCOM_BASELINE_NET_REVENUE
        , of_ty.FULL_LINE_BASELINE_GROSS_DEMAND AS TY_FULL_LINE_BASELINE_GROSS_DEMAND
        , of_ty.NCOM_BASELINE_GROSS_DEMAND AS TY_NCOM_BASELINE_GROSS_DEMAND
        , of_ty.RACK_BASELINE_GROSS_DEMAND AS TY_RACK_BASELINE_GROSS_DEMAND
        , of_ty.RCOM_BASELINE_GROSS_DEMAND AS TY_RCOM_BASELINE_GROSS_DEMAND
        , of_ty.FULL_LINE_BASELINE_Units AS TY_FULL_LINE_BASELINE_Units
        , of_ty.NCOM_BASELINE_Units AS TY_NCOM_BASELINE_Units
        , of_ty.RACK_BASELINE_Units AS TY_RACK_BASELINE_Units
        , of_ty.RCOM_BASELINE_Units AS TY_RCOM_BASELINE_Units
        , of_ty.FULL_LINE_BASELINE_Trips AS TY_FULL_LINE_BASELINE_Trips
        , of_ty.NCOM_BASELINE_Trips AS TY_NCOM_BASELINE_Trips
        , of_ty.RACK_BASELINE_Trips AS TY_RACK_BASELINE_Trips
        , of_ty.RCOM_BASELINE_Trips AS TY_RCOM_BASELINE_Trips
        , of_ty.FULL_LINE_BASELINE_Traffic AS TY_FULL_LINE_BASELINE_Traffic
        , of_ty.NCOM_BASELINE_Traffic AS TY_NCOM_BASELINE_Traffic
        , of_ty.RACK_BASELINE_Traffic AS TY_RACK_BASELINE_Traffic
        , of_ty.RCOM_BASELINE_Traffic AS TY_RCOM_BASELINE_Traffic
        , of_ty.FULL_LINE_BASELINE_New_Customer AS TY_FULL_LINE_BASELINE_New_Customer
        , of_ty.NCOM_BASELINE_New_Customer AS TY_NCOM_BASELINE_New_Customer
        , of_ty.RACK_BASELINE_New_Customer AS TY_RACK_BASELINE_New_Customer
        , of_ty.RCOM_BASELINE_New_Customer AS TY_RCOM_BASELINE_New_Customer
        , of_ty.FULL_LINE_BASELINE_Existing_Customer AS TY_FULL_LINE_BASELINE_Existing_Customer
        , of_ty.NCOM_BASELINE_Existing_Customer AS TY_NCOM_BASELINE_Existing_Customer
        , of_ty.RACK_BASELINE_Existing_Customer AS TY_RACK_BASELINE_Existing_Customer
        , of_ty.RCOM_BASELINE_Existing_Customer AS TY_RCOM_BASELINE_Existing_Customer
        -- LY
        , CAST(0 AS FLOAT) as LY_FULL_LINE_Net_Revenue
        , CAST(0 AS FLOAT) as LY_NCOM_Net_Revenue
        , CAST(0 AS FLOAT) as LY_RACK_Net_Revenue
        , CAST(0 AS FLOAT) as LY_RCOM_Net_Revenue
        , CAST(0 AS FLOAT) as LY_FULL_LINE_Gross_Demand
        , CAST(0 AS FLOAT) as LY_NCOM_Gross_Demand
        , CAST(0 AS FLOAT) as LY_RACK_Gross_Demand
        , CAST(0 AS FLOAT) as LY_RCOM_Gross_Demand
        , CAST(0 AS FLOAT) as LY_FULL_LINE_Units
        , CAST(0 AS FLOAT) as LY_NCOM_Units
        , CAST(0 AS FLOAT) as LY_RACK_Units
        , CAST(0 AS FLOAT) as LY_RCOM_Units
        , CAST(0 AS FLOAT) as LY_FULL_LINE_Trips
        , CAST(0 AS FLOAT) as LY_NCOM_Trips
        , CAST(0 AS FLOAT) as LY_RACK_Trips
        , CAST(0 AS FLOAT) as LY_RCOM_Trips
        , CAST(0 AS FLOAT) as LY_FULL_LINE_Existing_Customer
        , CAST(0 AS FLOAT) as LY_NCOM_Existing_Customer
        , CAST(0 AS FLOAT) as LY_RACK_Existing_Customer
        , CAST(0 AS FLOAT) as LY_RCOM_Existing_Customer
        , CAST(0 AS FLOAT) as LY_FULL_LINE_New_Customer
        , CAST(0 AS FLOAT) as LY_NCOM_New_Customer
        , CAST(0 AS FLOAT) as LY_RACK_New_Customer
        , CAST(0 AS FLOAT) as LY_RCOM_New_Customer
        , CAST(0 AS FLOAT) as LY_FULL_LINE_Traffic
        , CAST(0 AS FLOAT) as LY_NCOM_Traffic
        , CAST(0 AS FLOAT) as LY_RACK_Traffic
        , CAST(0 AS FLOAT) as LY_RCOM_Traffic
        , CAST(0 AS FLOAT) as LY_Spend
        , CAST(0 AS FLOAT) as LY_FULL_LINE_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as LY_NCOM_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as LY_RACK_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as LY_RCOM_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as LY_FULL_LINE_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as LY_NCOM_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as LY_RACK_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as LY_RCOM_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as LY_FULL_LINE_BASELINE_Units
        , CAST(0 AS FLOAT) as LY_NCOM_BASELINE_Units
        , CAST(0 AS FLOAT) as LY_RACK_BASELINE_Units
        , CAST(0 AS FLOAT) as LY_RCOM_BASELINE_Units
        , CAST(0 AS FLOAT) as LY_FULL_LINE_BASELINE_Trips
        , CAST(0 AS FLOAT) as LY_NCOM_BASELINE_Trips
        , CAST(0 AS FLOAT) as LY_RACK_BASELINE_Trips
        , CAST(0 AS FLOAT) as LY_RCOM_BASELINE_Trips
        , CAST(0 AS FLOAT) as LY_FULL_LINE_BASELINE_Traffic
        , CAST(0 AS FLOAT) as LY_NCOM_BASELINE_Traffic
        , CAST(0 AS FLOAT) as LY_RACK_BASELINE_Traffic
        , CAST(0 AS FLOAT) as LY_RCOM_BASELINE_Traffic
        , CAST(0 AS FLOAT) as LY_FULL_LINE_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as LY_NCOM_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as LY_RACK_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as LY_RCOM_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as LY_FULL_LINE_BASELINE_Existing_Customer
        , CAST(0 AS FLOAT) as LY_NCOM_BASELINE_Existing_Customer
        , CAST(0 AS FLOAT) as LY_RACK_BASELINE_Existing_Customer
        , CAST(0 AS FLOAT) as LY_RCOM_BASELINE_Existing_Customer
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {kpi_scorecard_t2_schema}.optimine_processed_data_final of_ty
            on d.day_date =  of_ty.TimePeriod
    UNION ALL
    --LY
    SELECT
        d.day_date AS day_dt
        ,d.DAY_NUM
       ,CONCAT(d.month_short_desc,' ',d.week_desc) AS FISCAL_WEEK
       , of_ly.Analysis_Category_Name AS Analysis_Category_Name
        , of_ly.NMNFlag AS NMNFlag
        , of_ly.PlatformType AS PlatformType
        , of_ly.AcqRetType AS AcqRetType
        , of_ly.AdFormat AS AdFormat
        , of_ly.LoyaltyStatus AS LoyaltyStatus
        , of_ly.Category AS Category
        , of_ly.Banner AS Banner
        , of_ly.flagship_flag AS FlagshipFlag
        -- TY
        , CAST(0 AS FLOAT) AS TY_FULL_LINE_Net_Revenue
        , CAST(0 AS FLOAT) AS TY_NCOM_Net_Revenue
        , CAST(0 AS FLOAT) AS TY_RACK_Net_Revenue
        , CAST(0 AS FLOAT) AS TY_RCOM_Net_Revenue
        , CAST(0 AS FLOAT) AS TY_FULL_LINE_Gross_Demand
        , CAST(0 AS FLOAT) AS TY_NCOM_Gross_Demand
        , CAST(0 AS FLOAT) AS TY_RACK_Gross_Demand
        , CAST(0 AS FLOAT) AS TY_RCOM_Gross_Demand
        , CAST(0 AS FLOAT) AS TY_FULL_LINE_Units
        , CAST(0 AS FLOAT) AS TY_NCOM_Units
        , CAST(0 AS FLOAT) AS TY_RACK_Units
        , CAST(0 AS FLOAT) AS TY_RCOM_Units
        , CAST(0 AS FLOAT) AS TY_FULL_LINE_Trips
        , CAST(0 AS FLOAT) AS TY_NCOM_Trips
        , CAST(0 AS FLOAT) AS TY_RACK_Trips
        , CAST(0 AS FLOAT) AS TY_RCOM_Trips
        , CAST(0 AS FLOAT) AS TY_FULL_LINE_Existing_Customer
        , CAST(0 AS FLOAT) AS TY_NCOM_Existing_Customer
        , CAST(0 AS FLOAT) AS TY_Rack_Existing_Customer
        , CAST(0 AS FLOAT) AS TY_RCOM_Existing_Customer
        , CAST(0 AS FLOAT) AS TY_FULL_LINE_New_Customer
        , CAST(0 AS FLOAT) AS TY_NCOM_New_Customer
        , CAST(0 AS FLOAT) AS TY_Rack_New_Customer
        , CAST(0 AS FLOAT) AS TY_RCOM_New_Customer
        , CAST(0 AS FLOAT) AS TY_FULL_LINE_Traffic
        , CAST(0 AS FLOAT) AS TY_NCOM_Traffic
        , CAST(0 AS FLOAT) AS TY_Rack_Traffic
        , CAST(0 AS FLOAT) AS TY_RCOM_Traffic
        , CAST(0 AS FLOAT) AS TY_Spend
        , CAST(0 AS FLOAT) as TY_FULL_LINE_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as TY_NCOM_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as TY_RACK_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as TY_RCOM_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as TY_FULL_LINE_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as TY_NCOM_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as TY_RACK_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as TY_RCOM_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as TY_FULL_LINE_BASELINE_Units
        , CAST(0 AS FLOAT) as TY_NCOM_BASELINE_Units
        , CAST(0 AS FLOAT) as TY_RACK_BASELINE_Units
        , CAST(0 AS FLOAT) as TY_RCOM_BASELINE_Units
        , CAST(0 AS FLOAT) as TY_FULL_LINE_BASELINE_Trips
        , CAST(0 AS FLOAT) as TY_NCOM_BASELINE_Trips
        , CAST(0 AS FLOAT) as TY_RACK_BASELINE_Trips
        , CAST(0 AS FLOAT) as TY_RCOM_BASELINE_Trips
        , CAST(0 AS FLOAT) as TY_FULL_LINE_BASELINE_Traffic
        , CAST(0 AS FLOAT) as TY_NCOM_BASELINE_Traffic
        , CAST(0 AS FLOAT) as TY_RACK_BASELINE_Traffic
        , CAST(0 AS FLOAT) as TY_RCOM_BASELINE_Traffic
        , CAST(0 AS FLOAT) as TY_FULL_LINE_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as TY_NCOM_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as TY_RACK_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as TY_RCOM_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as TY_FULL_LINE_BASELINE_Existing_Customer
        , CAST(0 AS FLOAT) as TY_NCOM_BASELINE_Existing_Customer
        , CAST(0 AS FLOAT) as TY_RACK_BASELINE_Existing_Customer
        , CAST(0 AS FLOAT) as TY_RCOM_BASELINE_Existing_Customer
        -- LY
        , of_ly.FULL_LINE_Net_Revenue AS LY_FULL_LINE_Net_Revenue
        , of_ly.NCOM_Net_Revenue AS LY_NCOM_Net_Revenue
        , of_ly.RACK_Net_Revenue AS LY_RACK_Net_Revenue
        , of_ly.RCOM_Net_Revenue AS LY_RCOM_Net_Revenue
        , of_ly.FULL_LINE_Gross_Demand AS LY_FULL_LINE_Gross_Demand
        , of_ly.NCOM_Gross_Demand AS LY_NCOM_Gross_Demand
        , of_ly.RACK_Gross_Demand AS LY_RACK_Gross_Demand
        , of_ly.RCOM_Gross_Demand AS LY_RCOM_Gross_Demand
        , of_ly.FULL_LINE_Units AS LY_FULL_LINE_Units
        , of_ly.NCOM_Units AS LY_NCOM_Units
        , of_ly.RACK_Units AS LY_RACK_Units
        , of_ly.RCOM_Units AS LY_RCOM_Units
        , of_ly.FULL_LINE_Trips AS LY_FULL_LINE_Trips
        , of_ly.NCOM_Trips AS LY_NCOM_Trips
        , of_ly.RACK_Trips AS LY_RACK_Trips
        , of_ly.RCOM_Trips AS LY_RCOM_Trips
        , of_ly.FULL_LINE_Existing_Customer AS LY_FULL_LINE_Existing_Customer
        , of_ly.NCOM_Existing_Customer AS LY_NCOM_Existing_Customer
        , of_ly.Rack_Existing_Customer AS LY_Rack_Existing_Customer
        , of_ly.RCOM_Existing_Customer AS LY_RCOM_Existing_Customer
        , of_ly.FULL_LINE_New_Customer AS LY_FULL_LINE_New_Customer
        , of_ly.NCOM_New_Customer AS LY_NCOM_New_Customer
        , of_ly.Rack_New_Customer AS LY_Rack_New_Customer
        , of_ly.RCOM_New_Customer AS LY_RCOM_New_Customer
        , of_ly.FULL_LINE_Traffic AS LY_FULL_LINE_Traffic
        , of_ly.NCOM_Traffic AS LY_NCOM_Traffic
        , of_ly.Rack_Traffic AS LY_Rack_Traffic
        , of_ly.RCOM_Traffic AS LY_RCOM_Traffic
        , of_ly.ReportedCost AS LY_Spend
        , of_ly.FULL_LINE_BASELINE_NET_REVENUE AS LY_FULL_LINE_BASELINE_NET_REVENUE
        , of_ly.NCOM_BASELINE_NET_REVENUE AS LY_NCOM_BASELINE_NET_REVENUE
        , of_ly.RACK_BASELINE_NET_REVENUE AS LY_RACK_BASELINE_NET_REVENUE
        , of_ly.RCOM_BASELINE_NET_REVENUE AS LY_RCOM_BASELINE_NET_REVENUE
        , of_ly.FULL_LINE_BASELINE_GROSS_DEMAND AS LY_FULL_LINE_BASELINE_GROSS_DEMAND
        , of_ly.NCOM_BASELINE_GROSS_DEMAND AS LY_NCOM_BASELINE_GROSS_DEMAND
        , of_ly.RACK_BASELINE_GROSS_DEMAND AS LY_RACK_BASELINE_GROSS_DEMAND
        , of_ly.RCOM_BASELINE_GROSS_DEMAND AS LY_RCOM_BASELINE_GROSS_DEMAND
        , of_ly.FULL_LINE_BASELINE_Units AS LY_FULL_LINE_BASELINE_Units
        , of_ly.NCOM_BASELINE_Units AS LY_NCOM_BASELINE_Units
        , of_ly.RACK_BASELINE_Units AS LY_RACK_BASELINE_Units
        , of_ly.RCOM_BASELINE_Units AS LY_RCOM_BASELINE_Units
        , of_ly.FULL_LINE_BASELINE_Trips AS LY_FULL_LINE_BASELINE_Trips
        , of_ly.NCOM_BASELINE_Trips AS LY_NCOM_BASELINE_Trips
        , of_ly.RACK_BASELINE_Trips AS LY_RACK_BASELINE_Trips
        , of_ly.RCOM_BASELINE_Trips AS LY_RCOM_BASELINE_Trips
        , of_ly.FULL_LINE_BASELINE_Traffic AS LY_FULL_LINE_BASELINE_Traffic
        , of_ly.NCOM_BASELINE_Traffic AS LY_NCOM_BASELINE_Traffic
        , of_ly.RACK_BASELINE_Traffic AS LY_RACK_BASELINE_Traffic
        , of_ly.RCOM_BASELINE_Traffic AS LY_RCOM_BASELINE_Traffic
        , of_ly.FULL_LINE_BASELINE_New_Customer AS LY_FULL_LINE_BASELINE_New_Customer
        , of_ly.NCOM_BASELINE_New_Customer AS LY_NCOM_BASELINE_New_Customer
        , of_ly.RACK_BASELINE_New_Customer AS LY_RACK_BASELINE_New_Customer
        , of_ly.RCOM_BASELINE_New_Customer AS LY_RCOM_BASELINE_New_Customer
        , of_ly.FULL_LINE_BASELINE_Existing_Customer AS LY_FULL_LINE_BASELINE_Existing_Customer
        , of_ly.NCOM_BASELINE_Existing_Customer AS LY_NCOM_BASELINE_Existing_Customer
        , of_ly.RACK_BASELINE_Existing_Customer AS LY_RACK_BASELINE_Existing_Customer
        , of_ly.RCOM_BASELINE_Existing_Customer AS LY_RCOM_BASELINE_Existing_Customer
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {kpi_scorecard_t2_schema}.optimine_processed_data_final of_ly
            on d.last_year_day_date_realigned =  of_ly.TimePeriod
) opf
WHERE opf.day_dt <= (select max(TimePeriod ) from {kpi_scorecard_t2_schema}.optimine_processed_data_final)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;


SET QUERY_BAND = NONE FOR SESSION;

