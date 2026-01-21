SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=optimine_finalvw_box_level_11521_ACE_ENG;
     Task_Name=optimine_finalvw_box_level;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.optimine_finalvw_box_level
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-08-21
--Note:
-- This view supports the MBR Dashboard.


REPLACE VIEW {kpi_scorecard_t2_schema}.optimine_finalvw_box_level
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
	, Box
     -- TY
    , sum(TY_Net_Revenue) as TY_Net_Revenue
    , sum(TY_Gross_Demand) as TY_Gross_Demand
    , sum(TY_Units) as TY_Units
    , sum(TY_Trips) as TY_Trips
    , sum(TY_Existing_Customer) as TY_Existing_Customer
    , sum(TY_New_Customer) as TY_New_Customer
    , sum(TY_Traffic) as TY_Traffic
    , sum(TY_Spend) as TY_Spend
    , sum(TY_BASELINE_NET_REVENUE) as TY_BASELINE_NET_REVENUE
    , sum(TY_BASELINE_GROSS_DEMAND) as TY_BASELINE_GROSS_DEMAND
    , sum(TY_BASELINE_Units) as TY_BASELINE_Units
    , sum(TY_BASELINE_Trips) as TY_BASELINE_Trips
    , sum(TY_BASELINE_Traffic) as TY_BASELINE_Traffic
    , sum(TY_BASELINE_New_Customer) as TY_BASELINE_New_Customer
    , sum(TY_BASELINE_Existing_Customer) as TY_BASELINE_Existing_Customer
     -- LY
    , sum(LY_Net_Revenue) as LY_Net_Revenue
    , sum(LY_Gross_Demand) as LY_Gross_Demand
    , sum(LY_Units) as LY_Units
    , sum(LY_Trips) as LY_Trips
    , sum(LY_Existing_Customer) as LY_Existing_Customer
    , sum(LY_New_Customer) as LY_New_Customer
    , sum(LY_Traffic) as LY_Traffic
    , sum(LY_Spend) as LY_Spend
    , sum(LY_BASELINE_NET_REVENUE) as LY_BASELINE_NET_REVENUE
    , sum(LY_BASELINE_GROSS_DEMAND) as LY_BASELINE_GROSS_DEMAND
    , sum(LY_BASELINE_Units) as LY_BASELINE_Units
    , sum(LY_BASELINE_Trips) as LY_BASELINE_Trips
    , sum(LY_BASELINE_Traffic) as LY_BASELINE_Traffic
    , sum(LY_BASELINE_New_Customer) as LY_BASELINE_New_Customer
    , sum(LY_BASELINE_Existing_Customer) as LY_BASELINE_Existing_Customer
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
		, of_ty.Box AS Box
        -- TY
        , of_ty.Net_Revenue AS TY_Net_Revenue
        , of_ty.Gross_Demand AS TY_Gross_Demand
        , of_ty.Units AS TY_Units
        , of_ty.Trips AS TY_Trips
        , of_ty.Existing_Customer AS TY_Existing_Customer
        , of_ty.New_Customer AS TY_New_Customer
        , of_ty.Traffic AS TY_Traffic
        , of_ty.ReportedCost AS TY_Spend
        , of_ty.BASELINE_NET_REVENUE AS TY_BASELINE_NET_REVENUE
        , of_ty.BASELINE_GROSS_DEMAND AS TY_BASELINE_GROSS_DEMAND
        , of_ty.BASELINE_Units AS TY_BASELINE_Units
        , of_ty.BASELINE_Trips AS TY_BASELINE_Trips
        , of_ty.BASELINE_Traffic AS TY_BASELINE_Traffic
        , of_ty.BASELINE_New_Customer AS TY_BASELINE_New_Customer
        , of_ty.BASELINE_Existing_Customer AS TY_BASELINE_Existing_Customer
        -- LY
        , CAST(0 AS FLOAT) as LY_Net_Revenue
        , CAST(0 AS FLOAT) as LY_Gross_Demand
        , CAST(0 AS FLOAT) as LY_Units
        , CAST(0 AS FLOAT) as LY_Trips
        , CAST(0 AS FLOAT) as LY_Existing_Customer
        , CAST(0 AS FLOAT) as LY_New_Customer
        , CAST(0 AS FLOAT) as LY_Traffic
        , CAST(0 AS FLOAT) as LY_Spend
        , CAST(0 AS FLOAT) as LY_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as LY_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as LY_BASELINE_Units
        , CAST(0 AS FLOAT) as LY_BASELINE_Trips
        , CAST(0 AS FLOAT) as LY_BASELINE_Traffic
        , CAST(0 AS FLOAT) as LY_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as LY_BASELINE_Existing_Customer
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {kpi_scorecard_t2_schema}.optimine_processed_data_box_level_vw of_ty
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
		, of_ly.Box AS Box
        -- TY
        , CAST(0 AS FLOAT) AS TY_Net_Revenue
        , CAST(0 AS FLOAT) AS TY_Gross_Demand
        , CAST(0 AS FLOAT) AS TY_Units
        , CAST(0 AS FLOAT) AS TY_Trips
		, CAST(0 AS FLOAT) AS TY_Existing_Customer
        , CAST(0 AS FLOAT) AS TY_New_Customer
        , CAST(0 AS FLOAT) AS TY_Traffic
        , CAST(0 AS FLOAT) AS TY_Spend
        , CAST(0 AS FLOAT) as TY_BASELINE_NET_REVENUE
        , CAST(0 AS FLOAT) as TY_BASELINE_GROSS_DEMAND
        , CAST(0 AS FLOAT) as TY_BASELINE_Units
        , CAST(0 AS FLOAT) as TY_BASELINE_Trips
        , CAST(0 AS FLOAT) as TY_BASELINE_Traffic
        , CAST(0 AS FLOAT) as TY_BASELINE_New_Customer
        , CAST(0 AS FLOAT) as TY_BASELINE_Existing_Customer
        -- LY
        , of_ly.Net_Revenue AS LY_Net_Revenue
        , of_ly.Gross_Demand AS LY_Gross_Demand
        , of_ly.Units AS LY_Units
        , of_ly.Trips AS LY_Trips
        , of_ly.Existing_Customer AS LY_Existing_Customer
        , of_ly.New_Customer AS LY_New_Customer
        , of_ly.Traffic AS LY_Traffic
        , of_ly.ReportedCost AS LY_Spend
        , of_ly.BASELINE_NET_REVENUE AS LY_BASELINE_NET_REVENUE
        , of_ly.BASELINE_GROSS_DEMAND AS LY_BASELINE_GROSS_DEMAND
        , of_ly.BASELINE_Units AS LY_BASELINE_Units
        , of_ly.BASELINE_Trips AS LY_BASELINE_Trips
        , of_ly.BASELINE_Traffic AS LY_BASELINE_Traffic
        , of_ly.BASELINE_New_Customer AS LY_BASELINE_New_Customer
        , of_ly.BASELINE_Existing_Customer AS LY_BASELINE_Existing_Customer
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {kpi_scorecard_t2_schema}.optimine_processed_data_box_level_vw of_ly
            on d.last_year_day_date_realigned =  of_ly.TimePeriod
) opf
WHERE opf.day_dt <= (select max(TimePeriod ) from {kpi_scorecard_t2_schema}.optimine_processed_data_box_level_vw)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13;


SET QUERY_BAND = NONE FOR SESSION;

