SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=monthly_insights_vw_11521_ACE_ENG;
     Task_Name=monthly_insights_vw;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.monthly_insights_vw
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-10-28
--Note:
-- This view supports the Monthly Insights Dashboard.


REPLACE VIEW {kpi_scorecard_t2_schema}.monthly_insights_vw
AS
LOCK ROW FOR ACCESS 
 SELECT 
Month_Idnt,
Media_type,
Analysis_Category_Name,
	  sum(TY_FULL_LINE_Net_Revenue) as TY_FULL_LINE_Net_Revenue
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
    , sum(TY_Nord_Spend) as TY_Nord_Spend
    , sum(TY_Rack_Spend) as TY_Rack_Spend
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
    , sum(LY_Nord_Spend) as LY_Nord_Spend
    , sum(LY_Rack_Spend) as LY_Rack_Spend
	     -- LM
    , sum(LM_FULL_LINE_Net_Revenue) as LM_FULL_LINE_Net_Revenue
    , sum(LM_NCOM_Net_Revenue) as LM_NCOM_Net_Revenue
    , sum(LM_RACK_Net_Revenue) as LM_RACK_Net_Revenue
    , sum(LM_RCOM_Net_Revenue) as LM_RCOM_Net_Revenue
    , sum(LM_FULL_LINE_Gross_Demand) as LM_FULL_LINE_Gross_Demand
    , sum(LM_NCOM_Gross_Demand) as LM_NCOM_Gross_Demand
    , sum(LM_RACK_Gross_Demand) as LM_RACK_Gross_Demand
    , sum(LM_RCOM_Gross_Demand) as LM_RCOM_Gross_Demand
    , sum(LM_FULL_LINE_Units) as LM_FULL_LINE_Units
    , sum(LM_NCOM_Units) as LM_NCOM_Units
    , sum(LM_RACK_Units) as LM_RACK_Units
    , sum(LM_RCOM_Units) as LM_RCOM_Units
    , sum(LM_FULL_LINE_Trips) as LM_FULL_LINE_Trips
    , sum(LM_NCOM_Trips) as LM_NCOM_Trips
    , sum(LM_RACK_Trips) as LM_RACK_Trips
    , sum(LM_RCOM_Trips) as LM_RCOM_Trips
    , sum(LM_FULL_LINE_Existing_Customer) as LM_FULL_LINE_Existing_Customer
    , sum(LM_NCOM_Existing_Customer) as LM_NCOM_Existing_Customer
    , sum(LM_Rack_Existing_Customer) as LM_Rack_Existing_Customer
    , sum(LM_RCOM_Existing_Customer) as LM_RCOM_Existing_Customer
    , sum(LM_FULL_LINE_New_Customer) as LM_FULL_LINE_New_Customer
    , sum(LM_NCOM_New_Customer) as LM_NCOM_New_Customer
    , sum(LM_Rack_New_Customer) as LM_Rack_New_Customer
    , sum(LM_RCOM_New_Customer) as LM_RCOM_New_Customer
    , sum(LM_FULL_LINE_Traffic) as LM_FULL_LINE_Traffic
    , sum(LM_NCOM_Traffic) as LM_NCOM_Traffic
    , sum(LM_Rack_Traffic) as LM_Rack_Traffic
    , sum(LM_RCOM_Traffic) as LM_RCOM_Traffic
    , sum(LM_Nord_Spend) as LM_Nord_Spend
	, sum(LM_Rack_Spend) as LM_Rack_Spend
FROM
(SELECT
    b.month_idnt
    , case  
	    WHEN (Analysis_Category_Name LIKE '%Online%' OR Analysis_Category_Name LIKE '%In-Store%')  Then 'Base' 
	    WHEN (Analysis_Category_Name LIKE '%Organic Social%'
              OR Analysis_Category_Name LIKE '%SEO%'
              OR Analysis_Category_Name LIKE '%App Push%'
              OR Analysis_Category_Name LIKE '%Email%')
         THEN 'Unpaid'    
         ELSE 'Paid'          
              END AS Media_Type
    , Analysis_Category_Name
      -- TY
	,  sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Net_Revenue ELSE TY_FULL_LINE_Net_Revenue END) as TY_FULL_LINE_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_NET_REVENUE ELSE TY_NCOM_Net_Revenue END) as TY_NCOM_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_NET_REVENUE ELSE TY_RACK_Net_Revenue END) as TY_RACK_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_NET_REVENUE ELSE TY_RCOM_Net_Revenue END) as TY_RCOM_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_GROSS_DEMAND ELSE TY_FULL_LINE_Gross_Demand END) as TY_FULL_LINE_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_GROSS_DEMAND ELSE TY_NCOM_Gross_Demand END) as TY_NCOM_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_GROSS_DEMAND ELSE TY_RACK_Gross_Demand END) as TY_RACK_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_GROSS_DEMAND ELSE TY_RCOM_Gross_Demand END) as TY_RCOM_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Units ELSE TY_FULL_LINE_Units END) as TY_FULL_LINE_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_Units ELSE TY_NCOM_Units END) as TY_NCOM_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_Units ELSE TY_RACK_Units END) as TY_RACK_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_Units ELSE TY_RCOM_Units END) as TY_RCOM_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Trips ELSE TY_FULL_LINE_Trips END) as TY_FULL_LINE_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_Trips ELSE TY_NCOM_Trips END) as TY_NCOM_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_Trips ELSE TY_RACK_Trips END) as TY_RACK_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_Trips ELSE TY_RCOM_Trips END) as TY_RCOM_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Existing_Customer ELSE TY_FULL_LINE_Existing_Customer END) as TY_FULL_LINE_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_Existing_Customer ELSE TY_NCOM_Existing_Customer END) as TY_NCOM_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_Existing_Customer ELSE TY_Rack_Existing_Customer END) as TY_Rack_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_Existing_Customer ELSE TY_RCOM_Existing_Customer END) as TY_RCOM_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_New_Customer ELSE TY_FULL_LINE_New_Customer END) as TY_FULL_LINE_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_New_Customer ELSE TY_NCOM_New_Customer END) as TY_NCOM_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_New_Customer ELSE TY_Rack_New_Customer END) as TY_Rack_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_New_Customer ELSE TY_RCOM_New_Customer END) as TY_RCOM_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Traffic ELSE TY_FULL_LINE_Traffic END) as TY_FULL_LINE_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_Traffic ELSE TY_NCOM_Traffic END) as TY_NCOM_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_Traffic ELSE TY_Rack_Traffic END) as TY_Rack_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_Traffic ELSE TY_RCOM_Traffic END) as TY_RCOM_Traffic
    , sum(CASE WHEN Analysis_Category_Name LIKE '%Nordstrom%' then TY_Spend end) as TY_Nord_Spend 
	, sum(CASE WHEN Analysis_Category_Name LIKE '%Rack%' then TY_Spend end) as TY_Rack_Spend 
     -- LY
	, sum(CASE WHEN Media_Type = 'Base' THEN LY_FULL_LINE_BASELINE_Net_Revenue ELSE LY_FULL_LINE_Net_Revenue END) as LY_FULL_LINE_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_NCOM_BASELINE_NET_REVENUE ELSE LY_NCOM_Net_Revenue END) as LY_NCOM_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RACK_BASELINE_NET_REVENUE ELSE LY_RACK_Net_Revenue END) as LY_RACK_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RCOM_BASELINE_NET_REVENUE ELSE LY_RCOM_Net_Revenue END) as LY_RCOM_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_FULL_LINE_BASELINE_GROSS_DEMAND ELSE LY_FULL_LINE_Gross_Demand END) as LY_FULL_LINE_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_NCOM_BASELINE_GROSS_DEMAND ELSE LY_NCOM_Gross_Demand END) as LY_NCOM_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RACK_BASELINE_GROSS_DEMAND ELSE LY_RACK_Gross_Demand END) as LY_RACK_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RCOM_BASELINE_GROSS_DEMAND ELSE LY_RCOM_Gross_Demand END) as LY_RCOM_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_FULL_LINE_BASELINE_Units ELSE LY_FULL_LINE_Units END) as LY_FULL_LINE_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_NCOM_BASELINE_Units ELSE LY_NCOM_Units END) as LY_NCOM_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RACK_BASELINE_Units ELSE LY_RACK_Units END) as LY_RACK_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RCOM_BASELINE_Units ELSE LY_RCOM_Units END) as LY_RCOM_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_FULL_LINE_BASELINE_Trips ELSE LY_FULL_LINE_Trips END) as LY_FULL_LINE_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_NCOM_BASELINE_Trips ELSE LY_NCOM_Trips END) as LY_NCOM_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RACK_BASELINE_Trips ELSE LY_RACK_Trips END) as LY_RACK_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RCOM_BASELINE_Trips ELSE LY_RCOM_Trips END) as LY_RCOM_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_FULL_LINE_BASELINE_Existing_Customer ELSE LY_FULL_LINE_Existing_Customer END) as LY_FULL_LINE_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_NCOM_BASELINE_Existing_Customer ELSE LY_NCOM_Existing_Customer END) as LY_NCOM_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RACK_BASELINE_Existing_Customer ELSE LY_Rack_Existing_Customer END) as LY_Rack_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RCOM_BASELINE_Existing_Customer ELSE LY_RCOM_Existing_Customer END) as LY_RCOM_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_FULL_LINE_BASELINE_New_Customer ELSE LY_FULL_LINE_New_Customer END) as LY_FULL_LINE_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_NCOM_BASELINE_New_Customer ELSE LY_NCOM_New_Customer END) as LY_NCOM_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RACK_BASELINE_New_Customer ELSE LY_Rack_New_Customer END) as LY_Rack_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RCOM_BASELINE_New_Customer ELSE LY_RCOM_New_Customer END) as LY_RCOM_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_FULL_LINE_BASELINE_Traffic ELSE LY_FULL_LINE_Traffic END) as LY_FULL_LINE_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_NCOM_BASELINE_Traffic ELSE LY_NCOM_Traffic END) as LY_NCOM_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RACK_BASELINE_Traffic ELSE LY_Rack_Traffic END) as LY_Rack_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN LY_RCOM_BASELINE_Traffic ELSE LY_RCOM_Traffic END) as LY_RCOM_Traffic
    , sum(CASE WHEN Analysis_Category_Name LIKE '%Nordstrom%' then LY_Spend end) as LY_Nord_Spend 
	, sum(CASE WHEN Analysis_Category_Name LIKE '%Rack%' then LY_Spend end) as LY_Rack_Spend  
	        -- LM
        , CAST(0 AS FLOAT) as LM_FULL_LINE_Net_Revenue
        , CAST(0 AS FLOAT) as LM_NCOM_Net_Revenue
        , CAST(0 AS FLOAT) as LM_RACK_Net_Revenue
        , CAST(0 AS FLOAT) as LM_RCOM_Net_Revenue
        , CAST(0 AS FLOAT) as LM_FULL_LINE_Gross_Demand
        , CAST(0 AS FLOAT) as LM_NCOM_Gross_Demand
        , CAST(0 AS FLOAT) as LM_RACK_Gross_Demand
        , CAST(0 AS FLOAT) as LM_RCOM_Gross_Demand
        , CAST(0 AS FLOAT) as LM_FULL_LINE_Units
        , CAST(0 AS FLOAT) as LM_NCOM_Units
        , CAST(0 AS FLOAT) as LM_RACK_Units
        , CAST(0 AS FLOAT) as LM_RCOM_Units
        , CAST(0 AS FLOAT) as LM_FULL_LINE_Trips
        , CAST(0 AS FLOAT) as LM_NCOM_Trips
        , CAST(0 AS FLOAT) as LM_RACK_Trips
        , CAST(0 AS FLOAT) as LM_RCOM_Trips
        , CAST(0 AS FLOAT) as LM_FULL_LINE_Existing_Customer
        , CAST(0 AS FLOAT) as LM_NCOM_Existing_Customer
        , CAST(0 AS FLOAT) as LM_RACK_Existing_Customer
        , CAST(0 AS FLOAT) as LM_RCOM_Existing_Customer
        , CAST(0 AS FLOAT) as LM_FULL_LINE_New_Customer
        , CAST(0 AS FLOAT) as LM_NCOM_New_Customer
        , CAST(0 AS FLOAT) as LM_RACK_New_Customer
        , CAST(0 AS FLOAT) as LM_RCOM_New_Customer
        , CAST(0 AS FLOAT) as LM_FULL_LINE_Traffic
        , CAST(0 AS FLOAT) as LM_NCOM_Traffic
        , CAST(0 AS FLOAT) as LM_RACK_Traffic
        , CAST(0 AS FLOAT) as LM_RCOM_Traffic
        , CAST(0 AS FLOAT) as LM_Nord_Spend
	    , CAST(0 AS FLOAT) as LM_Rack_Spend
	from {kpi_scorecard_t2_schema}.optimine_finalvw a 
	left join prd_nap_usr_vws.DAY_CAL_454_DIM b on a.day_dt = b.day_date group by 1,2,3
	union all 
SELECT
CASE   WHEN MOD(b.month_idnt,100) = 12 THEN b.month_idnt +89
                ELSE b.month_idnt + 1
              END AS month_idnt    
    , case
	    WHEN (Analysis_Category_Name LIKE '%Online%' OR Analysis_Category_Name LIKE '%In-Store%')  Then 'Base' 
	    WHEN (Analysis_Category_Name LIKE '%Organic Social%'
              OR Analysis_Category_Name LIKE '%SEO%'
              OR Analysis_Category_Name LIKE '%App Push%'
              OR Analysis_Category_Name LIKE '%Email%')
         THEN 'Unpaid'
     ELSE 'Paid'         
              END AS Media_Type
    , Analysis_Category_Name
     -- TY
		,CAST(0 AS FLOAT) as TY_FULL_LINE_Net_Revenue
        , CAST(0 AS FLOAT) as TY_NCOM_Net_Revenue
        , CAST(0 AS FLOAT) as TY_RACK_Net_Revenue
        , CAST(0 AS FLOAT) as TY_RCOM_Net_Revenue
        , CAST(0 AS FLOAT) as TY_FULL_LINE_Gross_Demand
        , CAST(0 AS FLOAT) as TY_NCOM_Gross_Demand
        , CAST(0 AS FLOAT) as TY_RACK_Gross_Demand
        , CAST(0 AS FLOAT) as TY_RCOM_Gross_Demand
        , CAST(0 AS FLOAT) as TY_FULL_LINE_Units
        , CAST(0 AS FLOAT) as TY_NCOM_Units
        , CAST(0 AS FLOAT) as TY_RACK_Units
        , CAST(0 AS FLOAT) as TY_RCOM_Units
        , CAST(0 AS FLOAT) as TY_FULL_LINE_Trips
        , CAST(0 AS FLOAT) as TY_NCOM_Trips
        , CAST(0 AS FLOAT) as TY_RACK_Trips
        , CAST(0 AS FLOAT) as TY_RCOM_Trips
        , CAST(0 AS FLOAT) as TY_FULL_LINE_Existing_Customer
        , CAST(0 AS FLOAT) as TY_NCOM_Existing_Customer
        , CAST(0 AS FLOAT) as TY_RACK_Existing_Customer
        , CAST(0 AS FLOAT) as TY_RCOM_Existing_Customer
        , CAST(0 AS FLOAT) as TY_FULL_LINE_New_Customer
        , CAST(0 AS FLOAT) as TY_NCOM_New_Customer
        , CAST(0 AS FLOAT) as TY_RACK_New_Customer
        , CAST(0 AS FLOAT) as TY_RCOM_New_Customer
        , CAST(0 AS FLOAT) as TY_FULL_LINE_Traffic
        , CAST(0 AS FLOAT) as TY_NCOM_Traffic
        , CAST(0 AS FLOAT) as TY_RACK_Traffic
        , CAST(0 AS FLOAT) as TY_RCOM_Traffic
        , CAST(0 AS FLOAT) as TY_Nord_Spend
		, CAST(0 AS FLOAT) as TY_Rack_Spend
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
        , CAST(0 AS FLOAT) as LY_Nord_Spend
		, CAST(0 AS FLOAT) as LY_Rack_Spend
	     -- LM
	,  sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Net_Revenue ELSE TY_FULL_LINE_Net_Revenue END) as LM_FULL_LINE_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_NET_REVENUE ELSE TY_NCOM_Net_Revenue END) as LM_NCOM_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_NET_REVENUE ELSE TY_RACK_Net_Revenue END) as LM_RACK_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_NET_REVENUE ELSE TY_RCOM_Net_Revenue END) as LM_RCOM_Net_Revenue
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_GROSS_DEMAND ELSE TY_FULL_LINE_Gross_Demand END) as LM_FULL_LINE_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_GROSS_DEMAND ELSE TY_NCOM_Gross_Demand END) as LM_NCOM_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_GROSS_DEMAND ELSE TY_RACK_Gross_Demand END) as LM_RACK_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_GROSS_DEMAND ELSE TY_RCOM_Gross_Demand END) as LM_RCOM_Gross_Demand
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Units ELSE TY_FULL_LINE_Units END) as LM_FULL_LINE_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_Units ELSE TY_NCOM_Units END) as LM_NCOM_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_Units ELSE TY_RACK_Units END) as LM_RACK_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_Units ELSE TY_RCOM_Units END) as LM_RCOM_Units
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Trips ELSE TY_FULL_LINE_Trips END) as LM_FULL_LINE_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_Trips ELSE TY_NCOM_Trips END) as LM_NCOM_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_Trips ELSE TY_RACK_Trips END) as LM_RACK_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_Trips ELSE TY_RCOM_Trips END) as LM_RCOM_Trips
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Existing_Customer ELSE TY_FULL_LINE_Existing_Customer END) as LM_FULL_LINE_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_Existing_Customer ELSE TY_NCOM_Existing_Customer END) as LM_NCOM_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_Existing_Customer ELSE TY_Rack_Existing_Customer END) as LM_Rack_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_Existing_Customer ELSE TY_RCOM_Existing_Customer END) as LM_RCOM_Existing_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_New_Customer ELSE TY_FULL_LINE_New_Customer END) as LM_FULL_LINE_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_New_Customer ELSE TY_NCOM_New_Customer END) as LM_NCOM_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_New_Customer ELSE TY_Rack_New_Customer END) as LM_Rack_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_New_Customer ELSE TY_RCOM_New_Customer END) as LM_RCOM_New_Customer
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_FULL_LINE_BASELINE_Traffic ELSE TY_FULL_LINE_Traffic END) as LM_FULL_LINE_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_NCOM_BASELINE_Traffic ELSE TY_NCOM_Traffic END) as LM_NCOM_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RACK_BASELINE_Traffic ELSE TY_Rack_Traffic END) as LM_Rack_Traffic
    , sum(CASE WHEN Media_Type = 'Base' THEN TY_RCOM_BASELINE_Traffic ELSE TY_RCOM_Traffic END) as LM_RCOM_Traffic
    , sum(CASE WHEN Analysis_Category_Name LIKE '%Nordstrom%' then TY_Spend end) as LM_Nord_Spend 
	, sum(CASE WHEN Analysis_Category_Name LIKE '%Rack%' then TY_Spend end) as LM_Rack_Spend 
	from {kpi_scorecard_t2_schema}.optimine_finalvw a
	left join prd_nap_usr_vws.DAY_CAL_454_DIM b on a.day_dt = b.day_date group by 1,2,3)a
	 group by 1,2,3;


SET QUERY_BAND = NONE FOR SESSION;

