SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=weekly_pacing_vw_11521_ACE_ENG;
     Task_Name=weekly_pacing_vw;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.weekly_pacing_vw
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-10-28
--Note:
-- This view supports the Weekly Pacing Dashboard.


REPLACE VIEW {kpi_scorecard_t2_schema}.weekly_pacing_vw
AS
LOCK ROW FOR ACCESS 
 SELECT week_idnt,		
        media_type,
		Analysis_Category_Name,
        SUM(FULL_LINE_NET_REVENUE) AS FULL_LINE_NET_REVENUE,
        SUM(NCOM_NET_REVENUE) AS NCOM_NET_REVENUE,
        SUM(RACK_NET_REVENUE) AS RACK_NET_REVENUE,
        SUM(RCOM_NET_REVENUE) AS RCOM_NET_REVENUE,
        SUM(FULL_LINE_GROSS_DEMAND) AS FULL_LINE_GROSS_DEMAND,
        SUM(NCOM_GROSS_DEMAND) AS NCOM_GROSS_DEMAND,
        SUM(RACK_GROSS_DEMAND) AS RACK_GROSS_DEMAND,
        SUM(RCOM_GROSS_DEMAND) AS RCOM_GROSS_DEMAND,
        SUM(FULL_LINE_Traffic) AS FULL_LINE_Traffic,
        SUM(NCOM_Traffic) AS NCOM_Traffic,
        SUM(RACK_Traffic) AS RACK_Traffic,
        SUM(RCOM_Traffic) AS RCOM_Traffic,
        SUM(FULL_LINE_Trips) AS FULL_LINE_Trips,
		SUM(NCOM_Trips) AS NCOM_Trips,
        SUM(RACK_Trips) AS RACK_Trips,
		SUM(RCOM_Trips) AS RCOM_Trips,
        sum(New_Spend_Nord) as New_Spend_Nord,
		sum(New_Spend_Rack) as New_Spend_Rack,
        sum(Spend_Nord) as Spend_Nord,
		sum(Spend_Rack) as Spend_Rack,
		sum(New_Impressions_Nord) as New_Impressions_Nord,
		sum(New_Impressions_Rack) as New_Impressions_Rack,
        sum(Impressions_Nord) as Impressions_Nord,
		sum(Impressions_Rack) as Impressions_Rack,
		SUM(LW_FULL_LINE_NET_REVENUE) AS LW_FULL_LINE_NET_REVENUE,
        SUM(LW_NCOM_NET_REVENUE) AS LW_NCOM_NET_REVENUE,
        SUM(LW_RACK_NET_REVENUE) AS LW_RACK_NET_REVENUE,
        SUM(LW_RCOM_NET_REVENUE) AS LW_RCOM_NET_REVENUE,
        SUM(LW_FULL_LINE_GROSS_DEMAND) AS LW_FULL_LINE_GROSS_DEMAND,
        SUM(LW_NCOM_GROSS_DEMAND) AS LW_NCOM_GROSS_DEMAND,
        SUM(LW_RACK_GROSS_DEMAND) AS LW_RACK_GROSS_DEMAND,
        SUM(LW_RCOM_GROSS_DEMAND) AS LW_RCOM_GROSS_DEMAND,
        SUM(LW_FULL_LINE_Traffic) AS LW_FULL_LINE_Traffic,
        SUM(LW_NCOM_Traffic) AS LW_NCOM_Traffic,
        SUM(LW_RACK_Traffic) AS LW_RACK_Traffic,
        SUM(LW_RCOM_Traffic) AS LW_RCOM_Traffic,
        SUM(LW_FULL_LINE_Trips) AS LW_FULL_LINE_Trips,
		SUM(LW_NCOM_Trips) AS LW_NCOM_Trips,
        SUM(LW_RACK_Trips) AS LW_RACK_Trips,
		SUM(LW_RCOM_Trips) AS LW_RCOM_Trips,
        sum(LW_New_Spend_Nord) as LW_New_Spend_Nord,
		sum(LW_New_Spend_RACK) as LW_New_Spend_RACK,
        sum(LW_Spend_Nord) as LW_Spend_Nord,
		sum(LW_Spend_RACK) as LW_Spend_RACK,
		sum(LW_New_Impressions_Nord) as LW_New_Impressions_Nord,
		sum(LW_New_Impressions_RACK) as LW_New_Impressions_RACK,
        sum(LW_Impressions_Nord) as LW_Impressions_Nord,
		sum(LW_Impressions_RACK) as LW_Impressions_RACK,
        SUM(LY_FULL_LINE_Net_Revenue) AS LY_FULL_LINE_Net_Revenue,
        SUM(LY_NCOM_Net_Revenue) AS LY_NCOM_Net_Revenue,
        SUM(LY_RACK_Net_Revenue) AS LY_RACK_Net_Revenue,
        SUM(LY_RCOM_Net_Revenue) AS LY_RCOM_Net_Revenue,
        SUM(LY_FULL_LINE_Gross_Demand) AS LY_FULL_LINE_Gross_Demand,
        SUM(LY_NCOM_Gross_Demand) AS LY_NCOM_Gross_Demand,
        SUM(LY_RACK_Gross_Demand) AS LY_RACK_Gross_Demand,
        SUM(LY_RCOM_Gross_Demand) AS LY_RCOM_Gross_Demand,
        SUM(LY_FULL_LINE_Traffic) AS LY_FULL_LINE_Traffic,
        SUM(LY_NCOM_Traffic) AS LY_NCOM_Traffic,
        SUM(LY_RACK_Traffic) AS LY_RACK_Traffic,
        SUM(LY_RCOM_Traffic) AS LY_RCOM_Traffic,
        SUM(LY_FULL_LINE_Trips) AS LY_FULL_LINE_Trips,
		SUM(LY_NCOM_Trips) AS LY_NCOM_Trips,
        SUM(LY_RACK_Trips) AS LY_RACK_Trips,
		SUM(LY_RCOM_Trips) AS LY_RCOM_Trips,
        SUM(LY_Spend_Nord) AS LY_Spend_Nord,
		SUM(LY_Spend_Rack) AS LY_Spend_Rack
 FROM (SELECT b.week_idnt,
 CASE
       WHEN Comp_status <> 'New'
         AND (Analysis_Category_Name LIKE '%Organic Social%'
              OR Analysis_Category_Name LIKE '%SEO%'
              OR Analysis_Category_Name LIKE '%App Push%'
              OR Analysis_Category_Name LIKE '%Email%')
         THEN 'Unpaid'
    WHEN Comp_status <> 'New'
         AND (Analysis_Category_Name NOT LIKE '%Organic Social%'
              AND Analysis_Category_Name NOT LIKE '%SEO%'
              AND Analysis_Category_Name NOT LIKE '%App Push%'
              AND Analysis_Category_Name NOT LIKE '%Email%'
              AND Analysis_Category_Name NOT LIKE '%Postcard%'
              AND Analysis_Category_Name NOT LIKE '%Print%'
              AND Analysis_Category_Name NOT LIKE '%Audio%'
              AND Analysis_Category_Name NOT LIKE '%Linear TV%'
              AND Analysis_Category_Name NOT LIKE '%OOH%')
         THEN 'Paid'
    ELSE Comp_status
              END AS Media_Type,  
			  Analysis_Category_Name,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Value3 ELSE 0 END) AS FULL_LINE_NET_REVENUE,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Value4 ELSE 0 END) AS NCOM_NET_REVENUE,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Value3 ELSE 0 END) AS RACK_NET_REVENUE,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Value4 ELSE 0 END) AS RCOM_NET_REVENUE,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Value5 ELSE 0 END) AS FULL_LINE_GROSS_DEMAND,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Count6 ELSE 0 END) AS NCOM_GROSS_DEMAND,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Value5 ELSE 0 END) AS RACK_GROSS_DEMAND,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Count6 ELSE 0 END) AS RCOM_GROSS_DEMAND,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_Traffic%') or Analysis_Setup_Instance_Name LIKE ('%NordstromTraffic%')
			  THEN Estimated_Count2 ELSE 0 END) AS FULL_LINE_Traffic,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_Traffic%') or Analysis_Setup_Instance_Name LIKE ('%NordstromTraffic%')
			  THEN Estimated_Count3 ELSE 0 END) AS NCOM_Traffic,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_Traffic%') THEN Estimated_Count2 ELSE 0 END) AS RACK_Traffic,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_Traffic%') THEN Estimated_Count3 ELSE 0 END) AS RCOM_Traffic,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Count7 ELSE 0 END) AS FULL_LINE_Trips,
			  SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Count8 ELSE 0 END) AS NCOM_Trips,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Count7 ELSE 0 END) AS RACK_Trips,
			  SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Count8 ELSE 0 END) AS RCOM_Trips,
              SUM(CASE WHEN Comp_status='New' and Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Executed_Spend ELSE 0 END) AS New_Spend_Nord,
			  SUM(CASE WHEN Comp_status='New' and Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Executed_Spend ELSE 0 END) AS New_Spend_Rack,
              SUM(CASE WHEN Comp_status<>'New' and Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%')THEN Executed_Spend ELSE 0 END) AS Spend_Nord,
			  SUM(CASE WHEN Comp_status<>'New' and Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Executed_Spend ELSE 0 END) AS Spend_Rack,
			  SUM(CASE WHEN Comp_status='New' and Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Executed_Audience ELSE 0 END) AS New_Impressions_Nord,
			  SUM(CASE WHEN Comp_status='New' and Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Executed_Audience ELSE 0 END) AS New_Impressions_Rack,
              SUM(CASE WHEN Comp_status<>'New' and Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%')THEN Executed_Audience ELSE 0 END) AS Impressions_Nord,
			  SUM(CASE WHEN Comp_status<>'New' and Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Executed_Audience ELSE 0 END) AS Impressions_Rack,
			CAST(0 AS FLOAT) AS LW_FULL_LINE_NET_REVENUE,
			CAST(0 AS FLOAT) AS LW_NCOM_NET_REVENUE,
			CAST(0 AS FLOAT) AS LW_RACK_NET_REVENUE,
			CAST(0 AS FLOAT) AS LW_RCOM_NET_REVENUE,
			CAST(0 AS FLOAT) AS LW_FULL_LINE_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS LW_NCOM_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS LW_RACK_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS LW_RCOM_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS LW_FULL_LINE_Traffic,
			CAST(0 AS FLOAT) AS LW_NCOM_Traffic,
			CAST(0 AS FLOAT) AS LW_RACK_Traffic,
			CAST(0 AS FLOAT) AS LW_RCOM_Traffic,
			CAST(0 AS FLOAT) AS LW_FULL_LINE_Trips,
			CAST(0 AS FLOAT) AS LW_NCOM_Trips,
			CAST(0 AS FLOAT) AS LW_RACK_Trips,
			CAST(0 AS FLOAT) AS LW_RCOM_Trips,
			CAST(0 AS FLOAT) as LW_New_Spend_Nord,
			CAST(0 AS FLOAT) as LW_New_Spend_RACK,
			CAST(0 AS FLOAT) as LW_Spend_Nord,
			CAST(0 AS FLOAT) as LW_Spend_RACK,
			CAST(0 AS FLOAT) as LW_New_Impressions_Nord,
			CAST(0 AS FLOAT) as LW_New_Impressions_RACK,
			CAST(0 AS FLOAT) as LW_Impressions_Nord,
			CAST(0 AS FLOAT) as LW_Impressions_RACK,			  
              CAST(0 AS FLOAT) AS LY_FULL_LINE_Net_Revenue,
              CAST(0 AS FLOAT) AS LY_NCOM_Net_Revenue,
              CAST(0 AS FLOAT) AS LY_RACK_Net_Revenue,
              CAST(0 AS FLOAT) AS LY_RCOM_Net_Revenue,
              CAST(0 AS FLOAT) AS LY_FULL_LINE_Gross_Demand,
              CAST(0 AS FLOAT) AS LY_NCOM_Gross_Demand,
              CAST(0 AS FLOAT) AS LY_RACK_Gross_Demand,
              CAST(0 AS FLOAT) AS LY_RCOM_Gross_Demand,
              CAST(0 AS FLOAT) AS LY_FULL_LINE_Traffic,
              CAST(0 AS FLOAT) AS LY_NCOM_Traffic,
              CAST(0 AS FLOAT) AS LY_RACK_Traffic,
              CAST(0 AS FLOAT) AS LY_RCOM_Traffic,
              CAST(0 AS FLOAT) AS LY_FULL_LINE_Trips,
			  CAST(0 AS FLOAT) AS LY_NCOM_Trips,
              CAST(0 AS FLOAT) AS LY_RACK_Trips,
			  CAST(0 AS FLOAT) AS LY_RCOM_Trips,
              CAST(0 AS FLOAT) AS LY_Spend_Nord,
			  CAST(0 AS FLOAT) AS LY_Spend_Rack
       FROM {kpi_scorecard_t2_schema}.media_pacing_report a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.Date_Start = b.day_date
       GROUP BY 1,
                2,
                3
		UNION ALL
			SELECT
			CASE  
                  WHEN MOD(b.week_idnt,100) > 51 THEN b.week_idnt +49
                ELSE b.week_idnt +1
              END AS week_idnt, 			  
CASE
    WHEN Comp_status <> 'New'
         AND (Analysis_Category_Name LIKE '%Organic Social%'
              OR Analysis_Category_Name LIKE '%SEO%'
              OR Analysis_Category_Name LIKE '%App Push%'
              OR Analysis_Category_Name LIKE '%Email%')
         THEN 'Unpaid'
    WHEN Comp_status <> 'New'
         AND (Analysis_Category_Name NOT LIKE '%Organic Social%'
              AND Analysis_Category_Name NOT LIKE '%SEO%'
              AND Analysis_Category_Name NOT LIKE '%App Push%'
              AND Analysis_Category_Name NOT LIKE '%Email%'
              AND Analysis_Category_Name NOT LIKE '%Postcard%'
              AND Analysis_Category_Name NOT LIKE '%Print%'
              AND Analysis_Category_Name NOT LIKE '%Audio%'
              AND Analysis_Category_Name NOT LIKE '%Linear TV%'
              AND Analysis_Category_Name NOT LIKE '%OOH%')
         THEN 'Paid'
    ELSE Comp_status
END AS Media_Type,
			  Analysis_Category_Name,
			CAST(0 AS FLOAT) AS FULL_LINE_NET_REVENUE,
			CAST(0 AS FLOAT) AS NCOM_NET_REVENUE,
			CAST(0 AS FLOAT) AS RACK_NET_REVENUE,
			CAST(0 AS FLOAT) AS RCOM_NET_REVENUE,
			CAST(0 AS FLOAT) AS FULL_LINE_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS NCOM_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS RACK_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS RCOM_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS FULL_LINE_Traffic,
			CAST(0 AS FLOAT) AS NCOM_Traffic,
			CAST(0 AS FLOAT) AS RACK_Traffic,
			CAST(0 AS FLOAT) AS RCOM_Traffic,
			CAST(0 AS FLOAT) AS FULL_LINE_Trips,
			CAST(0 AS FLOAT) AS NCOM_Trips,
			CAST(0 AS FLOAT) AS RACK_Trips,
			CAST(0 AS FLOAT) AS RCOM_Trips,
			CAST(0 AS FLOAT) as New_Spend_Nord,
			CAST(0 AS FLOAT) as New_Spend_RACK,
			CAST(0 AS FLOAT) as Spend_Nord,
			CAST(0 AS FLOAT) as Spend_RACK,
			CAST(0 AS FLOAT) as New_Impressions_Nord,
			CAST(0 AS FLOAT) as New_Impressions_RACK,
			CAST(0 AS FLOAT) as Impressions_Nord,
			CAST(0 AS FLOAT) as Impressions_RACK,			  
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Value3 ELSE 0 END) AS LW_FULL_LINE_NET_REVENUE,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Value4 ELSE 0 END) AS LW_NCOM_NET_REVENUE,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Value3 ELSE 0 END) AS LW_RACK_NET_REVENUE,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Value4 ELSE 0 END) AS LW_RCOM_NET_REVENUE,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Value5 ELSE 0 END) AS LW_FULL_LINE_GROSS_DEMAND,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Count6 ELSE 0 END) AS LW_NCOM_GROSS_DEMAND,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Value5 ELSE 0 END) AS LW_RACK_GROSS_DEMAND,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Count6 ELSE 0 END) AS LW_RCOM_GROSS_DEMAND,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_Traffic%') or Analysis_Setup_Instance_Name LIKE ('%NordstromTraffic%')
			  THEN Estimated_Count2 ELSE 0 END) AS LW_FULL_LINE_Traffic,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_Traffic%') or Analysis_Setup_Instance_Name LIKE ('%NordstromTraffic%') 
			  THEN Estimated_Count3 ELSE 0 END) AS LW_NCOM_Traffic,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_Traffic%') THEN Estimated_Count2 ELSE 0 END) AS LW_RACK_Traffic,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_Traffic%') THEN Estimated_Count3 ELSE 0 END) AS LW_RCOM_Traffic,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Count7 ELSE 0 END) AS LW_FULL_LINE_Trips,
			  SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Estimated_Count8 ELSE 0 END) AS LW_NCOM_Trips,
              SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Count7 ELSE 0 END) AS LW_RACK_Trips,
			  SUM(CASE WHEN Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Estimated_Count8 ELSE 0 END) AS LW_RCOM_Trips,
              SUM(CASE WHEN Comp_status='New' and Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Executed_Spend ELSE 0 END) AS LW_New_Spend_Nord,
			  SUM(CASE WHEN Comp_status='New' and Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Executed_Spend ELSE 0 END) AS LW_New_Spend_Rack,
              SUM(CASE WHEN Comp_status<>'New' and Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%')THEN Executed_Spend ELSE 0 END) AS LW_Spend_Nord,
			  SUM(CASE WHEN Comp_status<>'New' and Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Executed_Spend ELSE 0 END) AS LW_Spend_Rack,
			  SUM(CASE WHEN Comp_status='New' and Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%') THEN Executed_Audience ELSE 0 END) AS LW_New_Impressions_Nord,
			  SUM(CASE WHEN Comp_status='New' and Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Executed_Audience ELSE 0 END) AS LW_New_Impressions_Rack,
              SUM(CASE WHEN Comp_status<>'New' and Analysis_Setup_Instance_Name LIKE ('%Nordstrom_SalesTrips%')THEN Executed_Audience ELSE 0 END) AS LW_Impressions_Nord,
			  SUM(CASE WHEN Comp_status<>'New' and Analysis_Setup_Instance_Name LIKE ('%Rack_SalesTrips%') THEN Executed_Audience ELSE 0 END) AS LW_Impressions_Rack,			  
              CAST(0 AS FLOAT) AS LY_FULL_LINE_Net_Revenue,
              CAST(0 AS FLOAT) AS LY_NCOM_Net_Revenue,
              CAST(0 AS FLOAT) AS LY_RACK_Net_Revenue,
              CAST(0 AS FLOAT) AS LY_RCOM_Net_Revenue,
              CAST(0 AS FLOAT) AS LY_FULL_LINE_Gross_Demand,
              CAST(0 AS FLOAT) AS LY_NCOM_Gross_Demand,
              CAST(0 AS FLOAT) AS LY_RACK_Gross_Demand,
              CAST(0 AS FLOAT) AS LY_RCOM_Gross_Demand,
              CAST(0 AS FLOAT) AS LY_FULL_LINE_Traffic,
              CAST(0 AS FLOAT) AS LY_NCOM_Traffic,
              CAST(0 AS FLOAT) AS LY_RACK_Traffic,
              CAST(0 AS FLOAT) AS LY_RCOM_Traffic,
              CAST(0 AS FLOAT) AS LY_FULL_LINE_Trips,
			  CAST(0 AS FLOAT) AS LY_NCOM_Trips,
              CAST(0 AS FLOAT) AS LY_RACK_Trips,
			  CAST(0 AS FLOAT) AS LY_RCOM_Trips,
              CAST(0 AS FLOAT) AS LY_Spend_Nord,
			  CAST(0 AS FLOAT) AS LY_Spend_Rack
       FROM {kpi_scorecard_t2_schema}.media_pacing_report a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.Date_Start = b.day_date
       GROUP BY 1,
                2,
                3
       UNION ALL
       SELECT CASE  
                  WHEN MOD(b.week_idnt,100) > 52 THEN b.week_idnt +51
                ELSE b.week_idnt +99
              END AS week_idnt,	
  case  WHEN (Analysis_Category_Name LIKE '%Organic Social%'
              OR Analysis_Category_Name LIKE '%SEO%'
              OR Analysis_Category_Name LIKE '%App Push%'
              OR Analysis_Category_Name LIKE '%Email%')
         THEN 'Unpaid'
    WHEN (Analysis_Category_Name NOT LIKE '%Organic Social%'
              AND Analysis_Category_Name NOT LIKE '%SEO%'
              AND Analysis_Category_Name NOT LIKE '%App Push%'
              AND Analysis_Category_Name NOT LIKE '%Email%'
              AND Analysis_Category_Name NOT LIKE '%Postcard%'
              AND Analysis_Category_Name NOT LIKE '%Print%'
              AND Analysis_Category_Name NOT LIKE '%Audio%'
              AND Analysis_Category_Name NOT LIKE '%Linear TV%'
              AND Analysis_Category_Name NOT LIKE '%OOH%')
         THEN 'Paid'
         ELSE 'Other'
              END AS Media_Type,
			  Analysis_Category_Name,
			CAST(0 AS FLOAT) AS FULL_LINE_NET_REVENUE,
			CAST(0 AS FLOAT) AS NCOM_NET_REVENUE,
			CAST(0 AS FLOAT) AS RACK_NET_REVENUE,
			CAST(0 AS FLOAT) AS RCOM_NET_REVENUE,
			CAST(0 AS FLOAT) AS FULL_LINE_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS NCOM_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS RACK_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS RCOM_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS FULL_LINE_Traffic,
			CAST(0 AS FLOAT) AS NCOM_Traffic,
			CAST(0 AS FLOAT) AS RACK_Traffic,
			CAST(0 AS FLOAT) AS RCOM_Traffic,
			CAST(0 AS FLOAT) AS FULL_LINE_Trips,
			CAST(0 AS FLOAT) AS NCOM_Trips,
			CAST(0 AS FLOAT) AS RACK_Trips,
			CAST(0 AS FLOAT) AS RCOM_Trips,
			CAST(0 AS FLOAT) as New_Spend_Nord,
			CAST(0 AS FLOAT) as New_Spend_RACK,
			CAST(0 AS FLOAT) as Spend_Nord,
			CAST(0 AS FLOAT) as Spend_RACK,
			CAST(0 AS FLOAT) as New_Impressions_Nord,
			CAST(0 AS FLOAT) as New_Impressions_RACK,
			CAST(0 AS FLOAT) as Impressions_Nord,
			CAST(0 AS FLOAT) as Impressions_RACK,
			  CAST(0 AS FLOAT) AS LW_FULL_LINE_NET_REVENUE,
			CAST(0 AS FLOAT) AS LW_NCOM_NET_REVENUE,
			CAST(0 AS FLOAT) AS LW_RACK_NET_REVENUE,
			CAST(0 AS FLOAT) AS LW_RCOM_NET_REVENUE,
			CAST(0 AS FLOAT) AS LW_FULL_LINE_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS LW_NCOM_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS LW_RACK_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS LW_RCOM_GROSS_DEMAND,
			CAST(0 AS FLOAT) AS LW_FULL_LINE_Traffic,
			CAST(0 AS FLOAT) AS LW_NCOM_Traffic,
			CAST(0 AS FLOAT) AS LW_RACK_Traffic,
			CAST(0 AS FLOAT) AS LW_RCOM_Traffic,
			CAST(0 AS FLOAT) AS LW_FULL_LINE_Trips,
			CAST(0 AS FLOAT) AS LW_NCOM_Trips,
			CAST(0 AS FLOAT) AS LW_RACK_Trips,
			CAST(0 AS FLOAT) AS LW_RCOM_Trips,
			CAST(0 AS FLOAT) as LW_New_Spend_Nord,
			CAST(0 AS FLOAT) as LW_New_Spend_RACK,
			CAST(0 AS FLOAT) as LW_Spend_Nord,
			CAST(0 AS FLOAT) as LW_Spend_RACK,
			CAST(0 AS FLOAT) as LW_New_Impressions_Nord,
			CAST(0 AS FLOAT) as LW_New_Impressions_RACK,
			CAST(0 AS FLOAT) as LW_Impressions_Nord,
			CAST(0 AS FLOAT) as LW_Impressions_RACK,
              SUM(TY_FULL_LINE_Net_Revenue) AS LY_FULL_LINE_Net_Revenue,
              SUM(TY_NCOM_Net_Revenue) AS LY_NCOM_Net_Revenue,
              SUM(TY_RACK_Net_Revenue) AS LY_RACK_Net_Revenue,
              SUM(TY_RCOM_Net_Revenue) AS LY_RCOM_Net_Revenue,
              SUM(TY_FULL_LINE_Gross_Demand) AS LY_FULL_LINE_Gross_Demand,
              SUM(TY_NCOM_Gross_Demand) AS LY_NCOM_Gross_Demand,
              SUM(TY_RACK_Gross_Demand) AS LY_RACK_Gross_Demand,              
              SUM(TY_RCOM_Gross_Demand) AS LY_RCOM_Gross_Demand,
              SUM(TY_FULL_LINE_Traffic) AS LY_FULL_LINE_Traffic,
              SUM(TY_NCOM_Traffic) AS LY_NCOM_Traffic,
              SUM(TY_RACK_Traffic) AS LY_RACK_Traffic,
              SUM(TY_RCOM_Traffic) AS LY_RCOM_Traffic,
              SUM(TY_FULL_LINE_Trips) AS LY_FULL_LINE_Trips,
			  SUM(TY_NCOM_Trips) AS LY_NCOM_Trips,
              SUM(TY_RACK_Trips) AS LY_RACK_Trips,
			  SUM(TY_RCOM_Trips) AS LY_RCOM_Trips,
			  SUM(CASE WHEN Banner LIKE ('%Nordstrom%')THEN TY_Spend ELSE 0 END) AS LY_Spend_Nord,
			  SUM(CASE WHEN Banner LIKE ('%Rack%')THEN TY_Spend ELSE 0 END) AS LY_Spend_Rack
       FROM {kpi_scorecard_t2_schema}.optimine_finalvw a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.day_dt = b.day_date
       GROUP BY 1,
                2,
                3) c
 WHERE week_idnt between (SELECT MIN(week_idnt)
                     FROM {kpi_scorecard_t2_schema}.media_pacing_report a
                       LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.Date_Start = b.day_date)
					   and (SELECT MAX(week_idnt)
                     FROM {kpi_scorecard_t2_schema}.media_pacing_report a
                       LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.Date_Start = b.day_date)					  
 GROUP BY 1,
          2,
          3;


SET QUERY_BAND = NONE FOR SESSION;
