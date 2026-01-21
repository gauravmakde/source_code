SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=loyalty_incremental_sales_11521_ACE_ENG;
     Task_Name=loyalty_incremental_sales;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.loyalty_incremental_sales
Owner: Analytics Engineering
Modified:22/12/2023 

-- To automate the Loyalty Incremental Sales 

*/


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

CREATE Multiset VOLATILE TABLE _variables
AS
( select start_date , end_date from start_varibale, end_varibale)
WITH data PRIMARY INDEX (start_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE delete_from_year
AS
(SELECT min(fiscal_year_num) AS start_year, max(fiscal_year_num) as end_year
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM, _variables
WHERE day_date between start_date and end_date)
WITH data PRIMARY INDEX (start_year) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE delete_from_month
AS
(SELECT distinct month_abrv as month_desc
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM, _variables
WHERE day_date between start_date and end_date)
WITH data PRIMARY INDEX (month_desc) ON COMMIT PRESERVE ROWS;

DELETE FROM {kpi_scorecard_t2_schema}.loyalty_incremental_sales, delete_from_year
WHERE report_year between start_year and end_year and report_month in (select * from delete_from_month) ;

INSERT INTO {kpi_scorecard_t2_schema}.loyalty_incremental_sales
SELECT report_year,
       report_month,
       banner,
       SUM(card_sales) AS card_sales,
       SUM(member_sales) AS member_sales,
       current_timestamp as dw_sys_load_tmstp
FROM (SELECT dc.year_num AS report_year,
             dc.month_short_desc AS report_month,
             (CASE WHEN intent_store_num IN (742,780) THEN NULL WHEN sd.business_unit_desc = 'FULL LINE' THEN 'Nordstrom' WHEN sd.business_unit_desc = 'OFFPRICE ONLINE' THEN 'Nordstrom Rack' WHEN sd.business_unit_desc = 'N.COM' THEN 'Nordstrom' WHEN sd.business_unit_desc = 'Rack' THEN 'Nordstrom Rack' END) AS banner,
             SUM(net_tender_spend_usd_amt) AS card_sales,
             SUM(net_nontender_spend_usd_amt) AS member_sales
      FROM (SELECT loyalty_id,
                   base_points,
                   bonus_points,
                   total_points,
                   net_nontender_spend_usd_amt,
                   credit_channel_code,
                   net_tender_spend_usd_amt,
                   net_outside_spend_usd_amt,
                   intent_store_num,
                   business_day_date
            FROM PRD_NAP_USR_VWS.LOYALTY_TRANSACTION_FACT_VW ltfv) AS a
        LEFT JOIN PRD_NAP_USR_VWS.STORE_DIM sd ON a.intent_store_num = sd.store_num
        LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL dc ON a.business_day_date = dc.day_date, _variables
      WHERE business_day_date between start_date and end_date
      AND   banner IN ('Nordstrom','Nordstrom Rack')
      GROUP BY 1,
               2,
               3
      UNION ALL
      SELECT dc.year_num AS report_year,
             dc.month_short_desc AS report_month,
             (CASE WHEN intent_store_num IN (742,780) THEN NULL WHEN sd.business_unit_desc IN ('FULL LINE','OFFPRICE ONLINE','N.COM','Rack') THEN 'JWN' END) AS banner,
             SUM(net_tender_spend_usd_amt) AS card_sales,
             SUM(net_nontender_spend_usd_amt) AS member_sales
      FROM (SELECT loyalty_id,
                   base_points,
                   bonus_points,
                   total_points,
                   net_nontender_spend_usd_amt,
                   credit_channel_code,
                   net_tender_spend_usd_amt,
                   net_outside_spend_usd_amt,
                   intent_store_num,
                   business_day_date
            FROM PRD_NAP_USR_VWS.LOYALTY_TRANSACTION_FACT_VW ltfv) AS a
        LEFT JOIN PRD_NAP_USR_VWS.STORE_DIM sd ON a.intent_store_num = sd.store_num
        LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL dc ON a.business_day_date = dc.day_date, _variables
      WHERE business_day_date between start_date and end_date
      AND   banner IN ('JWN')
      GROUP BY 1,
               2,
               3) a
GROUP BY 1,
         2,
         3;                 
        
SET QUERY_BAND = NONE FOR SESSION; 
