/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=camelot_data_dedup_11521_ACE_ENG;
     Task_Name=camelot_data_dedup;'
     FOR SESSION VOLATILE;


/*
Table: T2DL_DAS_MMM.camelot_data_dedup
Owner: Analytics Engineering
Modified:05/01/2024



*/



CREATE MULTISET VOLATILE TABLE daily_cost_split AS (
SELECT  start_date,
		end_date,
		BANNER,
        campaign,
        dma,
        platform,
        channel,
        BAR,
        Ad_Type,
        Funnel,
        External_Funding,        
	  case when end_date <> start_date and end_date is not null then impressions/(end_date - start_date +1)
	  when end_date is null then impressions/(current_date() - start_date +1)
	  else impressions/1 end as DAILY_Impressions,
	  case when end_date <> start_date and end_date is not null then Cost/(end_date - start_date +1)
	   when end_date is null then Cost/(current_date() - start_date +1)
	  else Cost/1 end as DAILY_Cost
FROM  {mmm_t2_schema}.camelot_data tl
) WITH DATA PRIMARY INDEX(start_date) ON COMMIT PRESERVE ROWS;


CREATE Multiset VOLATILE TABLE _variables
AS
( select min(start_date) as start_date_1 , max(end_date) as end_date_1 from daily_cost_split)
WITH data PRIMARY INDEX (start_date_1) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE Camelot_daily_dedup AS (
SELECT	cal.day_date as DAY_DT,
		splt.BANNER,
        splt.campaign,
        splt.dma,
        splt.platform,
        splt.channel,
        splt.BAR,
        splt.Ad_Type,
        splt.Funnel,
        splt.External_Funding,
        sum(splt.DAILY_Impressions) AS Impressions,
        sum(splt.DAILY_Cost) AS Cost
FROM 	prd_nap_usr_vws.day_cal cal
INNER JOIN daily_cost_split splt ON cal.day_date BETWEEN splt.start_date AND splt.end_date, _variables
WHERE cal.day_date BETWEEN start_date_1 AND end_date_1
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA PRIMARY INDEX(DAY_DT,banner,campaign,dma,channel,platform) ON COMMIT PRESERVE ROWS;

DELETE
FROM  {mmm_t2_schema}.camelot_data_dedup;
;

INSERT INTO {mmm_t2_schema}.camelot_data_dedup
SELECT a.*,CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  Camelot_daily_dedup a
WHERE 1=1
;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
