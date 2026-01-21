/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_usl_rolling_52wk_calendar_11521_ACE_ENG;
     Task_Name=ddl_usl_rolling_52wk_calendar;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.rolling_52wk_calendar
Team/Owner: Customer Analytics - Grant Coffey, Niharika Srivastava
Date Created/Modified: Mar 22 2024*/



/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Create a Fiscal Calendar (spanning 2009 - end of current fiscal year)
 * where "Years" = consecutive rolling 52-week periods
 * 
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/

create MULTISET table {usl_t2_schema}.usl_rolling_52wk_calendar,
     FALLBACK,
	 NO BEFORE JOURNAL,
	 NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(    day_date date 
    ,day_num integer 
    ,day_desc varchar(30)
    ,week_num integer
    ,week_desc varchar(30)
    ,month_num integer
    ,month_short_desc varchar(30)
    ,quarter_num integer
    ,halfyear_num integer
    ,year_num integer
    ,month_454_num integer
    ,year_454_num integer
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) primary index (day_date) ;

SET QUERY_BAND = NONE FOR SESSION;

