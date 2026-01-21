SET QUERY_BAND = 'App_ID=APP08823;
     DAG_ID=ddl_affiliates_cost_daily_fact_11521_ACE_ENG;
     Task_Name=ddl_affiliates_cost_daily_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_FUNNEL_IO.affiliates_cost_daily_fact
Team/Owner: AE
Date Created/Modified: 2/21/2023

Note:

*/


CREATE MULTISET TABLE {funnel_io_t2_schema}.affiliates_cost_daily_fact
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
      stats_date              DATE FORMAT 'YY/MM/DD',
      banner                  varchar(4),
      country                 char(2),
      low_funnel_daily_cost   FLOAT,
      mid_funnel_daily_cost   FLOAT,
      dw_sys_load_tmstp       TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX (stats_date, banner, country)
PARTITION BY RANGE_N(stats_date between date '2019-01-01' and date '2025-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {funnel_io_t2_schema}.affiliates_cost_daily_fact IS 'Affiliates cost data by date, banner, country';


SET QUERY_BAND = NONE FOR SESSION;