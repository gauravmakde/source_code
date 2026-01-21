/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09142;
     DAG_ID=ddl_flash_event_output_11521_ACE_ENG;
     Task_Name=ddl_flash_event_output;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_digitalmerch_flash.flash_event_output
Team/Owner: Nicole Miao, Sean Larkin, Cassie Zhang, Rae Ann Boswell
Date Created/Modified: 06/28/2024, modified on 06/28/2024
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'flash_event_output', OUT_RETURN_MSG);

create multiset table {digital_merch_t2_schema}.flash_event_output
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
        event_id            varchar(10)
        ,event_name         varchar(8000) 
        ,featured_flag      integer compress
        ,private_sale_flag  integer compress
        ,unique_brands      integer compress
        ,branded_flag       varchar(20) compress
        ,brand_name         varchar(200) compress
        ,event_start        date
        ,event_end          date
        ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(event_id, event_name, event_start, event_end)
partition by range_n(event_start BETWEEN DATE '2021-01-01' AND DATE '2055-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {digital_merch_t2_schema}.flash_event_output IS 'A table containing all the flash events happened on r.com since FY23';


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

 
 
