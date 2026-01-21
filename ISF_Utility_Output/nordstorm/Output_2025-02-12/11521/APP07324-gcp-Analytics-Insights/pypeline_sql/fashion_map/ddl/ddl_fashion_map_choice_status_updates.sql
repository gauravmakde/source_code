SET QUERY_BAND = 'App_ID=APP08907; 
     DAG_ID=ddl_fashion_map_choice_status_updates_11521_ACE_ENG;
     Task_Name=ddl_fashion_map_choice_status_updates;'
     FOR SESSION VOLATILE; 


/*
T2/Table Name: T2DL_DAS_DIGENG.FASHION_MAP_CHOICE_STATUS_UPDATES
Team/Owner: AI Digital/Soren Stime, Robert Legg
Date Created/Modified: 09/18/2024,TBD

Note:
Fashion Map table to track the current selling status/price of a product. This helps users filter for the results they want returned from the model. 
*/
 
-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'FASHION_MAP_CHOICE_STATUS_UPDATES', OUT_RETURN_MSG);

create multiset table {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    web_style_num VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    color_num VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    channel_brand VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,  
    dw_sys_load_date DATE format 'YYYY-MM-DD',
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(web_style_num)
PARTITION BY RANGE_N(dw_sys_load_date  BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY ,
NO RANGE)
 ;

-- Table Comment (STANDARD)
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES IS 'Customer choice table for tracking product updates that would impact the status of a product for fashion map.';
-- Column comments (OPTIONAL)
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES.web_style_num IS 'Unique ID maintained in Web for a Style';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES.color_num IS 'Represent code maintained for a ite(m(s) color within Nordstrom';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES.channel_brand IS '	Represents the brand of the customer channel for Nordstrom family of businesses Eg: NORDSTROM,NORDSTROM_RACK';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES.dw_sys_load_date IS 'Date that the record was added to the table';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES.dw_sys_load_tmstp IS 'Timestamp that the record was added to the table';


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (web_style_num), --  primary index
                    COLUMN (dw_sys_load_date)  --  partition
on {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES;

SET QUERY_BAND = NONE FOR SESSION;
