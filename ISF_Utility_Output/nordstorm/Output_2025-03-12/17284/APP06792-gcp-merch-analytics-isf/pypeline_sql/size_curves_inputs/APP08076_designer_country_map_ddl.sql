/*
Name: Size Curves Designer Country Map
APPID-Name: APP08076 Data Driven Size Curves
Purpose: Creates a table with product hierarchy, size and style-color information at the sku level.
    - size_sku_groupid
Variable(s):    {{environment_schema} }T2DL_DAS_SIZE
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_monthly_main
Author(s): Sara Riker
Date Created: 8/5/2024
Date Last Updated: 8/5/2024
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'designer_map{env_suffix}', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.designer_map{env_suffix},
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         price_lvl                  VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,dept_idnt                  INTEGER
        ,dept_desc                  VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,class_idnt                 INTEGER
        ,class_desc                 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,supplier_id                VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,supplier_name              VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,size_1_frame               VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,size_country               VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,size_country_code          BYTEINT
        ,designer_idnt              VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,designer_desc              VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,designer_jda_subclass_sc   INTEGER COMPRESS 
        ,groupid                    VARCHAR(74) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,designer_groupid           VARCHAR(74) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,rcd_update_timestamp       TIMESTAMP(6) WITH TIME ZONE 
    )
UNIQUE PRIMARY INDEX (price_lvl, groupid, size_1_frame);