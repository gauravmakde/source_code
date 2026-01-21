/*
Name: Size Curves SKU Groupid
APPID-Name: APP08076 Data Driven Size Curves
Purpose: Creates a table with product hierarchy, size and style-color information at the sku level.
    - size_sku_groupid
Variable(s):    {{environment_schema}} T2DL_DAS_SIZE
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_monthly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:2/21/2023
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'size_sku_groupid{env_suffix}', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.size_sku_groupid{env_suffix},
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         price_lvl                       VARCHAR(2)
        ,department_description          VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,department_id                   INTEGER NOT NULL
        ,designer_flag                   INTEGER
        ,designer_mapped                 INTEGER
        ,supplier_name                   VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,supplier_id                     VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,class_description               VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,class_id                        INTEGER NOT NULL
        ,subclass_description            VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,subclass_id                     INTEGER NOT NULL
        ,style_id                        VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,vpn                             VARCHAR(72) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,choice_id                       VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,sku_id                          VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,supp_size                       VARCHAR(80) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,size_1_id                       VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,size_1_rank                     DECIMAL(6,2) 
        ,size_1_frame                    VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Numbers', 'Letters')
        ,size_1_description              VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,size_2_id                       VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,size_2_rank                     DECIMAL(6,2) COMPRESS
        ,size_2_description              VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,units_sold                      INTEGER COMPRESS
        ,latest_year                     INTEGER COMPRESS
        ,groupid                         VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,groupid_size                    VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,supplier_class_units_sold       INTEGER COMPRESS
        ,dept_units_sold                 INTEGER COMPRESS
        ,jda_subclass_sc                 INTEGER COMPRESS
        ,jda_subclass_c                  INTEGER COMPRESS
        ,rcd_update_timestamp            TIMESTAMP(6) WITH TIME ZONE
    )
    UNIQUE PRIMARY INDEX (price_lvl, sku_id);
