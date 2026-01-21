/*
Name: Size Curves Sales Tables
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - channel_size_sales_data: table with sales related data at the fiscal year half-channel-cc-size1-size2 level. It is used to create bulk level curves. 
    - location_size_sales_data: table with sales related data at the fiscal year half-location-cc-size1-size2 level. it is used to create location level curves
Variable(s):    {{environment_schema}} T2DL_DAS_SIZE
                {{env_suffix}}'' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_monthly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:2/21/2023
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'channel_size_sales_data{env_suffix}', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.channel_size_sales_data{env_suffix},
        FALLBACK,
        NO BEFORE JOURNAL,
        NO AFTER JOURNAL,
        CHECKSUM = DEFAULT,
        DEFAULT MERGEBLOCKRATIO,
        MAP = TD_MAP1
        (
             price_lvl                       VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS('fp','op')
            ,half                            VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS('H1','H2')
            ,half_idnt                       INTEGER
            ,chnl_idnt                       VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC
            ,class_frame                     VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC  NOT NULL
            ,groupid_frame                   VARCHAR (30) CHARACTER SET UNICODE NOT CASESPECIFIC
            ,designer_flag                   INTEGER
            ,cc                              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
            ,include_ind                     SMALLINT COMPRESS (0, 1) NOT NULL
            ,department_id                   INTEGER NOT NULL
            ,class_id                        INTEGER NOT NULL
            ,size_1_id                       VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
            ,size_2_id                       VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC
            ,size_1_rank                     DECIMAL(6,2) COMPRESS
            ,size_2_rank                     DECIMAL(6,2) COMPRESS
            ,jda_subclass_sc                 INTEGER COMPRESS
            ,jda_subclass_c                  INTEGER COMPRESS
            ,supplier_name                   VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
            ,included_cc_per_groupid         INTEGER COMPRESS
            ,included_cc_per_class           INTEGER COMPRESS
            ,cc_rank_groupid                 INTEGER COMPRESS
            ,cc_rank_class                   INTEGER COMPRESS
            ,units_sold                      INTEGER NOT NULL
            ,units_sold_avg_weekly           FLOAT NOT NULL
            ,units_sold_avg_weekly_ratio     FLOAT NOT NULL
            ,cc_wks_to_break                 INTEGER NOT NULL
            ,original_cc_count               INTEGER NOT NULL
            ,used_in_curve_gen_cc_count      INTEGER NOT NULL
            ,rcd_update_timestamp            TIMESTAMP(6) WITH TIME ZONE 
        )
        UNIQUE PRIMARY INDEX (half_idnt, chnl_idnt, groupid_frame, cc, size_1_id, size_2_id);


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'location_size_sales_data{env_suffix}', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.location_size_sales_data{env_suffix},
        FALLBACK,
        NO BEFORE JOURNAL,
        NO AFTER JOURNAL,
        CHECKSUM = DEFAULT,
        DEFAULT MERGEBLOCKRATIO,
        MAP = TD_MAP1
        (
             price_lvl                       VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS('fp','op')
            ,half                            VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS('H1','H2')
            ,half_idnt                       INTEGER
            ,loc_idnt                        INTEGER
            ,class_frame                     VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
            ,groupid_frame                   VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
            ,cc                              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
            ,include_ind                     SMALLINT COMPRESS (0, 1) NOT NULL
            ,department_id                   INTEGER NOT NULL
            ,class_id                        INTEGER NOT NULL
            ,size_1_id                       VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
            ,size_2_id                       VARCHAR(6)
            ,size_1_rank                     DECIMAL(6,2) COMPRESS
            ,size_2_rank                     DECIMAL(6,2) COMPRESS
            ,included_cc_per_groupid         INTEGER COMPRESS
            ,included_cc_per_class           INTEGER COMPRESS
            ,cc_rank_groupid                 INTEGER COMPRESS
            ,cc_rank_class                   INTEGER COMPRESS
            ,units_sold                      INTEGER NOT NULL
            ,units_sold_avg_weekly           FLOAT NOT NULL
            ,units_sold_avg_weekly_ratio     FLOAT NOT NULL
            ,rcd_update_timestamp            TIMESTAMP(6) WITH TIME ZONE 
        )
        UNIQUE PRIMARY INDEX (half_idnt, loc_idnt, groupid_frame, cc, size_1_id, size_2_id);