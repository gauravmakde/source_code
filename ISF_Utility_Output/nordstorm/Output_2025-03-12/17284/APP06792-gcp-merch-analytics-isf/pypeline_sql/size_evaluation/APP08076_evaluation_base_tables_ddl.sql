

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'size_eval_cal', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.size_eval_cal,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         day_date                       DATE
        ,week_idnt                      INTEGER
        ,week_label                     VARCHAR(17) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,ly_week_num_realigned          INTEGER
        ,ly_month_idnt_realigned        INTEGER
        ,ly_half_idnt_realigned         INTEGER
        ,week_end_date                  BYTEINT
        ,month_idnt                     INTEGER
        ,month_label                    VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,month_end_date                 BYTEINT
        ,month_end_week_idnt            BYTEINT
        ,quarter_idnt                   INTEGER
        ,quarter_label                  VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,quarter_end_date               BYTEINT
        ,quarter_end_week_idnt          INTEGER
        ,quarter_end_month_idnt         BYTEINT
        ,half_idnt                      INTEGER
        ,half_label                     VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,half_end_quarter_idnt          BYTEINT
        ,fiscal_year_num                INTEGER     
        ,hist_ind                       BYTEINT
        ,current_month                  BYTEINT
        ,last_completed_week            BYTEINT
        ,last_completed_month           BYTEINT
        ,last_completed_3_months        BYTEINT
        ,next_3_months                  BYTEINT
        ,update_timestamp               TIMESTAMP(6) WITH TIME ZONE 
    )
UNIQUE PRIMARY INDEX (day_date, week_idnt, month_idnt, quarter_idnt, half_idnt, ly_week_num_realigned)
;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'locations', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.locations,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         store_num                      INTEGER 
        ,store_name                     VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,channel_num                    INTEGER 
        ,banner                         VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,selling_channel                VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,store_address_state            CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,store_dma_desc                 VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,store_location_latitude        FLOAT(15)
        ,store_location_longitude       FLOAT(15)
        ,region_desc                    VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,region_short_desc              VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,update_timestamp               TIMESTAMP(6) WITH TIME ZONE 
    ) 
UNIQUE PRIMARY INDEX (store_num)
;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'supp_size_hierarchy', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.supp_size_hierarchy,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         sku_idnt                       VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,choice_id                      VARCHAR(22) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,supplier_idnt                  VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,supplier_name                  VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,supp_part_num                  VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,vpn                            VARCHAR(132) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,style_id                       VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,div_idnt                       INTEGER 
        ,div_desc                       VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,grp_idnt                       INTEGER
        ,grp_desc                       VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,dept_idnt                      INTEGER
        ,dept_desc                      VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,class_idnt                     INTEGER 
        ,class_desc                     VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,sbclass_idnt                   INTEGER
        ,sbclass_desc                   VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,npg_ind                        CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,supp_size                      VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,size_1_id                      VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,size_1_desc                    VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,size_1_rank                    DECIMAL(22,2)
        ,size_1_frame                   VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,size_2_id                      VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC 
        ,size_2_desc                    VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,size_2_rank                    DECIMAL(22,2)
        ,update_timestamp               TIMESTAMP(6) WITH TIME ZONE 
    )
UNIQUE PRIMARY INDEX (sku_idnt, dept_idnt)
;