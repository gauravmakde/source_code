CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'accuracy', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.accuracy,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         eval_month_idnt                INTEGER
        ,store_num                      INTEGER 
        ,store_name                     VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,channel_num                    INTEGER 
        ,banner                         VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,selling_channel                VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,store_address_state            CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,store_dma_desc                 VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,store_location_latitude        FLOAT(15)
        ,store_location_longitude       FLOAT(15)
        ,region_desc                    VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,region_short_desc              VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,division                       VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,subdivision                    VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,department                     VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,sales_u_dept_loc               INTEGER
        ,sales_r_dept_loc               INTEGER
        ,eoh_dept_loc                   INTEGER 
        ,receipts_dept_loc              INTEGER 
        ,reg_sales_dept_loc             INTEGER
        ,reg_eoh_dept_loc               INTEGER 
        ,sales_u_dept_bulk              INTEGER
        ,sales_r_dept_bulk              INTEGER
        ,eoh_dept_bulk                  INTEGER 
        ,receipts_dept_bulk             INTEGER 
        ,reg_sales_dept_bulk            INTEGER
        ,reg_eoh_dept_bulk              INTEGER 
        ,wmae_baseline_sls_bulk         FLOAT(15)
        ,wmae_baseline_sls_loc          FLOAT(15)
        ,wmae_baseline_rcpt_bulk        FLOAT(15)
        ,wmae_baseline_rcpt_loc         FLOAT(15)
        ,wmae_sales_bulk                FLOAT(15)
        ,wmae_sales_loc                 FLOAT(15)
        ,wmae_reg_sales_bulk            FLOAT(15)
        ,wmae_reg_sales_loc             FLOAT(15)
        ,wmae_eoh_bulk                  FLOAT(15)
        ,wmae_eoh_loc                   FLOAT(15)
        ,wmae_receipts_bulk             FLOAT(15)
        ,wmae_receipts_loc              FLOAT(15)
        ,wmae_sales_rcpts_bulk          FLOAT(15)
        ,wmae_sales_rcpts_loc           FLOAT(15)
        ,wmae_reg_sales_rcpts_bulk      FLOAT(15)
        ,wmae_reg_sales_rcpts_loc       FLOAT(15)
        ,wmae_sales_eoh_bulk            FLOAT(15)
        ,wmae_sales_eoh_loc             FLOAT(15)
        ,wmae_reg_sales_eoh_bulk        FLOAT(15)
        ,wmae_reg_sales_eoh_loc         FLOAT(15)
        ,wmae_reg_eoh_bulk              FLOAT(15)
        ,wmae_reg_eoh_loc               FLOAT(15)
        ,update_timestamp               TIMESTAMP(6) WITH TIME ZONE 
    ) 
UNIQUE PRIMARY INDEX (eval_month_idnt, store_num, department)
;