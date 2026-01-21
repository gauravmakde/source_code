/*
Name: Size Curves Evaluation Actuals
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    {environment_schema} T2DL_DAS_SIZE
                {env_suffix} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_eval_vws
Author(s): Sara Riker
Date Created: 6/12/24

Creates: 
    - size_actuals

Dependancies: 
    - size_eval_cal_week_vw
    - supp_size_hierarchy
    - size_curves_model_rec
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'size_actuals', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.size_actuals,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         channel_num                        INTEGER 
        ,store_num                          INTEGER
        ,ly_ty                              VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,npg_ind                            VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,dept_idnt                          INTEGER 
        ,class_frame                        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,supplier_frame                     VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,size_1_frame                       VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,size_1_rank                        FLOAT(15)
        ,size_1_id                          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,rp_ind                             VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        -- Net Sales
        -- units
        ,net_sales_u                        INTEGER
        ,ds_net_sales_u                     INTEGER
        ,reg_net_sales_u                    INTEGER
        ,pro_net_sales_u                    INTEGER
        ,clr_net_sales_u                    INTEGER
        --retail
        ,net_sales_r                        DECIMAL(38,4)
        ,ds_net_sales_r                     DECIMAL(38,4)
        ,reg_net_sales_r                    DECIMAL(38,4)
        ,pro_net_sales_r                    DECIMAL(38,4)   
        ,clr_net_sales_r                    DECIMAL(38,4)
        --cost
        ,net_sales_c                        DECIMAL(38,4)
        ,ds_net_sales_c                     DECIMAL(38,4)
        ,reg_net_sales_c                    DECIMAL(38,4)
        ,pro_net_sales_c                    DECIMAL(38,4)
        ,clr_net_sales_c                    DECIMAL(38,4)
        --returns
        --unit
        ,returns_u                          INTEGER
        ,ds_returns_u                       INTEGER
        ,reg_returns_u                      INTEGER
        ,pro_returns_u                      INTEGER
        ,clr_returns_u                      INTEGER
        --retail
        ,returns_r                          DECIMAL(38,4)
        ,ds_returns_r                       DECIMAL(38,4)
        ,reg_returns_r                      DECIMAL(38,4)
        ,pro_returns_r                      DECIMAL(38,4)               
        ,clr_returns_r                      DECIMAL(38,4)
        --cost
        ,returns_c                          DECIMAL(38,4)
        ,ds_returns_c                       DECIMAL(38,4)
        ,reg_returns_c                      DECIMAL(38,4)
        ,pro_returns_c                      DECIMAL(38,4)
        ,clr_returns_c                      DECIMAL(38,4)
        --gross sales
        --unit
        ,gross_sales_u                      INTEGER
        ,ds_gross_sales_u                   INTEGER
        ,reg_gross_sales_u                  INTEGER
        ,pro_gross_sales_u                  INTEGER
        ,clr_gross_sales_u                  INTEGER
        --retail
        ,gross_sales_r                      DECIMAL(38,4)
        ,ds_gross_sales_r                   DECIMAL(38,4)
        ,reg_gross_sales_r                  DECIMAL(38,4)
        ,pro_gross_sales_r                  DECIMAL(38,4)
        ,clr_gross_sales_r                  DECIMAL(38,4)   
        --cost
        ,gross_sales_c                      DECIMAL(38,4)
        ,ds_gross_sales_c                   DECIMAL(38,4)
        ,reg_gross_sales_c                  DECIMAL(38,4)
        ,pro_gross_sales_c                  DECIMAL(38,4)
        ,clr_gross_sales_c                  DECIMAL(38,4)
        --product margin
        ,product_margin                     DECIMAL(38,4)
        ,ds_product_margin                  DECIMAL(38,4)
        ,reg_product_margin                 DECIMAL(38,4)
        ,pro_product_margin                 DECIMAL(38,4)
        ,clr_product_margin                 DECIMAL(38,4)
        -- inventory
        ,eoh_u                              INTEGER
        ,reg_eoh_u                          INTEGER
        ,cl_eoh_u                           INTEGER
        ,eoh_r                              DECIMAL(38,4)
        ,reg_eoh_r                          DECIMAL(38,4)
        ,cl_eoh_r                           DECIMAL(38,4)
        ,eoh_c                              DECIMAL(38,4)
        ,reg_eoh_c                          DECIMAL(38,4)
        ,cl_eoh_c                           DECIMAL(38,4)
        -- receipts
        ,receipts_u                         INTEGER
        ,receipts_c                         DECIMAL(38,4)
        ,receipts_r                         DECIMAL(38,4)
        -- DS reciepts 
        ,ds_receipts_u                      INTEGER
        ,ds_receipts_c                      DECIMAL(38,4)
        ,ds_receipts_r                      DECIMAL(38,4)
        -- close outs
        ,close_out_receipt_u                INTEGER
        ,close_out_receipt_c                DECIMAL(38,4)
        ,close_out_receipt_r                DECIMAL(38,4)
        -- sized close outs
        ,close_out_sized_receipt_u          INTEGER
        ,close_out_sized_receipt_c          DECIMAL(38,4)
        ,close_out_sized_receipt_r          DECIMAL(38,4)
        -- casepacks
        ,casepack_receipt_u                 INTEGER
        ,casepack_receipt_c                 DECIMAL(38,4)
        ,casepack_receipt_r                 DECIMAL(38,4)
        -- prepacks
        ,prepack_receipt_u                  INTEGER
        ,prepack_receipt_c                  DECIMAL(38,4)
        ,prepack_receipt_r                  DECIMAL(38,4)
        -- Item
        ,sized_receipt_u                    INTEGER
        ,sized_receipt_c                    DECIMAL(38,4)
        ,sized_receipt_r                    DECIMAL(38,4)
        -- unknown
        ,unknown_receipt_u                  INTEGER
        ,unknown_receipt_c                  DECIMAL(38,4)
        ,unknown_receipt_r                  DECIMAL(38,4)
        -- transfers
        ,transfer_out_u                     INTEGER
        ,transfer_in_u                      INTEGER
        ,racking_last_chance_out_u          INTEGER
        ,flx_in_u                           INTEGER
        ,stock_balance_out_u                INTEGER
        ,stock_balance_in_u                 INTEGER
        ,reserve_stock_in_u                 INTEGER
        ,pah_in_u                           INTEGER
        ,cust_return_out_u                  INTEGER
        ,cust_return_in_u                   INTEGER
        ,cust_transfer_out_u                INTEGER
        ,cust_transfer_in_u                 INTEGER
        ,update_timestamp                   TIMESTAMP(6) WITH TIME ZONE 
    )
PRIMARY INDEX(channel_num, store_num, ly_ty, npg_ind, dept_idnt, class_frame, size_1_frame, size_1_rank, size_1_id)
;