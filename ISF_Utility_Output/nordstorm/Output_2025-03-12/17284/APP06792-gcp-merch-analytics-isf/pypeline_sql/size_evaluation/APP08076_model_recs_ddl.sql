/*
Name: Size Curves Evaluation Model Recs
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    {environment_schema} T2DL_DAS_SIZE
                {env_suffix} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_eval_vws
Author(s): Sara Riker
Date Created: 6/12/24

Creates: 
    - size_curves_model_rec

Dependancies: 
    - size_curve_location_baseline_class_hist
    - size_curve_baseline_hist
    - size_curve_location_cluster_class_hist
    - po_types
    - supp_size_hierarchy
    - location_model_actuals_vw
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'size_curves_model_rec', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.size_curves_model_rec,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         half_idnt                      INTEGER
        ,channel_num                    INTEGER 
        ,store_num                      INTEGER 
        ,cluster_id                     INTEGER
        ,cluster_name                   VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,dept_idnt                      INTEGER
        ,supplier_frame                 VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,class_frame                    VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,groupid_frame                  VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,frame                          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,size_1_rank                    FLOAT(15)
        ,size_1_id                      VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
        ,bulk_ratio                     FLOAT(15)
        ,cluster_ratio                  FLOAT(15)
        ,update_timestamp               TIMESTAMP(6) WITH TIME ZONE 
    ) 
PRIMARY INDEX(half_idnt, channel_num, cluster_id, dept_idnt, supplier_frame, class_frame, groupid_frame, frame, size_1_rank, size_1_id);