/*
Name: Size Curves Flag
APPID-Name: APP08076 Data Driven Size Curves
Purpose: Creates a table with flags indicating whether size curves should be generated for a given fiscal half-price level-department-class and fiscal half-price level-department-supplier-class
    - size_curves_flags_scenarios: stores all scenarios of supplier and class units
    - size_curves_flags: final table with set supplier/class unit cutoffs to be ingetsted into MLP
Variable(s):    {{environment_schema}} T2DL_DAS_SIZE
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_monthly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:2/21/2023
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'size_curves_flags_scenarios{env_suffix}', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.size_curves_flags_scenarios{env_suffix},
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         half                            VARCHAR(2)
        ,price_lvl                       VARCHAR(2)
        ,department_id                   INTEGER
        ,class_id                        INTEGER
        ,supplier_id                     VARCHAR(10)
        ,supplier_name                   VARCHAR(50) COMPRESS
        ,units_sold_supplier             INTEGER NOT NULL
        ,units_sold_class                INTEGER NOT NULL
        ,generate_curve_ind_class        SMALLINT COMPRESS (0, 1)
        ,generate_curve_ind_supplier     SMALLINT COMPRESS (0, 1)
        ,op_class_units_param            INTEGER 
        ,op_supp_units_param             INTEGER        
        ,fp_class_units_param            INTEGER 
        ,fp_supp_units_param             INTEGER
        ,rcd_update_timestamp            TIMESTAMP(6) WITH TIME ZONE 
    )
    PRIMARY INDEX (half, price_lvl, department_id, class_id, supplier_id);


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'size_curves_flags{env_suffix}', OUT_RETURN_MSG);
CREATE TABLE {environment_schema}.size_curves_flags{env_suffix},
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
         half                            VARCHAR(2)
        ,price_lvl                       VARCHAR(2)
        ,department_id                   INTEGER
        ,class_id                        INTEGER
        ,supplier_id                     VARCHAR(10)
        ,supplier_name                   VARCHAR(50) COMPRESS
        ,units_sold_supplier             INTEGER NOT NULL
        ,units_sold_class                INTEGER NOT NULL
        ,generate_curve_ind_class        SMALLINT COMPRESS (0, 1)
        ,generate_curve_ind_supplier     SMALLINT COMPRESS (0, 1)
        ,op_class_units_param            INTEGER 
        ,op_supp_units_param             INTEGER        
        ,fp_class_units_param            INTEGER 
        ,fp_supp_units_param             INTEGER
        ,rcd_update_timestamp            TIMESTAMP(6) WITH TIME ZONE 
    )
    UNIQUE PRIMARY INDEX (half, price_lvl, department_id, class_id, supplier_id);