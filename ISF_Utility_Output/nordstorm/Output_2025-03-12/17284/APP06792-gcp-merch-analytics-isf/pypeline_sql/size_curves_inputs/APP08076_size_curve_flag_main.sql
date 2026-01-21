/*
Name: Size Curves Flag
APPID-Name: APP08076 Data Driven Size Curves
Purpose: Updates a table with flags indicating whether size curves should be generated for a given fiscal half-price level-department-class and fiscal half-price level-department-supplier-class
    - size_curves_flags_scenarios: stores all scenarios of supplier and class units
    - size_curves_flags: final table with set supplier/class unit cutoffs to be ingetsted into MLP
Variable(s):    {{environment_schema}} T2DL_DAS_SIZE
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
                {{fp_class_units}} integer for execption cut off for number of units a class must have sold to be included for Nordstrom
                {{fp_supp_units}} integer for execption cut off for number of units a Supplier must have sold to be included for Nordstrom
                {{op_class_units}} integer for execption cut off for number of units a class must have sold to be included for Nordstrom Rack
                {{op_supp_units}} integer for execption cut off for number of units a Supplier must have sold to be included for Nordstrom Rack

DAG: APP08076_size_curves_monthly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:2/21/2023
*/


-- begin
-- DROP TABLE total_units_sold;
CREATE MULTISET VOLATILE TABLE total_units_sold AS (
    SELECT
         price_lvl
        ,half
        ,half_idnt
        ,chnl_idnt
        ,groupid_frame
        ,department_id 
        ,class_id 
        ,strtok(groupid_frame, '~~', 2) AS supplier_id 
        ,supplier_name 
        ,designer_flag
        ,SUM(units_sold) AS units_sold
    FROM {environment_schema}.channel_size_sales_data{env_suffix}
    GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA
PRIMARY INDEX (price_lvl, half_idnt, chnl_idnt, groupid_frame)
ON COMMIT PRESERVE ROWS;


-- calculate total units sold on year half-price lvl-department level
-- DROP TABLE dept_sum;
CREATE MULTISET VOLATILE TABLE dept_sum AS (
    SELECT
         half
        ,price_lvl
        ,department_id
        ,SUM(units_sold) AS units_sold_dept 
    FROM total_units_sold
    GROUP BY 1,2,3
) WITH DATA
PRIMARY INDEX (half, price_lvl, department_id)
ON COMMIT PRESERVE ROWS;

-- DROP TABLE class_sum;
CREATE MULTISET VOLATILE TABLE class_sum AS (
-- for each department, sort class by total units_sold and calculate the cumulative units sold
-- this step is needed to apply 95-5 rule later
    SELECT 
         half
        ,price_lvl
        ,department_id
        ,designer_flag
        ,class_id
        ,units_sold_class
        ,SUM(units_sold_class) OVER (PARTITION BY half, price_lvl, department_id ORDER BY units_sold_class DESC ROWS UNBOUNDED PRECEDING) AS units_sold_cum
    FROM (
    -- calculate total units sold on year half-price lvl-department-class level
            SELECT  
                 half
                ,price_lvl
                ,department_id
                ,designer_flag
                ,class_id
                ,SUM(units_sold) AS units_sold_class 
            FROM total_units_sold
            GROUP BY 1,2,3,4,5
        ) class_sales
) WITH DATA
PRIMARY INDEX (half, price_lvl, department_id, class_id)
ON COMMIT PRESERVE ROWS;


-- apply class 95-5 rule (step 3)

-- DROP TABLE class_results;
CREATE MULTISET VOLATILE TABLE class_results AS (
    SELECT
         half
        ,price_lvl
        ,department_id
        ,class_id
        ,units_sold_class
        ,units_sold_cum
        ,units_sold_dept
        ,excl_ind
        ,group_rank
        ,CASE WHEN excl_ind = 0 
                OR (excl_ind = 1 AND group_rank = 1) 
                OR (price_lvl = 'op' AND units_sold_class > {op_class_units}) -- 500
                OR (price_lvl = 'fp' AND designer_flag = 1 AND units_sold_class > 150) 
                OR (price_lvl = 'fp' AND units_sold_class > {fp_class_units}) THEN 1
              ELSE 0 
              END AS generate_curve_ind_class
    FROM (
    -- step 2: generate group_rank on half-price_lvl-department_id-excl_ind level (see below)
        SELECT
             half
            ,price_lvl
            ,department_id
            ,class_id
            ,units_sold_class
            ,units_sold_cum
            ,units_sold_dept
            ,excl_ind
            ,designer_flag
            ,ROW_NUMBER() OVER (PARTITION BY half, price_lvl, department_id, excl_ind ORDER BY units_sold_class DESC) AS group_rank

        FROM (
        -- step 1: join class_sum & dept_sum and generate excl_ind (see example above)
                SELECT  
                     cl.half
                    ,cl.price_lvl
                    ,cl.department_id
                    ,cl.class_id
                    ,cl.units_sold_class
                    ,cl.units_sold_cum
                    ,dep.units_sold_dept
                    ,designer_flag
                    ,CASE WHEN CAST(cl.units_sold_cum AS FLOAT) / NULLIF(CAST(dep.units_sold_dept AS FLOAT),0) > 0.95 THEN 1 
                          ELSE 0
                          END AS excl_ind
                FROM class_sum cl
                JOIN dept_sum dep
                  ON cl.half = dep.half
                 AND cl.price_lvl = dep.price_lvl
                 AND cl.department_id = dep.department_id
            ) class_flag
        ) ranks
)WITH DATA
PRIMARY INDEX (half, price_lvl, department_id, class_id)
ON COMMIT PRESERVE ROWS;



-- for the supplier level curve, if the units sold of the supplier are greater than 1000, then we include the supplier
-- DROP TABLE supplier_sum;
CREATE MULTISET VOLATILE TABLE supplier_sum AS (
    SELECT
         half
        ,price_lvl
        ,department_id
        ,class_id
        ,supplier_id
        ,supplier_name
        ,units_sold_supplier
        ,CASE WHEN price_lvl = 'fp' AND designer_flag = 1 AND units_sold_supplier > 200 THEN 1
              WHEN price_lvl = 'op' AND units_sold_supplier > {op_supp_units} THEN 1 -- 1000
              WHEN price_lvl = 'fp' AND units_sold_supplier > {fp_supp_units} THEN 1
              ELSE 0 
              END AS generate_curve_ind_supplier
    FROM (
-- calculate total units sold on year half-price lvl-department-class-supplier level
            SELECT
                 half
                ,price_lvl
                ,department_id
                ,class_id
                ,supplier_id
                ,supplier_name
                ,designer_flag
                ,SUM(units_sold) AS units_sold_supplier
            FROM total_units_sold
            GROUP BY 1,2,3,4,5,6,7
        ) supplier_sales
) WITH DATA
PRIMARY INDEX (half, price_lvl, department_id, class_id, supplier_id)
ON COMMIT PRESERVE ROWS;


DELETE FROM {environment_schema}.size_curves_flags{env_suffix} ALL;
INSERT INTO {environment_schema}.size_curves_flags{env_suffix}
SELECT
     sup.half
    ,sup.price_lvl
    ,sup.department_id
    ,sup.class_id
    ,sup.supplier_id
    ,sup.supplier_name
    ,sup.units_sold_supplier
    ,cl.units_sold_class
    ,cl.generate_curve_ind_class
    ,sup.generate_curve_ind_supplier
    ,{op_class_units} AS op_class_units_param
    ,{op_supp_units} AS op_supp_units_param
    ,{fp_class_units} AS fp_class_units_param
    ,{fp_supp_units} AS fp_supp_units_param
    ,CURRENT_TIMESTAMP AS rcd_update_timestamp
FROM supplier_sum sup
JOIN class_results cl
  ON sup.half = cl.half
 AND sup.price_lvl = cl.price_lvl
 AND sup.department_id = cl.department_id
 AND sup.class_id = cl.class_id
;

COLLECT STATS 
     COLUMN(half, price_lvl, department_id, class_id, supplier_id) 
    ON {environment_schema}.size_curves_flags{env_suffix};