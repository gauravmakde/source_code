-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=assortment_supplier_group_cluster_plan_kafka_to_teradata_10976_tech_nap_merch;
-- Task_Name=load_fact_table;'
-- FOR SESSION VOLATILE;

-- ET;

DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_assortment_suppgrp_cluster_plan_fact AS tgt WHERE EXISTS (
  SELECT
      1
    FROM
      `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_assortment_suppgrp_cluster_plan_ldg AS stg
    WHERE LOWER(tgt.supp_group_id) = LOWER(stg.supp_group_id)
     AND LOWER(tgt.country) = LOWER(stg.country)
     AND LOWER(tgt.cluster_name) = LOWER(stg.cluster_name)
     AND LOWER(tgt.category) = LOWER(stg.category)
     AND tgt.month_num = (
      SELECT
          max(cal.month_idnt)
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS cal
        WHERE LOWER(stg.month_label) = LOWER(cal.month_label))
     AND tgt.alternate_inventory_model = stg.alternate_inventory_model
     AND LOWER(rtrim(stg.last_updated_time_in_millis, ' ')) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS')
     AND tgt.last_updated_time_in_millis < CAST(TRUNC(CAST(stg.last_updated_time_in_millis AS FLOAT64)) as INT64) 
);
--ET;
DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_assortment_suppgrp_cluster_plan_ldg AS stg 
WHERE LOWER(rtrim(stg.last_updated_time_in_millis, ' ')) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS')
 AND EXISTS (
  SELECT
      1
    FROM
      `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_assortment_suppgrp_cluster_plan_fact AS tgt
    inner join (select max(month_idnt) as max_month_idnt,ANY_VALUE(month_label) AS month_label FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw ) AS cal ON LOWER(stg.month_label) = LOWER(cal.month_label)
    WHERE LOWER(tgt.supp_group_id) = LOWER(stg.supp_group_id)
     AND LOWER(tgt.country) = LOWER(stg.country)
     AND LOWER(tgt.cluster_name) = LOWER(stg.cluster_name)
     AND LOWER(tgt.category) = LOWER(stg.category)
     AND tgt.month_num = max_month_idnt
     AND LOWER(tgt.alternate_inventory_model) = LOWER(stg.alternate_inventory_model)
     AND CAST(TRUNC(CAST(stg.last_updated_time_in_millis AS FLOAT64)) as INT64) <= tgt.last_updated_time_in_millis
);
-- ET;


insert into `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_assortment_suppgrp_cluster_plan_fact
(      event_time,
       event_time_tz,
       plan_published_date,
       supp_group_id,
       country,
       cluster_name,
       category,
       month_num,
       month_label,
       last_updated_time_in_millis,
       last_updated_time,
       last_updated_time_tz,
       alternate_inventory_model,
       replenishment_receipt_units,
       replenishment_receipt_retail_currcycd,
       replenishment_receipt_retail_amt,
       replenishment_receipt_cost_currcycd,
       replenishment_receipt_cost_amt,
       replenishment_receipt_less_reserve_units,
       replenishment_receipt_less_reserve_retail_currcycd,
       replenishment_receipt_less_reserve_retail_amt,
       replenishment_receipt_less_reserve_cost_currcycd,
       replenishment_receipt_less_reserve_cost_amt,
       non_replenishment_receipt_units,
       non_replenishment_receipt_retail_currcycd,
       non_replenishment_receipt_retail_amt,
       non_replenishment_receipt_cost_currcycd,
       non_replenishment_receipt_cost_amt,
       non_replenishment_receipt_less_reserve_units,
       non_replenishment_receipt_less_reserve_retail_currcycd,
       non_replenishment_receipt_less_reserve_retail_amt,
       non_replenishment_receipt_less_reserve_cost_currcycd,
       non_replenishment_receipt_less_reserve_cost_amt,
       drop_ship_receipt_units,
       drop_ship_receipt_retail_currcycd,
       drop_ship_receipt_retail_amt,
       drop_ship_receipt_cost_currcycd,
       drop_ship_receipt_cost_amt,
       average_inventory_units,
       average_inventory_retail_currcycd,
       average_inventory_retail_amt,
       average_inventory_cost_currcycd,
       average_inventory_cost_amt,
       beginning_of_period_inventory_units,
       beginning_of_period_inventory_retail_currcycd,
       beginning_of_period_inventory_retail_amt,
       beginning_of_period_inventory_cost_currcycd,
       beginning_of_period_inventory_cost_amt,
       beginning_of_period_inventory_target_units,
       beginning_of_period_inventory_target_retail_currcycd,
       beginning_of_period_inventory_target_retail_amt,
       beginning_of_period_inventory_target_cost_currcycd,
       beginning_of_period_inventory_target_cost_amt,
       return_to_vendor_units,
       return_to_vendor_retail_currcycd,
       return_to_vendor_retail_amt,
       return_to_vendor_cost_currcycd,
       return_to_vendor_cost_amt,
       rack_transfer_units,
       rack_transfer_retail_currcycd,
       rack_transfer_retail_amt,
       rack_transfer_cost_currcycd,
       rack_transfer_cost_amt,
       active_inventory_in_units,
       active_inventory_in_retail_currcycd,
       active_inventory_in_retail_amt,
       active_inventory_in_cost_currcycd,
       active_inventory_in_cost_amt,
       plannable_inventory_units,
       plannable_inventory_retail_currcycd,
       plannable_inventory_retail_amt,
       plannable_inventory_cost_currcycd,
       plannable_inventory_cost_amt,
       plannable_inventory_receipt_less_reserve_units,
       plannable_inventory_receipt_less_reserve_retail_currcycd,
       plannable_inventory_receipt_less_reserve_retail_amt,
       plannable_inventory_receipt_less_reserve_cost_currcycd,
       plannable_inventory_receipt_less_reserve_cost_amt,
       shrink_units,
       shrink_retail_currcycd,
       shrink_retail_amt,
       shrink_cost_currcycd,
       shrink_cost_amt,
       net_sales_units,
       net_sales_retail_currcycd,
       net_sales_retail_amt,
       net_sales_cost_currcycd,
       net_sales_cost_amt,
       demand_units,
       demand_dollar_currcycd,
       demand_dollar_amt,
       gross_sales_units,
       gross_sales_dollar_currcycd,
       gross_sales_dollar_amt,
       returns_units,
       returns_dollar_currcycd,
       returns_dollar_amt,
       product_margin_retail_currcycd,
       product_margin_retail_amt,
       demand_next_two_month_run_rate,
       sales_next_two_month_run_rate,
       dw_sys_load_tmstp
)
SELECT
    CAST(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(EVENT_TIME) AS TIMESTAMP) AS EVENT_TIME,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(EVENT_TIME) AS TIMESTAMP) AS STRING)) AS EVENT_TIME_TZ,
    Cast(PLAN_PUBLISHED_DATE AS DATE) AS PLAN_PUBLISHED_DATE,
    SUPP_GROUP_ID AS SUPP_GROUP_ID,
    COUNTRY AS COUNTRY,
    CLUSTER_NAME AS CLUSTER_NAME,
    CATEGORY AS CATEGORY,
    COALESCE(cal.MONTH_IDNT, -1) AS MONTH_NUM,
    stg.MONTH_LABEL AS MONTH_LABEL,
	  Cast(TRUNC(CAST(LAST_UPDATED_TIME_IN_MILLIS AS FLOAT64)) AS BIGINT) AS LAST_UPDATED_TIME_IN_MILLIS,
    `{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`( Cast(TRUNC(CAST(LAST_UPDATED_TIME_IN_MILLIS AS FLOAT64))  AS BIGINT) ) AS LAST_UPDATED_TIME,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`( Cast(TRUNC(CAST(LAST_UPDATED_TIME_IN_MILLIS AS FLOAT64))  AS BIGINT) ) AS STRING)) AS LAST_UPDATED_TIME_TZ,
    ALTERNATE_INVENTORY_MODEL AS ALTERNATE_INVENTORY_MODEL,
    Cast(REPLENISHMENT_RECEIPT_UNITS AS NUMERIC) AS REPLENISHMENT_RECEIPT_UNITS,
    Coalesce(REPLENISHMENT_RECEIPT_RETAIL_CURRCYCD , 'N/A') AS REPLENISHMENT_RECEIPT_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(REPLENISHMENT_RECEIPT_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(REPLENISHMENT_RECEIPT_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS REPLENISHMENT_RECEIPT_RETAIL_AMT,
    Coalesce(REPLENISHMENT_RECEIPT_COST_CURRCYCD , 'N/A') AS REPLENISHMENT_RECEIPT_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(REPLENISHMENT_RECEIPT_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(REPLENISHMENT_RECEIPT_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS REPLENISHMENT_RECEIPT_COST_AMT,
    Cast(REPLENISHMENT_RECEIPT_LESS_RESERVE_UNITS AS NUMERIC) AS REPLENISHMENT_RECEIPT_LESS_RESERVE_UNITS,
    Coalesce(REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD , 'N/A') AS REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_AMT,
    Coalesce(REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_CURRCYCD , 'N/A') AS REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_AMT,
    Cast(NON_REPLENISHMENT_RECEIPT_UNITS AS NUMERIC) AS NON_REPLENISHMENT_RECEIPT_UNITS,
    Coalesce(NON_REPLENISHMENT_RECEIPT_RETAIL_CURRCYCD , 'N/A') AS NON_REPLENISHMENT_RECEIPT_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(NON_REPLENISHMENT_RECEIPT_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(NON_REPLENISHMENT_RECEIPT_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS NON_REPLENISHMENT_RECEIPT_RETAIL_AMT,
    Coalesce(NON_REPLENISHMENT_RECEIPT_COST_CURRCYCD , 'N/A') AS NON_REPLENISHMENT_RECEIPT_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(NON_REPLENISHMENT_RECEIPT_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(NON_REPLENISHMENT_RECEIPT_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS NON_REPLENISHMENT_RECEIPT_COST_AMT,
    Cast(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_UNITS AS NUMERIC) AS NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_UNITS,
    Coalesce(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD , 'N/A') AS NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_AMT,
    Coalesce(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_CURRCYCD , 'N/A') AS NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_AMT,
    Cast(DROP_SHIP_RECEIPT_UNITS AS NUMERIC) AS DROP_SHIP_RECEIPT_UNITS,
    Coalesce(DROP_SHIP_RECEIPT_RETAIL_CURRCYCD , 'N/A') AS DROP_SHIP_RECEIPT_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(DROP_SHIP_RECEIPT_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(DROP_SHIP_RECEIPT_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS DROP_SHIP_RECEIPT_RETAIL_AMT,
    Coalesce(DROP_SHIP_RECEIPT_COST_CURRCYCD , 'N/A') AS DROP_SHIP_RECEIPT_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(DROP_SHIP_RECEIPT_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(DROP_SHIP_RECEIPT_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS DROP_SHIP_RECEIPT_COST_AMT,
    Cast(AVERAGE_INVENTORY_UNITS AS NUMERIC) AS AVERAGE_INVENTORY_UNITS,
    Coalesce(AVERAGE_INVENTORY_RETAIL_CURRCYCD , 'N/A') AS AVERAGE_INVENTORY_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(AVERAGE_INVENTORY_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(AVERAGE_INVENTORY_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS AVERAGE_INVENTORY_RETAIL_AMT,
    Coalesce(AVERAGE_INVENTORY_COST_CURRCYCD , 'N/A') AS AVERAGE_INVENTORY_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(AVERAGE_INVENTORY_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(AVERAGE_INVENTORY_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS AVERAGE_INVENTORY_COST_AMT,
    Cast(BEGINNING_OF_PERIOD_INVENTORY_UNITS AS NUMERIC) AS BEGINNING_OF_PERIOD_INVENTORY_UNITS,
    Coalesce(BEGINNING_OF_PERIOD_INVENTORY_RETAIL_CURRCYCD , 'N/A') AS BEGINNING_OF_PERIOD_INVENTORY_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(BEGINNING_OF_PERIOD_INVENTORY_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(BEGINNING_OF_PERIOD_INVENTORY_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS BEGINNING_OF_PERIOD_INVENTORY_RETAIL_AMT,
    Coalesce(BEGINNING_OF_PERIOD_INVENTORY_COST_CURRCYCD , 'N/A') AS BEGINNING_OF_PERIOD_INVENTORY_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(BEGINNING_OF_PERIOD_INVENTORY_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(BEGINNING_OF_PERIOD_INVENTORY_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS BEGINNING_OF_PERIOD_INVENTORY_COST_AMT,
    Cast(BEGINNING_OF_PERIOD_INVENTORY_TARGET_UNITS AS NUMERIC) AS BEGINNING_OF_PERIOD_INVENTORY_TARGET_UNITS,
    Coalesce(BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_CURRCYCD , 'N/A') AS BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_AMT,
    Coalesce(BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_CURRCYCD , 'N/A') AS BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_AMT,
    Cast(RETURN_TO_VENDOR_UNITS AS NUMERIC) AS RETURN_TO_VENDOR_UNITS,
    Coalesce(RETURN_TO_VENDOR_RETAIL_CURRCYCD , 'N/A') AS RETURN_TO_VENDOR_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(RETURN_TO_VENDOR_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(RETURN_TO_VENDOR_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS BEGINNING_OF_PERIOD_INVENTORY_RETAIL_AMT,
    Coalesce(RETURN_TO_VENDOR_COST_CURRCYCD , 'N/A') AS RETURN_TO_VENDOR_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(RETURN_TO_VENDOR_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(RETURN_TO_VENDOR_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS BEGINNING_OF_PERIOD_INVENTORY_COST_AMT,
    Cast(RACK_TRANSFER_UNITS AS NUMERIC) AS RACK_TRANSFER_UNITS,
    Coalesce(RACK_TRANSFER_RETAIL_CURRCYCD , 'N/A') AS RACK_TRANSFER_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(RACK_TRANSFER_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(RACK_TRANSFER_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS RACK_TRANSFER_RETAIL_AMT,
    Coalesce(RACK_TRANSFER_COST_CURRCYCD , 'N/A') AS RACK_TRANSFER_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(RACK_TRANSFER_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(RACK_TRANSFER_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS RACK_TRANSFER_COST_AMT,
    Cast(ACTIVE_INVENTORY_IN_UNITS AS NUMERIC) AS ACTIVE_INVENTORY_IN_UNITS,
    Coalesce(ACTIVE_INVENTORY_IN_RETAIL_CURRCYCD , 'N/A') AS ACTIVE_INVENTORY_IN_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(ACTIVE_INVENTORY_IN_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(ACTIVE_INVENTORY_IN_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS ACTIVE_INVENTORY_IN_RETAIL_AMT,
    Coalesce(ACTIVE_INVENTORY_IN_COST_CURRCYCD , 'N/A') AS ACTIVE_INVENTORY_IN_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(ACTIVE_INVENTORY_IN_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(ACTIVE_INVENTORY_IN_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS ACTIVE_INVENTORY_IN_COST_AMT,
    Cast(PLANNABLE_INVENTORY_UNITS AS NUMERIC) AS PLANNABLE_INVENTORY_UNITS,
    Coalesce(PLANNABLE_INVENTORY_RETAIL_CURRCYCD , 'N/A') AS PLANNABLE_INVENTORY_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(PLANNABLE_INVENTORY_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(PLANNABLE_INVENTORY_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS PLANNABLE_INVENTORY_RETAIL_AMT,
    Coalesce(PLANNABLE_INVENTORY_COST_CURRCYCD , 'N/A') AS PLANNABLE_INVENTORY_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(PLANNABLE_INVENTORY_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(PLANNABLE_INVENTORY_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS PLANNABLE_INVENTORY_COST_AMT,
    Cast(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_UNITS AS NUMERIC) AS PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_UNITS,
    Coalesce(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD , 'N/A') AS PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_AMT,
    Coalesce(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_CURRCYCD , 'N/A') AS PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_AMT,
    Cast(SHRINK_UNITS AS NUMERIC) AS SHRINK_UNITS,
    Coalesce(SHRINK_RETAIL_CURRCYCD , 'N/A') AS SHRINK_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(SHRINK_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(SHRINK_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS SHRINK_RETAIL_AMT,
    Coalesce(SHRINK_COST_CURRCYCD , 'N/A') AS SHRINK_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(SHRINK_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(SHRINK_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS SHRINK_COST_AMT,
    Cast(NET_SALES_UNITS AS NUMERIC) AS NET_SALES_UNITS,
    Coalesce(NET_SALES_RETAIL_CURRCYCD , 'N/A') AS NET_SALES_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(NET_SALES_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(NET_SALES_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS NET_SALES_RETAIL_AMT,
    Coalesce(NET_SALES_COST_CURRCYCD , 'N/A') AS NET_SALES_COST_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(NET_SALES_COST_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(NET_SALES_COST_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS NET_SALES_COST_AMT,
    Cast(DEMAND_UNITS AS NUMERIC) AS DEMAND_UNITS,
    Coalesce(DEMAND_DOLLAR_CURRCYCD , 'N/A') AS DEMAND_DOLLAR_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(DEMAND_DOLLAR_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(DEMAND_DOLLAR_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS DEMAND_DOLLAR_AMT,
    Cast(GROSS_SALES_UNITS AS NUMERIC) AS GROSS_SALES_UNITS,
    Coalesce(GROSS_SALES_DOLLAR_CURRCYCD , 'N/A') AS GROSS_SALES_DOLLAR_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(GROSS_SALES_DOLLAR_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(GROSS_SALES_DOLLAR_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS GROSS_SALES_DOLLAR_AMT,
    Cast(RETURNS_UNITS AS NUMERIC) AS RETURNS_UNITS,
    Coalesce(RETURNS_DOLLAR_CURRCYCD , 'N/A') AS RETURNS_DOLLAR_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(RETURNS_DOLLAR_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(RETURNS_DOLLAR_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS RETURNS_DOLLAR_AMT,
    Coalesce(PRODUCT_MARGIN_RETAIL_CURRCYCD , 'N/A') AS PRODUCT_MARGIN_RETAIL_CURRCYCD ,
    Cast(Cast(TRUNC(CAST(PRODUCT_MARGIN_RETAIL_UNITS AS FLOAT64)) AS INTEGER) + (Cast(TRUNC(CAST(PRODUCT_MARGIN_RETAIL_NANOS AS FLOAT64)) AS INTEGER) / Power(10 ,9)) AS NUMERIC ) AS PRODUCT_MARGIN_RETAIL_AMT,
    DEMAND_NEXT_TWO_MONTH_RUN_RATE,
    SALES_NEXT_TWO_MONTH_RUN_RATE,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DW_SYS_LOAD_TMSTP
FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_assortment_suppgrp_cluster_plan_ldg stg
LEFT JOIN (SELECT DISTINCT MONTH_IDNT, MONTH_LABEL FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw) cal
  ON  LOWER(stg.MONTH_LABEL) = LOWER(cal.MONTH_LABEL)
WHERE
    LOWER(SUPP_GROUP_ID) <> LOWER('SUPP_GROUP_ID');
-- ET;

-- COLLECT STATS ON prd.merch_assortment_suppgrp_cluster_plan_fact;

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;
