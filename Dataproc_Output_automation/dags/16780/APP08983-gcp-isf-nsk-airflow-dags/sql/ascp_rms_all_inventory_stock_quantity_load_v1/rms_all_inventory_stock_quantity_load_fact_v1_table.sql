/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/
 
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT',
   '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT',
   'ascp_rms_all_inventory_stock_quantity_load_v1',
   'ldg_to_fact_5',
   0,'LOAD_START', '',
   current_datetime('PST8PDT'),
   'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1');

/***********************************************************************************
-- DELETE failed records from staging table
************************************************************************************/


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.INVENTORY_STOCK_QUANTITY_LOGICAL_V1_LDG WHERE inventory_stock_quantity_logical_v1_ldg.value_updated_time IS NULL
 OR inventory_stock_quantity_logical_v1_ldg.rms_sku_id IS NULL
 OR inventory_stock_quantity_logical_v1_ldg.location_id IS NULL
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.value_updated_time) = ''
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.rms_sku_id) = ''
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.location_id) = ''
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.value_updated_time) = '""'
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.rms_sku_id) = '""'
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.location_id) = '""'
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.value_updated_time) = 'NULL'
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.rms_sku_id) = 'NULL'
 OR rtrim(inventory_stock_quantity_logical_v1_ldg.location_id) = 'NULL'			   
 OR SAFE_CAST(replace(replace(inventory_stock_quantity_logical_v1_ldg.value_updated_time, 'Z', '+00:00'), 'T', ' ') AS TIMESTAMP) IS NULL
 OR inventory_stock_quantity_logical_v1_ldg.last_received_date IS NOT NULL
 AND rtrim(inventory_stock_quantity_logical_v1_ldg.last_received_date) <> ''
 AND SAFE_CAST(concat(inventory_stock_quantity_logical_v1_ldg.last_received_date, '.000+00:00') AS TIMESTAMP) IS NULL;


/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT',
   '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT',
   'ascp_rms_all_inventory_stock_quantity_load_v1',
   'ldg_to_fact_5',
   1,'INTERMEDIATE', 'DELETE failed records from staging table',
   current_datetime('PST8PDT'),
   'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1');


/***********************************************************************************
-- Add old records to error table
************************************************************************************/

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_logical_ldg_v1_err
  SELECT
cast(ldg.value_updated_time as string),
cast(ldg.rms_sku_id as string),
cast(ldg.location_id as string),
cast(ldg.immediately_sellable_qty as string),
cast(ldg.stock_on_hand_qty as string),
cast(ldg.unavailable_qty as string),
cast(ldg.cust_order_return as string),
cast(ldg.pre_inspect_customer_return as string),
cast(ldg.unusable_qty as string),
cast(ldg.rtv_reqst_resrv as string),
cast(ldg.met_reqst_resrv as string),
cast(ldg.tc_preview as string),
cast(ldg.tc_clubhouse as string),
cast(ldg.tc_mini_1 as string),
cast(ldg.tc_mini_2 as string),
cast(ldg.tc_mini_3 as string),
cast(ldg.problem as string),
cast(ldg.damaged_return as string),
cast(ldg.damaged_cosmetic_return as string),
cast(ldg.ertm_holds as string),
cast(ldg.nd_unavailable_qty as string),
cast(ldg.pb_holds_qty as string),
cast(ldg.com_co_holds as string),
cast(ldg.wm_holds as string),
cast(ldg.store_reserve as string),
cast(ldg.tc_holds as string),
cast(ldg.returns_holds as string),
cast(ldg.fp_holds as string),
cast(ldg.transfers_reserve_qty as string),
cast(ldg.return_to_vendor_qty as string),
cast(ldg.in_transit_qty as string),
cast(ldg.store_transfer_reserved_qty as string),
cast(ldg.store_transfer_expected_qty as string),
cast(ldg.back_order_reserve_qty as string),
cast(ldg.oms_back_order_reserve_qty as string),
cast(ldg.on_replenishment as string),
cast(ldg.last_received_date as string),
cast(ldg.location_type as string),
cast(ldg.epm_id as string),
cast(ldg.upc as string),
cast(ldg.return_inspection_qty as string),
cast(ldg.receiving_qty as string),
cast(ldg.damage_qty as string),
cast(ldg.hold_qty as string),
cast(ldg.csn as string),
cast(ldg.op_seq_no as string),
cast(ldg.position_no as string),
cast(ldg.dw_sys_load_tmstp as string)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_logical_v1_fact AS ldg
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_logical_v1_fact
    ON ldg.rms_sku_id = inventory_stock_quantity_logical_v1_fact.rms_sku_id
     AND ldg.location_id = inventory_stock_quantity_logical_v1_fact.location_id
     AND coalesce(ldg.location_type , '0') = coalesce(inventory_stock_quantity_logical_v1_fact.location_type , '0')
     AND (CAST(trim(cast(ldg.csn as string)) as BIGNUMERIC) < inventory_stock_quantity_logical_v1_fact.csn
     OR CAST(trim(cast(ldg.csn as string)) as BIGNUMERIC) = inventory_stock_quantity_logical_v1_fact.csn
     AND CAST(trim(cast(ldg.op_seq_no as string)) as BIGNUMERIC) < inventory_stock_quantity_logical_v1_fact.op_seq_no
     OR CAST(trim(cast(ldg.csn as string)) as BIGNUMERIC) = inventory_stock_quantity_logical_v1_fact.csn
     AND CAST(trim(cast(ldg.op_seq_no as string)) as BIGNUMERIC) = inventory_stock_quantity_logical_v1_fact.op_seq_no
     AND CAST(trim(cast(ldg.position_no as string)) as BIGNUMERIC) < inventory_stock_quantity_logical_v1_fact.position_no)
;



/***********************************************************************************
-- Delete old records from staging table
************************************************************************************/

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.INVENTORY_STOCK_QUANTITY_LOGICAL_V1_LDG AS ldg WHERE EXISTS (
  SELECT
      1
    FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_logical_v1_fact  as inventory_stock_quantity_logical_v1_fact
    WHERE ldg.rms_sku_id = inventory_stock_quantity_logical_v1_fact.rms_sku_id
     AND ldg.location_id = inventory_stock_quantity_logical_v1_fact.location_id
     AND coalesce(ldg.location_type , '0') = coalesce(inventory_stock_quantity_logical_v1_fact.location_type, '0')
     AND (CAST(trim(cast(ldg.csn as string) ) as BIGNUMERIC) < inventory_stock_quantity_logical_v1_fact.csn
     OR CAST(trim(cast(ldg.csn as string) ) as BIGNUMERIC) = inventory_stock_quantity_logical_v1_fact.csn
     AND CAST(trim(cast(ldg.op_seq_no as string) ) as BIGNUMERIC) < inventory_stock_quantity_logical_v1_fact.op_seq_no
     OR CAST(trim(cast(ldg.csn as string) ) as BIGNUMERIC) = inventory_stock_quantity_logical_v1_fact.csn
     AND CAST(trim(cast(ldg.op_seq_no as string) ) as BIGNUMERIC) = inventory_stock_quantity_logical_v1_fact.op_seq_no
     AND CAST(trim(cast(ldg.position_no as string) ) as BIGNUMERIC) < inventory_stock_quantity_logical_v1_fact.position_no)
);


/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT',
   '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT',
   'ascp_rms_all_inventory_stock_quantity_load_v1',
   'ldg_to_fact_5',
   2,'INTERMEDIATE', 'DELETE old records from staging table',
   current_datetime('PST8PDT'),
   'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1');

/***********************************************************************************
-- Merge into the target fact table
************************************************************************************/



MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_logical_v1_fact AS fact USING (	   
  SELECT
      CAST(trim(replace(replace(value_updated_time, 'Z', '+00:00'), 'T', ' ')) as TIMESTAMP) AS value_updated_time,
      `{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE(trim(replace(replace(collate(value_updated_time,''), 'Z', '+00:00'), 'T', ' '))) AS value_updated_time_tz,
      rms_sku_id,
      location_id,
      CAST(TRUNC(CAST(trim(immediately_sellable_qty)AS FLOAT64)) as INT64) AS immediately_sellable_qty,
       CAST(TRUNC(CAST(trim(stock_on_hand_qty) AS FLOAT64)) as INT64) AS stock_on_hand_qty,
      CAST(TRUNC(CAST(trim(unavailable_qty) AS FLOAT64)) as INT64) AS unavailable_qty,
      CAST(TRUNC(CAST(trim(cust_order_return) AS FLOAT64)) as INT64) AS cust_order_return,
      CAST(TRUNC(CAST(trim(pre_inspect_customer_return) AS FLOAT64)) as INT64) AS pre_inspect_customer_return,
      CAST(TRUNC(CAST(trim(unusable_qty) AS FLOAT64)) as INT64) AS unusable_qty,
      CAST(TRUNC(CAST(trim(rtv_reqst_resrv) AS FLOAT64)) as INT64) AS rtv_reqst_resrv,
      CAST(TRUNC(CAST(trim(met_reqst_resrv) AS FLOAT64)) as INT64) AS met_reqst_resrv,
      CAST(TRUNC(CAST(trim(tc_preview) AS FLOAT64)) as INT64) AS tc_preview,
      CAST(TRUNC(CAST(trim(tc_clubhouse) AS FLOAT64)) as INT64) AS tc_clubhouse,
      CAST(TRUNC(CAST(trim(tc_mini_1) AS FLOAT64)) as INT64) AS tc_mini_1,
      CAST(TRUNC(CAST(trim(tc_mini_2) AS FLOAT64)) as INT64) AS tc_mini_2,
      CAST(TRUNC(CAST(trim(tc_mini_3) AS FLOAT64)) as INT64) AS tc_mini_3,
      CAST(TRUNC(CAST(trim(problem) AS FLOAT64)) as INT64) AS problem,
      CAST(TRUNC(CAST(trim(damaged_return) AS FLOAT64)) as INT64) AS damaged_return,
      CAST(TRUNC(CAST(trim(damaged_cosmetic_return) AS FLOAT64)) as INT64) AS damaged_cosmetic_return,
      CAST(TRUNC(CAST(trim(ertm_holds) AS FLOAT64)) as INT64) AS ertm_holds,
      CAST(TRUNC(CAST(trim(nd_unavailable_qty) AS FLOAT64)) as INT64) AS nd_unavailable_qty,
      CAST(TRUNC(CAST(trim(pb_holds_qty) AS FLOAT64)) as INT64) AS pb_holds_qty,
      CAST(TRUNC(CAST(trim(com_co_holds) AS FLOAT64)) as INT64) AS com_co_holds,
      CAST(TRUNC(CAST(trim(wm_holds) AS FLOAT64)) as INT64) AS wm_holds,
      CAST(TRUNC(CAST(trim(store_reserve) AS FLOAT64)) as INT64) AS store_reserve,
      CAST(TRUNC(CAST(trim(tc_holds) AS FLOAT64)) as INT64) AS tc_holds,
      CAST(TRUNC(CAST(trim(returns_holds) AS FLOAT64)) as INT64) AS returns_holds,
      CAST(TRUNC(CAST(trim(fp_holds) AS FLOAT64)) as INT64) AS fp_holds,
      CAST(TRUNC(CAST(trim(transfers_reserve_qty) AS FLOAT64)) as INT64) AS transfers_reserve_qty,
      CAST(TRUNC(CAST(trim(return_to_vendor_qty) AS FLOAT64)) as INT64) AS return_to_vendor_qty,
      CAST(TRUNC(CAST(trim(in_transit_qty) AS FLOAT64)) as INT64) AS in_transit_qty,
      CAST(TRUNC(CAST(trim(store_transfer_reserved_qty) AS FLOAT64)) as INT64) AS store_transfer_reserved_qty,
      CAST(TRUNC(CAST(trim(store_transfer_expected_qty) AS FLOAT64)) as INT64) AS store_transfer_expected_qty,
      CAST(TRUNC(CAST(trim(back_order_reserve_qty) AS FLOAT64)) as INT64) AS back_order_reserve_qty,
      CAST(TRUNC(CAST(trim(oms_back_order_reserve_qty) AS FLOAT64)) as INT64) AS oms_back_order_reserve_qty,
      on_replenishment,
      CASE
        WHEN last_received_date IS NULL
         OR rtrim(last_received_date) = '' THEN CAST(NULL as TIMESTAMP)
        ELSE CAST(trim(concat(last_received_date, '+00:00')) as TIMESTAMP)
      END AS last_received_date,
      case when last_received_date is null or last_received_date = '' then NULL
      else  '+00:00' 
      end as last_received_date_tz,
      location_type,
      epm_id,
      upc,
      CAST(TRUNC(CAST(trim(return_inspection_qty) AS FLOAT64)) as INT64) AS return_inspection_qty,
      CAST(TRUNC(CAST(trim(receiving_qty) AS FLOAT64)) as INT64) AS receiving_qty,
      CAST(TRUNC(CAST(trim(damage_qty) AS FLOAT64)) as INT64) AS damage_qty,
      CAST(TRUNC(CAST(trim(hold_qty) AS FLOAT64)) as INT64) AS hold_qty,
      CAST(trim(csn) as BIGNUMERIC) AS csn,
      CAST(trim(op_seq_no) as BIGNUMERIC) AS op_seq_no,
      CAST(trim(position_no) as BIGNUMERIC) AS position_no,
      (
        SELECT
            batch_id
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
          WHERE rtrim(subject_area_nm) = 'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1'
      ) AS dw_batch_id,
      (
        SELECT
            curr_batch_date
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
          WHERE rtrim(subject_area_nm) = 'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1'
      ) AS dw_batch_date,
      timestamp(current_datetime('PST8PDT'))  as dw_sys_load_tmstp,
      `{{params.gcp_project_id}}`.jwn_udf.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
      timestamp(current_datetime('PST8PDT'))  as dw_sys_updt_tmstp,
      `{{params.gcp_project_id}}`.jwn_udf.DEFAULT_TZ_PST() as dw_sys_updt_tmstp_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.INVENTORY_STOCK_QUANTITY_LOGICAL_V1_LDG
    QUALIFY row_number() OVER (PARTITION BY rms_sku_id, location_id, location_type ORDER BY csn DESC, op_seq_no DESC, position_no DESC) = 1
) AS ldg
ON ldg.rms_sku_id = fact.rms_sku_id
 AND ldg.location_id = fact.location_id
 AND (ldg.location_type = fact.location_type
 OR ldg.location_type IS NULL
 AND fact.location_type IS NULL)
   WHEN MATCHED THEN UPDATE SET
		  
    value_updated_time = ldg.value_updated_time, 
    value_updated_time_tz = ldg.value_updated_time_tz,
    immediately_sellable_qty = ldg.immediately_sellable_qty, 
    stock_on_hand_qty = ldg.stock_on_hand_qty, 
    unavailable_qty = ldg.unavailable_qty, 
    cust_order_return = ldg.cust_order_return, 
    pre_inspect_customer_return = ldg.pre_inspect_customer_return, 
    unusable_qty = ldg.unusable_qty, 
    rtv_reqst_resrv = ldg.rtv_reqst_resrv, 
    met_reqst_resrv = ldg.met_reqst_resrv, 
    tc_preview = ldg.tc_preview, 
    tc_clubhouse = ldg.tc_clubhouse, 
    tc_mini_1 = ldg.tc_mini_1, 
    tc_mini_2 = ldg.tc_mini_2, 
    tc_mini_3 = ldg.tc_mini_3, 
    problem = ldg.problem, 
    damaged_return = ldg.damaged_return, 
    damaged_cosmetic_return = ldg.damaged_cosmetic_return, 
    ertm_holds = ldg.ertm_holds, 
    nd_unavailable_qty = ldg.nd_unavailable_qty, 
    pb_holds_qty = ldg.pb_holds_qty, 
    com_co_holds = ldg.com_co_holds, 
    wm_holds = ldg.wm_holds, 
    store_reserve = ldg.store_reserve, 
    tc_holds = ldg.tc_holds, 
    returns_holds = ldg.returns_holds, 
    fp_holds = ldg.fp_holds, 
    transfers_reserve_qty = ldg.transfers_reserve_qty, 
    return_to_vendor_qty = ldg.return_to_vendor_qty, 
    in_transit_qty = ldg.in_transit_qty, 
    store_transfer_reserved_qty = ldg.store_transfer_reserved_qty, 
    store_transfer_expected_qty = ldg.store_transfer_expected_qty, 
    back_order_reserve_qty = ldg.back_order_reserve_qty, 
    oms_back_order_reserve_qty = ldg.oms_back_order_reserve_qty, 
    on_replenishment = ldg.on_replenishment, 
    last_received_date = ldg.last_received_date, 
    last_received_date_tz = ldg.last_received_date_tz,
    location_type = ldg.location_type, 
    epm_id = ldg.epm_id, 
    upc = ldg.upc, 
    return_inspection_qty = ldg.return_inspection_qty, 
    receiving_qty = ldg.receiving_qty, 
    damage_qty = ldg.damage_qty, 
    hold_qty = ldg.hold_qty, 
    csn = CAST(trim( cast(ldg.csn as string)) as BIGNUMERIC), 
    op_seq_no = CAST(trim( cast(ldg.op_seq_no as string)) as BIGNUMERIC), 
    position_no = CAST(trim( cast(ldg.position_no as string)) as BIGNUMERIC), 
    dw_batch_id = ldg.dw_batch_id, 
    dw_batch_date = ldg.dw_batch_date, 
    dw_sys_updt_tmstp = ldg.dw_sys_updt_tmstp ,
    dw_sys_updt_tmstp_tz =ldg.dw_sys_updt_tmstp_tz
   WHEN NOT MATCHED BY TARGET THEN
    INSERT (value_updated_time, rms_sku_id, location_id, immediately_sellable_qty, stock_on_hand_qty, unavailable_qty, cust_order_return, pre_inspect_customer_return, unusable_qty, rtv_reqst_resrv, met_reqst_resrv, tc_preview, tc_clubhouse, tc_mini_1, tc_mini_2, tc_mini_3, problem, damaged_return, damaged_cosmetic_return, ertm_holds, nd_unavailable_qty, pb_holds_qty, com_co_holds, wm_holds, store_reserve, tc_holds, returns_holds, fp_holds, transfers_reserve_qty, return_to_vendor_qty, in_transit_qty, store_transfer_reserved_qty, store_transfer_expected_qty, back_order_reserve_qty, oms_back_order_reserve_qty, on_replenishment, last_received_date, location_type, epm_id, upc, return_inspection_qty, receiving_qty, damage_qty, hold_qty, csn, op_seq_no, position_no, dw_batch_id, dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz,dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz,value_updated_time_tz,last_received_date_tz)
    VALUES (ldg.value_updated_time, ldg.rms_sku_id, ldg.location_id, ldg.immediately_sellable_qty, ldg.stock_on_hand_qty, ldg.unavailable_qty, ldg.cust_order_return, ldg.pre_inspect_customer_return, ldg.unusable_qty, ldg.rtv_reqst_resrv, ldg.met_reqst_resrv, ldg.tc_preview, ldg.tc_clubhouse, ldg.tc_mini_1, ldg.tc_mini_2, ldg.tc_mini_3, ldg.problem, ldg.damaged_return, ldg.damaged_cosmetic_return, ldg.ertm_holds, ldg.nd_unavailable_qty, ldg.pb_holds_qty, ldg.com_co_holds, ldg.wm_holds, ldg.store_reserve, ldg.tc_holds, ldg.returns_holds, ldg.fp_holds, ldg.transfers_reserve_qty, ldg.return_to_vendor_qty, ldg.in_transit_qty, ldg.store_transfer_reserved_qty, ldg.store_transfer_expected_qty, ldg.back_order_reserve_qty, ldg.oms_back_order_reserve_qty, ldg.on_replenishment, ldg.last_received_date, ldg.location_type, ldg.epm_id, ldg.upc, ldg.return_inspection_qty, ldg.receiving_qty, ldg.damage_qty, ldg.hold_qty, ldg.csn, ldg.op_seq_no, ldg.position_no, ldg.dw_batch_id, ldg.dw_batch_date, ldg.dw_sys_load_tmstp,ldg.dw_sys_load_tmstp_tz,ldg.dw_sys_updt_tmstp,ldg.dw_sys_updt_tmstp_tz,ldg.value_updated_time_tz,ldg.last_received_date_tz)
;
			
		
/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/



CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT',
												  
   '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT',
   'ascp_rms_all_inventory_stock_quantity_load_v1',
   'ldg_to_fact_5',
   3,'INTERMEDIATE', 'Load LOGICAL_FACT table',
   current_datetime('PST8PDT'),
   'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1');


/***********************************************************************************
-- Delete snapshot for current date if exist
************************************************************************************/


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_by_day_logical_v1_fact
 WHERE inventory_stock_quantity_by_day_logical_v1_fact.snapshot_date = (
  SELECT
      curr_batch_date
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
    WHERE rtrim(subject_area_nm) = 'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1'
)
 OR inventory_stock_quantity_by_day_logical_v1_fact.snapshot_date <= (
  SELECT
      date_add(CAST(curr_batch_date as DATE), interval -(5 * 12) MONTH)
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
    WHERE rtrim(subject_area_nm) = 'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1'
);


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
  ('INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT',
   '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT',
   'ascp_rms_all_inventory_stock_quantity_load_v1',
   'ldg_to_fact_5',
   4,'INTERMEDIATE', 'Delete DAY_LOGICAL_FACT snapshot',
   current_datetime('PST8PDT'),
   'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1');


/***********************************************************************************
-- Create inventory daily snapshot
************************************************************************************/


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_by_day_logical_v1_fact(snapshot_date, value_updated_time, rms_sku_id, location_id, immediately_sellable_qty, stock_on_hand_qty, unavailable_qty, cust_order_return, pre_inspect_customer_return, unusable_qty, rtv_reqst_resrv, met_reqst_resrv, tc_preview, tc_clubhouse, tc_mini_1, tc_mini_2, tc_mini_3, problem, damaged_return, damaged_cosmetic_return, ertm_holds, nd_unavailable_qty, pb_holds_qty, com_co_holds, wm_holds, store_reserve, tc_holds, returns_holds, fp_holds, transfers_reserve_qty, return_to_vendor_qty, in_transit_qty, store_transfer_reserved_qty, store_transfer_expected_qty, back_order_reserve_qty, oms_back_order_reserve_qty, on_replenishment, last_received_date, location_type, epm_id, upc, return_inspection_qty, receiving_qty, damage_qty, hold_qty, dw_batch_id, dw_batch_date, dw_sys_load_tmstp, dw_sys_updt_tmstp,last_received_date_tz,value_updated_time_tz,dw_sys_load_tmstp_tz,dw_sys_updt_tmstp_tz)
    SELECT
      (
        SELECT
            curr_batch_date
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
          WHERE rtrim(subject_area_nm) = 'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1'
      ) AS snapshot_date,
      inventory_stock_quantity_logical_v1_fact.value_updated_time_utc,
      inventory_stock_quantity_logical_v1_fact.rms_sku_id,
      inventory_stock_quantity_logical_v1_fact.location_id,
      inventory_stock_quantity_logical_v1_fact.immediately_sellable_qty,
      inventory_stock_quantity_logical_v1_fact.stock_on_hand_qty,
      inventory_stock_quantity_logical_v1_fact.unavailable_qty,
      inventory_stock_quantity_logical_v1_fact.cust_order_return,
      inventory_stock_quantity_logical_v1_fact.pre_inspect_customer_return,
      inventory_stock_quantity_logical_v1_fact.unusable_qty,
      inventory_stock_quantity_logical_v1_fact.rtv_reqst_resrv,
      inventory_stock_quantity_logical_v1_fact.met_reqst_resrv,
      inventory_stock_quantity_logical_v1_fact.tc_preview,
      inventory_stock_quantity_logical_v1_fact.tc_clubhouse,
      inventory_stock_quantity_logical_v1_fact.tc_mini_1,
      inventory_stock_quantity_logical_v1_fact.tc_mini_2,
      inventory_stock_quantity_logical_v1_fact.tc_mini_3,
      inventory_stock_quantity_logical_v1_fact.problem,
      inventory_stock_quantity_logical_v1_fact.damaged_return,
      inventory_stock_quantity_logical_v1_fact.damaged_cosmetic_return,
      inventory_stock_quantity_logical_v1_fact.ertm_holds,
      inventory_stock_quantity_logical_v1_fact.nd_unavailable_qty,
      inventory_stock_quantity_logical_v1_fact.pb_holds_qty,
      inventory_stock_quantity_logical_v1_fact.com_co_holds,
      inventory_stock_quantity_logical_v1_fact.wm_holds,
      inventory_stock_quantity_logical_v1_fact.store_reserve,
      inventory_stock_quantity_logical_v1_fact.tc_holds,
      inventory_stock_quantity_logical_v1_fact.returns_holds,
      inventory_stock_quantity_logical_v1_fact.fp_holds,
      inventory_stock_quantity_logical_v1_fact.transfers_reserve_qty,
      inventory_stock_quantity_logical_v1_fact.return_to_vendor_qty,
      inventory_stock_quantity_logical_v1_fact.in_transit_qty,
      inventory_stock_quantity_logical_v1_fact.store_transfer_reserved_qty,
      inventory_stock_quantity_logical_v1_fact.store_transfer_expected_qty,
      inventory_stock_quantity_logical_v1_fact.back_order_reserve_qty,
      inventory_stock_quantity_logical_v1_fact.oms_back_order_reserve_qty,
      inventory_stock_quantity_logical_v1_fact.on_replenishment,
      inventory_stock_quantity_logical_v1_fact.last_received_date_utc,
      inventory_stock_quantity_logical_v1_fact.location_type,
      inventory_stock_quantity_logical_v1_fact.epm_id,
      inventory_stock_quantity_logical_v1_fact.upc,
      inventory_stock_quantity_logical_v1_fact.return_inspection_qty,
      inventory_stock_quantity_logical_v1_fact.receiving_qty,
      inventory_stock_quantity_logical_v1_fact.damage_qty,
      inventory_stock_quantity_logical_v1_fact.hold_qty,
      inventory_stock_quantity_logical_v1_fact.dw_batch_id,
      inventory_stock_quantity_logical_v1_fact.dw_batch_date,
      inventory_stock_quantity_logical_v1_fact.dw_sys_load_tmstp_utc,
      inventory_stock_quantity_logical_v1_fact.dw_sys_updt_tmstp_utc,
      inventory_stock_quantity_logical_v1_fact.last_received_date_tz,
      inventory_stock_quantity_logical_v1_fact.value_updated_time_tz,
      inventory_stock_quantity_logical_v1_fact.dw_sys_load_tmstp_tz,
      inventory_stock_quantity_logical_v1_fact.dw_sys_updt_tmstp_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.INVENTORY_STOCK_QUANTITY_LOGICAL_V1_FACT
    WHERE inventory_stock_quantity_logical_v1_fact.stock_on_hand_qty <> 0
     OR inventory_stock_quantity_logical_v1_fact.in_transit_qty <> 0
     OR inventory_stock_quantity_logical_v1_fact.store_transfer_reserved_qty <> 0
     OR inventory_stock_quantity_logical_v1_fact.store_transfer_expected_qty <> 0
     OR inventory_stock_quantity_logical_v1_fact.rtv_reqst_resrv <> 0
;

/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT',
												  
   '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT',
   'ascp_rms_all_inventory_stock_quantity_load_v1',
   'ldg_to_fact_5',
   5,'INTERMEDIATE', 'Load DAY_LOGICAL_FACT snapshot',
   current_datetime('PST8PDT'),
   'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1');

/***********************************************************************************
-- DATA_TIMELINESS_METRIC
************************************************************************************/

    CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT',
   '`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT',
   'ascp_rms_all_inventory_stock_quantity_load_v1',
   'ldg_to_fact_5',
   6,'LOAD_END', '',
   current_datetime('PST8PDT'),
   'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1');



   /***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
-- COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.INVENTORY_STOCK_QUANTITY_LOGICAL_V1_FACT INDEX ( rms_sku_id, location_id );

-- COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_V1_FACT INDEX ( rms_sku_id, location_id );

/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1(
  'INVENTORY_STOCK_QUANTITY_LOGICAL_V1_LDG_TO_BASE',
  'NAP_ASCP_INVENTORY_STOCK_QUANTITY_LOGICAL_V1',
  "`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG",
  'INVENTORY_STOCK_QUANTITY_LOGICAL_V1_LDG',
  "`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT",
  'INVENTORY_STOCK_QUANTITY_LOGICAL_V1_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat( rms_sku_id, location_id, location_type )',
  'concat( rms_sku_id, location_id, location_type )',
  null,
  null,
  'Y'
);

