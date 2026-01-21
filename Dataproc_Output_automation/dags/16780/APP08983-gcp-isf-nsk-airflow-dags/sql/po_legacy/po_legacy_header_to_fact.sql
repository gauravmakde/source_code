/***********************************************************************************
-- Insert The Timeliness Load Data
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('PURCHASE_ORDER_FACT',  '{{params.dbenv}}_NAP_FCT',  'po_legacy_kafka_to_orc_td',  'ldg_to_fact',  1,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'SCOI_PO_LEGACY');


/***********************************************************************************
-- Collect all validation failure records in error tables
************************************************************************************/
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.purchase_order_fact_err (purchase_order_num, accounts_payable_comments,
 acknowledgement_received_ind, agent_edi_ind, broker_id, buyer_id, buyingagent_id, close_date, comments,
 consolidator_edi_ind, consolidator_id, country_of_manufacturer, crossreference_external_id, currency, department_id,
 discharge_entry_port, dropship_ind, earliest_ship_date, edi_ind, edi_sent_ind, end_ship_date, exchange_rate,
 exclude_from_availability_ind, external_target, factory_id, freight_payment_method, freight_terms, id, import_country,
 import_full_edi_manufacturer_date, import_full_edi_manufacturer_sent_ind, import_order_ind, instore_scheduled_date,
 include_on_order_ind, sendonly_ticket_partner_edi_ind, lading_port, latest_ship_date, manufacturer_comments,
 manufacturer_earliest_ship_date, manufacturer_edi_ind, manufacturer_id, manufacturer_latest_ship_date,
 manufacturer_payment_method, manufacturer_payment_terms, nordstrom_com_ind, nordstrom_productgroup_costcenter,
 nordstrom_productgroup_ind, open_to_buy_endofweek_changed_date, open_to_buy_endofweek_date,
 open_to_buy_endofweek_date_changedby, order_from_vendor_address_sequence_num, order_from_vendor_id, order_type,
 origin_code, original_approval_date, original_approval_id, out_of_country_ind, packing_method, payment_method,
 pickup_date, plan_season_id, planner_id, premark_ind, promotion_id, purchaseorder_type, purchase_type,
 quality_check_ind, rack_compare_at_season_code, secondary_factory_id, shipping_method, start_ship_date, status, terms,
 ticket_edi_date, ticket_edi_ind, ticket_format, ticket_partner_id, ticket_retail_date, title_pass_responsibility,
 transportation_responsibility, updatedby_id, vendor_purchase_order_id, written_date, error_table, error_code,
 error_desc, dw_batch_id, dw_batch_date)
(SELECT TRIM(purchaseorder_externalid) AS purchaseorder_externalid,
  purchaseorder_accountspayablecomments,
  purchaseorder_acknowledgementreceivedindicator,
  purchaseorder_agentediindicator,
  purchaseorder_brokerid,
  purchaseorder_buyerid,
  purchaseorder_buyingagentid,
  CAST(purchaseorder_closedate AS DATE) AS purchaseorder_closedate,
  purchaseorder_comments,
  purchaseorder_consolidatorediindicator,
  purchaseorder_consolidatorid,
  purchaseorder_countryofmanufacturer,
  purchaseorder_crossreferenceexternalid,
  purchaseorder_currency,
  CAST(TRUNC(CAST(purchaseorder_departmentid AS FLOAT64)) AS INTEGER) AS purchaseorder_departmentid,
  purchaseorder_dischargeentryport,
  purchaseorder_dropshipindicator,
  CAST(purchaseorder_earliestshipdate AS DATE) AS purchaseorder_earliestshipdate,
  purchaseorder_ediindicator,
  purchaseorder_edisentindicator,
  CAST(purchaseorder_endshipdate AS DATE) AS purchaseorder_endshipdate,
  ROUND(CAST(purchaseorder_exchangerate AS NUMERIC), 9) AS purchaseorder_exchangerate,
  purchaseorder_excludefromavailabilityindicator,
  purchaseorder_externaltarget,
  purchaseorder_factoryid,
  purchaseorder_freightpaymentmethod,
  purchaseorder_freightterms,
  purchaseorder_id,
  purchaseorder_importcountry,
  CAST(purchaseorder_importfulledimanufacturerdate AS DATE) AS purchaseorder_importfulledimanufacturerdate,
  purchaseorder_importfulledimanufacturersentindicator,
  purchaseorder_importorderindicator,
  CAST(purchaseorder_instorescheduleddate AS DATE) AS purchaseorder_instorescheduleddate,
  purchaseorder_includeonorderindicator,
  purchaseorder_sendonlyticketpartnerediindicator,
  purchaseorder_ladingport,
  CAST(purchaseorder_latestshipdate AS DATE) AS purchaseorder_latestshipdate,
  purchaseorder_manufacturercomments,
  CAST(purchaseorder_manufacturerearliestshipdate AS DATE) AS purchaseorder_manufacturerearliestshipdate,
  purchaseorder_manufacturerediindicator,
  purchaseorder_manufacturerid,
  CAST(purchaseorder_manufacturerlatestshipdate AS DATE) AS purchaseorder_manufacturerlatestshipdate,
  purchaseorder_manufacturerpaymentmethod,
  purchaseorder_manufacturerpaymentterms,
  purchaseorder_nordstromcomindicator,
  purchaseorder_nordstromproductgroupcostcenter,
  purchaseorder_nordstromproductgroupindicator,
  CAST(purchaseorder_opentobuyendofweekchangeddate AS DATE) AS purchaseorder_opentobuyendofweekchangeddate,
  CAST(purchaseorder_opentobuyendofweekdate AS DATE) AS purchaseorder_opentobuyendofweekdate,
  purchaseorder_opentobuyendofweekdatechangedby,
  purchaseorder_orderfromvendoraddresssequencenumber,
  purchaseorder_orderfromvendorid,
  purchaseorder_ordertype,
  purchaseorder_originindicator,
  CAST(purchaseorder_originalapprovaldate AS DATE) AS purchaseorder_originalapprovaldate,
  purchaseorder_originalapprovalid,
  purchaseorder_outofcountryindicator,
  purchaseorder_packingmethod,
  purchaseorder_paymentmethod,
  CAST(purchaseorder_pickupdate AS DATE) AS purchaseorder_pickupdate,
  purchaseorder_planseasonid,
  purchaseorder_plannerid,
  purchaseorder_premarkindicator,
  purchaseorder_promotionid,
  purchaseorder_purchaseordertype,
  purchaseorder_purchasetype,
  purchaseorder_qualitycheckindicator,
  purchaseorder_rackcompareatseasoncode,
  purchaseorder_secondaryfactoryid,
  purchaseorder_shippingmethod,
  CAST(purchaseorder_startshipdate AS DATE) AS purchaseorder_startshipdate,
  purchaseorder_status,
  purchaseorder_terms,
  CAST(purchaseorder_ticketedidate AS DATE) AS purchaseorder_ticketedidate,
  purchaseorder_ticketediindicator,
  purchaseorder_ticketformat,
  purchaseorder_ticketpartnerid,
  CAST(purchaseorder_ticketretaildate AS DATE) AS purchaseorder_ticketretaildate,
  purchaseorder_titlepassresponsibility,
  purchaseorder_transportationresponsibility,
  purchaseorder_updatedbyid,
  purchaseorder_vendorpurchaseorderid,
  CAST(purchaseorder_writtendate AS DATE) AS purchaseorder_writtendate,
  'PURCHASE_ORDER_FACT' AS error_table,
  2 AS error_code,
  'Date/Number/Mandatory Field Validation Failed in PURCHASE_ORDER_FACT Table' AS error_desc,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER')) AS dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_ldg AS ldg
 WHERE purchaseorder_externalid IS NULL);
/***********************************************************************************
-- Insert The Timeliness Load Data
************************************************************************************/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('PURCHASE_ORDER_FACT',  '{{params.dbenv}}_NAP_FCT',  'po_legacy_kafka_to_orc_td',  'ldg_to_fact',  2,  'INTERMEDIATE',  'Load the PURCHASE_ORDER_FACT_ERR with records containing errors',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'SCOI_PO_LEGACY');


/***********************************************************************************
-- Insert to and/or update the target fact table
************************************************************************************/
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.purchase_order_fact AS po_fct
USING (SELECT TRIM(purchaseorder_externalid) AS purchaseorder_externalid, purchaseorder_accountspayablecomments, purchaseorder_acknowledgementreceivedindicator, purchaseorder_agentediindicator, purchaseorder_brokerid, purchaseorder_buyerid, purchaseorder_buyingagentid, purchaseorder_closedate, purchaseorder_comments, purchaseorder_consolidatorediindicator, purchaseorder_consolidatorid, purchaseorder_countryofmanufacturer, purchaseorder_crossreferenceexternalid, purchaseorder_currency, purchaseorder_departmentid, purchaseorder_dischargeentryport, purchaseorder_dropshipindicator, purchaseorder_earliestshipdate, purchaseorder_ediindicator, purchaseorder_edisentindicator, purchaseorder_endshipdate, purchaseorder_exchangerate, purchaseorder_excludefromavailabilityindicator, purchaseorder_externaltarget, purchaseorder_factoryid, purchaseorder_freightpaymentmethod, purchaseorder_freightterms, purchaseorder_id, purchaseorder_importcountry, purchaseorder_importfulledimanufacturerdate, purchaseorder_importfulledimanufacturersentindicator, purchaseorder_importorderindicator, purchaseorder_instorescheduleddate, purchaseorder_includeonorderindicator, purchaseorder_sendonlyticketpartnerediindicator, purchaseorder_ladingport, purchaseorder_latestshipdate, purchaseorder_manufacturercomments, purchaseorder_manufacturerearliestshipdate, purchaseorder_manufacturerediindicator, purchaseorder_manufacturerid, purchaseorder_manufacturerlatestshipdate, purchaseorder_manufacturerpaymentmethod, purchaseorder_manufacturerpaymentterms, purchaseorder_nordstromcomindicator, purchaseorder_nordstromproductgroupcostcenter, purchaseorder_nordstromproductgroupindicator, purchaseorder_opentobuyendofweekchangeddate, purchaseorder_opentobuyendofweekdate, purchaseorder_opentobuyendofweekdatechangedby, purchaseorder_orderfromvendoraddresssequencenumber, purchaseorder_orderfromvendorid, purchaseorder_ordertype, purchaseorder_originindicator, purchaseorder_originalapprovaldate, purchaseorder_originalapprovalid, purchaseorder_outofcountryindicator, purchaseorder_packingmethod, purchaseorder_paymentmethod, purchaseorder_pickupdate, purchaseorder_planseasonid, purchaseorder_plannerid, purchaseorder_premarkindicator, purchaseorder_promotionid, purchaseorder_purchaseordertype, purchaseorder_purchasetype, purchaseorder_qualitycheckindicator, purchaseorder_rackcompareatseasoncode, purchaseorder_secondaryfactoryid, purchaseorder_shippingmethod, purchaseorder_startshipdate, purchaseorder_status, purchaseorder_terms, purchaseorder_ticketedidate, purchaseorder_ticketediindicator, purchaseorder_ticketformat, purchaseorder_ticketpartnerid, purchaseorder_ticketretaildate, purchaseorder_titlepassresponsibility, purchaseorder_transportationresponsibility, purchaseorder_updatedbyid, purchaseorder_vendorpurchaseorderid, purchaseorder_writtendate, (SELECT batch_id
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER')) AS dw_batch_id, (SELECT curr_batch_date
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER')) AS dw_batch_date, 
            CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp, 
            `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz,
            CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_updt_tmstp,
            `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_ldg AS sdc
    WHERE purchaseorder_externalid IS NOT NULL
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY purchaseorder_externalid ORDER BY metadata_eventreceivedtimestamp DESC)) = 1) AS qry
ON LOWER(qry.purchaseorder_externalid) = LOWER(po_fct.purchase_order_num)
WHEN MATCHED THEN UPDATE SET
    accounts_payable_comments = qry.purchaseorder_accountspayablecomments,
    acknowledgement_received_ind = qry.purchaseorder_acknowledgementreceivedindicator,
    agent_edi_ind = qry.purchaseorder_agentediindicator,
    broker_id = qry.purchaseorder_brokerid,
    buyer_id = CAST(TRUNC(CAST(qry.purchaseorder_buyerid AS FLOAT64)) AS INTEGER),
    buyingagent_id = CAST(TRUNC(CAST(qry.purchaseorder_buyingagentid AS FLOAT64)) AS INTEGER),
    close_date = CAST(qry.purchaseorder_closedate AS DATE),
    comments = qry.purchaseorder_comments,
    consolidator_edi_ind = qry.purchaseorder_consolidatorediindicator,
    consolidator_id = CAST(TRUNC(CAST(qry.purchaseorder_consolidatorid AS FLOAT64)) AS INTEGER),
    country_of_manufacturer = qry.purchaseorder_countryofmanufacturer,
    crossreference_external_id = qry.purchaseorder_crossreferenceexternalid,
    currency = qry.purchaseorder_currency,
    department_id = CAST(TRUNC(CAST(qry.purchaseorder_departmentid AS FLOAT64)) AS INTEGER),
    discharge_entry_port = qry.purchaseorder_dischargeentryport,
    dropship_ind = qry.purchaseorder_dropshipindicator,
    earliest_ship_date = CAST(qry.purchaseorder_earliestshipdate AS DATE),
    edi_ind = qry.purchaseorder_ediindicator,
    edi_sent_ind = qry.purchaseorder_edisentindicator,
    end_ship_date = CAST(qry.purchaseorder_endshipdate AS DATE),
    exchange_rate = CAST(ROUND(CAST(qry.purchaseorder_exchangerate AS BIGNUMERIC), 0) AS NUMERIC),
    exclude_from_availability_ind = qry.purchaseorder_excludefromavailabilityindicator,
    external_target = qry.purchaseorder_externaltarget,
    factory_id = CAST(TRUNC(CAST(qry.purchaseorder_factoryid AS FLOAT64)) AS INTEGER),
    freight_payment_method = qry.purchaseorder_freightpaymentmethod,
    freight_terms = qry.purchaseorder_freightterms,
    id = qry.purchaseorder_id,
    import_country = qry.purchaseorder_importcountry,
    import_full_edi_manufacturer_date = CAST(qry.purchaseorder_importfulledimanufacturerdate AS DATE),
    import_full_edi_manufacturer_sent_ind = qry.purchaseorder_importfulledimanufacturersentindicator,
    import_order_ind = qry.purchaseorder_importorderindicator,
    instore_scheduled_date = CAST(qry.purchaseorder_instorescheduleddate AS DATE),
    include_on_order_ind = qry.purchaseorder_includeonorderindicator,
    sendonly_ticket_partner_edi_ind = qry.purchaseorder_sendonlyticketpartnerediindicator,
    lading_port = qry.purchaseorder_ladingport,
    latest_ship_date = CAST(qry.purchaseorder_latestshipdate AS DATE),
    manufacturer_comments = qry.purchaseorder_manufacturercomments,
    manufacturer_earliest_ship_date = CAST(qry.purchaseorder_manufacturerearliestshipdate AS DATE),
    manufacturer_edi_ind = qry.purchaseorder_manufacturerediindicator,
    manufacturer_id = CAST(TRUNC(CAST(qry.purchaseorder_manufacturerid AS FLOAT64)) AS INTEGER),
    manufacturer_latest_ship_date = CAST(qry.purchaseorder_manufacturerlatestshipdate AS DATE),
    manufacturer_payment_method = qry.purchaseorder_manufacturerpaymentmethod,
    manufacturer_payment_terms = qry.purchaseorder_manufacturerpaymentterms,
    nordstrom_com_ind = qry.purchaseorder_nordstromcomindicator,
    nordstrom_productgroup_costcenter = qry.purchaseorder_nordstromproductgroupcostcenter,
    nordstrom_productgroup_ind = qry.purchaseorder_nordstromproductgroupindicator,
    open_to_buy_endofweek_changed_date = CAST(qry.purchaseorder_opentobuyendofweekchangeddate AS DATE),
    open_to_buy_endofweek_date = CAST(qry.purchaseorder_opentobuyendofweekdate AS DATE),
    open_to_buy_endofweek_date_changedby = qry.purchaseorder_opentobuyendofweekdate,
    order_from_vendor_address_sequence_num = qry.purchaseorder_orderfromvendoraddresssequencenumber,
    order_from_vendor_id = qry.purchaseorder_orderfromvendorid,
    order_type = qry.purchaseorder_ordertype,
    origin_code = qry.purchaseorder_originindicator,
    original_approval_date = CAST(qry.purchaseorder_originalapprovaldate AS DATE),
    original_approval_id = qry.purchaseorder_originalapprovalid,
    out_of_country_ind = qry.purchaseorder_outofcountryindicator,
    packing_method = qry.purchaseorder_packingmethod,
    payment_method = qry.purchaseorder_paymentmethod,
    pickup_date = CAST(qry.purchaseorder_pickupdate AS DATE),
    plan_season_id = qry.purchaseorder_planseasonid,
    planner_id = qry.purchaseorder_plannerid,
    premark_ind = qry.purchaseorder_premarkindicator,
    promotion_id = CAST(TRUNC(CAST(qry.purchaseorder_promotionid AS FLOAT64)) AS INTEGER),
    purchaseorder_type = qry.purchaseorder_purchaseordertype,
    purchase_type = qry.purchaseorder_purchasetype,
    quality_check_ind = qry.purchaseorder_qualitycheckindicator,
    rack_compare_at_season_code = qry.purchaseorder_rackcompareatseasoncode,
    secondary_factory_id = CAST(TRUNC(CAST(qry.purchaseorder_secondaryfactoryid AS FLOAT64)) AS INTEGER),
    shipping_method = CAST(TRUNC(CAST(qry.purchaseorder_shippingmethod AS FLOAT64)) AS INTEGER),
    start_ship_date = CAST(qry.purchaseorder_startshipdate AS DATE),
    status = qry.purchaseorder_status,
    terms = CAST(TRUNC(CAST(qry.purchaseorder_terms AS FLOAT64)) AS INTEGER),
    ticket_edi_date = CAST(qry.purchaseorder_ticketedidate AS DATE),
    ticket_edi_ind = qry.purchaseorder_ticketediindicator,
    ticket_format = qry.purchaseorder_ticketformat,
    ticket_partner_id = qry.purchaseorder_ticketpartnerid,
    ticket_retail_date = CAST(qry.purchaseorder_ticketretaildate AS DATE),
    title_pass_responsibility = qry.purchaseorder_titlepassresponsibility,
    transportation_responsibility = qry.purchaseorder_transportationresponsibility,
    updatedby_id = qry.purchaseorder_updatedbyid,
    vendor_purchase_order_id = qry.purchaseorder_vendorpurchaseorderid,
    written_date = CAST(qry.purchaseorder_writtendate AS DATE),
    dw_batch_id = qry.dw_batch_id,
    dw_batch_date = qry.dw_batch_date,
    dw_sys_updt_tmstp = qry.dw_sys_updt_tmstp,
    dw_sys_updt_tmstp_tz = qry.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN INSERT VALUES(qry.purchaseorder_externalid, qry.purchaseorder_accountspayablecomments, qry.purchaseorder_acknowledgementreceivedindicator, qry.purchaseorder_agentediindicator, qry.purchaseorder_brokerid, CAST(TRUNC(CAST(qry.purchaseorder_buyerid AS FLOAT64)) AS INTEGER), CAST(TRUNC(CAST(qry.purchaseorder_buyingagentid AS FLOAT64)) AS INTEGER), CAST(qry.purchaseorder_closedate AS DATE), qry.purchaseorder_comments, qry.purchaseorder_consolidatorediindicator, CAST(TRUNC(CAST(qry.purchaseorder_consolidatorid AS FLOAT64)) AS INTEGER), qry.purchaseorder_countryofmanufacturer, qry.purchaseorder_crossreferenceexternalid, qry.purchaseorder_currency, CAST(TRUNC(CAST(qry.purchaseorder_departmentid AS FLOAT64)) AS INTEGER), qry.purchaseorder_dischargeentryport, qry.purchaseorder_dropshipindicator, CAST(qry.purchaseorder_earliestshipdate AS DATE), qry.purchaseorder_ediindicator, qry.purchaseorder_edisentindicator, CAST(qry.purchaseorder_endshipdate AS DATE), CAST(ROUND(CAST(qry.purchaseorder_exchangerate AS BIGNUMERIC), 0) AS NUMERIC), qry.purchaseorder_excludefromavailabilityindicator, qry.purchaseorder_externaltarget, CAST(TRUNC(CAST(qry.purchaseorder_factoryid AS FLOAT64)) AS INTEGER), qry.purchaseorder_freightpaymentmethod, qry.purchaseorder_freightterms, qry.purchaseorder_id, qry.purchaseorder_importcountry, CAST(qry.purchaseorder_importfulledimanufacturerdate AS DATE), qry.purchaseorder_importfulledimanufacturersentindicator, qry.purchaseorder_importorderindicator, CAST(qry.purchaseorder_instorescheduleddate AS DATE), qry.purchaseorder_includeonorderindicator, qry.purchaseorder_sendonlyticketpartnerediindicator, qry.purchaseorder_ladingport, CAST(qry.purchaseorder_latestshipdate AS DATE), qry.purchaseorder_manufacturercomments, CAST(qry.purchaseorder_manufacturerearliestshipdate AS DATE), qry.purchaseorder_manufacturerediindicator, CAST(TRUNC(CAST(qry.purchaseorder_manufacturerid AS FLOAT64)) AS INTEGER), CAST(qry.purchaseorder_manufacturerlatestshipdate AS DATE), qry.purchaseorder_manufacturerpaymentmethod, qry.purchaseorder_manufacturerpaymentterms, qry.purchaseorder_nordstromcomindicator, qry.purchaseorder_nordstromproductgroupcostcenter, qry.purchaseorder_nordstromproductgroupindicator, CAST(qry.purchaseorder_opentobuyendofweekchangeddate AS DATE), CAST(qry.purchaseorder_opentobuyendofweekdate AS DATE), qry.purchaseorder_opentobuyendofweekdatechangedby, qry.purchaseorder_orderfromvendoraddresssequencenumber, qry.purchaseorder_orderfromvendorid, qry.purchaseorder_ordertype, qry.purchaseorder_originindicator, CAST(qry.purchaseorder_originalapprovaldate AS DATE), qry.purchaseorder_originalapprovalid, qry.purchaseorder_outofcountryindicator, qry.purchaseorder_packingmethod, qry.purchaseorder_paymentmethod, CAST(qry.purchaseorder_pickupdate AS DATE), qry.purchaseorder_planseasonid, qry.purchaseorder_plannerid, qry.purchaseorder_premarkindicator, CAST(TRUNC(CAST(qry.purchaseorder_promotionid AS FLOAT64)) AS INTEGER), qry.purchaseorder_purchaseordertype, qry.purchaseorder_purchasetype, qry.purchaseorder_qualitycheckindicator, qry.purchaseorder_rackcompareatseasoncode, CAST(TRUNC(CAST(qry.purchaseorder_secondaryfactoryid AS FLOAT64)) AS INTEGER), CAST(TRUNC(CAST(qry.purchaseorder_shippingmethod AS FLOAT64)) AS INTEGER), CAST(qry.purchaseorder_startshipdate AS DATE), qry.purchaseorder_status, CAST(TRUNC(CAST(qry.purchaseorder_terms AS FLOAT64)) AS INTEGER), CAST(qry.purchaseorder_ticketedidate AS DATE), qry.purchaseorder_ticketediindicator, qry.purchaseorder_ticketformat, qry.purchaseorder_ticketpartnerid, CAST(qry.purchaseorder_ticketretaildate AS DATE), qry.purchaseorder_titlepassresponsibility, qry.purchaseorder_transportationresponsibility, qry.purchaseorder_updatedbyid, qry.purchaseorder_vendorpurchaseorderid, CAST(qry.purchaseorder_writtendate AS DATE), qry.dw_batch_id, qry.dw_batch_date, qry.dw_sys_load_tmstp,qry.dw_sys_load_tmstp_tz, qry.dw_sys_updt_tmstp,qry.dw_sys_updt_tmstp_tz);

/***********************************************************************************
-- Insert The Timeliness Load Data
************************************************************************************/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('PURCHASE_ORDER_FACT',  '{{params.dbenv}}_NAP_FCT',  'po_legacy_kafka_to_orc_td',  'ldg_to_fact',  3,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'SCOI_PO_LEGACY');
/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
-- COMMIT TRANSACTION;

