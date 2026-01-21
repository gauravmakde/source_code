CREATE TEMPORARY TABLE latest_po
  AS
    SELECT
        purchase_order_legacy.purchaseorder,
        metadata.eventreceivedtimestamp AS metadata_eventreceivedtimestamp,
        metadata.revisionid AS metadata_revisionid
      FROM
        `{{params.gcp_project_id}}`.scoi.purchase_order_legacy
      WHERE purchase_order_legacy.batch_id = (
        SELECT
            batch_id
          FROM
            `{{params.gcp_project_id}}`.scoi.po_legacy_batch_control
      )
;


CREATE TEMPORARY  TABLE latest_po_item AS
  SELECT
      latest_po.purchaseorder,
      latest_po.metadata_eventreceivedtimestamp,
      latest_po.metadata_revisionid,
      purchaseorder_items
  FROM
      latest_po,
     UNNEST(latest_po.purchaseorder.items) AS purchaseorder_items
;



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_ldg;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_ldg 
SELECT
    purchaseorder.externalid AS purchaseorder_externalid,
    cast(format_datetime('%Y-%m-%d %H:%M:%E3S%Ez', timestamp(metadata_eventreceivedtimestamp )) as string) AS metadata_eventreceivedtimestamp,
    cast(latest_po.metadata_revisionid as string) as metadata_revisionid,
    substr(regexp_replace(COLLATE(purchaseorder.accountspayablecomments, ''), r'[\r\n]', ' '),0,999) AS purchaseorder_accountspayablecomments,
    cast(purchaseorder.acknowledgementreceivedindicator as string) AS purchaseorder_acknowledgementreceivedindicator,
    cast(purchaseorder.agentediindicator as string) AS purchaseorder_agentediindicator,
    purchaseorder.brokerid AS purchaseorder_brokerid,
    purchaseorder.buyerid AS purchaseorder_buyerid,
    purchaseorder.buyingagentid AS purchaseorder_buyingagentid,
    purchaseorder.closedate AS purchaseorder_closedate,
    substr(regexp_replace(COLLATE(purchaseorder.comments, ''), r'[\r\n]', ' '),0,999) AS purchaseorder_comments,
    cast(purchaseorder.consolidatorediindicator as string) AS purchaseorder_consolidatorediindicator,
    purchaseorder.consolidatorid AS purchaseorder_consolidatorid,
    purchaseorder.countryofmanufacturer AS purchaseorder_countryofmanufacturer,
    purchaseorder.crossreferenceexternalid AS purchaseorder_crossreferenceexternalid,
    purchaseorder.currency AS purchaseorder_currency,
    cast(purchaseorder.departmentid as string) AS purchaseorder_departmentid,
    purchaseorder.dischargeentryport AS purchaseorder_dischargeentryport,
    cast(purchaseorder.dropshipindicator as string) AS purchaseorder_dropshipindicator,
    purchaseorder.earliestshipdate AS purchaseorder_earliestshipdate,
    cast(purchaseorder.ediindicator as string) AS purchaseorder_ediindicator,
    cast(purchaseorder.edisentindicator as string) AS purchaseorder_edisentindicator,
    purchaseorder.endshipdate AS purchaseorder_endshipdate,
    cast(purchaseorder.exchangerate as string) AS purchaseorder_exchangerate,
    cast(purchaseorder.excludefromavailabilityindicator as string) AS purchaseorder_excludefromavailabilityindicator,
    purchaseorder.externaltarget AS purchaseorder_externaltarget,
    purchaseorder.factoryid AS purchaseorder_factoryid,
    purchaseorder.freightpaymentmethod AS purchaseorder_freightpaymentmethod,
    purchaseorder.freightterms AS purchaseorder_freightterms,
    purchaseorder.id AS purchaseorder_id,
    purchaseorder.importcountry AS purchaseorder_importcountry,
    purchaseorder.importfulledimanufacturerdate AS purchaseorder_importfulledimanufacturerdate,
    cast(purchaseorder.importfulledimanufacturersentindicator as string) AS purchaseorder_importfulledimanufacturersentindicator,
    cast(purchaseorder.importorderindicator as string) AS purchaseorder_importorderindicator,
    purchaseorder.instorescheduleddate AS purchaseorder_instorescheduleddate,
    cast(purchaseorder.includeonorderindicator as string) AS purchaseorder_includeonorderindicator,
    cast(purchaseorder.sendonlyticketpartnerediindicator as string) AS purchaseorder_sendonlyticketpartnerediindicator,
    purchaseorder.ladingport AS purchaseorder_ladingport,
    purchaseorder.latestshipdate AS purchaseorder_latestshipdate,
    substr(regexp_replace(COLLATE(purchaseorder.manufacturercomments, ''), r'[\r\n]', ' '),0,999) AS purchaseorder_manufacturercomments,
    purchaseorder.manufacturerearliestshipdate AS purchaseorder_manufacturerearliestshipdate,
    cast(purchaseorder.manufacturerediindicator as string) AS purchaseorder_manufacturerediindicator,
    purchaseorder.manufacturerid AS purchaseorder_manufacturerid,
    purchaseorder.manufacturerlatestshipdate AS purchaseorder_manufacturerlatestshipdate,
    purchaseorder.manufacturerpaymentmethod AS purchaseorder_manufacturerpaymentmethod,
    purchaseorder.manufacturerpaymentterms AS purchaseorder_manufacturerpaymentterms,
    cast(purchaseorder.nordstromcomindicator as string) AS purchaseorder_nordstromcomindicator,
    purchaseorder.nordstromproductgroupcostcenter AS purchaseorder_nordstromproductgroupcostcenter,
    cast(purchaseorder.nordstromproductgroupindicator as string) AS purchaseorder_nordstromproductgroupindicator,
    purchaseorder.opentobuyendofweekchangeddate AS purchaseorder_opentobuyendofweekchangeddate,
    purchaseorder.opentobuyendofweekdate AS purchaseorder_opentobuyendofweekdate,
    purchaseorder.opentobuyendofweekdatechangedby AS purchaseorder_opentobuyendofweekdatechangedby,
    purchaseorder.orderfromvendoraddresssequencenumber AS purchaseorder_orderfromvendoraddresssequencenumber,
    purchaseorder.orderfromvendorid AS purchaseorder_orderfromvendorid,
    purchaseorder.ordertype AS purchaseorder_ordertype,
    purchaseorder.originindicator AS purchaseorder_originindicator,
    purchaseorder.originalapprovaldate AS purchaseorder_originalapprovaldate,
    purchaseorder.originalapprovalid AS purchaseorder_originalapprovalid,
    cast(purchaseorder.outofcountryindicator as string) AS purchaseorder_outofcountryindicator,
    purchaseorder.packingmethod AS purchaseorder_packingmethod,
    purchaseorder.paymentmethod AS purchaseorder_paymentmethod,
    purchaseorder.pickupdate AS purchaseorder_pickupdate,
    purchaseorder.planseasonid AS purchaseorder_planseasonid,
    purchaseorder.plannerid AS purchaseorder_plannerid,
    cast(purchaseorder.premarkindicator as string) AS purchaseorder_premarkindicator,
    purchaseorder.promotionid AS purchaseorder_promotionid,
    purchaseorder.purchaseordertype AS purchaseorder_purchaseordertype,
    purchaseorder.purchasetype AS purchaseorder_purchasetype,
    cast(purchaseorder.qualitycheckindicator as string) AS purchaseorder_qualitycheckindicator,
    purchaseorder.rackcompareatseasoncode AS purchaseorder_rackcompareatseasoncode,
    purchaseorder.secondaryfactoryid AS purchaseorder_secondaryfactoryid,
    purchaseorder.shippingmethod AS purchaseorder_shippingmethod,
    purchaseorder.startshipdate AS purchaseorder_startshipdate,
    purchaseorder.status AS purchaseorder_status,
    purchaseorder.terms AS purchaseorder_terms,
    purchaseorder.ticketedidate AS purchaseorder_ticketedidate,
    cast(purchaseorder.ticketediindicator as string) AS purchaseorder_ticketediindicator,
    purchaseorder.ticketformat AS purchaseorder_ticketformat,
    purchaseorder.ticketpartnerid AS purchaseorder_ticketpartnerid,
    purchaseorder.ticketretaildate AS purchaseorder_ticketretaildate,
    purchaseorder.titlepassresponsibility AS purchaseorder_titlepassresponsibility,
    purchaseorder.transportationresponsibility AS purchaseorder_transportationresponsibility,
    purchaseorder.updatedbyid AS purchaseorder_updatedbyid,
    purchaseorder.vendorpurchaseorderid AS purchaseorder_vendorpurchaseorderid,
    purchaseorder.writtendate AS purchaseorder_writtendate
  FROM
    latest_po
;



CREATE TEMPORARY TABLE latest_po_ship
  AS
    SELECT
        latest_po_item.purchaseorder,
        latest_po_item.metadata_eventreceivedtimestamp,
        latest_po_item.metadata_revisionid,
        latest_po_item.purchaseorder_items,
        purchaseorder_items_shiplocations
        --explode(purchaseorder_items.shiplocations) AS purchaseorder_items_shiplocations
      FROM
        latest_po_item,
        UNNEST(purchaseorder_items.shiplocations) AS purchaseorder_items_shiplocations
;



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_item_shiplocation_ldg;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_item_shiplocation_ldg 
  SELECT
      purchaseorder.externalid,
      cast(format_datetime('%Y-%m-%d %H:%M:%E3S%Ez', timestamp(metadata_eventreceivedtimestamp )) as string) AS metadata_eventreceivedtimestamp,
      cast(latest_po_ship.metadata_revisionid as string ) AS metadata_revisionid,
      CAST(purchaseorder_items.casepackindicator AS STRING) AS purchaseorder_items_casepackindicator,
      purchaseorder_items.costsource AS purchaseorder_items_costsource,
      purchaseorder_items.earliestshipdate AS purchaseorder_items_earliestshipdate,
      purchaseorder_items.externalitemid AS purchaseorder_items_externalitemid,
      CAST( purchaseorder_items.initialunitcost AS STRING) AS purchaseorder_items_initialunitcost,
     CAST ( purchaseorder_items.itemid AS INT64) AS purchaseorder_items_itemid,
      purchaseorder_items.latestshipdate AS purchaseorder_items_latestshipdate,
      cast (purchaseorder_items.nonscalingindicator as string) AS purchaseorder_items_nonscalingindicator,
      purchaseorder_items.origincountry AS purchaseorder_items_origincountry,
      purchaseorder_items.referenceitem AS purchaseorder_items_referenceitem,
      purchaseorder_items_shiplocations.cancelcode AS purchaseorder_items_shiplocations_cancelcode,
      purchaseorder_items_shiplocations.canceldate AS purchaseorder_items_shiplocations_canceldate,
      purchaseorder_items_shiplocations.canceledbyid AS purchaseorder_items_shiplocations_canceledbyid,
      purchaseorder_items_shiplocations.closedate AS purchaseorder_items_shiplocations_closedate,
      substr(regexp_replace(COLLATE(purchaseorder_items_shiplocations.distributiondescription, ''), r'[\r\n]', ' '),0,999) AS purchaseorder_items_shiplocations_distributiondescription,
      purchaseorder_items_shiplocations.distributionid AS purchaseorder_items_shiplocations_distributionid,
      purchaseorder_items_shiplocations.distributionmethod AS purchaseorder_items_shiplocations_distributionmethod,
      purchaseorder_items_shiplocations.distributionstatus AS purchaseorder_items_shiplocations_distributionstatus,
      purchaseorder_items_shiplocations.estimatedinstockdate AS purchaseorder_items_shiplocations_estimatedinstockdate,
      purchaseorder_items_shiplocations.externaldistributionid AS purchaseorder_items_shiplocations_externaldistributionid,
      purchaseorder_items_shiplocations.id AS purchaseorder_items_shiplocations_id,
      purchaseorder_items_shiplocations.lastreceived AS purchaseorder_items_shiplocations_lastreceived,
      cast(purchaseorder_items_shiplocations.nonscalingindicator as string) AS purchaseorder_items_shiplocations_nonscalingindicator,
      purchaseorder_items_shiplocations.originindicator AS purchaseorder_items_shiplocations_originindicator,
      purchaseorder_items_shiplocations.originalreplenishquantity AS purchaseorder_items_shiplocations_originalreplenishquantity,
      purchaseorder_items_shiplocations.quantitycanceled AS purchaseorder_items_shiplocations_quantitycanceled,
      purchaseorder_items_shiplocations.quantityordered AS purchaseorder_items_shiplocations_quantityordered,
      purchaseorder_items_shiplocations.quantityprescaled AS purchaseorder_items_shiplocations_quantityprescaled,
      purchaseorder_items_shiplocations.quantityreceived AS purchaseorder_items_shiplocations_quantityreceived,
      purchaseorder_items_shiplocations.quantityreceivedallocatednotreleased AS purchaseorder_items_shiplocations_quantityreceivedallocatednotreleased,
      purchaseorder_items_shiplocations.releasedate AS purchaseorder_items_shiplocations_releasedate,
      cast(purchaseorder_items_shiplocations.unitretail as String) AS purchaseorder_items_shiplocations_unitretail,
      purchaseorder_items.supplierpacksize AS purchaseorder_items_supplierpacksize,
      CAST(purchaseorder_items.unitcost AS STRING) AS purchaseorder_items_unitcost,
      purchaseorder_items.universalproductcode AS purchaseorder_items_universalproductcode,
      purchaseorder_items.universalproductcodesupplement AS purchaseorder_items_universalproductcodesupplement
    FROM
      latest_po_ship
;


CREATE TEMPORARY TABLE latest_po_dist
  AS
    SELECT
        latest_po_item.purchaseorder,
        latest_po_item.metadata_eventreceivedtimestamp,
        latest_po_item.metadata_revisionid,
        latest_po_item.purchaseorder_items,
        purchaseorder_items_distributelocations
        --explode(purchaseorder_items.distributelocations) AS purchaseorder_items_distributelocations
      FROM
        latest_po_item,
        UNNEST(purchaseorder_items.distributelocations) AS purchaseorder_items_distributelocations
;



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_item_distributelocation_ldg;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_item_distributelocation_ldg 
  SELECT
      purchaseorder.externalid,
      cast(format_datetime('%Y-%m-%d %H:%M:%E3S%Ez', timestamp(latest_po_dist.metadata_eventreceivedtimestamp )) as string) AS metadata_eventreceivedtimestamp,
      CAST ( latest_po_dist.metadata_revisionid AS STRING) AS latest_po_dist,
      CAST(purchaseorder_items.casepackindicator AS STRING) AS purchaseorder_items_casepackindicator,
      purchaseorder_items.costsource AS purchaseorder_items_costsource,
      purchaseorder_items_distributelocations.distributionid AS purchaseorder_items_distributelocations_distributionid,
      purchaseorder_items_distributelocations.externaldistributionid AS purchaseorder_items_distributelocations_externaldistributionid,
      purchaseorder_items_distributelocations.id AS purchaseorder_items_distributelocations_id,
      CAST(purchaseorder_items_distributelocations.nonscalingindicator AS STRING) AS purchaseorder_items_distributelocations_nonscalingindicator,
      purchaseorder_items_distributelocations.quantityallocated AS purchaseorder_items_distributelocations_quantityallocated,
      purchaseorder_items_distributelocations.quantitycanceled AS purchaseorder_items_distributelocations_quantitycanceled,
      purchaseorder_items_distributelocations.quantityprescaled AS purchaseorder_items_distributelocations_quantityprescaled,
      purchaseorder_items_distributelocations.quantityreceived AS purchaseorder_items_distributelocations_quantityreceived,
      purchaseorder_items_distributelocations.quantitytransferred AS purchaseorder_items_distributelocations_quantitytransferred,
      purchaseorder_items_distributelocations.shiplocationid AS purchaseorder_items_distributelocations_shiplocationid,
      purchaseorder_items.earliestshipdate AS purchaseorder_items_earliestshipdate,
      purchaseorder_items.externalitemid AS purchaseorder_items_externalitemid,
      CAST (purchaseorder_items.initialunitcost AS STRING) AS purchaseorder_items_initialunitcost,
      cast (purchaseorder_items.itemid as int64) AS purchaseorder_items_itemid,
      purchaseorder_items.latestshipdate AS purchaseorder_items_latestshipdate,
      CAST(purchaseorder_items.nonscalingindicator AS STRING) AS purchaseorder_items_nonscalingindicator,
      purchaseorder_items.origincountry AS purchaseorder_items_origincountry,
      purchaseorder_items.referenceitem AS purchaseorder_items_referenceitem,
      purchaseorder_items.supplierpacksize AS purchaseorder_items_supplierpacksize,
      cast(purchaseorder_items.unitcost AS STRING) AS purchaseorder_items_unitcost,
      purchaseorder_items.universalproductcode AS purchaseorder_items_universalproductcode,
      purchaseorder_items.universalproductcodesupplement AS purchaseorder_items_universalproductcodesupplement
    FROM
      latest_po_dist
;