TRUNCATE TABLE `{{params.gcp_project_id}}`.ascp.inventory_merchandise_return_to_vendor_v0_tmp;

INSERT INTO `{{params.gcp_project_id}}`.ascp.inventory_merchandise_return_to_vendor_v0_tmp 
  SELECT
      s.last_updated_millis,
      s.returntovendornumber,
      s.returntovendoreventids,
      s.externalreferencenumber,
      s.fromlocation,
      s.vendornumber,
      s.vendoraddress,
      s.returnauthorizationnumber,
      s.comments,
      s.createtime,
      s.latestupdatetime,
      s.latestcanceltime,
      s.returntovendordetails,
      s.returntovendorcanceleddetails,
      s.returntovendorshippeddetails
    FROM
      (
        SELECT
            u.*,
            row_number() OVER (PARTITION BY u.returntovendornumber ORDER BY u.last_updated_millis DESC) AS rn
          FROM
            (
              SELECT
                  0 AS last_updated_millis,
                  inventory_merchandise_return_to_vendor.returntovendornumber,
                  inventory_merchandise_return_to_vendor.returntovendoreventids,
                  inventory_merchandise_return_to_vendor.externalreferencenumber,
                  inventory_merchandise_return_to_vendor.fromlocation,
                  inventory_merchandise_return_to_vendor.vendornumber,
                  inventory_merchandise_return_to_vendor.vendoraddress,
                  inventory_merchandise_return_to_vendor.returnauthorizationnumber,
                  inventory_merchandise_return_to_vendor.comments,
                  inventory_merchandise_return_to_vendor.createtime,
                  inventory_merchandise_return_to_vendor.latestupdatetime,
                  inventory_merchandise_return_to_vendor.latestcanceltime,
                  inventory_merchandise_return_to_vendor.returntovendordetails,
                  inventory_merchandise_return_to_vendor.returntovendorcanceleddetails,
                  inventory_merchandise_return_to_vendor.returntovendorshippeddetails
                FROM
                  `{{params.gcp_project_id}}`.ascp.inventory_merchandise_return_to_vendor
              UNION ALL
              select
    last_updated_millis
   ,returnToVendorNumber
   ,returnToVendorEventIds
   ,externalReferenceNumber
   ,fromLocation
   ,vendorNumber
   ,vendorAddress
   ,returnAuthorizationNumber
   ,comments
   ,cast(createTime as datetime)
   ,cast(latestUpdateTime as datetime)
   ,cast (latestCancelTime AS DATETIME),
   array(SELECT STRUCT(
            d.returnToVendorDetail,
            CAST(d.latestUpdatedTime AS DATETIME) AS latestUpdatedTime,
            CAST(d.latestCanceledTime AS DATETIME) AS latestCanceledTime,
            CAST(d.shippedTime AS DATETIME) AS shippedTime
        )FROM UNNEST(returntovendordetails) AS d
   ),
   array(SELECT STRUCT(
            d.returnToVendorDetail,
            CAST(d.latestUpdatedTime AS DATETIME) AS latestUpdatedTime,
            CAST(d.latestCanceledTime AS DATETIME) AS latestCanceledTime,
            CAST(d.shippedTime AS DATETIME) AS shippedTime
        )FROM UNNEST(returnToVendorCanceledDetails) AS d
   ),
   array(SELECT STRUCT(
            d.returnToVendorDetail,
            CAST(d.latestUpdatedTime AS DATETIME) AS latestUpdatedTime,
            CAST(d.latestCanceledTime AS DATETIME) AS latestCanceledTime,
            CAST(d.shippedTime AS DATETIME) AS shippedTime
        )FROM UNNEST(returnToVendorShippedDetails) AS d
   )FROM
                  `{{params.gcp_project_id}}`.ascp.inventory_merchandise_return_to_vendor_v0_kafka
            ) AS u
      ) AS s
    WHERE s.rn = 1
;
TRUNCATE TABLE `{{params.gcp_project_id}}`.ascp.inventory_merchandise_return_to_vendor;
INSERT INTO `{{params.gcp_project_id}}`.ascp.inventory_merchandise_return_to_vendor (returntovendornumber, returntovendoreventids, externalreferencenumber, fromlocation, vendornumber, vendoraddress, returnauthorizationnumber, comments, createtime, latestupdatetime, latestcanceltime, returntovendordetails, returntovendorcanceleddetails, returntovendorshippeddetails)
  SELECT
      inventory_merchandise_return_to_vendor_v0_tmp.returntovendornumber,
      inventory_merchandise_return_to_vendor_v0_tmp.returntovendoreventids,
      inventory_merchandise_return_to_vendor_v0_tmp.externalreferencenumber,
      inventory_merchandise_return_to_vendor_v0_tmp.fromlocation,
      inventory_merchandise_return_to_vendor_v0_tmp.vendornumber,
      inventory_merchandise_return_to_vendor_v0_tmp.vendoraddress,
      inventory_merchandise_return_to_vendor_v0_tmp.returnauthorizationnumber,
      inventory_merchandise_return_to_vendor_v0_tmp.comments,
      inventory_merchandise_return_to_vendor_v0_tmp.createtime,
      inventory_merchandise_return_to_vendor_v0_tmp.latestupdatetime,
      inventory_merchandise_return_to_vendor_v0_tmp.latestcanceltime,
      inventory_merchandise_return_to_vendor_v0_tmp.returntovendordetails,
      inventory_merchandise_return_to_vendor_v0_tmp.returntovendorcanceleddetails,
      inventory_merchandise_return_to_vendor_v0_tmp.returntovendorshippeddetails
    FROM
      `{{params.gcp_project_id}}`.ascp.inventory_merchandise_return_to_vendor_v0_tmp
;