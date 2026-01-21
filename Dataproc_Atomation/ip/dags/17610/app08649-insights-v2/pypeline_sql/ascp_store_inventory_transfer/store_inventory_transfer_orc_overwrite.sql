create temporary view tmp using ORC
OPTIONS (path "s3://{s3_bucket_orc}/temporary/ascp-store-inventory-transfer-lifecycle-tmp");

insert overwrite table `ascp-store-inventory-transfer-lifecycle-v1`
SELECT inventoryTransferId,
   inventoryTransferIdType,
   createdDetail, 
   canceledDetail, 
   shippedDetails, 
   receivedDetails
FROM tmp;