create temporary view tmp using ORC
OPTIONS (path "s3://{s3_bucket_orc}/temporary/ascp-store-inventory-allocation-lifecycle-tmp");

insert overwrite table `ascp-store-inventory-allocation-lifecycle-v1`
SELECT inventoryTransferId, 
   inventoryTransferIdType, 
   allocationReceivedDetails
FROM tmp;