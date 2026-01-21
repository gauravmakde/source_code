create temporary view tmp using ORC
OPTIONS (path "s3://{s3_bucket_orc}/temporary/ascp-store-invalid-carton-lifecycle-tmp");

insert overwrite table `ascp-store-invalid-carton-lifecycle-v1`
SELECT
   cartonNumber, 
   createdSystemTime, 
   lastUpdatedSystemTime, 
   lastTriggeringEventType, 
   scannedDetails
FROM tmp;