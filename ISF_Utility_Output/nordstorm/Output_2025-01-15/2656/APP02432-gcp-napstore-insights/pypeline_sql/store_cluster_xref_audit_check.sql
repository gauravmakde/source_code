-- Check for duplicate store numbers in the excel data
--Ensures there are no duplicate store numbers in the staging table.
ABORT 'Duplicate store nums found in excel' where exists(
SELECT store_num FROM {db_env}_nap_stg.STORE_CLUSTER_XREF_LDG
GROUP BY store_num
HAVING COUNT(*) >1);

-- Check for special characters in the assortment_cluster column
--Ensures the assortment_cluster column contains only alphanumeric characters and spaces.
ABORT 'Special characters found in assortment_cluster' where exists(
SELECT store_num, assortment_cluster
FROM {db_env}_nap_stg.STORE_CLUSTER_XREF_LDG
WHERE REGEXP_SIMILAR(assortment_cluster, '[^a-zA-Z0-9 ]') = 1);

-- Check for non-null and non-integer store numbers
-- Ensures that store_num is either null or a valid integer.
ABORT 'Check for non-null and non-integer store_num' where exists(
SELECT store_num
FROM {db_env}_nap_stg.STORE_CLUSTER_XREF_LDG
WHERE store_num IS NOT NULL AND SYSLIB.ISNUMERIC(store_num) = 0 and store_num<>'Store_num') ; 

-- Check for store numbers not found in store_dim
--Ensures that all store numbers in the staging table exist in the dimension table.
ABORT 'Check for store_num not found in store_dim' where exists(
SELECT ldg.store_num
FROM {db_env}_nap_stg.STORE_CLUSTER_XREF_LDG ldg
LEFT JOIN {db_env}_nap_dim.STORE_DIM dim ON ldg.store_num = dim.store_num
WHERE dim.store_num IS NULL and SYSLIB.ISNUMERIC(ldg.store_num) = 1 ) ; 
