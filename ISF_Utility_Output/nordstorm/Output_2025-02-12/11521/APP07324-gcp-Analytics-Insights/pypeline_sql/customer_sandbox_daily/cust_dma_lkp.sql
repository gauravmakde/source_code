SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=cust_dma_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Customer DMA Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


/* Pulls all distinct DMAs from prd_nap_usr_vws.analytical_customer */
CREATE MULTISET VOLATILE TABLE dma_mapping AS (
SELECT DISTINCT us_dma_desc AS cust_dma_desc
              , us_dma_code AS cust_dma_num
FROM prd_nap_usr_vws.analytical_customer
UNION
SELECT DISTINCT ca_dma_desc AS cust_dma_desc
              , ca_dma_code AS cust_dma_num
FROM prd_nap_usr_vws.analytical_customer
WHERE ca_dma_desc IS NOT NULL
) WITH DATA PRIMARY INDEX(cust_dma_num) ON COMMIT PRESERVE ROWS;


/*Assign Region to DMAs from the previous table (given there isn't a standard dma to region mapping table in NAP) */
-- deleting all rows from prod table before rebuild
DELETE FROM {str_t2_schema}.cust_dma_lkp;

INSERT INTO {str_t2_schema}.cust_dma_lkp
SELECT COALESCE(cust_dma_desc,'Unknown')
     , COALESCE(cust_dma_num,-1)
     , CASE WHEN UPPER(SUBSTRING(cust_dma_desc,1,9))
              IN ('LOS ANGEL','BAKERSFIE','SANTA BAR','SAN DIEGO','PALM SPRI','YUMA AZ-E') THEN 'SCAL'
            WHEN UPPER(SUBSTRING(cust_dma_desc,1,7))
              IN ('RICHMON','ROANOKE','NORFOLK','HARRISO') THEN 'NORTHEAST'
            WHEN UPPER(SUBSTRING(cust_dma_desc,1,11)) = 'CHARLOTTESV' THEN 'NORTHEAST'
            WHEN SUBSTRING(oreplace(cust_dma_desc,')',''),length(oreplace(cust_dma_desc,')',''))-1,2)
              IN ('AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT') THEN 'CANADA'
            WHEN SUBSTRING(oreplace(cust_dma_desc,')',''),length(oreplace(cust_dma_desc,')',''))-1,2)
              IN ('IA','IL','IN','KS','KY','MI','MN','MO','ND','NE','OH','SD','WI') THEN 'MIDWEST'
            WHEN SUBSTRING(oreplace(cust_dma_desc,')',''),length(oreplace(cust_dma_desc,')',''))-1,2)
              IN ('CT','DC','DE','MA','MD','ME','NH','NJ','NY','PA','RI','VT','WV') THEN 'NORTHEAST'
            WHEN SUBSTRING(oreplace(cust_dma_desc,')',''),length(oreplace(cust_dma_desc,')',''))-1,2)
              IN ('AK','CA','ID','MT','NV','OR','WA','WY') THEN 'NORTHWEST'
            WHEN SUBSTRING(oreplace(cust_dma_desc,')',''),length(oreplace(cust_dma_desc,')',''))-1,2) = 'HI' THEN 'SCAL'
            WHEN SUBSTRING(oreplace(cust_dma_desc,')',''),length(oreplace(cust_dma_desc,')',''))-1,2)
              IN ('AL','FL','GA','MS','NC','PR','SC','TN','VA') THEN 'SOUTHEAST'
            WHEN SUBSTRING(oreplace(cust_dma_desc,')',''),length(oreplace(cust_dma_desc,')',''))-1,2)
              IN ('AR','AZ','CO','LA','NM','OK','TX','UT') THEN 'SOUTHWEST'
            ELSE 'UNKNOWN'
            END AS cust_region_desc
     , CASE WHEN cust_region_desc = 'SCAL' THEN 1
            WHEN cust_region_desc = 'NORTHEAST' THEN 2
            WHEN cust_region_desc = 'CANADA' THEN 3
            WHEN cust_region_desc = 'NORTHWEST' THEN 4
            WHEN cust_region_desc = 'SOUTHEAST' THEN 5
            WHEN cust_region_desc = 'SOUTHWEST' THEN 6
            WHEN cust_region_desc = 'MIDWEST' THEN 7
            WHEN cust_region_desc = 'UNKNOWN' THEN 8
            END AS cust_region_num
     , CASE WHEN cust_region_desc = 'CANADA' THEN 'CANADA'
            WHEN cust_region_desc <> 'UNKNOWN' THEN 'US'
            ELSE 'UNKNOWN'
            END AS cust_country_desc
     , CASE WHEN cust_country_desc = 'CANADA' THEN 2
            WHEN cust_country_desc = 'US' THEN 1
            WHEN cust_country_desc = 'UNKNOWN' THEN 3
            END AS cust_country_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM dma_mapping
;


SET QUERY_BAND = NONE FOR SESSION;
