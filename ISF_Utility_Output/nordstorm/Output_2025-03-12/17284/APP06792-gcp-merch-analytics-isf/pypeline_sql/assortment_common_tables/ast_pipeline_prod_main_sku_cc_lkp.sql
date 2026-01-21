/*
Purpose:        Inserts data in table `sku_cc_lkp` for Sku CC Lookup
Variable(s):    {{environment_schema}} T2DL_DAS_ASSORTMENT_DIM (prod) or T3DL_ACE_ASSORTMENT
                {{env_suffix}} '' or '_dev' table suffix for prod testing
Author(s):      Sara Riker & Christine Buckler
*/


DELETE FROM {environment_schema}.sku_cc_lkp{env_suffix} ALL;

INSERT INTO {environment_schema}.sku_cc_lkp{env_suffix}  
SELECT
     rms_sku_num AS sku_idnt
    ,channel_country
    ,TRIM(COALESCE(dept_num,'UNKNOWN') || '_' || COALESCE(prmy_supp_num,'UNKNOWN') || '_' || COALESCE(supp_part_num,'UNKNOWN') || '_' || COALESCE(color_num,'UNKNOWN')) AS customer_choice -- dept || supplier || vpn || color
    ,current_timestamp AS update_timestamp 
FROM prd_nap_usr_vws.product_sku_dim_vw;

COLLECT STATS
     PRIMARY INDEX (sku_idnt, customer_choice)
    ,COLUMN (customer_choice)
    ,COLUMN (channel_country, customer_choice)
     ON {environment_schema}.sku_cc_lkp{env_suffix};