/*
Purpose:        Inserts data in table `receipt_sku_loc_week_agg_fact_madm`
                Contains receipt history from 2019 - 2020 from MADM
                This is a static table and not a scheduled job.
Variable(s):    {{environment_schema}} T2DL_DAS_ASSORTMENT_DIM (prod) or T3DL_ACE_ASSORTMENT
                {{env_suffix}} '' or '_dev' table suffix for prod testing
                {{start_date}} date to start for output data 
                {{end_date}} date to end for output data
Author(s):      Sara Scott
*/


-- begin 

-- Aggregate monthly receipts in date range from MADM
INSERT INTO {environment_schema}.receipt_sku_loc_week_agg_fact_madm{env_suffix}
       SELECT 
   	a.sku_idnt
   	, a.week_num
   	, a.week_start_day_date
   	, a.week_end_day_date
   	, a.mnth_idnt
    , channel_num
   	, a.store_num
    , a.price_type 
    , MAX(a.rp_ind) as rp_ind
   	, SUM(receipt_po_units) AS receipt_po_units
   	, SUM(receipt_po_retail) AS receipt_po_retail
   	, SUM(receipt_ds_units) AS receipt_ds_units
   	, SUM(receipt_ds_retail) AS receipt_ds_retail
   	, SUM(receipt_rsk_units) AS receipt_rsk_units
   	, SUM(receipt_rsk_retail) AS receipt_rsk_retail
   FROM
(
   SELECT
        r.sku_idnt AS sku_idnt
        ,cal.week_idnt as week_num
        ,cal.week_start_day_date
        ,cal.week_end_day_date
        ,cal.month_idnt as mnth_idnt
        ,r.loc_idnt as store_num
        ,CASE WHEN rcpt_po_clr_units + rcpt_dsh_clr_units > 0 THEN 'C' ELSE 'R' END AS price_type
        ,MAX(r.rp_product_ind) as rp_ind
        ,SUM(r.rcpt_po_tot_units) as receipt_po_units
        ,SUM(r.rcpt_po_tot_retl$) as receipt_po_retail
        ,SUM(r.rcpt_dsh_tot_units) as receipt_ds_units
        ,SUM(r.rcpt_dsh_tot_retl$) as receipt_ds_retail
        ,CAST(0 AS INTEGER) as receipt_rsk_units
        ,CAST(0 as INTEGER) as receipt_rsk_retail
    FROM prd_ma_bado.rcpt_sku_ld_cvw r
    JOIN prd_nap_usr_vws.day_cal_454_dim cal
      ON r.day_idnt = cal.day_idnt
    WHERE cal.day_date BETWEEN {start_date} AND {end_date}
      AND (r.rcpt_po_tot_units > 0 or r.rcpt_dsh_tot_units > 0)
      AND UPPER(r.po_type) <> 'PU' -- Photography Unit, ordered for the photo studio
     GROUP BY 1,2,3,4,5,6,7

    UNION ALL 
    
    SELECT 
    	r.sku_idnt AS sku_idnt
    	,cal.week_idnt as week_num
    	,cal.week_start_day_date
    	,cal.week_end_day_date
    	,cal.month_idnt as mnth_idnt
    	,r.loc_idnt as store_num
      , CASE WHEN CLRC_IND = 'Y'  THEN 'C' ELSE 'R' END as price_tpye
      ,MAX(r.rp_product_ind) as rp_ind
        ,CAST(0 AS INTEGER) as receipt_po_units
        ,CAST(0 AS INTEGER) as receipt_po_retail
        ,CAST(0 AS INTEGER) as receipt_ds_units
        ,CAST(0 AS INTEGER) as receipt_ds_retail
        ,SUM(r.inv_tsfr_rsk_in_units) as receipt_rsk_units
        ,SUM(r.inv_tsfr_rsk_in_retl$) as receipt_rsk_retail
    FROM PRD_MA_BADO.INV_TSFR_SKU_LD_CVW r
    JOIN prd_nap_usr_vws.day_cal_454_dim cal 
      ON r.day_idnt = cal.day_idnt
    WHERE cal.day_date BETWEEN {start_date} AND {end_date}
    AND r.inv_tsfr_rsk_in_units > 0
    GROUP BY 1,2,3,4,5,6,7
    ) a
    JOIN prd_nap_usr_vws.STORE_DIM sd
    ON a.store_num = sd.store_num
    GROUP BY 1,2,3,4,5,6,7,8;

-- end

COLLECT STATS
    PRIMARY INDEX (sku_idnt, week_num, store_num)
    ,COLUMN (sku_idnt)
    ,COLUMN (week_num)
    ,COLUMN (store_num)
    ,COLUMN (week_num, store_num)
    ON {environment_schema}.receipt_sku_loc_week_agg_fact_madm{env_suffix};
