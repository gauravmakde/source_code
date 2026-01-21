/*
Purpose:        Inserts data in table `receipt_sku_loc_week_agg_fact`
                Contains receipt history from 2021+ from NAP
Variable(s):    {{environment_schema}} T2DL_DAS_ASSORTMENT_DIM (prod) or T3DL_ACE_ASSORTMENT
                {{env_suffix}} '' or '_dev' table suffix for prod testing
				{{start_date}} date to start for output data
				{{end_date}} date to end for output data
Author(s):      Sara Scott
*/


-- begin

--DROP TABLE dates
CREATE MULTISET VOLATILE TABLE dates AS (
		SELECT 
			week_idnt 
			, week_start_day_date
			, week_end_day_date
			, MIN(week_start_day_date) OVER(ORDER BY week_idnt) as window_start_date
			, MAX(week_end_day_date) OVER (ORDER BY week_idnt) as window_end_date
		FROM prd_nap_usr_vws.day_cal_454_dim 
		WHERE day_date BETWEEN {start_date} and {end_date}
		GROUP BY 1,2,3
		                                            
)
WITH DATA
PRIMARY INDEX (week_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
    PRIMARY INDEX (week_idnt)
    ON dates;

DELETE FROM {environment_schema}.receipt_sku_loc_week_agg_fact{env_suffix} WHERE week_start_day_date >= (SELECT DISTINCT window_start_date FROM dates) and week_end_day_date <= (SELECT DISTINCT window_end_date FROM dates); 
INSERT INTO {environment_schema}.receipt_sku_loc_week_agg_fact{env_suffix}
   SELECT 
   	  sku_idnt
   	, week_num 
   	, week_start_day_date
   	, week_end_day_date
   	, month_num AS mnth_idnt
   	, channel_num 
   	, store_num 
   	, ds_ind
   	, rp_ind
   	, npg_ind
   	, ss_ind
   	, gwp_ind
	, CASE WHEN SUM(receipts_clearance_units) + SUM(receipts_crossdock_clearance_units) > 0 THEN 'C' ELSE 'R' end as price_type
   	, SUM(receipts_regular_units + receipts_clearance_units  + receipts_crossdock_regular_units + receipts_crossdock_clearance_units) AS receipt_tot_units
   	, SUM(receipts_regular_retail + receipts_clearance_retail  + receipts_crossdock_regular_retail + receipts_crossdock_clearance_retail) AS receipt_tot_retail
   	, SUM(receipts_regular_cost + receipts_clearance_cost  + receipts_crossdock_regular_cost + receipts_crossdock_clearance_cost) AS receipt_tot_cost
   	, SUM(CASE WHEN ds_ind = 'N' THEN receipts_regular_units + receipts_clearance_units  + receipts_crossdock_regular_units + receipts_crossdock_clearance_units ELSE 0 END) AS receipt_po_units
   	, SUM(CASE WHEN ds_ind = 'N' THEN receipts_regular_retail + receipts_clearance_retail  + receipts_crossdock_regular_retail + receipts_crossdock_clearance_retail ELSE 0 END) AS receipt_po_retail
   	, SUM(CASE WHEN ds_ind = 'N' THEN receipts_regular_cost + receipts_clearance_cost  + receipts_crossdock_regular_cost + receipts_crossdock_clearance_cost ELSE 0 END) AS receipt_po_cost
   	, SUM(CASE WHEN ds_ind = 'Y' THEN receipts_regular_units + receipts_clearance_units  + receipts_crossdock_regular_units + receipts_crossdock_clearance_units ELSE 0 END) AS receipt_ds_units
   	, SUM(CASE WHEN ds_ind = 'Y' THEN receipts_regular_retail + receipts_clearance_retail  + receipts_crossdock_regular_retail + receipts_crossdock_clearance_retail ELSE 0 END) AS receipt_ds_retail
   	, SUM(CASE WHEN ds_ind = 'Y' THEN receipts_regular_cost + receipts_clearance_cost  + receipts_crossdock_regular_cost + receipts_crossdock_clearance_cost ELSE 0 END) AS receipt_ds_cost
   	, SUM(receipt_rsk_units) as receipt_rsk_units
   	, SUM(receipt_rsk_retail) as receipt_rsk_retail
   	, SUM(receipt_rsk_cost) as receipt_rsk_cost
   	, SUM(receipt_pah_units) as receipt_pah_units
   	, SUM(receipt_pah_retail) AS receipt_pah_retail
   	, SUM(receipt_pah_cost) AS receipt_pah_cost
   	, SUM(receipt_flx_units) as receipt_flx_units 
   	, SUM(receipt_flx_retail) as receipt_flx_retail
   	, SUM(receipt_flx_cost) as receipt_flx_cost
   	FROM (
			SELECT
				rms_sku_num AS sku_idnt
				,week_num
				,week_start_day_date
				,week_end_day_date
				,month_num
				,channel_num
				,store_num
				,dropship_ind as ds_ind
				,rp_ind
				,npg_ind
				,smart_sample_ind as ss_ind
				,gift_with_purchase_ind as gwp_ind
				,SUM(rcpt.receipts_regular_units) AS receipts_regular_units
				,SUM(rcpt.receipts_regular_retail) AS receipts_regular_retail
				,SUM(rcpt.receipts_regular_cost) AS receipts_regular_cost
				,SUM(rcpt.receipts_clearance_units) AS receipts_clearance_units 
				,SUM(rcpt.receipts_clearance_retail) AS receipts_clearance_retail
				,SUM(rcpt.receipts_clearance_cost) AS receipts_clearance_cost
				,SUM(rcpt.receipts_crossdock_regular_units) AS receipts_crossdock_regular_units 
				,SUM(rcpt.receipts_crossdock_regular_retail) AS receipts_crossdock_regular_retail
				,SUM(rcpt.receipts_crossdock_regular_cost) AS receipts_crossdock_regular_cost
				,SUM(rcpt.receipts_crossdock_clearance_units) AS receipts_crossdock_clearance_units 
				,SUM(rcpt.receipts_crossdock_clearance_retail) AS receipts_crossdock_clearance_retail
				,SUM(rcpt.receipts_crossdock_clearance_cost) AS receipts_crossdock_clearance_cost
				,CAST(0 AS INTEGER) as receipt_rsk_units
				,CAST(0 AS INTEGER) as receipt_rsk_retail
				,CAST(0 AS INTEGER) as receipt_rsk_cost
				,CAST(0 AS INTEGER) as receipt_pah_units
				,CAST(0 AS INTEGER) as receipt_pah_retail
				,CAST(0 AS INTEGER) as receipt_pah_cost
				,CAST(0 AS INTEGER) as receipt_flx_units
				,CAST(0 AS INTEGER) as receipt_flx_retail
				,CAST(0 AS INTEGER) as receipt_flx_cost
			FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW rcpt
			JOIN dates on rcpt.week_num = dates.week_idnt
			--AND (rcpt.receipts_regular_units > 0 or rcpt.receipts_clearance_units > 0 or rcpt.receipts_crossdock_regular_units > 0 or rcpt.receipts_crossdock_clearance_units > 0) --removing this clause to bring in negative receipts which other reporting areas are bringing in
			GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

			UNION ALL 
			
			SELECT
				 rms_sku_num AS sku_idnt 
				,week_num
				,week_start_day_date
				,week_end_day_date
				,month_num
				,channel_num
				,store_num
				,'N' AS ds_ind
				,rp_ind
				,npg_ind
				,smart_sample_ind as ss_ind
				,gift_with_purchase_ind as gwp_ind
				,CAST(0 AS INTEGER) as receipts_regular_units
				,CAST(0 AS INTEGER) as receipts_regular_retail
				,CAST(0 AS INTEGER) as receipts_regular_cost
				,CAST(0 AS INTEGER) as receipts_clearance_units 
				,CAST(0 AS INTEGER) as receipts_clearance_retail
				,CAST(0 AS INTEGER) as receipts_clearance_cost
				,CAST(0 AS INTEGER) as receipts_crossdock_regular_units 
				,CAST(0 AS INTEGER) as receipts_crossdock_regular_retail
				,CAST(0 AS INTEGER) as receipts_crossdock_regular_cost
				,CAST(0 AS INTEGER) as receipts_crossdock_clearance_units 
				,CAST(0 AS INTEGER) as receipts_crossdock_clearance_retail
				,CAST(0 AS INTEGER) as receipts_crossdock_clearance_cost
				,SUM(rcpt.reservestock_transfer_in_units) as receipt_rsk_units
				,SUM(rcpt.reservestock_transfer_in_retail) as receipt_rsk_retail
				,SUM(rcpt.reservestock_transfer_in_cost) as receipt_rsk_cost
				,SUM(rcpt.packandhold_transfer_in_units) as receipt_pah_units
				,SUM(rcpt.packandhold_transfer_in_retail) as receipt_pah_retail
				,SUM(rcpt.packandhold_transfer_in_cost) as receipt_pah_cost
				,SUM(rcpt.racking_transfer_in_units) as receipt_flx_units
				,SUM(rcpt.racking_transfer_in_retail) as receipt_flx_retail
				,SUM(rcpt.racking_transfer_in_cost) as receipt_flx_cost
			FROM prd_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw rcpt
			JOIN dates on rcpt.week_num = dates.week_idnt
			--AND (rcpt.reservestock_transfer_in_units > 0 or rcpt.packandhold_transfer_in_units > 0 or rcpt.racking_transfer_in_units > 0)  --removing this clause to bring in negative receipts which other reporting areas are bringing in
			GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
		) sub 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;

-- end

COLLECT STATS
    PRIMARY INDEX (sku_idnt, week_num, store_num)
    ,COLUMN (sku_idnt)
    ,COLUMN (week_num)
    ,COLUMN (store_num)
    ,COLUMN (week_num, store_num)
    ON {environment_schema}.receipt_sku_loc_week_agg_fact{env_suffix};
