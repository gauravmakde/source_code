/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=sales_financial_fact_vw;
     Task_Name=sales_financial_fact_vw;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_USL.sales_financial_fact_vw
Prior Table Layer: T2DL_DAS_USL.sales_fact
Team/Owner: Customer Analytics
Date Created/Modified: Feb 29 2024

Note:
-- Purpose of the table: Table to directly get transaction related metrics, attributes, and associated financial metrics
-- Update Cadence: Daily

*/
    
/*
 * I will be adding several metrics to the sales_fact table. The financial metrics being added are:
 * 	units_gross
 * 	sales_gross
 * 	sales_net
 * 	sales_regprice
 * 
 * Otherwise, the full table will be pulled to create an additional layer of information.
 */

REPLACE VIEW {usl_t2_schema}.sales_financial_fact_vw
AS
LOCK ROW FOR ACCESS

SELECT
	SF.date_id
	, SF.global_tran_id
	, SF.line_item_seq_num
	, SF.store_number
	, SF.acp_id
	, SF.sku_id
	-- , SF.sale_date
	, SF.trip_id
	, SF.employee_discount_flag
	, SF.transaction_type_id
	, SF.device_id
	, SF.ship_method_id
	, SF.price_type_id
--	, SF.line_item_activity_type_desc
--	, SF.tran_type_code
--	, SF.reversal_flag
--	, SF.line_item_quantity
--	, SF.line_item_net_amt_currency_code
--	, SF.original_line_item_amt_currency_code
--	, SF.original_line_item_amt
--	, SF.line_net_usd_amt
	-- , SF.line_net_usd_amt
--	, SF.merch_dept_ind
--	, SF.non_gc_line_net_usd_amt
--	, SF.line_item_regular_price
	, SF.units_returned
	, SF.sales_returned
--	, SF.giftcard_flag
--	, SF.merch_flag
--	, SF.dw_sys_load_tmstp
	,
	-- start :: units_gross
	--SUM(
	
	CAST(CASE WHEN SF.line_item_activity_type_desc = 'SALE' THEN SF.line_item_quantity ELSE 0 END 
				*
			CASE WHEN (SF.tran_type_code = 'VOID' AND SF.reversal_flag = 'N') OR (SF.tran_type_code <> 'VOID' AND SF.reversal_flag = 'Y') THEN -1 ELSE 1 END
	AS integer) 
	--)
	AS units_gross
	
	-- END :: units_gross
	,
	 -- start :: sales_gross
	--SUM(
	
  	(CASE 
	  	WHEN SF.line_item_activity_type_desc = 'SALE' THEN
  	        (CASE
	  	        WHEN SF.line_item_net_amt_currency_code <> SF.original_line_item_amt_currency_code AND SF.original_line_item_amt_currency_code IS NOT NULL THEN SF.original_line_item_amt --Cross-border returns, use original amt
  	          	WHEN SF.line_net_usd_amt >0 AND SF.tran_type_code ='SALE' THEN SF.line_net_usd_amt
  	          	ELSE SF.line_net_usd_amt
  	        END)
  	        	*
  	        (CASE 
	  	        WHEN (SF.tran_type_code = 'VOID' AND SF.reversal_flag = 'N') OR (SF.tran_type_code <> 'VOID' AND SF.reversal_flag = 'Y') THEN -1 --Voids & reversals, sign flip
  	          	ELSE 1
  	        END)
  	    ELSE 0 END)
  	--)
	AS sales_gross

  -- end :: sales_gross
	
	,
  -- start :: sales_net

	--SUM(
  	((CASE
  	    WHEN SF.line_item_activity_type_desc = 'SALE' THEN
  	        (CASE
  	          	WHEN SF.line_item_net_amt_currency_code <> SF.original_line_item_amt_currency_code AND SF.original_line_item_amt_currency_code IS NOT NULL THEN SF.original_line_item_amt --Cross-border returns, use original amt
  	          	WHEN SF.line_net_usd_amt >0 AND SF.tran_type_code ='SALE' THEN SF.line_net_usd_amt
  	          	ELSE SF.line_net_usd_amt
  	        END)
  	        	*
  	        (CASE
  	          WHEN (SF.tran_type_code = 'VOID' AND SF.reversal_flag = 'N') OR (SF.tran_type_code <> 'VOID' AND SF.reversal_flag = 'Y') THEN -1 --Voids & reversals, sign flip
  	          ELSE 1
  	        END)
  	    ELSE 0
  	END)
  	--)
  		-
  	--SUM(
  	(CASE 
	  	WHEN SF.merch_dept_ind = 'Y' AND SF.line_item_activity_type_desc = 'RETURN' THEN
  	    	(CASE 
	  	    	WHEN SF.line_item_net_amt_currency_code <> SF.original_line_item_amt_currency_code AND SF.original_line_item_amt_currency_code IS NOT NULL THEN -1 * SF.original_line_item_amt --Cross-border returns, use original amt and sign flip
  	          	ELSE SF.line_net_usd_amt
  	     	END)
  	   			* 
  	   		(CASE 
	  	   		WHEN (SF.tran_type_code = 'VOID' AND SF.reversal_flag = 'N') OR (SF.tran_type_code <> 'VOID' AND SF.reversal_flag = 'Y') THEN 1 --Voids & reversals, sign flip
  	          	ELSE -1
  	     	END) --Sign flip to phrase as positive
  	  	ELSE 0
  	END))
  	--)
	AS sales_net

  -- end :: sales_net
	
	,

  -- start :: sales_regprice
--sum(
	  CAST(

	 		(CASE WHEN SF.line_item_activity_type_desc = 'SALE' THEN SF.line_item_quantity ELSE 0 END)
	      		*
	    	(CASE WHEN (SF.tran_type_code = 'VOID' AND SF.reversal_flag = 'N') OR (SF.tran_type_code <> 'VOID' AND SF.reversal_flag = 'Y') THEN -1 ELSE 1 END) --Voids & reversals, sign flip

	  AS integer) * SF.line_item_regular_price 
	  
	  AS sales_regprice
--)


-- end :: sales_regprice
	
FROM T2DL_DAS_USL.sales_fact AS SF
WHERE 1 = 1
	AND SF.store_number NOT IN (141, 173)
	AND SF.tran_type_code IN ('SALE', 'RETN','EXCH','VOID')
	AND SF.giftcard_flag = 0
	AND SF.merch_flag = 1
;
/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;