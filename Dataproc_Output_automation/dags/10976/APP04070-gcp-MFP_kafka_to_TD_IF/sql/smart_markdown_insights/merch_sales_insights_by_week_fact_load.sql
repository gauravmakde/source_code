SET QUERY_BAND = 'App_ID=app04070;
DAG_ID=smartmarkdown_insights_10976_tech_nap_merch;
Task_Name=merch_sales_insights_by_week_fact_load;'
FOR SESSION VOLATILE;

ET;


CREATE MULTISET VOLATILE TABLE MERCH_SMD_INSIGHTS_SALES_DT_BY_WEEK_VT
(
	snapshot_date DATE FORMAT 'YYYY-MM-DD' NOT NULL,
	rms_style_num	VARCHAR(10)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	color_num	VARCHAR(10)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	channel_country	VARCHAR(10)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	channel_brand	VARCHAR(20)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	selling_channel	VARCHAR(20)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	first_receipt_date DATE FORMAT 'YYYY-MM-DD',
	last_receipt_date DATE FORMAT 'YYYY-MM-DD',
	first_sales_date DATE FORMAT 'YYYY-MM-DD'
)
PRIMARY INDEX (snapshot_date,rms_style_num,color_num,channel_country,channel_brand,selling_channel)
ON COMMIT PRESERVE ROWS;

	

ET;

INSERT INTO MERCH_SMD_INSIGHTS_SALES_DT_BY_WEEK_VT
	(	
				snapshot_date
				,rms_style_num
				,color_num
				,channel_country
				,channel_brand
				,selling_channel
				,first_receipt_date
				,last_receipt_date				
				,first_sales_date
	)
	SELECT 
				cmibwf.snapshot_date
				,cmibwf.rms_style_num
				,cmibwf.color_num 
				,cmibwf.channel_country
				,cmibwf.channel_brand
				,cmibwf.selling_channel
				,MIN(mrdrwf.first_receipt_date) AS first_receipt_date
				,MAX(mrdrwf.last_receipt_date) AS last_receipt_date				
				,MIN(cmsv.business_day_date) AS first_sales_date
			FROM
			PRD_NAP_BASE_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT cmibwf							
			JOIN PRD_NAP_BASE_VWS.ETL_BATCH_DT_LKUP etl
				ON 	cmibwf.snapshot_date = etl.dw_batch_dt	
			JOIN PRD_NAP_BASE_VWS.CLEARANCE_MARKDOWN_SALES_VW cmsv
				ON cmibwf.rms_style_num = cmsv.rms_style_num
				AND cmibwf.color_num     = cmsv.color_num
				AND cmibwf.channel_country = cmsv.channel_country
				AND cmibwf.channel_brand   = cmsv.channel_brand																					
				AND cmibwf.selling_channel = cmsv.selling_channel
			JOIN PRD_NAP_BASE_VWS.MERCH_RECEIPT_DATE_RESET_APPLIED_WEEK_FACT mrdrwf
				ON  mrdrwf.rms_style_num = cmibwf.rms_style_num
				AND mrdrwf.color_num = cmibwf.color_num
				AND mrdrwf.channel_country = cmibwf.channel_country
				AND mrdrwf.channel_brand   = cmibwf.channel_brand
				AND mrdrwf.selling_channel = cmibwf.selling_channel
				AND mrdrwf.week_end_date = cmibwf.snapshot_date	
			WHERE  etl.interface_code= 'CMD_WKLY'
			AND cmsv.business_day_date <= etl.dw_batch_dt
			AND cmsv.business_day_date >= mrdrwf.first_receipt_date
			GROUP BY 1,2,3,4,5,6;

ET;			
			
INSERT INTO MERCH_SMD_INSIGHTS_SALES_DT_BY_WEEK_VT
	(	
				snapshot_date
				,rms_style_num
				,color_num
				,channel_country
				,channel_brand
				,selling_channel					
				,first_sales_date
	)
	SELECT 
				cmibwf.snapshot_date
				,cmibwf.rms_style_num
				,cmibwf.color_num 
				,cmibwf.channel_country
				,cmibwf.channel_brand
				,cmibwf.selling_channel								
				,MIN(cmsv.business_day_date) AS first_sales_date
			FROM
			PRD_NAP_BASE_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT cmibwf							
			JOIN PRD_NAP_BASE_VWS.ETL_BATCH_DT_LKUP etl
				ON 	cmibwf.snapshot_date = etl.dw_batch_dt	
			JOIN PRD_NAP_BASE_VWS.CLEARANCE_MARKDOWN_SALES_VW cmsv
				ON cmibwf.rms_style_num = cmsv.rms_style_num
				AND cmibwf.color_num     = cmsv.color_num
				AND cmibwf.channel_country = cmsv.channel_country
				AND cmibwf.channel_brand   = cmsv.channel_brand																					
				AND cmibwf.selling_channel = cmsv.selling_channel
			LEFT JOIN MERCH_SMD_INSIGHTS_SALES_DT_BY_WEEK_VT mswsdvt
				ON  mswsdvt.rms_style_num = cmibwf.rms_style_num
				AND mswsdvt.color_num = cmibwf.color_num
				AND mswsdvt.channel_country = cmibwf.channel_country
				AND mswsdvt.channel_brand   = cmibwf.channel_brand
				AND mswsdvt.selling_channel = cmibwf.selling_channel
				AND mswsdvt.snapshot_date = cmibwf.snapshot_date		
			WHERE  etl.interface_code= 'CMD_WKLY'
			AND cmsv.business_day_date <= etl.dw_batch_dt
			AND mswsdvt.rms_style_num IS NULL
			GROUP BY 1,2,3,4,5,6;			

ET;
	 
COLLECT STATISTICS  
COLUMN(snapshot_date,rms_style_num,color_num,channel_country,channel_brand,selling_channel)
on MERCH_SMD_INSIGHTS_SALES_DT_BY_WEEK_VT ; 

ET;


UPDATE tgt 
FROM PRD_NAP_BASE_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT tgt,
MERCH_SMD_INSIGHTS_SALES_DT_BY_WEEK_VT src
SET
	first_sales_date = src.first_sales_date,
	weeks_sales = ((SELECT TRUNC(etl.dw_batch_dt) from PRD_NAP_BASE_VWS.ETL_BATCH_DT_LKUP etl WHERE  etl.interface_code= 'CMD_WKLY') - TRUNC(src.first_sales_date)/7),
    dw_sys_updt_tmstp = CURRENT_TIMESTAMP
WHERE src.snapshot_date = tgt.snapshot_date
AND src.rms_style_num = tgt.rms_style_num
AND src.color_num  = tgt.color_num
AND src.channel_country = tgt.channel_country
AND src.channel_brand   = tgt.channel_brand
AND src.selling_channel = tgt.selling_channel;

ET;

-- at OMNI level
UPDATE tgt FROM PRD_NAP_BASE_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT tgt,
(
    SELECT
		mswsdvt.snapshot_date,
        mswsdvt.rms_style_num,
        mswsdvt.color_num,
        mswsdvt.channel_country,
        mswsdvt.channel_brand
		,MIN(mswsdvt.first_sales_date) AS first_sales_date		
		,((SELECT TRUNC(etl.dw_batch_dt) from PRD_NAP_BASE_VWS.ETL_BATCH_DT_LKUP etl WHERE  etl.interface_code= 'CMD_WKLY') - TRUNC(MIN(mswsdvt.first_sales_date))/7.0) AS weeks_sales
		FROM
MERCH_SMD_INSIGHTS_SALES_DT_BY_WEEK_VT mswsdvt
GROUP BY 1,2,3,4,5
) src
SET
    first_sales_date = src.first_sales_date,
	weeks_sales = src.weeks_sales,
    dw_sys_updt_tmstp = CURRENT_TIMESTAMP
WHERE src.snapshot_date = tgt.snapshot_date
AND src.rms_style_num = tgt.rms_style_num
AND src.color_num  = tgt.color_num
AND src.channel_country = tgt.channel_country
AND src.channel_brand   = tgt.channel_brand
AND (tgt.selling_channel = 'OMNI' OR tgt.total_inv_qty   = 0);


ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;