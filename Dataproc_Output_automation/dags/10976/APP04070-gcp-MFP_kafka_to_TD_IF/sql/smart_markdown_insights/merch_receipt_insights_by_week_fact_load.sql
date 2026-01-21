SET QUERY_BAND = 'App_ID=app04070;
DAG_ID=smartmarkdown_insights_10976_tech_nap_merch;
Task_Name=merch_receipt_insights_by_week_fact_load;'
FOR SESSION VOLATILE;

ET;


CREATE MULTISET VOLATILE TABLE MERCH_SMD_INSIGHTS_BY_WEEK_VT
(
	snapshot_date DATE FORMAT 'YYYY-MM-DD' NOT NULL,
	rms_style_num	VARCHAR(10)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	color_num	VARCHAR(10)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	channel_country	VARCHAR(10)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	channel_brand	VARCHAR(20)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	selling_channel	VARCHAR(20)	CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
	first_receipt_date DATE FORMAT 'YYYY-MM-DD',
	last_receipt_date DATE FORMAT 'YYYY-MM-DD',
	first_rack_date DATE FORMAT 'YYYY-MM-DD',
	last_rack_date DATE FORMAT 'YYYY-MM-DD',
	rack_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y')
)
PRIMARY INDEX (snapshot_date,rms_style_num,color_num,channel_country,channel_brand,selling_channel)
ON COMMIT PRESERVE ROWS;

ET;
	
INSERT INTO MERCH_SMD_INSIGHTS_BY_WEEK_VT
	(	
				snapshot_date
				,rms_style_num
				,color_num
				,channel_country
				,channel_brand
				,selling_channel
				,first_receipt_date
				,last_receipt_date
				,first_rack_date
				,last_rack_date
				,rack_ind
	)SELECT 
				cmibwf.snapshot_date
				,cmibwf.rms_style_num
				,cmibwf.color_num 
				,cmibwf.channel_country
				,cmibwf.channel_brand
				,cmibwf.selling_channel
				,MIN(mrdrwf.first_receipt_date) AS first_receipt_date
				,MAX(mrdrwf.last_receipt_date) AS last_receipt_date
				,MIN(mrdrwf.first_rack_date) AS first_rack_date
				,MAX(mrdrwf.last_rack_date) AS last_rack_date
				,MAX(mrdrwf.rack_ind) AS rack_ind
			FROM
			PRD_NAP_BASE_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT cmibwf							
			JOIN PRD_NAP_BASE_VWS.ETL_BATCH_DT_LKUP etl
			ON 	cmibwf.snapshot_date = etl.dw_batch_dt			
			JOIN PRD_NAP_BASE_VWS.MERCH_RECEIPT_DATE_RESET_APPLIED_WEEK_FACT mrdrwf
				ON  mrdrwf.rms_style_num = cmibwf.rms_style_num
				AND mrdrwf.color_num = cmibwf.color_num
				AND mrdrwf.channel_country = cmibwf.channel_country
				AND mrdrwf.channel_brand   = cmibwf.channel_brand
				AND mrdrwf.selling_channel = cmibwf.selling_channel
				AND mrdrwf.week_end_date = cmibwf.snapshot_date	
			WHERE  etl.interface_code= 'CMD_WKLY'
			GROUP BY 1,2,3,4,5,6;
ET;
	 
COLLECT STATISTICS 
COLUMN(snapshot_date,rms_style_num,color_num,channel_country,channel_brand,selling_channel)
ON MERCH_SMD_INSIGHTS_BY_WEEK_VT ; 

ET;


UPDATE tgt 
FROM PRD_NAP_BASE_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT tgt,
MERCH_SMD_INSIGHTS_BY_WEEK_VT src
SET
    first_receipt_date = src.first_receipt_date,
	last_receipt_date = src.last_receipt_date,
	first_rack_date = src.first_rack_date,
	rack_ind = src.rack_ind,
	available_to_sell = ROUND(((SELECT etl.dw_batch_dt from PRD_NAP_BASE_VWS.ETL_BATCH_DT_LKUP etl WHERE  etl.interface_code= 'CMD_WKLY') - src.first_receipt_date)/7.0),
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
		mswvt.snapshot_date,
        mswvt.rms_style_num,
        mswvt.color_num,
        mswvt.channel_country,
        mswvt.channel_brand
		,MIN(mswvt.first_receipt_date) AS first_receipt_date
		,MAX(mswvt.last_receipt_date) AS last_receipt_date
		,MIN(mswvt.first_rack_date) AS first_rack_date
		,MAX(mswvt.last_rack_date) AS last_rack_date
		,MAX(mswvt.rack_ind) AS rack_ind
		,ROUND(((SELECT etl.dw_batch_dt from PRD_NAP_BASE_VWS.ETL_BATCH_DT_LKUP etl WHERE  etl.interface_code= 'CMD_WKLY') - MIN(mswvt.first_receipt_date))/7.0) AS available_to_sell
FROM MERCH_SMD_INSIGHTS_BY_WEEK_VT mswvt
GROUP BY 1,2,3,4,5
) src
SET
    first_receipt_date = src.first_receipt_date,
	last_receipt_date = src.last_receipt_date,
	first_rack_date = src.first_rack_date,
	rack_ind = src.rack_ind,
	available_to_sell = src.available_to_sell,
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