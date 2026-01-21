 /* ***********************************************************************************************************
* Landing to DIM DML.
* Version: 1.00
* ************************************************************************************************************
*/ 
MERGE INTO {db_env}_NAP_BASE_VWS.STORE_CLUSTER_XREF_DIM AS tgt
USING(
	SELECT *
	FROM (
		SELECT
		store_num,
		store_name,
		assortment_cluster,
		(SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE Subject_Area_Nm ='STORE_CLUSTER_XREF' ) as dw_batch_date
	FROM
		{db_env}_NAP_STG.STORE_CLUSTER_XREF_LDG ldg
		 WHERE REGEXP_SIMILAR(store_num, '^[0-9]+$')=1
	) AS ldg
    WHERE NOT EXISTS
    	( SELECT *
    	  FROM {db_env}_NAP_BASE_VWS.STORE_CLUSTER_XREF_DIM dim
    	  WHERE	ldg.store_num = dim.store_num 
		  		AND ldg.store_name=dim.store_name
		  		AND ldg.assortment_cluster = dim.assortment_cluster 
		)
	) AS src
	ON
	( src.store_num = tgt.store_num	
	)
WHEN MATCHED THEN
UPDATE
SET
	assortment_cluster=src.assortment_cluster,
	store_name=src.store_name,
	dw_batch_date = src.dw_batch_date,
	dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT
	(
	tgt.assortment_cluster,
	tgt.store_num,
	tgt.store_name,
	tgt.dw_batch_date,
	tgt.dw_sys_load_tmstp,
	tgt.dw_sys_updt_tmstp
	)
VALUES (
	src.assortment_cluster,
	src.store_num,
	src.store_name,
	src.dw_batch_date,
	CURRENT_TIMESTAMP(0),
	CURRENT_TIMESTAMP(0)
	
);