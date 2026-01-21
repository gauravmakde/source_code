
--------------------- BEGIN: Dataset DDL'S

-- 2021.08.05 - Initial pass at the DDL creation of dataset
-- DROP TABLE {environment_schema}.{table_name};
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', '{table_name}{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.{table_name}{env_suffix} ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
	(
	ROW_ID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	,SKU_IDNT VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	,DAY_DT DATE FORMAT 'YYYY-MM-DD'
	,PRICE_TYPE VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,LOC_IDNT INTEGER 
	,SALES_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,SALES_DOLLARS DECIMAL(12,2) 
	,RETURN_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,RETURN_DOLLARS DECIMAL(12,2)
	,DEMAND_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,DEMAND_DOLLARS DECIMAL(12,2)
	,DEMAND_CANCEL_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,DEMAND_CANCEL_DOLLARS DECIMAL(12,2)
	,DEMAND_DROPSHIP_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,DEMAND_DROPSHIP_DOLLARS DECIMAL(12,2)
	,SHIPPED_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,SHIPPED_DOLLARS DECIMAL(12,2)
	,STORE_FULFILL_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,STORE_FULFILL_DOLLARS DECIMAL(12,2)
	,EOH_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,EOH_DOLLARS DECIMAL(12,2)
	,BOH_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,BOH_DOLLARS DECIMAL(12,2)
	,EOH_DROPSHIP_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,EOH_DROPSHIP_DOLLARS DECIMAL(12,2)
	,BOH_DROPSHIP_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,BOH_DROPSHIP_DOLLARS DECIMAL(12,2)
	,NONSELLABLE_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,RECEIPT_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,RECEIPT_DOLLARS DECIMAL(12,2)
	,RECEIPT_PO_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,RECEIPT_PO_DOLLARS DECIMAL(12,2)
	,RECEIPT_PAH_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,RECEIPT_PAH_DOLLARS DECIMAL(12,2)
	,RECEIPT_DROPSHIP_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,RECEIPT_DROPSHIP_DOLLARS DECIMAL(12,2)
	,RECEIPT_RESERVESTOCK_UNITS INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	,RECEIPT_RESERVESTOCK_DOLLARS DECIMAL(12,2)
	,CURRENT_PRICE DECIMAL(12,2) COMPRESS (0.02 ,99.95 ,99.97 ,89.97 ,79.97 ,69.97 ,59.97 ,49.97 ,44.97 ,39.97 ,34.97 ,29.97 ,24.97 ,19.97 ,14.97 )
	,REGULAR_PRICE DECIMAL(12,2) COMPRESS (0.02 ,99.95 ,99.97 ,89.97 ,79.97 ,69.97 ,59.97 ,49.97 ,44.97 ,39.97 ,34.97 ,29.97 ,24.97 ,19.97 ,14.97 )
	,INCOMPLETE_IND VARCHAR(1) COMPRESS ('Y','N')	
	,UPDATE_TIMESTAMP TIMESTAMP(6) WITH TIME ZONE
	,PROCESS_TIMESTAMP TIMESTAMP(6) WITH TIME ZONE
	,WEIGHTED_AVERAGE_COST DECIMAL(38,9)
    ,COST_OF_GOODS_SOLD DECIMAL(38,9)
    ,GROSS_MARGIN DECIMAL(38,9)
	)
-- Make ROW_ID an unique primary index after duplication issue is resolved
PRIMARY INDEX (ROW_ID)
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE);

-- 2021.08.05 - Error Handling Logs for pipeline build
-- DROP TABLE {environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS;
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'SKU_LOC_PRICETYPE_DAY_ERROR_LOGS{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS{env_suffix} ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
	(
	ROW_ID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	,BUS_DT DATE
	,DATASET_ISSUE VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	,PROCESS_DT DATE
	,PROCESS_TIMESTAMP TIMESTAMP(6) WITH TIME ZONE
)
-- Make ROW_ID an unique primary index after duplication issue is resolved
PRIMARY INDEX (ROW_ID)
PARTITION BY RANGE_N(BUS_DT BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE);

--------------------- END: Dataset DDL'S


--------------------- BEGIN: VIEW DDL'S
-- 2021.10.08 - data health view for the Merch Sku Data Pipeline
	-- These view will only show 7 days rolling history. The underlying dataset will contain the historical
	-- The average day frames variations are due to ensuring the exponentially weighted averages cover enough days for each 7 and 30 day criteria.	
	-- 2021.10.14 - Altered view to show 7 days (8 in reality but includes today which is partial). Fixed the moving average issue for both the 7 and 30 day criteria's.
REPLACE VIEW {environment_schema}.SKU_LOC_PRICETYPE_DAY_HEALTH_VW
AS
LOCK ROW FOR ACCESS
select a.day_dt
	,a.job_name
	,a.kpi_name
	,a.source_kpi
	,CAST(a.source_kpi_emavg AS DECIMAL(15,2)) as moving_average_7_days
	,CAST((a.source_kpi - a.source_kpi_emavg)/a.source_kpi as decimal(15,2)) moving_average_7_days_diff
	,CAST(b.source_kpi_emavg as DECIMAL(15,2)) as moving_average_30_days
	,CAST((b.source_kpi - b.source_kpi_emavg)/b.source_kpi as decimal(15,2)) moving_average_30_days_diff
	,a.source_kpi_comparison
	,a.source_kpi_comparison_percent_diff
	,a.process_timestamp
from 
	(select *
	from movingaverage ( 
		on (select * from {environment_schema}.SKU_LOC_PRICETYPE_DAY_HEALTH where day_dt between current_date - 8 and current_date) partition by job_name,kpi_name order by day_dt
		USING 
		mavgtype ('E')
		TargetColumns ('source_kpi')
		windowsize ('7')
		includefirst ('false')
	) as dt ) a join 
	(select *
	from movingaverage ( 
		on (select * from {environment_schema}.SKU_LOC_PRICETYPE_DAY_HEALTH where day_dt between current_date - 31 and current_date) partition by job_name,kpi_name order by day_dt
		USING 
		mavgtype ('E')
		TargetColumns ('source_kpi')
		windowsize ('30')
		includefirst ('true')
	) as dt ) b
		on a.day_dt = b.day_dt
			and a.job_name = b.job_name
			and a.kpi_name = b.kpi_name;

-- 2021.10.19 - Error logs view for the Merch Sku Data Pipeline
	-- This view will show the records that did not make the final dataset due to having issues with the record itself
REPLACE VIEW {environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS_VW{env_suffix}
AS
LOCK ROW FOR ACCESS
select row_id
	,dataset_issue
	,process_dt
	,process_timestamp
from {environment_schema}.SKU_LOC_PRICETYPE_DAY_ERROR_LOGS{env_suffix};		

-- 2021.10.19 - Main output dataset view ddl for the Merch Sku Data Pipeline
REPLACE VIEW {environment_schema}.{view_name}{env_suffix}
AS
LOCK ROW FOR ACCESS
select row_id
	,sku_idnt
	,day_dt
	,price_type
	,loc_idnt
	,sales_units
	,sales_dollars
	,return_units
	,return_dollars
	,demand_units
	,demand_dollars
	,demand_cancel_units
	,demand_cancel_dollars
	,demand_dropship_units
	,demand_dropship_dollars
	,shipped_units
	,shipped_dollars
	,store_fulfill_units
	,store_fulfill_dollars
	,eoh_units
	,eoh_dollars
	,boh_units
	,boh_dollars
	,eoh_dropship_units
	,eoh_dropship_dollars
	,boh_dropship_units
	,boh_dropship_dollars
	,nonsellable_units
	,receipt_units
	,receipt_dollars
	,receipt_po_units
	,receipt_po_dollars
	,receipt_pah_units
	,receipt_pah_dollars
	,receipt_dropship_units
	,receipt_dropship_dollars
	,receipt_reservestock_units
	,receipt_reservestock_dollars
	,current_price
	,regular_price
	,incomplete_ind
	,weighted_average_cost
	,cost_of_goods_sold
	,gross_margin
	,update_timestamp
	,process_timestamp
from {environment_schema}.{table_name}{env_suffix};

--------------------- END: VIEW DDL'S