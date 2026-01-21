/*
MFP Inventory Channel Plans DDL
Author: Asiyah Fox
Date Created: 7/31/24
Date Last Updated: 7/31/24

Datalab: T2DL_DAS_ACE_MFP
Service Account: T2DL_NAP_SEL_BATCH
Creates Table: mfp_inv_channel_plans
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'mfp_inv_channel_plans{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.mfp_inv_channel_plans{env_suffix}
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1
(	
	 week_num                                       INTEGER
	,month_idnt                                     INTEGER
	,banner_country_num                             INTEGER
	,fulfill_type_num                               INTEGER
	,dept_num                                       INTEGER
	,channel_num                                    INTEGER
	,sp_beginning_of_period_active_qty              BIGINT
	,sp_beginning_of_period_active_cost_amt         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,op_beginning_of_period_active_qty              BIGINT
	,op_beginning_of_period_active_cost_amt         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,sp_ending_of_period_active_qty                 BIGINT
	,sp_ending_of_period_active_cost_amt            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,op_ending_of_period_active_qty                 BIGINT
	,op_ending_of_period_active_cost_amt            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,sp_receipts_active_qty                         BIGINT
	,sp_receipts_active_cost_amt                    DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,op_receipts_active_qty                         BIGINT
	,op_receipts_active_cost_amt                    DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,sp_receipts_reserve_qty                        BIGINT
	,sp_receipts_reserve_cost_amt                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,op_receipts_reserve_qty                        BIGINT
	,op_receipts_reserve_cost_amt	                DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,update_timestamp                               TIMESTAMP(6) WITH TIME ZONE	
)
PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
PARTITION BY RANGE_N (week_num BETWEEN 202201 AND 203201 EACH 1)
;
GRANT SELECT ON {environment_schema}.mfp_inv_channel_plans{env_suffix} TO PUBLIC
;