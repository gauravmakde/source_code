/*
ast_pipeline_prod_ddl_mfp_planned_history_report.sql
Author: Paria Avij
project: planned history report
Purpose: Create a table for planned history report
Date Created: 02/03/23
Date Updated: 05/17/23

Datalab: t2dl_das_open_to_buy,

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'planned_hist_report_mfp{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.planned_hist_report_mfp{env_suffix} ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     Division VARCHAR(20)
    ,Subdivision VARCHAR(20)
    ,Department VARCHAR(20)
    ,"Active vs Inactive" VARCHAR(10)
    ,snap_month_label VARCHAR(10)
    ,Month_Plans VARCHAR(10)
    ,DEPT_ID INTEGER
    ,BANNER VARCHAR(20)
	,COUNTRY VARCHAR(5)
	,MONTH_IDNT varchar(10)
	,MONTH_LABEL  varchar(10)
	,QUARTER_IDNT VARCHAR(10)
	,FISCAL_YEAR_NUM  VARCHAR(10)
	,SP_RCPTS_RESERVE_C decimal (20,4)
	,SP_RCPTS_ACTIVE_C decimal (20,4)
	,Sp_Rcpts_Less_Reserve_C decimal (20,4)
	,OP_RCPTS_RESERVE_C decimal (20,4)
	,OP_RCPTS_ACTIVE_C decimal (20,4) 
	,OP_Rcpts_Less_Reserve_C decimal (20,4) 
	,OP_BOP_Active_C decimal (20,4)
	,SP_BOP_Active_C decimal (20,4)
	,SP_net_Sales_C decimal (20,4)
	,LY_net_Sales_C decimal (20,4)
	,OP_net_Sales_C decimal (20,4)
	,TY_net_Sales_C decimal (20,4)
	,SP_WFC_Active_C decimal (20,4)
	,OP_WFC_Active_C decimal (20,4)
	,RP_ANTICIPATED_SPEND_C decimal (20,4)
	,RP_ANTICIPATED_SPEND_U decimal (20,4)
	,INACTIVE_CM_C decimal (20,4)
	,ACTIVE_CM_C decimal (20,4)
	,NRP_CM_C decimal (20,4)
	,RP_CM_C decimal (20,4)
	,CM_C decimal (20,4)
	,RCPTS_OO_INACTIVE_C decimal (20,4)
	,RCPTS_OO_ACTIVE_RP_C decimal (20,4)
	,RCPTS_OO_ACTIVE_NRP_C decimal (20,4)
	,Rp_Rcpt_Plan_C decimal (20,4)
	,RP_OO_and_CM_C decimal (20,4)
	,NRP_OO_and_CM_C decimal (20,4)
    ,rcd_update_timestamp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(Month_Plans, DEPT_ID,BANNER, COUNTRY,MONTH_LABEL)
;


Grant SELECT ON {environment_schema}.planned_hist_report_mfp{env_suffix} TO PUBLIC;