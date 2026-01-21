/*
ast_pipeline_prod_ddl_size_curve_eval.sql
Author: Paria Avij
project: Size curve evaluation
Purpose: Create a table for size curve dashboard
Date Created: 12/20/23
Datalab: t2dl_das_size,

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'size_curve_evaluation{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.size_curve_evaluation{env_suffix} ,FALLBACK ,	
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     size_1_id  VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,size_2_id  VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,groupid_frame VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,supplier_idnt VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,class_idnt VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,chnl_idnt VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,half VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,fiscal_year VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,size_1_rank FLOAT
    ,department_id INTEGER
	,ty_ly_ind VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
	,pct_procured FLOAT
	,pct_model FLOAT
	,sku_sales INTEGER
	,return_u INTEGER
	,eoh INTEGER
	,sku_gross_sales decimal (20,4)
	,sku_gross_reg_sales decimal (20,4)
	,sku_gross_promo_sales decimal (20,4)
	,sku_gross_clr_sales decimal (20,4) 
	,net_sales_retail decimal (20,4) 
	,net_sales_cost decimal (20,4)
	,pct_sales float
	,pct_eoh float
	,Ty_gross_sales_r decimal (20,4)
	,Ly_gross_sales_r decimal (20,4)
	,Ty_net_sales_r decimal (20,4)
	,Ly_net_sales_r decimal (20,4)
	,Ty_return_u INTEGER
	,Ly_return_u INTEGER
	,Ty_sku_sales INTEGER
	,Ly_sku_sales INTEGER
	,Ty_net_sales_c decimal (20,4)
	,Ly_net_sales_c decimal (20,4)
	,Ty_gross_reg_sales decimal (20,4)
	,Ly_gross_reg_sales decimal (20,4)
	,Ty_gross_promo_sales decimal (20,4)
	,Ly_gross_promo_sales decimal (20,4)
	,Ty_gross_clr_sales decimal (20,4)
	,Ly_gross_clr_sales decimal (20,4)
	,supplier_ VARCHAR (50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,class_ VARCHAR (50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,Department_ VARCHAR (50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,Percent_Sc_FLS_rcpt_units decimal (20,4)
	,Percent_Sc_RACK_rcpt_units decimal (20,4)
	,Percent_Sc_NRHL_rcpt_units decimal (20,4)
	,Percent_Sc_NCOM_rcpt_units decimal (20,4)
	,adoption VARCHAR (50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,rmse_pct_sales FLOAT
	,rmse_pct_procured FLOAT
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE
)
primary index (half,chnl_idnt,groupid_frame,size_1_id,size_2_id)
;


Grant SELECT ON {environment_schema}.size_curve_evaluation{env_suffix} TO PUBLIC;




 
	
	 
	 
	
	
	

