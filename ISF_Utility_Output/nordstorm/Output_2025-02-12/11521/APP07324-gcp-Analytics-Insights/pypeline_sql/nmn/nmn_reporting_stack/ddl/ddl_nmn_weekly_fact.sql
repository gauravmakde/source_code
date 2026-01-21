SET QUERY_BAND = 'App_ID=APP08172;
     DAG_ID=ddl_nmn_weekly_fact_11521_ACE_ENG;
     Task_Name=ddl_nmn_weekly_fact;'
     FOR SESSION VOLATILE;
     

/*
T2/Table Name: T2dl_DAS_NMN.nmn_weekly_fact
Team/Owner: NMN
Date Created/Modified:

Note:
-- To Support the NMN Report view 12
-- Weekly Refresh

*/

CREATE MULTISET TABLE {nmn_t2_schema}.nmn_weekly_fact ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      channel_num INTEGER,
      channel_country VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      week_idnt INTEGER,
      month_idnt INTEGER,
      quarter_idnt INTEGER,
      fiscal_year INTEGER,
      last_year_week_idnt VARCHAR(11) CHARACTER SET LATIN NOT CASESPECIFIC,
      mktg_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      nmn_ind INTEGER,
      div_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      div_num INTEGER,
      grp_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      grp_num INTEGER,
      dept_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dept_num INTEGER,
      class_desc VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_num INTEGER,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier_name VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      product_views INTEGER,
      instock_product_views INTEGER,
      order_units INTEGER,
      gross_sales_amt_usd DECIMAL(38,2),
      net_sales_amt_usd DECIMAL(38,2),
      net_sales_units DECIMAL(38,0),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX (channel_num,channel_country,week_idnt,mktg_type,finance_rollup,class_num,finance_detail,nmn_ind,div_desc,grp_desc,brand_name,supplier_name)
PARTITION BY RANGE_N(week_idnt  BETWEEN 202101  AND 202153  EACH 1 ,
202201  AND 202253  EACH 1 ,
202301  AND 202353  EACH 1 ,
202401  AND 202453  EACH 1 ,
202501  AND 202553  EACH 1);


COMMENT ON  {nmn_t2_schema}.nmn_weekly_fact IS 'Table to refresh NMN Reporting Stack Dashboard';
-- Column comments 
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.channel_num IS 'Selling channel of intent store';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.channel_country IS 'Country code of intent store';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.week_idnt IS 'Business week of transaction/funnel activity';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.month_idnt IS 'Business month of transaction/funnel activity';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.quarter_idnt IS 'Business quarter of transaction/funnel activity';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.fiscal_year IS 'Fiscal Year of transaction/funnel activity';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.last_year_week_idnt IS 'Corresponding previous year week to the business week';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.mktg_type IS 'Marketing type (Paid, Unpaid, Base) attributed to order';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.finance_rollup IS 'Finance-defined marketing attribution level of detail one step more granular than mktg_type';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.finance_detail	IS 'Finance-defined marketing attribution level of detail one step more granular than finance_rollup';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.nmn_ind IS 'Denotes whether the campaign is an NMN campaign or not';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.div_desc IS 'Division is the top level product heirarchy';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.div_num IS 'Integer value to identify a division';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.grp_desc IS 'Description given to the subdivision';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.grp_num IS 'Integer value to identify a subdivision';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.dept_desc IS 'Department to which the style belongs';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.dept_num IS 'Integer value to identify a department';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.class_desc	IS 'Class is a further division within the department';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.class_num IS 'Integer value to identify a class';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.supplier_name IS 'Order-from vendor of SKU';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.brand_name IS 'Brand name of SKU';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.npg_flag IS 'Denotes whether the SKU belongs to the Nordstrom Product Group or not';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.net_sales_amt_usd IS 'Attributed sales minus returns in US dollars';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.gross_sales_amt_usd	IS 'Attributed sales in US dollars';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.net_sales_units	IS 'Attributed units sold minus returned units';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.order_units IS 'Number of orders placed for funnel activity item';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.product_views IS 'Total number of views for funnel activity item';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.instock_product_views IS 'Number of views for funnel activity item that was instock at time of view';
COMMENT ON  {nmn_t2_schema}.NMN_WEEKLY_FACT.dw_sys_load_tmstp IS 'Data load timestamp';

SET QUERY_BAND = NONE FOR SESSION;
