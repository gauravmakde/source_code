SET QUERY_BAND = 'App_ID=APP08172;
DAG_ID=ddl_brand_weekly_fact_11521_ACE_ENG;
Task_Name=ddl_brand_weekly_fact;'
     FOR SESSION VOLATILE;
     

/*
T2/Table Name: T2DL_DAS_NMN.BRAND_WEEKLY_FACT
Team/Owner: Customer Engagement Analystics / NMN
Date Created/Modified: 2023-06-28

Note:
-- This table supports the NMN Reporting Stack Brand Overview dashboard
-- Weekly Refresh
*/

CREATE MULTISET TABLE {nmn_t2_schema}.BRAND_WEEKLY_FACT ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      channel_num INTEGER,
      channel_country CHAR(2) CHARACTER SET Unicode NOT CaseSpecific Compress ('CA','US'),
      week_idnt INTEGER,
      last_year_week_idnt INTEGER,
      month_idnt INTEGER,
      quarter_idnt INTEGER,
      fiscal_year_num INTEGER,
      div_label VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific,
      div_num INTEGER Compress (310 ,340 ,345 ,351 ,360 ,365 ),
      grp_label VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific,
      grp_num INTEGER,
      dept_label VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific,
      dept_num INTEGER,
      class_label VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific,
      class_num INTEGER,
      brand_name VARCHAR(100) CHARACTER SET Unicode NOT CaseSpecific,
      npg_flag CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress ('N','Y'),
      supplier_name VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific,
      net_sales_dollars_r DECIMAL(16,4),
      return_dollars_r DECIMAL(16,4),
      net_sales_units DECIMAL(16,0),
      return_units DECIMAL(16,0),
      eoh_units INTEGER Compress (0 ,1 ,2 ,3 ,4 ,5 ,6 ),
      receipt_units DECIMAL(16,0),
      product_views INTEGER Compress (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,23 ,24 ,25 ,26 ,27 ,28 ,29 ,30 ,31 ,32 ,33 ,34 ,35 ,36 ,37 ,38 ,39 ,40 ,41 ,42 ,43 ,44 ,45 ,46 ,47 ,48 ,49 ,50 ,51 ,52 ,53 ,54 ,55 ,56 ,57 ,58 ,59 ,60 ,61 ,62 ),
      order_units INTEGER Compress (0 ,1 ,2),
      allocated_traffic DECIMAL(12,4) Compress 0.0000,
      instock_allocated_traffic DECIMAL(12,4) Compress 0.0000,
      dw_sys_load_tmstp TIMESTAMP(6) NOT NULL DEFAULT Current_Timestamp(6)
) PRIMARY INDEX ( channel_num ,week_idnt ,dept_num ,class_num ,brand_name )
PARTITION BY Range_N(week_idnt  BETWEEN 202101  AND 202153  EACH 1 ,
202201  AND 202253  EACH 1 ,
202301  AND 202353  EACH 1 ,
202401  AND 202453  EACH 1 ,
202501  AND 202553  EACH 1 );

-- Table comment
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT IS 'Channel, Brand, Department-Class grain overview of retail sales, inventory, conversion, and traffic-weighted instock rate';

-- Column comments
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.channel_num IS 'Channel number of location';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.channel_country IS 'Country of channel number';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.week_idnt IS 'Fiscal year and week number';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.last_year_week_idnt IS 'week_idnt for week one year prior to week_idnt';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.month_idnt IS 'Fiscal year and month number';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.quarter_idnt IS 'Fiscal year and quarter number';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.fiscal_year_num IS 'Fiscal year';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.div_label IS 'Merchandising division number and description';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.div_num IS 'Merchandising division number';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.grp_label IS 'Merchandising subdivision number and description';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.grp_num IS 'Merchandising subdivision number';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.dept_label IS 'Merchandising department number and description';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.dept_num IS 'Merchandising department number';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.class_label IS 'Merchandising class number and description';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.class_num IS 'Merchandising class number';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.brand_name IS 'Brand name';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.npg_flag IS 'Indicates whether brand is Nordstrom-made';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.supplier_name IS 'Order-from supplier name';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.net_sales_dollars_r IS 'Retail value of sales in USD';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.return_dollars_r IS 'Retail value of returns in USD';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.net_sales_units IS 'Count of units sold';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.return_units IS 'Count of units returned';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.eoh_units IS 'Count of ending on-hand inventory units';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.receipt_units IS 'Count of units receipted';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.product_views IS 'Count of product views';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.order_units IS 'Count of units ordered';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.allocated_traffic IS 'Traffic for TWIST';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.instock_allocated_traffic IS 'Instock traffic for TWIST';
COMMENT ON {nmn_t2_schema}.BRAND_WEEKLY_FACT.dw_sys_load_tmstp IS 'Data load timestamp';

SET QUERY_BAND = NONE FOR SESSION;
