/*
Purpose:        Creates empty table in {{environment_schema}} for XREF PO Dashboard
                    xref_po_daily
Variable(s):     {{environment_schema}} T2DL_DAS_OPEN_TO_BUY (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'digital_selection_reporting_weekly_base{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.digital_selection_reporting_weekly_base{env_suffix} ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      week_idnt INTEGER NOT NULL,
      week_idnt_true INTEGER NOT NULL,
      fiscal_week_num INTEGER NOT NULL,
      month_idnt INTEGER NOT NULL,
      month_idnt_true INTEGER NOT NULL,
      month_abrv VARCHAR(10),
      month_label VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      fiscal_month_num INTEGER NOT NULL,
      month_start_day_date DATE FORMAT 'YYYY-MM-DD',
      month_end_day_date DATE FORMAT 'YYYY-MM-DD',
      month_end_day_date_true DATE FORMAT 'YYYY-MM-DD',
	week_end_day_date DATE FORMAT 'YYYY-MM-DD',
      week_end_day_date_true DATE FORMAT 'YYYY-MM-DD',
      quarter_idnt INTEGER NOT NULL,
      year_idnt INTEGER NOT NULL,
      ty_ly_ind VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('TY','LY'),
      last_full_week_ind INTEGER,
      customer_choice VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sku_idnt VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
	new_flag BYTEINT,
      cf_flag BYTEINT,
      rp_flag BYTEINT,
      dropship_flag BYTEINT,
      los_flag BYTEINT,
      demand_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 ,
      demand_units DECIMAL(12,0) NOT NULL DEFAULT 0.  COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
      sales_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 ,
      sales_units DECIMAL(12,0) NOT NULL DEFAULT 0.  COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
      net_sales_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 ,
      net_sales_units DECIMAL(12,0) NOT NULL DEFAULT 0.  COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
      return_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 ,
      return_units DECIMAL(12,0) NOT NULL DEFAULT 0.  COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
      receipt_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 ,
      receipt_units DECIMAL(12,0) NOT NULL DEFAULT 0.  COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
      product_views DECIMAL(38,6),
      instock_views DECIMAL(38,6),
      eoh_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 ,
      eoh_units DECIMAL(12,0) NOT NULL DEFAULT 0.  COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
      eoh_dollars_true DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 ,
      eoh_units_true DECIMAL(12,0) NOT NULL DEFAULT 0.  COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ))
      net_sales_reg_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00   ,
      net_sales_reg_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
      net_sales_clr_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00   ,
      net_sales_clr_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
      net_sales_pro_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00  ,
      net_sales_pro_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. );
PRIMARY INDEX ( week_idnt ,channel_brand ,sku_idnt )
PARTITION BY RANGE_N(week_idnt BETWEEN 201901 AND 202553 EACH 1);

GRANT SELECT ON {environment_schema}.digital_selection_reporting_weekly_base{env_suffix} TO PUBLIC;

