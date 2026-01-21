/*
Purpose:        Creates empty table in {{environment_schema}} for XREF PO Dashboard
                    xref_po_daily
Variable(s):     {{environment_schema}} T2DL_DAS_OPEN_TO_BUY (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
*/
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'xref_po_daily{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.xref_po_daily{env_suffix}
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1
(  
	 refresh_timestamp                      TIMESTAMP
	,purchase_order_number                  VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,channel_num                            VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,channel_brand                          VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,division                               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,subdivision                            VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,department                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,"class"                                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,subclass                               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,supplier_number                        VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,supplier_name                          VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,vpn                                    VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,style_desc                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,color_num                        		VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,supp_color                         	VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,customer_choice						VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,po_status								VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,edi_ind                                CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,otb_eow_date							DATE
	,otb_month 		                        VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,otb_month_idnt 						INTEGER
	,parent_otb_month_idnt					INTEGER
	,start_ship_date						DATE
	,end_ship_date							DATE
	,latest_approval_date					DATE
	,latest_close_event_tmstp_pacific		TIMESTAMP 
	,comments								VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC 
	,order_type                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,internal_po_ind                        CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,xref_po_ind                            CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,parent_po_ind                          CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,parent_po                              VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,npg_ind                                CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,gwp_ind								CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,po_type                                VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,purchase_type                          VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_idnt 							INTEGER
	,"Month"								VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, RCPT_COST 								DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, RCPT_RETAIL 							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, RCPT_UNITS 							INTEGER
	, QUANTITY_ORDERED 						INTEGER
	, QUANTITY_RECEIVED 					INTEGER
	, QUANTITY_CANCELED 					INTEGER
	, QUANTITY_OPEN 						INTEGER
	, UNIT_COST_AMT 						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, TOTAL_ANTICIPATED_RETAIL_AMT 			DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, DC_RCPT_UNITS							INTEGER
	, DC_RCPT_COST							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, vasn_sku_qty 							INTEGER
	, vasn_cost 							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, vasn_ind								CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	, XREF_RCPT_C							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, PARENT_RCPT_C							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, XREF_RCPT_U							INTEGER
	, PARENT_RCPT_U							INTEGER
	, XREF_OO_C								DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, PARENT_OO_C							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, XREF_OO_U								INTEGER
	, PARENT_OO_U							INTEGER
	, net_store_receipts					INTEGER
	, store_over_u							INTEGER
	, store_over_c							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, parent_po_open_w_xref_receipts_u		INTEGER
	, parent_po_open_w_xref_receipts_c		DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, parent_po_open_w_xref_receipts_r		DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, parent_po_open_w_xref_open_u			INTEGER
	, parent_po_open_w_xref_open_c			DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, parent_po_open_w_xref_open_r			DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, canceled_u							INTEGER
	, canceled_c							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, canceled_r							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, open_u								INTEGER
	, open_c								DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, open_r								DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, eom_ordered_u							INTEGER
	, eom_ordered_c							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, eom_ordered_r							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, og_ordered_u							INTEGER
	, og_ordered_c							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, og_ordered_r							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	)
PRIMARY INDEX (
	 purchase_order_number
	,internal_po_ind
	,channel_num
	,division
	,subdivision
	,department
	,subclass
	,vpn
	,style_desc
	,color_num
	,supp_color
	,supplier_number
	,supplier_name


	)
PARTITION BY RANGE_N(otb_eow_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE);

GRANT SELECT ON {environment_schema}.xref_po_daily{env_suffix} TO PUBLIC;