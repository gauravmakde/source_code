/*
Purpose:        Identify Canceled POs and Link Xref POs 
Variable(s):    {{environment_schema}} T2DL_DAS_OPEN_TO_BUY (prod) or {environment_schema}
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):      Sara Riker
Date Created:   1/31/24
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'po_sku_cancels', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.po_sku_cancels
        ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1
(  
     banner                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,purchase_order_number              VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,status                             VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,otb_month_idnt                     INTEGER
    ,otb_month_label                    VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,month_idnt                         INTEGER
    ,action_month_label                 VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,rms_sku_num                        VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,div_idnt                           INTEGER
    ,div_desc                           VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,grp_idnt                           INTEGER
    ,grp_desc                           VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,dept_idnt                          INTEGER
    ,dept_desc                          VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Division                           VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Subdivision                        VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Department                         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,quantrix_category                  VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,vendor_name                        VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,npg_ind                            VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,rp_ind                             VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,customer_choice                    VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,po_type                            VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,purchase_type                      VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,comments                           VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,internal_po_ind                    VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,parent_po_ind                      VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,xref_po_ind                        VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,xref_reversal_ind                  INTEGER
    ,parent_po                          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,unit_cost                          DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,total_expenses_per_unit            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,total_duty_per_unit                DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,original_ordered_u                 INTEGER
    ,original_ordered_c                 DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,approved_ordered_u                 INTEGER
    ,approved_ordered_c                 DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,ordered_u                          INTEGER
    ,ordered_c                          DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,supplier_cancel_u                  INTEGER
    ,supplier_cancel_c                  DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,buyer_cancel_u                     INTEGER
    ,buyer_cancel_c                     DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,other_cancel_u                     INTEGER
    ,other_cancel_c                     DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,inbound_rcpt_u                     INTEGER
    ,inbound_rcpt_c                     DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,outbound_rcpt_u                    INTEGER
    ,outbound_rcpt_c                    DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,on_order_u                         INTEGER
    ,on_order_c                         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,xref_inbound_rcpt_u                INTEGER
    ,xref_outbound_rcpt_u               INTEGER
    ,xref_on_order_u                    INTEGER 
    ,invalid_cancel_u                   INTEGER
    ,invalid_cancel_c                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,partial_invalid_cancel_u           INTEGER
    ,partial_invalid_cancel_c           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,reversal_cancel_u                  INTEGER
    ,reversal_cancel_c                  DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,potential_cancel_u                 INTEGER
    ,potential_cancel_c                 DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,potential_oo_cancel_u              INTEGER
    ,potential_oo_cancel_c              DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,false_cancel_u                     INTEGER
    ,false_cancel_c                     DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,potential_false_cancel_u           INTEGER
    ,potential_false_cancel_c           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,true_cancel_u                      INTEGER
    ,true_cancel_c                      DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,potential_true_cancel_u            INTEGER
    ,potential_true_cancel_c            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    ,update_timestamp                   TIMESTAMP
)
PRIMARY INDEX(purchase_order_number, rms_sku_num)
;

GRANT SELECT ON {environment_schema}.po_sku_cancels TO PUBLIC;