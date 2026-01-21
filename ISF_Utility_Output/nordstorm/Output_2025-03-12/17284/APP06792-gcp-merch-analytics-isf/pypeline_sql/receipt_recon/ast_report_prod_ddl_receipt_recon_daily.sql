CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'receipt_recon_daily', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.receipt_recon_daily
        ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1
(  
	 refresh_timestamp                      TIMESTAMP
	,month_label                            VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,otb_month_label                        VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,recon_month_start_day_date             DATE
	,recon_month_end_day_date               DATE
	,purchase_order_number                  VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,purchase_type                          VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,approval_date                          DATE
	,original_end_ship_date                 DATE
	,ownership_price_amt                    DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,written_date                           DATE
	,latest_edi_date                        DATE
	,po_vasn_signal                         CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Y', 'N')
	,po_type                                VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,order_type                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,bom_otb_eow_date                       DATE
	,bom_status                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,eom_otb_eow_date                       DATE
	,eom_status                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,eom_start_ship_date                    DATE
	,eom_end_ship_date                      DATE
	,cur_otb_eow_date                       DATE
	,cur_status                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,cur_start_ship_date                    DATE
	,cur_end_ship_date                      DATE
	,edi_ind                                CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,npg_ind                                CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,internal_po_ind                        CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,exclude_from_backorder_avail_ind       CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,xref_po_ind                            CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,parent_po_ind                          CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,parent_po                              VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,banner_country                         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,channel                                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,division                               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,subdivision                            VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,department                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,"class"                                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,subclass                               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,vpn                                    VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,style_desc                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,vpn_label                              VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,nrf_color_code                         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,color_desc                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,supplier_color                         VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,prmy_supp_num                          VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,supplier                               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,manufacturer                           VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,smart_sample_ind                       CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,gwp_ind                                CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,prepaid_supplier_ind                   CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC --COMPRESS ('Y', 'N')
	,moi_bom_ind                            BYTEINT
	,moi_eom_ind                            BYTEINT
	,moi_add_ind                            BYTEINT
	,moi_moved_in_ind                       BYTEINT
	,moi_moved_out_ind                      BYTEINT
	,oo_in_mth_ind                          BYTEINT
	,oo_prior_mth_ind                       BYTEINT
	,cancel_all_ind                         BYTEINT
	,cancel_in_mth_ind                      BYTEINT
	,rcpt_total_ind                         BYTEINT
	,rcpt_in_mth_ind                        BYTEINT
	,rcpt_xref_ind                          BYTEINT
	,rcpt_shift_in_prior_ind                BYTEINT
	,rcpt_shift_in_future_ind               BYTEINT
	,rcpt_over_ind                          BYTEINT
	,original_orders_u						INTEGER
	,original_orders_r						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,original_orders_c						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,bom_orders_u                           INTEGER
	,bom_orders_r                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,bom_orders_c                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,eom_orders_u                           INTEGER
	,eom_orders_r                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,eom_orders_c                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,cur_orders_u                           INTEGER
	,cur_orders_r                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,cur_orders_c                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,open_u                                 INTEGER
	,open_r                                 DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,open_c                                 DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,over_u                                 INTEGER
	,over_r                                 DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,over_c                                 DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,m_over_u                               INTEGER
	,m_over_r                               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,m_over_c                               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,m_cancels_u                            INTEGER
	,m_cancels_r                            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,m_cancels_c                            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,m_receipts_u                           INTEGER
	,m_receipts_r                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,m_receipts_c                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,c_cancels_u                            INTEGER
	,c_cancels_r                            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,c_cancels_c                            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,c_vasn_u                               INTEGER
	,c_vasn_r                               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,c_vasn_c                               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,c_receipts_u                           INTEGER
	,c_receipts_r                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00   
	,c_receipts_c                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,outbound_open_c 						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,outbound_open_u						INTEGER
	,outbound_open_r						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,outbound_receipts_c 					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,outbound_receipts_u					INTEGER
	,outbound_receipts_r					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,outbound_over_c						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,outbound_over_u						INTEGER
	,outbound_over_r						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,c_vasn_u_outbound						INTEGER
	,c_vasn_r_outbound						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,c_vasn_c_outbound						DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
)
PRIMARY INDEX (
	 purchase_order_number
	,bom_otb_eow_date
	,bom_status
	,eom_otb_eow_date
	,eom_status
	,eom_start_ship_date
	,eom_end_ship_date
	,cur_otb_eow_date
	,cur_status
	,cur_start_ship_date
	,cur_end_ship_date
	,internal_po_ind
	,channel
	,division
	,subdivision
	,department
	--,"class"
	,subclass
	,vpn
	,style_desc
	,vpn_label
	,nrf_color_code
	,color_desc
	,supplier_color
	,prmy_supp_num
	,supplier
	,manufacturer
	,smart_sample_ind
	,gwp_ind
	,prepaid_supplier_ind
	,moi_bom_ind
	,moi_eom_ind
	,moi_add_ind
	,moi_moved_in_ind
	,moi_moved_out_ind
	,oo_in_mth_ind
	,oo_prior_mth_ind
	,cancel_all_ind
	,cancel_in_mth_ind
	,rcpt_total_ind
	,rcpt_in_mth_ind
	,rcpt_xref_ind
	,rcpt_shift_in_prior_ind
	,rcpt_shift_in_future_ind
	,rcpt_over_ind
	)
PARTITION BY RANGE_N(cur_otb_eow_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE);
--COMMIT;
GRANT SELECT ON {environment_schema}.receipt_recon_daily TO PUBLIC;