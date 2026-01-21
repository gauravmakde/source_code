CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'npg_scorecard', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.npg_scorecard
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT 
	,DEFAULT MERGEBLOCKRATIO 
(	
product_attribute                 VARCHAR(50)
,dimension                        VARCHAR(100) 
,ty_ly_ind     					  VARCHAR(5)
,npg_ind                          VARCHAR(5)
,year_idnt                        INTEGER
,half_idnt                        INTEGER
,quarter_idnt                     INTEGER
,month_idnt                       INTEGER
,half_label                       VARCHAR(25)
,quarter_label                    VARCHAR(25)
,month_label                      VARCHAR(25) 
,customers                        INTEGER
,trips                            INTEGER
,ntn_customers                    INTEGER
,gross_items                      INTEGER
,net_items                        INTEGER
,gross_spend                      DECIMAL(12, 2)
,net_spend                        DECIMAL(12, 2)
)
PRIMARY INDEX(month_idnt,dimension);

GRANT SELECT ON {environment_schema}.npg_scorecard TO PUBLIC;