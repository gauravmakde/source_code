CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'MERCH_CUSTOMER_BRAND_BASE1', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.MERCH_CUSTOMER_BRAND_BASE1
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT 
	,DEFAULT MERGEBLOCKRATIO 
(	
LEVEL                             VARCHAR(50)
,DIM                              VARCHAR(100) 
,TY_LY_IND                        VARCHAR(5)
,NPG_IND                          VARCHAR(5)
,BANNER                           VARCHAR(20)
,YEAR_IDNT                        INTEGER
,HALF_IDNT                        INTEGER
,QUARTER_IDNT                     INTEGER
,MONTH_IDNT                       INTEGER
,HALF_LABEL                       VARCHAR(25)
,QUARTER_LABEL                    VARCHAR(25)
,MONTH_LABEL                      VARCHAR(25) 
,CUSTOMERS                        INTEGER
,TRIPS                            INTEGER
,NTN_CUSTOMERS                    INTEGER
,GROSS_ITEMS                      INTEGER
,NET_ITEMS                        INTEGER
,GROSS_SALES                      DECIMAL(12, 2)
,NET_SALES                        DECIMAL(12, 2)
)
PRIMARY INDEX(month_idnt,dim);

GRANT SELECT ON {environment_schema}.MERCH_CUSTOMER_BRAND_BASE1 TO PUBLIC;