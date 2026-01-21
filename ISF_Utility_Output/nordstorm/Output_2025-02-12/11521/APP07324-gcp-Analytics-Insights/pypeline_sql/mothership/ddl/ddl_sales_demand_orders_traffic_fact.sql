SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_mothership_sales_demand_orders_traffic_fact_11521_ACE_ENG;
     Task_Name=ddl_sales_demand_orders_traffic_fact;'
     FOR SESSION VOLATILE;

 /*
T2DL_DAS_MOTHERSHIP.SALES_DEMAND_ORDERS_TRAFFIC_FACT 
Description - This ddl creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) table of hourly sales, demand, orders, and traffic split by store number and platform (device, eg ios, web, mow)  
Contacts: Matthew Bond, Analytics
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'SALES_DEMAND_ORDERS_TRAFFIC_FACT', OUT_RETURN_MSG);


 CREATE MULTISET TABLE {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
			tran_date                	DATE NOT NULL,
			--hour_var					VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		business_unit_desc      	VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		data_source       		 	  VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		currency_code            	VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		store_num	                SMALLINT,
    		cts_segment					      VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		rfm_segment					      VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
		  	rewards_level				      VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		platform					        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,      		
     		demand_amt_excl_bopus		  DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
     		bopus_attr_store_amt		  DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
     		bopus_attr_digital_amt		DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
  			demand_canceled_amt			  DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
  			demand_units_excl_bopus		DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
  			bopus_attr_store_units		DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
  			bopus_attr_digital_units	DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
  			demand_canceled_units		  DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),	
     	 	gross_merch_sales_amt         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
		    merch_returns_amt             DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00, 
		    net_merch_sales_amt     	    DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,  
		    gross_operational_sales_amt   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00, 
		    operational_returns_amt       DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,     
		    net_operational_sales_amt     DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,     
		    gross_merch_sales_units       DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    merch_returns_units           DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    net_merch_sales_units   	    DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    gross_operational_sales_units       DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    operational_returns_units           DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    net_operational_sales_units   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),    
		    orders  					            DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    visitors  					          DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    viewing_visitors  			      DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    adding_visitors  			        DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    ordering_visitors  	  		    DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    sessions  					          DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    viewing_sessions  			      DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    adding_sessions  			        DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    ordering_sessions  			      DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    store_purchase_trips  	   	  DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    store_traffic  	   			      DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    ordering_visitors_or_store_purchase_trips  	   DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    sessions_or_store_traffic  	   DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    transactions_total  	  	   	DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    transactions_purchase  			  DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    transactions_return  			    DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    visits_total  			   		    DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    visits_purchase  	   	   		  DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    visits_return  	   			 	    DECIMAL(20,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
     PRIMARY INDEX (tran_date, business_unit_desc, store_num, data_source, cts_segment, rfm_segment, platform)
     PARTITION BY RANGE_N(tran_date BETWEEN DATE '2019-02-03' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN);
      
     
-- table comment
COMMENT ON  {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT IS '7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) table of hourly sales, demand, orders, and traffic split by store number and platform (device, eg ios, web, mow)';
;
--Column comments
COMMENT ON  {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT.data_source IS 'whether the data comes from NAP/ERTM or static r.com historical data from NRHL_redshift';
COMMENT ON  {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT.demand_amt_excl_bopus IS 'add demand_amt_excl_bopus to EITHER bopus_attr_store_amt OR bopus_attr_digital_amt to prevent double counting BOPUS demand amount (NEVER ADD ALL 3 COLUMNS)';
COMMENT ON  {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT.bopus_attr_store_amt IS 'add demand_amt_excl_bopus to EITHER bopus_attr_store_amt OR bopus_attr_digital_amt to prevent double counting BOPUS demand amount (NEVER ADD ALL 3 COLUMNS)';
COMMENT ON  {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT.bopus_attr_digital_amt IS 'add demand_amt_excl_bopus to EITHER bopus_attr_store_amt OR bopus_attr_digital_amt to prevent double counting BOPUS demand amount (NEVER ADD ALL 3 COLUMNS)';
COMMENT ON  {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT.demand_amt_excl_bopus IS 'add demand_units_excl_bopus to EITHER bopus_attr_store_units OR bopus_attr_digital_amt to prevent double counting BOPUS demand units (NEVER ADD ALL 3 COLUMNS)';
COMMENT ON  {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT.bopus_attr_store_amt IS 'add demand_units_excl_bopus to EITHER bopus_attr_store_units OR bopus_attr_digital_amt to prevent double counting BOPUS demand units (NEVER ADD ALL 3 COLUMNS)';
COMMENT ON  {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT.bopus_attr_digital_amt IS 'add demand_units_excl_bopus to EITHER bopus_attr_store_units OR bopus_attr_digital_amt to prevent double counting BOPUS demand units (NEVER ADD ALL 3 COLUMNS)';

SET QUERY_BAND = NONE FOR SESSION;