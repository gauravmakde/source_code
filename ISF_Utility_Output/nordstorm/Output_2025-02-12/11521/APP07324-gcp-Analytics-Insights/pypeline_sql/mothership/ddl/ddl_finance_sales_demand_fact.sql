SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=ddl_mothership_finance_sales_demand_fact_11521_ACE_ENG;
     Task_Name=ddl_finance_sales_demand_fact;'
     FOR SESSION VOLATILE;

/*
T2DL_DAS_MOTHERSHIP.FINANCE_SALES_DEMAND_FACT
Description - This ddl creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) table of hourly sales and demand split by store number, merch division, platform (device, eg ios, web, mow) and customer journey (eg bopus, store fulfill, ship to store, etc.)
Contacts: Brian Robinson, Finance; Matthew Bond, Analytics
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'FINANCE_SALES_DEMAND_FACT', OUT_RETURN_MSG);

 CREATE MULTISET TABLE {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
				tran_date                	    DATE NOT NULL,
			--hour_var				VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		business_unit_desc_bopus_in_store_demand       	VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		business_unit_desc_bopus_in_digital_demand      VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		data_source       			      VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		currency_code            	    VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		store_num_bopus_in_store_demand                	SMALLINT,
     		store_num_bopus_in_digital_demand               SMALLINT,
    		merch_division				        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		merch_subdivision			        VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		merch_price_type			        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		merch_role					          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		cts_segment					          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		rfm_segment					          VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
    		rewards_level				          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		platform					            VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
     		customer_journey			        VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC, 
     		demand_amt_bopus_in_store					  DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
     		demand_amt_bopus_in_digital					DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
			  demand_canceled_amt		 	      DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
			  demand_units_bopus_in_store					DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
			  demand_units_bopus_in_digital				DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
			  demand_canceled_units		      DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
     	 	gross_merch_sales_amt         DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
		    merch_returns_amt             DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
		    net_merch_sales_amt     	    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
		    gross_operational_sales_amt   DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
		    operational_returns_amt       DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
		    net_operational_sales_amt     DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
		    gross_merch_sales_units       DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    merch_returns_units           DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    net_merch_sales_units   	    DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    gross_operational_sales_units       DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    operational_returns_units           DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    net_operational_sales_units   DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
		    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
     PRIMARY INDEX (tran_date, customer_journey, merch_division, merch_role, platform, rfm_segment, rewards_level
     ,business_unit_desc_bopus_in_store_demand,business_unit_desc_bopus_in_digital_demand
     ,store_num_bopus_in_store_demand,store_num_bopus_in_digital_demand) 
     PARTITION BY RANGE_N(tran_date BETWEEN DATE '2019-02-03' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN);
      
-- table comment
COMMENT ON  {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT IS '7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) table of daily sales AND demand split by store number, merch division, platform (device, eg ios, web, mow) AND customer journey (eg bopus, store fulfill, ship to store, etc.) ';

--Column comments
COMMENT ON  {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT.data_source IS 'whether the data comes FROM NAP/ERTM or static r.com historical data FROM NRHL_redshift';
COMMENT ON  {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT.business_unit_desc_bopus_in_store_demand IS 'GROUP BY business_unit_desc_bopus_in_store_demand AND store_num_bopus_in_store_demand to get demand WITH bopus IN store channels';
COMMENT ON  {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT.business_unit_desc_bopus_in_digital_demand IS 'GROUP BY business_unit_desc_bopus_in_digital_demand AND store_num_bopus_in_digital_demand to get demand WITH bopus IN digital channels';
--  Occasionally this is -1 WHERE an order IN historical backfill was misclassified. These are valid orders, but we dont know if they are DTC or not. For questions ask Nikky Stephen'
COMMENT ON  {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT.store_num_bopus_in_store_demand IS 'GROUP BY business_unit_desc_bopus_in_store_demand AND store_num_bopus_in_store_demand to get demand WITH bopus IN store channels';
COMMENT ON  {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT.store_num_bopus_in_digital_demand IS 'GROUP BY business_unit_desc_bopus_in_digital_demand AND store_num_bopus_in_digital_demand to get demand WITH bopus IN digital channels.';


SET QUERY_BAND = NONE FOR SESSION;