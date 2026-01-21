SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_store_realestate_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_store_realestate_teradata_job;'
FOR SESSION VOLATILE;

ET;

CREATE VOLATILE multiset TABLE store_realestate (  
        store_num                              INTEGER NOT NULL,	
        store_name	                           VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        business_unit_desc 		               VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        channel_id	                           VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC ,
        banner	                               VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC ,
        channel_journey	                       VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC ,
        store_segment                          VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_open_date                        DATE FORMAT 'YYYY-MM-DD',
        store_close_date                       DATE FORMAT 'YYYY-MM-DD' COMPRESS,
        store_location_areatype                VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_location_centertype              VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_location_malltype                VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_location_tradeareatype           VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_location_qualityscore		       INTEGER,
        store_city		                       VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_msa		                       VARCHAR(60) CHARACTER SET LATIN NOT CASESPECIFIC,
        store_dma	                           VARCHAR(80) CHARACTER SET LATIN NOT CASESPECIFIC,
        store_postal_code	 	               VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_state	                  	       CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_region	                  	   VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        country	 	                           VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('CANADA','US'),
        store_dma_code	 	                   VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_time_zone	 	                   VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_commute_drivetime	 	           FLOAT,
        store_commute_drivedistance	           FLOAT,
        store_proximity_fls	 	               FLOAT,
        store_proximity_rack	 	           FLOAT,
        store_population_people	               INTEGER,	
        store_population_households	           INTEGER,	
        store_population_density	 	       FLOAT,
        store_population_students	           INTEGER,
        store_population_avghhsize		       DECIMAL(32,3),
        store_population_growth		           DECIMAL(32,3),
        store_income_median	                   INTEGER,
        store_income_75k	 	               DECIMAL(32,3),
        store_income_100k		               DECIMAL(32,3),
        store_income_growth	                   DECIMAL(32,3),	
        store_apparelspend_index	           FLOAT,
        store_population_density_segment	   VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_commute_drivetime_segment	 	   VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_commute_drivedistance_segment	   VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,	
        store_proximity_fls_segment	           VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,	
        store_proximity_rack_segment           VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
) ON COMMIT PRESERVE ROWS; 

ET;

insert into store_realestate
select distinct
-- standardized fields
 ds.store_num 
, ds.store_name
,case
			when business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
			when business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
			when business_unit_desc = 'RACK' then 'RACK STORE'
		    else business_unit_desc
		end as business_unit_desc 
, case
  WHEN REGEXP_INSTR(BUSINESS_UNIT_DESC, 'FULL LINE')>0 THEN 'FLS'
  WHEN REGEXP_INSTR(BUSINESS_UNIT_DESC, 'RACK')>0 THEN 'RS'
  WHEN REGEXP_INSTR(BUSINESS_UNIT_DESC, 'N[.]COM')>0 THEN 'NCOM'  
  WHEN REGEXP_INSTR(BUSINESS_UNIT_DESC, 'N[.]CA')>0 THEN 'NCA'  
  WHEN REGEXP_INSTR(BUSINESS_UNIT_DESC, 'OFFPRICE ONLINE')>0 THEN 'RCOM'        
  WHEN REGEXP_INSTR(BUSINESS_UNIT_DESC, 'TRUNK CLUB')>0 THEN 'TC'        
ELSE 'OTHER'  
END CHANNEL_ID
, CASE 
WHEN CHANNEL_ID IN ('FLS','NCOM') THEN 'NORDSTROM' 
WHEN CHANNEL_ID IN ('RS','RCOM') THEN 'NORDSTROM RACK'
ELSE 'OTHER'
end banner
, case 
when channel_id IN ('rcom','ncom','tc') then 'DIGITAL' 
when channel_id IN ('rs','fls') then 'STORE'
else 'OTHER'
end channel_journey

-- base store info
,coalesce(
case 
when channel_id not IN ('rs','fls') then 'UNDEFINED'
when store_segment = '' then 'UNDEFINED' 
else store_segment end,'UNDEFINED') store_segment
--, store_type_code,store_type_desc store_type
--, location_type_desc store_locationtype
--, selling_store_ind
 
,store_open_date
,store_close_date
 
-- location
, case when channel_id not IN ('rs','fls') then 'UNDEFINED' else coalesce(area_type,'UNDEFINED' ) end store_location_areatype
, case when channel_id not IN ('rs','fls') then 'UNDEFINED' else coalesce(center_type,'UNDEFINED') end  store_location_centertype
, case when channel_id not IN ('rs','fls') then 'UNDEFINED' else coalesce(mall_type,'UNDEFINED') end  store_location_malltype
, case when channel_id not IN ('rs','fls') then 'UNDEFINED' else coalesce(trade_area_type,'UNDEFINED') end  store_location_tradeareatype
, location_quality_score  as store_location_qualityscore
 
-- GEO
, STORE_ADDRESS_CITY STORE_CITY
, MSA STORE_MSA
, DMA STORE_DMA
, store_postal_code
, STORE_ADDRESS_STATE STORE_STATE
,       TRIM(
OREPLACE(
  OREPLACE(
    OREPLACE(
      LOWER(
    CASE
        WHEN REGEXP_INSTR(REGION_DESC,'CANADA')>0 THEN 'CANADA'
        WHEN REGION_DESC IN('N.CA','N.COM','HAUTELOOK','R.COM') THEN LOWER('DIGITAL')
        WHEN REGEXP_INSTR(REGION_DESC,'EAST COAST')>0 THEN LOWER('SOUTHEAST')
        WHEN REGEXP_INSTR(REGION_DESC,'ONLINE|HAUTELOOK|.COM')>0 THEN LOWER('DIGITAL')
        WHEN REGEXP_INSTR(REGION_DESC,'SCAL')>0 THEN 'SOUTHERN CALIFORNIA'        
        WHEN REGEXP_INSTR(REGION_DESC,'DISTRIBUTION|RESERVE')>0 THEN
            (CASE
                WHEN REGEXP_INSTR(REGION_DESC,'MANHATTAN')>0 THEN 'NORTHEAST'
                WHEN REGEXP_INSTR(REGION_DESC,'PUERTO RICO')>0 THEN 'SOUTHEAST'
                WHEN REGEXP_INSTR(REGION_DESC,'TEXAS|SOUTHWEST|HAWAII|SOUTH')>0 THEN 'SOUTHWEST'
                WHEN REGEXP_INSTR(REGION_DESC,'MW ')>0 THEN 'MIDWEST'
                WHEN REGEXP_INSTR(REGION_DESC,'ALASKA|WASHINGTON|OREGON|WA ')>0 THEN 'NORTHWEST'
                ELSE 'OTHER'
            END)
        WHEN REGEXP_INSTR(REGION_DESC,'NORTH|SOUTH|WEST|EAST')>0 THEN LOWER(REGION_DESC)
        ELSE 'OTHER'
    END
    )
    ,' RACKS','')
    ,' RACK','') 
    ,' FULL LINE','') ) STORE_REGION
, CASE 
	WHEN REGEXP_INSTR(LOWER(BUSINESS_UNIT_DESC), '.CA|CANADA')>0 THEN 'CANADA' 
	ELSE 'US' 
END COUNTRY
 
, STORE_DMA_CODE 
, STORE_TIME_ZONE 
--, STORE_LOCATION_LATITUDE STORE_LATITUDE
--, STORE_LOCATION_LONGITUDE STORE_LONGITUDE
 
-- LAYOUT
--, GROSS_SQUARE_FOOTAGE STORE_LAYOUT_SQFT
--, FLOORS STORE_LAYOUT_FLOORS
--, MAX_OCCUPANCY_LOAD STORE_LAYOUT_MAXOCCUPANCY
--, EXTERIOR_ENTRIES STORE_LAYOUT_ENTRIES
 
-- PROXIMITY/COMMUTE
, TA_AVG_DRIVETIME STORE_COMMUTE_DRIVETIME
, TA_AVG_DRIVEDISTANCE STORE_COMMUTE_DRIVEDISTANCE
, NEAREST_FLS_DRIVEDISTANCE STORE_PROXIMITY_FLS
, NEAREST_RACK_DRIVEDISTANCE STORE_PROXIMITY_RACK
 
-- POPULATION ATTRIBUTES
, POPULATION AS STORE_POPULATION_PEOPLE
, COALESCE(HOUSEHOLDS,0) STORE_POPULATION_HOUSEHOLDS
, POPULATION_DENSITY STORE_POPULATION_DENSITY
, STUDENT_POPULATION STORE_POPULATION_STUDENTS
, CASE WHEN cast(COALESCE(HOUSEHOLDS,0) as decimal(32,3)) = 0 THEN 0 ELSE  cast(COALESCE(POPULATION,0)as decimal(32,3))/ cast(COALESCE(HOUSEHOLDS,0)as decimal(32,3)) END STORE_POPULATION_AVGHHSIZE
, CASE WHEN cast(COALESCE(POPULATION,0)as decimal(32,3)) = 0 THEN 0 ELSE cast(COALESCE(POPULATION_5YR_PROJECTION,0)as decimal(32,3))/cast(COALESCE(POPULATION,0)as decimal(32,3)) END STORE_POPULATION_GROWTH
 
 
-- INCOME ATTRIBUTES
, MEDIAN_INCOME STORE_INCOME_MEDIAN
, CASE WHEN cast(COALESCE(HOUSEHOLDS,0)as decimal(32,3)) = 0 THEN 0 ELSE cast(COALESCE(HHLDS_75K_PLUS,0)as decimal(32,3))/cast(COALESCE(HOUSEHOLDS,0)as decimal(32,3)) END STORE_INCOME_75K
, CASE WHEN cast(COALESCE(HOUSEHOLDS,0)as decimal(32,3)) = 0 THEN 0 ELSE cast(COALESCE(HHLDS_100K_PLUS,0)as decimal(32,3))/cast(COALESCE(HOUSEHOLDS,0)as decimal(32,3)) END STORE_INCOME_100K
, CASE WHEN cast(COALESCE(HOUSEHOLDS,0)as decimal(32,3)) = 0 THEN 0 ELSE cast(COALESCE(HHLDS_100K_PLUS_5YR_PROJECTION,0)as decimal(32,3))/cast(COALESCE(HOUSEHOLDS,0)as decimal(32,3)) END STORE_INCOME_GROWTH
 
-- APPAREL
,APPAREL_SPEND_INDEX AS STORE_APPARELSPEND_INDEX
 
-- STORE SEGMENT
,CASE 
	WHEN channel_id not IN ('rs','fls') then 'UNDEFINED'
	WHEN STORE_POPULATION_DENSITY <1000 THEN '0 - 1K / MILE ~ LOW DENSITY'
	WHEN STORE_POPULATION_DENSITY <2000 THEN '1 - 2K / MILE ~ MEDIUM DENSITY'
	WHEN STORE_POPULATION_DENSITY <5000 THEN '2 - 5K / MILE ~ HIGH DENSITY'	
	ELSE  'VERY HIGH DENSITY (5K+ / MILE)'
END STORE_POPULATION_DENSITY_SEGMENT
,CASE 
	WHEN channel_id not IN ('rs','fls') then 'UNDEFINED'
	WHEN STORE_COMMUTE_DRIVETIME <15 THEN ' 0 - 15 MINS TO NEAREST STORE'
	WHEN STORE_COMMUTE_DRIVETIME <30 THEN '15 - 30 MINS TO NEAREST STORE'	
	ELSE '30 MINS + TO NEAREST STORE'
END STORE_COMMUTE_DRIVETIME_SEGMENT
,CASE 
    WHEN channel_id not IN ('rs','fls') then 'UNDEFINED'
	WHEN STORE_COMMUTE_DRIVEDISTANCE < 2 THEN ' 0 -  2 MILES TO NEAREST STORE'
	WHEN STORE_COMMUTE_DRIVEDISTANCE <10 THEN ' 2 - 10 MILES TO NEAREST STORE'
	ELSE  '10 MILES + TO NEAREST STORE'
END STORE_COMMUTE_DRIVEDISTANCE_SEGMENT
,CASE 
	WHEN channel_id not IN ('rs','fls') then 'UNDEFINED'
	WHEN STORE_PROXIMITY_FLS < 2 THEN ' 0 -  2 MILES TO NEAREST FLS'
	WHEN STORE_PROXIMITY_FLS <10 THEN ' 2 - 10 MILES TO NEAREST FLS'
	ELSE  '10 MILES + TO NEAREST FLS'
END STORE_PROXIMITY_FLS_SEGMENT
,CASE 
	WHEN STORE_PROXIMITY_RACK < 2 THEN ' 0 -  2 MILES TO NEAREST RACK'
	WHEN channel_id not IN ('rs','fls') then 'UNDEFINED'
	WHEN STORE_PROXIMITY_RACK <10 THEN ' 2 - 10 MILES TO NEAREST RACK'
	ELSE  '10 MILES + TO NEAREST RACK'
END STORE_PROXIMITY_RACK_SEGMENT
 
FROM {db_env}_NAP_USR_VWS.STORE_DIM ds
LEFT JOIN T2DL_DAS_REAL_ESTATE.RE_STORE_ATTRIBUTES SAT ON DS.STORE_NUM =SAT.STORE_NUMBER 
LEFT JOIN T2DL_DAS_REAL_ESTATE.RE_STORE_TRADE_AREA_ATTRIBUTES AS TS ON DS.STORE_NUM =TS.STORE_NUMBER
where business_unit_desc not like '%CANADA%';

ET;
--DELETING AND INSERTING DATA IN THE LANDING TABLE
DELETE FROM {proto_schema}.MMM_STORE_REALESTATE_LDG ALL;

ET;

INSERT INTO {proto_schema}.MMM_STORE_REALESTATE_LDG
select
store_num,                           
store_name,	                        
business_unit_desc, 		              
channel_id,	                        
banner,	                            
channel_journey,	                    
store_segment,                       
store_open_date,                     
store_close_date,                    
store_location_areatype,             
store_location_centertype,           
store_location_malltype,             
store_location_tradeareatype,        
store_location_qualityscore,		      
store_city,		                      
store_msa,		                        
store_dma,	                          
store_postal_code,	 	                
store_state,	                  	    
store_region,	                  	  
country,	 	                          
store_dma_code,	 	                  
store_time_zone,	 	                  
store_commute_drivetime,	 	          
store_commute_drivedistance,	        
store_proximity_fls,	 	              
store_proximity_rack,	 	            
store_population_people,	            
store_population_households,	        
store_population_density,	 	        
store_population_students,	          
store_population_avghhsize,		      
store_population_growth,		          
store_income_median,                
store_income_75k,	 	                
store_income_100k,		                
store_income_growth,	                
store_apparelspend_index,	          
store_population_density_segment,	  
store_commute_drivetime_segment,	 	  
store_commute_drivedistance_segment,	
store_proximity_fls_segment,	        
store_proximity_rack_segment,        
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from store_realestate;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;