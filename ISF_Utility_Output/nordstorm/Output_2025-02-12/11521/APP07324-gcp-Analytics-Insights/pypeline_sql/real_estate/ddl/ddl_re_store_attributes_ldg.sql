SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=re_store_attributes_11521_ACE_ENG;
     Task_Name=ddl_re_store_attributes_ldg;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_attributes_ldg
Owner: Rujira Achawanantakun
Modified: 2023-05-01
Note:
- This landing table is created as part of the re_store_attributes job that loads
data from S3 to teradata. The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{real_estate_t2_schema}', 're_store_attributes_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {real_estate_t2_schema}.re_store_attributes_ldg
	 ,FALLBACK
     ,NO BEFORE JOURNAL
     ,NO AFTER JOURNAL
     ,CHECKSUM = DEFAULT
     ,DEFAULT MERGEBLOCKRATIO
    (
	 store_number               int not null
	,store_status			    varchar(8)
	,store_type                 varchar(10)
	,store_name                 varchar(80)
	,address                    varchar(100)
	,city                       varchar(50)
	,state                      char(2) COMPRESS ('AL', 'AR', 'FL', 'GA', 'KY', 'LA', 'MD', 'MO', 'MS', 'NC', 'OK',
       'SC', 'TN', 'TX', 'VA', 'WA', 'WV', 'NY', 'IL', 'CA', 'CT', 'HI',
       'MA', 'MN', 'NJ', 'PA', 'NV', 'NH', 'AB', 'BC', 'AK', 'AZ', 'CO',
       'DE', 'IA', 'ID', 'IN', 'KS', 'ME', 'MI', 'ND', 'NE', 'NM', 'OH',
       'OR', 'PR', 'RI', 'SD', 'UT', 'WI', 'MB', 'MT', 'NB', 'ON', 'QC',
       'VT', 'WY', 'DC', 'GU', 'VI', 'AC', 'NL', 'NS', 'QL', 'SA', 'SK',
       'TA', 'Nj', 'QB', 'PE', 'PQ')
	,zip_code                   varchar(7)
	,country                    char(2) COMPRESS ('US','CA')
	,region                     varchar(15) COMPRESS ('Midwest', 'Southwest', 'Northwest', 'SCAL',
	    'Northeast', 'Southeast', 'Canada')
	,district_number		    int
	,district        		    varchar(30)
	,msa                        varchar(60)
	,msa_code                   varchar(5)
	,dma                        varchar(80)
	,dma_code                   varchar(3)
	,longitude                  float
	,latitude                   float
	,open_date                  date
	,relocation_date            date
	,remodel_date               date
	,close_date					date
	,covid19_open_date          date
	,floors                     int
	,sqft_gross                 float
	,sqft_sales                 float
	,sqft_net_sales             float
	,max_occupancy_load         float
	,exterior_entries           float
	,mall_entries               float
	,location_quality_score     int
	,area_type                  varchar(20) COMPRESS ('Suburban', 'Urban', 'Rural')
	,center_type                varchar(30) COMPRESS ('Super Regional Center', 'Regional Center',
	   'Urban Center', 'Lifestyle Center', 'Power Center', 'Off Price Center', 'Outlet Center',
	   'Strip Center')
	,mall_type                  varchar(15) COMPRESS ('Mall', 'Off-Mall', 'Freestanding')
	,mall_name                  varchar(80)
	,nearest_fls_number         int
	,nearest_fls_drivedistance  float
	,nearest_rack_number        int
	,nearest_rack_drivedistance float
	,trade_area_type            varchar(40) COMPRESS ('Dense Urban/Tourist 35%',
	    'Commuter/Tourist 45%', 'Standard 60%', 'Dense Urban/Tourist 25%', 'Standard 70%')
)
PRIMARY INDEX(store_number)
;
SET QUERY_BAND = NONE FOR SESSION;