SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_re_store_attributes_11521_ACE_ENG;
     Task_Name=ddl_re_store_attributes;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_attributes
Owner: Rujira Achawanantakun
Modified: 2023-05-01
Note:
- This ddl is used to create re_store_attributes table structure into tier 2 datalab.

*/
CREATE MULTISET TABLE {real_estate_t2_schema}.re_store_attributes
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
	,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(store_number)
;

-- Table Comment
COMMENT ON  {real_estate_t2_schema}.re_store_attributes IS 'Store information';
SET QUERY_BAND = NONE FOR SESSION;