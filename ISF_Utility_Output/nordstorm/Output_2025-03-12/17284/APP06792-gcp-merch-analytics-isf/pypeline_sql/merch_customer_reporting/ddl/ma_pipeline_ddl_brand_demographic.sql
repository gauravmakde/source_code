CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'merch_customer_demographic', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.merch_customer_demographic
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT 
	,DEFAULT MERGEBLOCKRATIO 
(	
 product_attribute            varchar(255),
 DIMENSION 					  varchar(150),
 dma 						  varchar(100),
 ty_ly_ind                    varchar(10),
 banner                       varchar(10),
 channel                      varchar(10),
 year_idnt                    integer,
 half_idnt                    varchar(10),
 quarter_idnt                 varchar(10),
 month_idnt                   varchar(15),
 half_label                   varchar(10),
 quarter_label                varchar(10),
 month_label                  varchar(10),
 avg_age                      integer,
 avg_ntn_age                  integer,
 avg_income                   integer,
 customers                    integer,
 customers_under_44           integer,
 male_customers               integer,
 female_customers             integer,
 unknown_gender_customers     integer,
 young_adult_customers        integer,
 early_career_customers       integer,
 mid_career_customers         integer,
 late_career_customers        integer,
 retired_customers            integer,
 unknown_age_customers        integer,
 core_target_age_customers    integer,
 core_target_higher_customers integer,
 core_target_lower_customers  integer,
 rural_customers              integer,
 urban_customers              integer,
 suburban_customers           integer,
 gross_items                  integer,
 gross_spend                  DECIMAL(12, 2),
 ntn_count                    integer
)
PRIMARY INDEX(month_idnt,channel,dimension);

GRANT SELECT ON {environment_schema}.merch_customer_demographic TO PUBLIC;