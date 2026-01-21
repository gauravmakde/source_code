SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_traffic_model_coeff_11521_ACE_ENG;
     Task_Name=rack_traffic_model_coeff_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view rack_traffic_model_coeff_vw_csv
	(
    coefficient_type 		STRING,
    start_date 				DATE,
    end_date 				DATE,
    store_number 			INTEGER,
    store_intercept 		DECIMAL(20,5),
    store_slope 			DECIMAL(20,5),
    store_intercept_pred 	DECIMAL(20,5),
    store_slope_pred 		DECIMAL(20,5),
    saturday_intercept 		DECIMAL(20,5),
    saturday_slope 			DECIMAL(20,5),
    region  				STRING,
    store_data_type 		STRING
	)
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/store_traffic/rack_traffic_model_coeff.csv",
	sep ",",
	header "true",
	dateFormat "M/d/yyyy");

 
-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table rack_traffic_model_coeff_output
select * from rack_traffic_model_coeff_vw_csv
;
