/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=customer_age_dim_11521_ACE_ENG;
     Task_Name=customer_age_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: customer_age_dim
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: April 29th 2024 

Note:
-- Purpose of the table: Table to directly get transaction related metrics and attributes
-- Update Cadence: Daily

*/

/*set date range as full or incremental
based on day of week (which is variable to allow
manual full loads any day of the week)*/
 
    
/*--------------------------------------------------------------------------------------------------
 * 
 * STEP 1: GENERATE A RANGE OF YEARS THAT WE WANT TO CUSTOMER AGE DATA FOR
 * 
 */-------------------------------------------------------------------------------------------------

CREATE VOLATILE MULTISET TABLE year_connect AS (
--
	SELECT DISTINCT
		EXTRACT(YEAR FROM day_date) AS year_num
		, MIN(day_date) AS year_start_day
		, MAX(day_date) AS year_end_day
	FROM prd_nap_usr_vws.day_cal AS DC
	WHERE 1 = 1
		AND EXTRACT(YEAR FROM day_date) >= (EXTRACT(YEAR FROM CURRENT_DATE) - 7)
		AND EXTRACT(YEAR FROM day_date) <= (EXTRACT(YEAR FROM CURRENT_DATE) + 2)
	GROUP BY 1
--
) WITH DATA PRIMARY INDEX(year_num) ON COMMIT PRESERVE ROWS;

/*--------------------------------------------------------------------------------------------------
 * 
 * STEP 2: GATHER INFORMATION ON THE MOST RECENTLY COLLECTED AGES FOR EACH CUSTOMER AND WHEN THAT AGE WAS COLLECTED AND WHERE
 * 
 */-------------------------------------------------------------------------------------------------

CREATE VOLATILE MULTISET TABLE customer_age_info AS ( 
--
	SELECT
		COALESCE(EXA.acp_id, PB.acp_id) AS acp_id
		, COALESCE(EXA.age_value, PB.model_age) AS age
		, CASE WHEN EXA.age_value IS NOT NULL THEN CAST(EXA.object_system_time AS DATE) ELSE CAST(PB.update_timestamp AS DATE) END AS age_date
		, CASE 
			WHEN EXA.age_value IS NOT NULL THEN 'Experian'
	        WHEN PB.model_age IS NOT NULL THEN 'Model'
	        ELSE 'Neither' 
	    END AS age_source
	    , CASE 
	    	WHEN EXA.age_value IS NOT NULL THEN EXA.age_type
	    	WHEN PB.model_age IS NOT NULL THEN 'Modeled Age'
	    	ELSE NULL
	    END AS age_type
	    -- , CASE 
		--     WHEN LENGTH(TRIM(EXA.birth_year_and_month)) = 6 AND ((EXTRACT(YEAR FROM age_date) - LEFT(EXA.birth_year_and_month,4)) = age OR (EXTRACT(YEAR FROM age_date) - LEFT(EXA.birth_year_and_month,4)) = (age + 1))
		--     	THEN (SELECT MIN(day_date) FROM prd_nap_usr_vws.day_cal WHERE EXTRACT(MONTH FROM day_date) = RIGHT(TRIM(EXA.birth_year_and_month),2) AND EXTRACT(YEAR FROM day_date)= EXTRACT(YEAR FROM age_date))
		--     ELSE age_date
	    -- END AS birth_date
	FROM (
	--
		SELECT
			DPD.acp_id
			, DPD.object_system_time
			, DPD.age_value
			, DPD.age_type
			-- , DPD.birth_year_and_month
		FROM prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS DPD
	--
	) AS EXA
	FULL OUTER JOIN (
	--
		SELECT 
			acp_id
			, model_age
			, update_timestamp
		FROM t2dl_das_age_model.age_scoring_p4_probability_1028
	--
	) AS PB
		ON EXA.acp_id = PB.acp_id
	WHERE 1 = 1
		AND COALESCE(EXA.age_value, PB.model_age) IS NOT NULL
--
) WITH DATA PRIMARY INDEX(acp_id, age_date, age_type) ON COMMIT PRESERVE ROWS; 

/*--------------------------------------------------------------------------------------------------
 * 
 * STEP 3: COMBINE THE YEARS AND COLLECTED AGE DATA TO CALCULATE A RANGE OF TIME FOR EACH CUSTOMER AND WHAT THEIR AGE WAS DURING THAT PERIOD
 * 
 */-------------------------------------------------------------------------------------------------

CREATE VOLATILE MULTISET TABLE cco_customer_ages_table AS (
--
	SELECT
		CAI.acp_id
		, YC.year_num
--		, YC.year_start_day
--		, YC.year_end_day
		, ADD_MONTHS(CAI.age_date, 12*(YC.year_num - EXTRACT(YEAR FROM CAI.age_date))) AS age_start_day
		, ADD_MONTHS(CAI.age_date, 12*(YC.year_num - EXTRACT(YEAR FROM CAI.age_date)) + 12) AS age_end_day
--		, CAI.age - CAST((CAI.age_date - YC.year_end_day) AS DECIMAL(15,3))/365.25 AS age_value
		, CAI.age - (EXTRACT(YEAR FROM CAI.age_date) - YC.year_num) AS age_value
		, CAI.age_source
		, CAI.age_type
		, CAI.age_date AS age_updated_date
	FROM customer_age_info AS CAI
	JOIN year_connect AS YC
		ON 1 = 1
	WHERE 1 = 1
--
) WITH DATA PRIMARY INDEX(acp_id, age_start_day, age_end_day, age_value, age_source, age_type) ON COMMIT PRESERVE ROWS;

/*--------------------------------------------------------------------------------------------------
 * 
 * STEP 4: CREATE THE FINAL CCO TABLE
 * 
 */-------------------------------------------------------------------------------------------------

DELETE FROM {cco_t2_schema}.customer_age_dim ALL;
 

INSERT INTO {cco_t2_schema}.customer_age_dim
	SELECT
		CAI.acp_id
		, CAI.age_start_day
		, CAI.age_end_day
		, CAI.age_value
		, CAI.age_source
		, CAI.age_type
		, CAI.age_updated_date
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM cco_customer_ages_table AS CAI
;

COLLECT STATISTICS
	COLUMN(acp_id)
	, COLUMN(age_source)
	, COLUMN(age_type)
	, COLUMN(age_start_day)
	, COLUMN(age_end_day)
ON {cco_t2_schema}.customer_age_dim;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;