/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=customer_age_dim_vw;
     Task_Name=customer_age_dim_vw;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_strategy.customer_age_dim_vw
Team/Owner: Customer Analytics
Date Created/Modified: June 19, 2024

Purpose of the view: Combine the experian and age model data to ensure that we have a clear and easy way of pulling age data. 

NOTE: This view provides customer age as of age_date, which is different for each acp_id. 
     To get accurate and comparable ages, you MUST adjust age_value using the end date of your reporting time period.
     (ex.) cast(age_value - (age_date - (select end_dt from date_lookup))/365.25 as integer) as age

*/


REPLACE VIEW {cco_t2_schema}.customer_age_dim_vw
AS
LOCK ROW FOR ACCESS

	select ac.acp_id
	       , coalesce(ex.age_value,m.model_age) age_value
	       , case when ex.acp_id is not null then 'Experian'
	              when m.acp_id is not null then 'Modeled'
	              else 'Unknown' end age_source
	       , birthday
	       , coalesce(ex.object_system_time,m.update_timestamp) as update_timestamp
	       , coalesce(ex.age_year_ex, m.update_timestamp) as age_date
	from PRD_NAP_USR_VWS.ANALYTICAL_CUSTOMER ac
	left join ( select acp_id
	                   , age_value as exp_value
	                   , object_system_time
	                   , case when length(trim(birth_year_and_month))=6 
	                               then cast(to_date(substring(birth_year_and_month,1,4)||'/'||substring(birth_year_and_month,5,2)||'/'||'15','YYYY/MM/DD') as date)
	                           end as birthday
					   , cast(to_date(cast(year(object_system_time) as varchar(4)) ||'-' || LPAD(cast(month(birthday) as varchar(2)),2,'0') || '-' || '15', 'YYYY-MM-DD') as date) as ty_birthdate
					   , (ty_birthdate - birthday)/365.25 as birthday_value
					   , coalesce(birthday_value, exp_value) as age_value
					   , coalesce(ty_birthdate, object_system_time) as age_year_ex
	            from prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim
	            where age_type = 'Exact Age' 
	            and age_value is not null) ex on ac.acp_id = ex.acp_id
	left join (select acp_id 
	                  , model_age
	                  , update_timestamp
	           from t2dl_das_age_model.new_age_model_scoring_all )m on ac.acp_id=m.acp_id
;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;

