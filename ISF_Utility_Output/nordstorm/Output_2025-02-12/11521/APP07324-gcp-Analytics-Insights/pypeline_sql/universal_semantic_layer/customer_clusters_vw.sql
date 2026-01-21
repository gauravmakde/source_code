SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=customer_clusters_vw_11521_ACE_ENG;
     Task_Name=customer_clusters_vw;'
     FOR SESSION VOLATILE;

/*


T2/View Name: t2dl_das_usl.customer_clusters_vw
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: May 11th 2023

Note:
-- View to directly get which bucket a customer is under regarding trips / atv by channel

*/

REPLACE VIEW {usl_t2_schema}.customer_clusters_vw
AS
LOCK ROW FOR ACCESS
select 
	cus.acp_id 
	,trips_1year_nstores
	,trips_1year_ncom
	,trips_1year_rstores
	,trips_1year_rcom

	,gross_spend_1year_nstores
	,gross_spend_1year_ncom
	,gross_spend_1year_rstores
	,gross_spend_1year_rcom

	,COALESCE(gross_spend_1year_nstores/trips_1year_nstores,0) atv_1year_nstores
	,COALESCE(gross_spend_1year_ncom/trips_1year_ncom,0) atv_1year_ncom
	,COALESCE(gross_spend_1year_rstores/trips_1year_rstores,0) atv_1year_rstores
	,COALESCE(gross_spend_1year_rcom/trips_1year_rcom,0) atv_1year_rcom

-- CLUSTERS:trips ---------
	,case
		when trips_1year_nstores  <= 0 then '00'
		when trips_1year_nstores  = 1 then '01'
		when trips_1year_nstores  = 2 then '02'
		when trips_1year_nstores >= 3 AND trips_1year_nstores <= 5 then '03 to 05'
		when trips_1year_nstores >= 6 AND trips_1year_nstores <=10 then '06 to 10'
		when trips_1year_nstores >= 11 then '11+'
	end customer_cluster_trips_nstores
	,case
		when trips_1year_ncom  <= 0 then '00'
		when trips_1year_ncom  = 1 then '01'
		when trips_1year_ncom  = 2 then '02'
		when trips_1year_ncom >= 3 AND trips_1year_ncom <= 5 then '03 to 05'
		when trips_1year_ncom >= 6 AND trips_1year_ncom <=10 then '06 to 10'
		when trips_1year_ncom >= 11 then '11+'
	end customer_cluster_trips_ncom
	,case
		when trips_1year_rstores  <= 0 then '00'
		when trips_1year_rstores  = 1 then '01'
		when trips_1year_rstores  = 2 then '02'
		when trips_1year_rstores >= 3 AND trips_1year_rstores <= 5 then '03 to 05'
		when trips_1year_rstores >= 6 AND trips_1year_rstores <=10 then '06 to 10'
		when trips_1year_rstores >= 11 then '11+'
	end customer_cluster_trips_rstores
	,case
		when trips_1year_rcom  <= 0 then '00'
		when trips_1year_rcom  = 1 then '01'
		when trips_1year_rcom  = 2 then '02'
		when trips_1year_rcom >= 3 AND trips_1year_rcom <= 5 then '03 to 05'
		when trips_1year_rcom >= 6 AND trips_1year_rcom <=10 then '06 to 10'
		when trips_1year_rcom >= 11 then '11+'
	end customer_cluster_trips_rcom
	,case
		when trips_1year_nstores <= 0							  then '$   0'
		when atv_1year_nstores <= 0 	                          then '$   0'
		when atv_1year_nstores > 0    AND atv_1year_nstores  < 50 then '$   0 - $ 49'
		when atv_1year_nstores >= 50  AND atv_1year_nstores < 100 then '$  50 - $ 99'
		when atv_1year_nstores >= 100 AND atv_1year_nstores < 150 then '$100 - $149'
		when atv_1year_nstores >= 150 AND atv_1year_nstores < 200 then '$150 - $199'
		when atv_1year_nstores >= 200 AND atv_1year_nstores < 300 then '$200 - $299'
		when atv_1year_nstores >= 300  then '$300+'
	end customer_cluster_atv_nstores
	,case
		when trips_1year_ncom <= 0							  then '$   0'
		when atv_1year_ncom <= 0 	                          then '$   0'
		when atv_1year_ncom > 0    AND atv_1year_ncom  < 50 then '$   0 - $ 49'
		when atv_1year_ncom >= 50  AND atv_1year_ncom < 100 then '$  50 - $ 99'
		when atv_1year_ncom >= 100 AND atv_1year_ncom < 150 then '$100 - $149'
		when atv_1year_ncom >= 150 AND atv_1year_ncom < 200 then '$150 - $199'
		when atv_1year_ncom >= 200 AND atv_1year_ncom < 300 then '$200 - $299'
		when atv_1year_ncom >= 300  then '$300+'
	end customer_cluster_atv_ncom
	,case
		when trips_1year_rstores <= 0							  then '$   0'
		when atv_1year_rstores <= 0 	                          then '$   0'
		when atv_1year_rstores > 0    AND atv_1year_rstores  < 50 then '$   0 - $ 49'
		when atv_1year_rstores >= 50  AND atv_1year_rstores < 100 then '$  50 - $ 99'
		when atv_1year_rstores >= 100 AND atv_1year_rstores < 150 then '$100 - $149'
		when atv_1year_rstores >= 150 AND atv_1year_rstores < 200 then '$150 - $199'
		when atv_1year_rstores >= 200 AND atv_1year_rstores < 300 then '$200 - $299'
		when atv_1year_rstores >= 300  then '$300+'
	end customer_cluster_atv_rstores
	,case
		when trips_1year_rcom <= 0							  then '$   0'
		when atv_1year_rcom <= 0 	                          then '$   0'
		when atv_1year_rcom > 0    AND atv_1year_rcom  < 50 then '$   0 - $ 49'
		when atv_1year_rcom >= 50  AND atv_1year_rcom < 100 then '$  50 - $ 99'
		when atv_1year_rcom >= 100 AND atv_1year_rcom < 150 then '$100 - $149'
		when atv_1year_rcom >= 150 AND atv_1year_rcom < 200 then '$150 - $199'
		when atv_1year_rcom >= 200 AND atv_1year_rcom < 300 then '$200 - $299'
		when atv_1year_rcom >= 300  then '$300+'
	end customer_cluster_atv_rcom
--customer_cluster_atv
from T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS cus
join T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_JOURNEY aq on aq.acp_id = cus.acp_id
join {usl_t2_schema}.acp_driver_dim ac on ac.acp_id = cus.acp_id
;

SET QUERY_BAND = NONE FOR SESSION;