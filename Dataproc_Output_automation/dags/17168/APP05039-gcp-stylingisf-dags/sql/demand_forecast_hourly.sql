--{autocommit_on};

-- SET QUERY_BAND = 'App_ID=app05039;LoginUser=SCH_NAP_STYL_BATCH;Job_Name=sqs_to_demand_forecast_hourly_v1;Data_Plane=Customer;Team_Email=TECH_ISF_DAS_STYLING@nordstrom.com;PagerDuty=DAS Digital Styling Services;Obj_Name=DEMAND_FORECAST_HOURLY_FACT;Conn_Type=JDBC;' FOR SESSION VOLATILE;



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.demand_forecast_hourly_fact
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.demand_forecast_hourly_ldg AS stg
 WHERE demand_forecast_hourly_fact.activity_date = cast(stg.activity_date as datetime)
  AND LOWER(demand_forecast_hourly_fact.forecast_country) = LOWER(forecast_country)
  AND LOWER(demand_forecast_hourly_fact.forecast_channel) = LOWER(forecast_channel));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.demand_forecast_hourly_fact (
    activity_date,
    demand_forecast,
    forecast_country,
    forecast_channel
)
(SELECT CAST(activity_date AS datetime)
        ,demand_forecast
        ,forecast_country
        ,forecast_channel
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.demand_forecast_hourly_ldg AS stg);


--COLLECT STATISTICS COLUMN(activity_date) ON PRD_NAP_FCT.DEMAND_FORECAST_HOURLY_FACT;

-- SET QUERY_BAND = NONE FOR SESSION;
