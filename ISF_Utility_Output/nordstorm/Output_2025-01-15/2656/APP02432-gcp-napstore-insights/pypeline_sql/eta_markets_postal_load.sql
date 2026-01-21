SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=eta_markets_load_2656_napstore_insights;
Task_Name=eta_markets_data_load_load_2_postal_s3_ldg;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view eta_markets_postal_input AS
select *
from kafka_eta_markets_postal_input;


-- Writing Kafka to Semantic Layer:
insert
overwrite table eta_markets_postal_ldg_table
select localMarket      as localMarket,
       cast(coarsePostalCode as string) as coarsePostalCode,
       updateTime       as updateTime
from eta_markets_postal_input;
