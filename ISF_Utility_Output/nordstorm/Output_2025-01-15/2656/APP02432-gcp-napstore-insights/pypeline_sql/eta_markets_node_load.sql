SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=eta_markets_load_2656_napstore_insights;
Task_Name=eta_markets_data_load_load_0_node_s3_ldg;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view eta_markets_node_input AS
select *
from kafka_eta_markets_node_input;

-- Writing Kafka to Semantic Layer:
insert
overwrite table eta_markets_node_ldg_table
select localMarket as localMarket,
       node        as node,
       updateTime  as updateTime
from eta_markets_node_input;
