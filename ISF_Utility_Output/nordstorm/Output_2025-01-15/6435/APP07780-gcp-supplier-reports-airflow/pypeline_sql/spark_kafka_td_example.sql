--Reading Data from  Source Kafka Topic Name=cpp_object_model_avro
create temporary view  temp_cpp_object_model AS select * from kafka_cpp_object_model_avro ;

--Extracting data from Kafka Table
create temporary view kafka_temp_view as
select customer.id as id_col,
customer.idType as type_col,
credentials.tokenizingCreditCardHVT.authority as authority,
'US' as country,
element_at(headers,'CreationTime') as CreationTime
from temp_cpp_object_model limit 1000;


-- Sink Writing Kafka Data to Teradata
insert overwrite table teradata_cpp_object_tbl
select distinct *  from kafka_temp_view;
