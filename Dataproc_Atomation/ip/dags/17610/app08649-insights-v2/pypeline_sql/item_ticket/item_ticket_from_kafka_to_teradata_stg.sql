SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=item_ticket_load_13569_DAS_SC_OUTBOUND_sc_outbound_insights_framework;
Task_Name=main_1_load_from_kafka_to_teradata_stg_table;'
FOR SESSION VOLATILE;

--Reading Data from  Source Kafka Topic
create temporary view temp_object_model AS select * from kafka_item_ticket_input;

-- Writing Kafka Data to S3 Path
insert into table item_ticket_parquet_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from temp_object_model;

-- Writing Kafka to Semantic Layer Stage Table
insert overwrite table item_ticket_stg_table
select eventTime as ticket_print_time,
          id as universal_unique_id,
          store as store_number,
          deviceId as device_id,
          employee.idType as employee_id_type,
          employee.id as employee_id,
          upc as upc_id,
          rmsSkuId as rms_sku_id,
          ticketFormat as ZPL_ticket_format,
          quantity as quantity,
          cast(regularPrice.currencyCode as string) as regular_price_currency_code,
          cast(cast(regularPrice.units AS INTEGER) + (regularPrice.nanos / 1000000000) AS DECIMAL (38, 9)
              ) AS regular_price_amount,
          cast(clearancePrice.currencyCode as string) as clearance_price_currency_code,
          cast(cast(clearancePrice.units AS INTEGER) + (clearancePrice.nanos / 1000000000) AS DECIMAL (38, 9)
              ) AS clearance_price_amount,
          cast(promotionPrice.currencyCode as string) as promotion_price_currency_code,
          cast(cast(promotionPrice.units AS INTEGER) + (promotionPrice.nanos / 1000000000) AS DECIMAL (38, 9)
              ) AS promotion_price_amount
from temp_object_model;

