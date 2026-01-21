SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=nsg_wifi_load_2656_napstore_insights;
Task_Name=s3_avro_to_orc_load_0;'
FOR SESSION VOLATILE;

--Source
--Reading from s3 bucket
CREATE TEMPORARY VIEW nsg_wifi_events_all USING AVRO
OPTIONS (path "s3://{nap_store_events_bucket_name}/wifi/integrations/nsg/wifi/hourly/store-network-wifi-traffic-events-avro/");

--Reading Events from partitioned folders
CREATE TEMPORARY VIEW nsg_wifi_events as
--Reading Events from previous day
(select
mac,
CAST((timestamp / 1000000) AS TIMESTAMP) AS timestamp,
store,
type,
bssid,
essid,
accessPointName,
accessControlMethod,
serverName,
datestamp,
datestamp AS business_date
from nsg_wifi_events_all
where year = year(current_date()-1) and month = month(current_date()-1) and day = day(current_date()-1) and hour in (09,10,11,12,13,14,15,16,17,18,19,20,21,22,23))
UNION ALL
--Reading Events from current day
(select
mac,
CAST((timestamp / 1000000) AS TIMESTAMP) AS timestamp,
store,
type,
bssid,
essid,
accessPointName,
accessControlMethod,
serverName,
datestamp,
datestamp AS business_date
from nsg_wifi_events_all
where year = year(current_date()) and month = month(current_date()) and day = day(current_date()) and hour in (00,01,02,03,04,05,06,07,08));

-- Sink
-- Writing ORC data to S3 Path
insert into table orc partition(business_date)
select *
from nsg_wifi_events;
