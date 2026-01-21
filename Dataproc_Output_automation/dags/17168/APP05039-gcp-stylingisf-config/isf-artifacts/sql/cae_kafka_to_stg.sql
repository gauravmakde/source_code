--Source
--Reading Data from  Source Kafka Topic Name=customer_activity_engaged_avro using SQL API CODE
create temporary view temp_customer_activity_engaged AS select * from kafka_customer_activity_engaged_avro;

--Transform Logic
--Extracting Required Column from Kafka (Cache in previous SQL)


create temporary view customer_activity_engaged_exploded as
select
  customer.id as customer_id,
  customer.idtype as customer_id_type,
  cast(NULL as string) as curation_curator_id,
  eventTime  as eventtime_pst,
  'Clicked into Curation' as styling_event,
  cast(NULL as string) as channel_brand,
  source.platform  as platform,
  element.id as element_id,
  explode_outer(context.digitalcontents) as digitalcontents
from temp_customer_activity_engaged where context is not null and context.digitalcontents is not null and context.digitalcontents!=array();

create temporary view customer_activity_engaged_filtered as
select
customer_id as customer_id,
customer_id_type as customer_id_type,
curation_curator_id as curation_curator_id,
eventtime_pst as eventtime_pst,
styling_event as styling_event,
digitalcontents.id as curation_id,
channel_brand as channel,
platform as platform,
element_id as element_id
from customer_activity_engaged_exploded
where digitalcontents.idtype = 'CURATION'
and lower(element_id) = 'account/yourstylists/styleboardpreview';

--Sink
---Writing Kafka Data to Teradata using SQL API CODE
insert overwrite table write_teradata_curation_styling_interaction_tbl select distinct * from customer_activity_engaged_filtered;