-- read from ORC
create temporary view chargeback as
select *
from scoi.chargeback_lifecycle
where batch_id = 'SQL.PARAM.AIRFLOW_RUN_ID';

-- VendorChargebackExempted
create temporary view exempted_exploded as
select
  chargebackId,
  lastupdatedtime,
  'VendorChargebackExempted' as event_name,
  vendorchargebackexempteddetails as exempted,
  batch_id
from chargeback
where vendorchargebackexempteddetails is not null;

-- VendorChargebackIssued
create temporary view issued_exploded as
select
  chargebackId,
  lastupdatedtime,
  'VendorChargebackIssued' as event_name,
  vendorchargebackissueddetails as issued,
  batch_id
from chargeback
where vendorchargebackissueddetails is not null;

-- VendorChargebackReversed
create temporary view reversed_exploded_base as
select
  chargebackId,
  lastupdatedtime,
  'VendorChargebackReversed' as event_name,
  explode(vendorchargebackreverseddetails) as reversed,
  batch_id
from chargeback;

create temporary view reversed_exploded as
select
  chargebackId,
  lastupdatedtime,
  event_name,
  reversed,
  batch_id
from reversed_exploded_base
where reversed is not null;

-- Create final table
create temporary view chargeback_events_temp as
select
    chargebackId as chargeback_id,
    lastupdatedtime as last_updated_time,
    event_name as event_name,
    substring(from_utc_timestamp(exempted.eventtime, 'America/Los_Angeles'), 0, 10) as event_date_pacific,
    date_format(exempted.eventtime, "yyyy-MM-dd HH:mm:ss.SSSxxx") as event_time,
    exempted.vendorid as vendor_id, 
    exempted.rmspurchaseorderid as rms_purchase_order_id, 
    exempted.invoicenumber as invoice_number, 
    exempted.inboundshipmentnumber as inbound_shipment_number, 
    exempted.vendorbilloflading as vendor_bill_of_lading, 
    exempted.amount.currencycode as amount_currency_code,
    exempted.amount.units as amount_units, 
    exempted.amount.nanos as amount_nanos, 
    exempted.units as units, 
    exempted.violationtype as violation_type, 
    exempted.exemptionreason as exemption_reason,
    batch_id as dw_batch_id
from exempted_exploded
UNION ALL
select
    chargebackId as chargeback_id,
    lastupdatedtime as last_updated_time,
    event_name as event_name,
    substring(from_utc_timestamp(issued.eventtime, 'America/Los_Angeles'), 0, 10) as event_date_pacific,
    date_format(issued.eventtime, "yyyy-MM-dd HH:mm:ss.SSSxxx") as event_time,
    issued.vendorid as vendor_id,
    issued.rmspurchaseorderid as rms_purchase_order_id, 
    issued.invoicenumber as invoice_number, 
    issued.inboundshipmentnumber as inbound_shipment_number, 
    issued.vendorbilloflading as vendor_bill_of_lading, 
    issued.amount.currencycode as amount_currency_code,
    issued.amount.units as amount_units, 
    issued.amount.nanos as amount_nanos, 
    issued.units as units,     
    issued.violationtype as violation_type, 
    null as exemption_reason,
    batch_id as dw_batch_id
from issued_exploded
UNION ALL
select
    chargebackId as chargeback_id,
    lastupdatedtime as last_updated_time,
    event_name as event_name,
    substring(from_utc_timestamp(reversed.eventtime, 'America/Los_Angeles'), 0, 10) as event_date_pacific,
    date_format(reversed.eventtime, "yyyy-MM-dd HH:mm:ss.SSSxxx") as event_time,
    null as vendor_id,
    null as rms_purchase_order_id, 
    null as invoice_number, 
    null as inbound_shipment_number, 
    null as vendor_bill_of_lading, 
    reversed.amount.currencycode as amount_currency_code,
    reversed.amount.units as amount_units, 
    reversed.amount.nanos as amount_nanos, 
    null as units,     
    null as violation_type, 
    null as exemption_reason,
    batch_id as dw_batch_id
from reversed_exploded
;

insert overwrite table CHARGEBACK_EVENT_LDG
select    
    chargeback_id,
    last_updated_time,
    event_name as event_name,
    event_date_pacific,
    event_time,
    vendor_id,
    rms_purchase_order_id, 
    invoice_number, 
    inbound_shipment_number, 
    vendor_bill_of_lading, 
    amount_currency_code,
    amount_units, 
    amount_nanos, 
    units,     
    violation_type, 
    exemption_reason,
    dw_batch_id
from chargeback_events_temp;
