
--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : pypeline_sql/inventory_rfid_program_event/csv_to_td_fct.sql
-- Author                  : Artur Karpunets
-- Description             : RFID In-program history backfill
-- ETL Run Frequency       : One time
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-02-26 Artur Karpunets        FA-11806: RFID In-program history backfill
-- 2024-09-27 Iurii Nosenko          FA-9815: RFID In-program history backfill
--*************************************************************************************************************************************
SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=sco_backfill_inventory_rfid_program_event_fact_17610_DAS_SC_OUTBOUND_APP08649_insights_v2;
Task_Name=main_job_1_load_csv_td_fct_step;
LoginUser={td_login_user};
Job_Name=sco_backfill_inventory_rfid_program_event_fact;
Data_Plane=Customer;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;

create temporary view backfill_inventory_rfid_program_event_input AS
select *
from backfill_inventory_rfid_program_event;

create temporary view backfill_inventory_rfid_program_event_final as
select ldg.*
from (select src.rms_sku_id as rms_sku_id,
             src.rfid_enabled as event_type_code,
             CURRENT_TIMESTAMP as event_tmstp,
             CURRENT_TIMESTAMP as event_tmstp_pacific,
             -1 as dw_batch_id,
             CURRENT_DATE() as dw_batch_date,
             CURRENT_TIMESTAMP as dw_sys_load_tmstp,
             CURRENT_TIMESTAMP as dw_sys_updt_tmstp
      from backfill_inventory_rfid_program_event_input src) ldg;

insert into table INVENTORY_RFID_PROGRAM_EVENT_FACT
      ( rms_sku_id
      , event_type_code
      , event_tmstp
      , event_tmstp_pacific
      , dw_batch_id
      , dw_batch_date
      , dw_sys_load_tmstp
      , dw_sys_updt_tmstp)
select distinct
        src.rms_sku_id,
        src.event_type_code,
        src.event_tmstp,
        src.event_tmstp_pacific,
        src.dw_batch_id,
        src.dw_batch_date,
        src.dw_sys_load_tmstp,
        src.dw_sys_updt_tmstp
    from backfill_inventory_rfid_program_event_final as src;
