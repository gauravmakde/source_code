--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : pypeline_sql/store_operations_assist_task/td_stg_to_fact.sql
-- Author                  : Sergii Petrovskyi
-- Description             : ETL to write from STORE_OPS_TASK_LDG to STORE_OPS_TASK_FACT table
-- Data Source             : customer-store-operations-assist-task-operational-avro Kafka topic
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-07-31  Sergii Petrovskyi FA-13276 Dataverse data
-- 2024-09-16  Iurii Nosenko FA-13276 Dataverse data - finalization
--*************************************************************************************************************************************

-- Create temporary table to get latest data from staging table without duplicates of records
create
VOLATILE MULTISET table
#STORE_OPS_TASK_LDG
as (
    select
      taskType_id task_type_id,
      taskType_name task_type_name,
      subject,
      owningBusinessUnit_id owning_business_unit_id,
      owningBusinessUnit_name owning_business_unit_name,
      outletzone_id outlet_zone_id,
      outletzone_name outlet_zone_name,
      activity_status_code,
      state_code,
      priority_code,
      scheduledStart scheduled_start,
      scheduledEnd scheduled_end,
      cast(createdTime as timestamp(6) with time zone) latest_event_tmstp_pacific,
      cast(cast(createdTime as timestamp(6) with time zone) at 'America Pacific' as date format 'YYYY-MM-DD') as latest_event_date_pacific,
      batch_id dw_batch_id,
      batch_date dw_batch_date,
      current_timestamp(0) as dw_sys_load_tmstp,
      current_timestamp(0) as dw_sys_updt_tmstp
      from {db_env}_NAP_BASE_VWS.STORE_OPS_TASK_LDG sotl
      left join (select batch_id, batch_date
                        from {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                        where interface_code = 'STORE_OPS_TASK_FACT'
                              and dw_sys_end_tmstp is null) bi
            on 1=1
    )
with data
           primary index (task_type_id, subject, owning_business_unit_id, outlet_zone_id, scheduled_end)
on commit preserve rows;
ET;

-- Collect stats for tables
COLLECT statistics index (task_type_id, subject, owning_business_unit_id, outlet_zone_id, scheduled_end) on
#STORE_OPS_TASK_LDG;
ET;

--Merge data based on partition and business key columns
merge
      into
      {db_env}_NAP_BASE_VWS.STORE_OPS_TASK_FACT as tgt
      using #STORE_OPS_TASK_LDG as src
      on
      tgt.task_type_id = src.task_type_id
      and tgt.subject = src.subject
      and tgt.owning_business_unit_id = src.owning_business_unit_id
      and tgt.outlet_zone_id = src.outlet_zone_id
      and tgt.scheduled_end = src.scheduled_end
when matched then
      update set
            task_type_name = src.task_type_name,
            owning_business_unit_name = src.owning_business_unit_name,
            outlet_zone_name = src.outlet_zone_name,
            activity_status_code = src.activity_status_code,
            state_code = src.state_code,
            priority_code = src.priority_code,
            scheduled_start = src.scheduled_start,
            latest_event_tmstp_pacific = src.latest_event_tmstp_pacific,
            latest_event_date_pacific = src.latest_event_date_pacific,
            dw_batch_id = src.dw_batch_id,
            dw_batch_date = src.dw_batch_date,
            dw_sys_updt_tmstp = src.dw_sys_updt_tmstp
when not matched then
      insert
            (task_type_id,
            task_type_name,
            subject,
            owning_business_unit_id,
            owning_business_unit_name,
            outlet_zone_id,
            outlet_zone_name,
            activity_status_code,
            state_code,
            priority_code,
            scheduled_start,
            scheduled_end,
            latest_event_tmstp_pacific,
            latest_event_date_pacific,
            dw_batch_id,
            dw_batch_date,
            dw_sys_load_tmstp,
            dw_sys_updt_tmstp)
      values (src.task_type_id,
            src.task_type_name,
            src.subject,
            src.owning_business_unit_id,
            src.owning_business_unit_name,
            src.outlet_zone_id,
            src.outlet_zone_name,
            src.activity_status_code,
            src.state_code,
            src.priority_code,
            src.scheduled_start,
            src.scheduled_end,
            src.latest_event_tmstp_pacific,
            src.latest_event_date_pacific,
            src.dw_batch_id,
            src.dw_batch_date,
            src.dw_sys_load_tmstp,
            src.dw_sys_updt_tmstp);
