--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : pypeline_sql/store_operations_assist_task/kafka_avro_to_td_stg.sql
-- Author                  : Sergii Petrovskyi
-- Description             : ETL to write Store Operation Task (Dataverse) data from kafka topic "customer-store-operations-assist-task-operational-avro" to
--                           STORE_OPS_TASK_LDG table
-- Data Source             : customer-store-operations-assist-task-operational-avro Kafka topic
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-07-31  Sergii Petrovskyi FA-13276 Dataverse data
-- 2024-09-16  Iurii Nosenko FA-13276 Dataverse data - finalization
--*************************************************************************************************************************************

--Reading Data from Source Kafka Topic
create
temporary view store_operations_assist_task_src as
select *
from (select
            (cast(src.createdtime as string) || '+00:00') as createdtime,
            tasktype.id as tasktype_id,
            tasktype.name as tasktype_name,
            subject,
            owningbusinessunit.id as owningbusinessunit_id,
            owningbusinessunit.name as owningbusinessunit_name,
            outletzone.id as outletzone_id,
            outletzone.name as outletzone_name,
            status as activity_status_code,
            state as state_code,
            priority as priority_code,
            (cast(src.scheduledstart as string) || '+00:00') scheduledstart,
            (cast(src.scheduledend as string) || '+00:00')   scheduledend,
            row_number()                                     over (partition by src.tasktype.id,
                                                                                src.subject,
                                                                                src.owningbusinessunit.id,
                                                                                src.outletzone.id,
                                                                                src.scheduledend order by cast(src.scheduledend as timestamp) desc) rn
      from store_operations_assist_task src
      where element_at(src.headers, 'Nord-Test') is null
        and element_at(src.headers, 'Nord-Load') is null)
where rn = 1;

----------------------------------------------------------------------------------------------------
---Writing Kafka Data to Teradata staging table: STORE_OPS_TASK_LDG
----------------------------------------------------------------------------------------------------
insert
overwrite table store_operations_assist_task_ldg
     (createdTime,
      taskType_id,
      taskType_name,
      subject,
      owningBusinessUnit_id,
      owningBusinessUnit_name,
      outletzone_id,
      outletzone_name,
      activity_status_code,
      state_code,
      priority_code,
      scheduledStart,
      scheduledEnd,
      dw_sys_load_tmstp)
select
      createdtime,
      tasktype_id,
      tasktype_name,
      subject,
      owningbusinessunit_id,
      owningbusinessunit_name,
      outletzone_id,
      outletzone_name,
      activity_status_code,
      state_code,
      priority_code,
      scheduledstart,
      scheduledend,
      current_timestamp as dw_sys_load_tmstp
from store_operations_assist_task_src;
