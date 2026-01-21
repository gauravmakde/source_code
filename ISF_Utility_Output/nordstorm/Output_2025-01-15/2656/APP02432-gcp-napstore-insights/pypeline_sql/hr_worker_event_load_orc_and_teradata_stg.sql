SET QUERY_BAND = '
App_ID=app02239;
DAG_ID=hr_worker_event_load_v1_2656_napstore_insights;
Task_Name=hr_worker_event_v1_load_orc_td_stg;'
FOR SESSION VOLATILE;

--Reading Data from  Source Kafka Topic
create temporary view temp_object_model AS select * from kafka_hr_worker_object_model_avro;

-- Sink
-- Writing Kafka Data to S3 Path
insert into table hr_worker_event_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from temp_object_model;

-- Writing Kafka to Semantic Layer Stage Table
insert overwrite table hr_worker_v2_ldg
select businessUnit.id as businessUnitId,
          businessUnit.description as businessUnitDescription,
          changeEffectiveDate as changeEffectiveDate,
          commissionPlanName as commissionPlanName,
          company.hierarchy as companyHierarchy,
          company.name as companyName,
          compensationCurrency as compensationCurrency,
          contingentWorker.projectedEndDate as contingentWorkerProjectedEndDate,
          contingentWorker.vendorName as contingentWorkerVendorName,
          contingentWorker.vendorNumber as contingentWorkerVendorNumber,
          corporateEmail as corporateEmail,
          cosmeticLineAssignment as cosmeticLineAssignment,
          costCenter.id as costCenterId,
          costCenter.name as costCenterName,
          discount.percent as discountPercent,
          discount.status as discountStatus,
          hireDate as hireDate,
          CAST(isJobExempt AS VARCHAR(100)) as isJobExempt,
          CAST(isPurged AS VARCHAR(100)) as isPurged,
          CAST(isOnsite AS VARCHAR(100)) as isOnsite,
          CAST(isRemoteWorker AS VARCHAR(100)) as isRemoteWorker,
          job.family as jobFamily,
          job.familyGroup as jobFamilyGroup,
          job.title as jobTitle,
          lanId as lanId,
          beautyLineAssignment as beautyLineAssignment,
          lineAssignment as lineAssignment,
          location.name as locationName,
          location.number as locationNumber,
          manager.lanId as managerLanId,
          manager.workerNumber as managerWorkerNumber,
          organizationDescription as organizationDescription,
          originalHireDate as originalHireDate,
          otherLineAssignment as otherLineAssignment,
          payRateType as payRateType,
          payroll.department as payrollDepartment,
          payroll.departmentDescription as payrollDepartmentDescription,
          payroll.store as payrollStore,
          payroll.storeDescription as payrollStoreDescription,
          region.description as regionDescription,
          region.number as regionNumber,
          terminationDate as terminationDate,
          workerNumber as workerNumber,
          workerStatus as workerStatus,
          worker.subType as workerSubType,
          worker.type as workerType,
          workLocationCountry as workLocationCountry,
          cast(decode(encode(firstName, 'US-ASCII'), 'US-ASCII') as string) as firstName,
          cast(decode(encode(lastName, 'US-ASCII'), 'US-ASCII') as string) as lastName,
          cast(decode(encode(middleName, 'US-ASCII'), 'US-ASCII') as string) as middleName,
          cast(decode(encode(preferredLastName, 'US-ASCII'), 'US-ASCII') as string) as preferredLastName,
          cast(decode(encode(preferredName, 'US-ASCII'), 'US-ASCII') as string) as preferredName
from temp_object_model;