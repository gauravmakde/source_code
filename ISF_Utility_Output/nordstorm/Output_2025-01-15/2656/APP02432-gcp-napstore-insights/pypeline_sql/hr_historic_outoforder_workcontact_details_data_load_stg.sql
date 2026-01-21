-- load historic out of order workcontact worker data from S3 location in AVRO format to Teradata sink:
CREATE OR REPLACE TEMPORARY VIEW hr_workcontact_details_input AS select * from s3_avro_input_for_workcontact_details;


-- worker teradata sink:
insert overwrite table hr_worker_v1_ldg
select  employeeId.id as worker_number,
        workerType as worker_type,
        CURRENT_TIMESTAMP as last_updated,
         transactionId as work_contact_details_transaction_id,
         workContact.corporateEmail  as corporate_email,
         workContact.workPhone as corporate_phone_number,
         effectiveDate as work_contact_change_effective_date,
         eventTime as work_contact_details_last_updated
from hr_workcontact_details_input;

