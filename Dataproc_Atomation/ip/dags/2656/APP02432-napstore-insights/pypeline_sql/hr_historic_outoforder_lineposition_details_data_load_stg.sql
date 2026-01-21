-- load historic out of order line position worker data from S3 location in AVRO format to Teradata sink:
CREATE OR REPLACE TEMPORARY VIEW hr_lineposition_details_input AS select * from s3_avro_input_for_lineposition_details;


-- worker teradata sink:
insert overwrite table hr_worker_v1_ldg
select  employeeId.id as worker_number,
        workerType as worker_type,
        CURRENT_TIMESTAMP as last_updated,
        transactionId as position_details_transaction_id,
        additionalPositionInformation.beautyLineAssignment as beauty_line_assignment,
        additionalPositionInformation.beautyLineAssignmentId as beauty_line_assignment_id,
        additionalPositionInformation.otherLineAssignment as other_line_assignment,
        CAST(additionalPositionInformation.lineAssignment as VARCHAR(2000)) as line_assignment,
        effectiveDate as line_assignment_change_effective_date,
        eventTime as line_position_details_last_updated
from hr_lineposition_details_input;

