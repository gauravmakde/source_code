--Reading Data from landing S3 bucket

create temporary table temp_paypal_settlement_report_hist using csv
options (
    path "s3://{dp495_s3_bucket}/csv/paypal_settlement_report_hist/",
    header "true"
);

--Writing data to new S3 Path
insert into table paypal_settlement_report_hist partition (year, month)
select distinct
    transaction_id,
    invoice_id,
    paypal_reference_id,
    paypal_reference_id_type,
    transaction_event_code,
    cast(
        transaction_initiation_date as timestamp
    ) as transaction_initiation_date,
    cast(
        transaction_completion_date as timestamp
    ) as transaction_completion_date,
    transaction_type,
    cast(gross_trasaction_amount as int) as gross_transaction_amount,
    gross_transaction_currency,
    fee_type,
    cast(fee_amount as int) as fee_amount,
    fee_currency,
    custom_field,
    consumer_id,
    payment_tracking_id,
    store_id,
    reportgeneration_date,
    cast(load_date as date) as load_date,
    cast(rps_txid as bigint) as rps_txid,
    cast(update_dt as timestamp) as update_dt,
    cast(publish_flag as int) as publish_flag,
    cast(status_id as int) as status_id,
    current_timestamp() as last_updated_time,
    date_format(transaction_completion_date, "yyyy") as year,
    date_format(transaction_completion_date, "MM") as month
from temp_paypal_settlement_report_hist;
