--Reading Data from landing S3 bucket
CREATE TEMPORARY TABLE temp_ent_paypal_transaction_dtl_rpt_csv USING CSV
OPTIONS (
    path "s3://{dp495_s3_bucket}/csv/ent_paypal_transaction_dtl_rpt/",
    header "true"
);

--Writing data to new S3 Path
INSERT INTO TABLE ent_paypal_transaction_dtl_rpt PARTITION (year, month)
SELECT
    transaction_id,
    invoice_id,
    paypal_referenceid,
    paypal_referenceid_type,
    transaction_eventcode,
    cast(
        transaction_initiation_date AS timestamp
    ) AS transaction_initiation_date,
    cast(
        transaction_completion_date AS timestamp
    ) AS transaction_completion_date,
    transaction_type,
    cast(gross_transaction_amount AS int) AS gross_transaction_amount,
    gross_transaction_currency,
    fee_type,
    cast(fee_amount AS int) AS fee_amount,
    fee_currency,
    transactional_status,
    cast(insurance_amount AS int) AS insurance_amount,
    cast(sales_tax_amount AS int) AS sales_tax_amount,
    cast(shipping_amount AS int) AS shipping_amount,
    transaction_subject,
    transaction_note,
    payers_accountid,
    payer_address_status,
    item_name,
    item_id,
    shipping_addresscity,
    shipping_addressstate,
    shipping_addresszip,
    shipping_addresscountry,
    shipping_method,
    payment_source,
    loyalty_cardnumber,
    checkout_type,
    cast(load_date AS date) AS load_date,
    current_timestamp() AS last_updated_time,
    date_format(load_date, "yyyy") AS year,
    date_format(load_date, "MM") AS month
FROM temp_ent_paypal_transaction_dtl_rpt_csv;
