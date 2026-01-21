/*
Table: T2DL_DAS_TRUST_ENGINE_PROD.chargeback_load
Owner: Rujira Achawanantakun
Modified: 2024-09-10
- This sql is used to read chargeback_load data from S3 and load it into
the staging table in tie 2 datalab.
*/

-- load data from s3 to a temp table
create or replace temporary view chargeback_load_csv USING CSV
OPTIONS (
    path "s3://dsa-fraud-prod/trust_engine/chargeback/chargeback_*",
    sep ",",
    header "true"
)
;
-- load data from a temp view to a staging table
insert into table chargeback_load_temp
    select
            case_id,
            loss_type_cd,
            loss_type,
            ft_reported_date,
            case_open_date,
            fraud_tag_date,
            transaction_date,
            ft_tran_id,
            trxn_amt,
            reference23_nr,
            industry_transaction_id,
            authorization_cd,
            mail_phone_indicator_cd,
            pos_entry_mode_cd,
            merchant_account_id,
            merchant_nm,
            merch_cat_code,
            merchant_postal_cd
	from chargeback_load_csv
	where 1=1
        and fraud_tag_date between date '2019-01-01' and date '2023-12-31'
;