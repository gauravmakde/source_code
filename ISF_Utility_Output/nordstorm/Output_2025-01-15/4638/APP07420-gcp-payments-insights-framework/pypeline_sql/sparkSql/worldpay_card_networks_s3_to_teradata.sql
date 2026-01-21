-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=worldpay_card_networks;
Task_Name=worldpay_card_networks_s3_to_teradata_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

CREATE TEMPORARY TABLE card_networks_tmp USING csv
OPTIONS (
    path 's3://{master_merchant_bin_files_bucket}/csv/',
    sep '|',
    header 'true'
);

INSERT OVERWRITE TABLE WORLDPAY_CARD_NETWORKS_DIM
SELECT DISTINCT
    id,
    network
FROM card_networks_tmp;
