-- RECON REPORT - FOR CORRECTING TRANSACTIONS IN VT - NEED VTOKEN--
-- sql given by analytics
SELECT
    '163' AS chain_code,
    CASE
        WHEN psbc.card_type = 'VISA' THEN '4-VISA'
        WHEN psbc.card_type = 'AMERICAN_EXPRESS' THEN '7-AMEX'
        WHEN psbc.card_type = 'MASTER_CARD' THEN '1-MasterCard'
        WHEN psbc.card_type = 'DISCOVER' THEN '6-Discover'
    END AS card_type,
    psbc.acquirer_merchant_identifier AS merchant_number,
    psbc.token_value AS account_number_or_token,
    psrntfv.transaction_type,
    psrntfv.settlement_amount AS amount,
    '1 - SNGL MAIL/PHONE' AS mail_phone_indicator, -- THIS IS CURRENTLY FKTOKEN
    ' ' AS internal_settlement,
    'correction' AS gen_merch_desc,
    ' ' AS cust_address,
    ' ' AS cust_zip,
    --expiration_date_month + expiration_date_year format example sb "01/24" mm/yy
    ' ' AS mcc_code,
    ' ' AS cust_service_phone,
    ' ' AS merch_ord_num,
    ' ' AS transaction_locator,
    ' ' AS sales_tax,
    ' ' AS customer_code,
    ' ' AS cvv2_value,
    ' ' AS auth_code,
    ' ' AS restriction_codes,
    ' ' AS gift_card_security_code,
    ' ' AS notes,
    ' ' AS gc_retrieval_reference_number,
    ' ' AS customer_field_1,
    ' ' AS customer_field_2,
    ' ' AS customer_field_3
FROM PREPROD_nap_usr_vws.payment_settlement_result_no_transaction_fact_vw AS psrntfv
INNER JOIN PREPROD_nap_usr_vws.payment_bank_card_settlement_result_fact AS psbc
    ON psrntfv.settlement_result_received_id = psbc.settlement_result_received_id
WHERE
    psrntfv.processing_date > current_date - 15 AND psrntfv.processing_date < current_date - 2
    AND psrntfv.transaction_type <> 'PAYMENT'
    AND psrntfv.store_number NOT IN ('808', '828'
    )
    AND psrntfv.tender_type = 'CREDIT_CARD'
    AND psbc.merchant_category_code <> 'RESTAURANT'
    AND psrntfv.channel_country = 'US';
