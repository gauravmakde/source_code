--Reading Data from landing S3 bucket
CREATE TEMPORARY TABLE temp_rps_transactions_csv USING CSV
OPTIONS (path "s3://{dp495_s3_bucket}/csv/rps_transactions/", header "true");

--Writing data to new S3 Path
INSERT INTO TABLE rps_transactions PARTITION (year, month)
SELECT DISTINCT
    CAST(txid AS BIGINT) AS txid,
    globaltransactionid,
    cardtype,
    cardsubtype,
    CAST(transactionversionnumber AS INT) AS transactionversionnumber,
    reversalcode,
    CAST(storenumber AS INT) AS storenumber,
    CAST(workstationnumber AS INT) AS workstationnumber,
    transactionnumber,
    (
        CASE
            WHEN
                CAST(transactiontimestamp AS DATE) < DATE '1950'
                THEN NULL
            ELSE CAST(transactiontimestamp AS TIMESTAMP)
        END
    ) AS transactiontimestamp,
    (
        CASE
            WHEN
                CAST(businessdate AS DATE) < DATE '1950'
                THEN NULL
            ELSE CAST(businessdate AS TIMESTAMP)
        END
    ) AS businessdate,
    businessrule,
    (
        CASE
            WHEN
                CAST(rtm_insdt AS DATE) < DATE '1950'
                THEN NULL
            ELSE CAST(rtm_insdt AS TIMESTAMP)
        END
    ) AS rtm_insdt,
    sodstransactionid,
    transactiontype,
    authorizationcode,
    currencycode,
    CAST(monetaryamount AS DECIMAL (12, 2)) AS monetaryamount,
    customerorderid,
    entrymethodcode,
    expirationdate,
    nameoncard,
    otglobaltransactionid,
    CAST(otstorenumber AS INT) AS otstorenumber,
    CAST(otworkstationnumber AS INT) AS otworkstationnumber,
    ottransactionnumber,
    ottransactiontype,
    (
        CASE
            WHEN
                CAST(otbusinessdate AS DATE) < DATE '1950'
                THEN NULL
            ELSE CAST(otbusinessdate AS DATE)
        END
    ) AS otbusinessdate,
    sourceid,
    sourcetype,
    tendermethodid,
    tendertype,
    CAST(voidamount AS DECIMAL (12, 2)) AS voidamount,
    CAST(taxamount AS DECIMAL (12, 2)) AS taxamount,
    statuscode,
    trimmedauthcode,
    shiptocity,
    shiptostate,
    shiptozip,
    shiptocountry,
    presale_type_cd,
    settlementidentifier,
    tenderidentifier,
    payeridentifier,
    orderidentifier,
    qualcode,
    recordidentifier,
    ndirectordersuffix,
    salespersonid,
    postransactioncode,
    pspreleasenumber,
    customerflag,
    customerid,
    followupsalespersonid,
    activitysourcecode,
    (
        CASE
            WHEN
                CAST(balancedate AS DATE) < DATE '1950'
                THEN NULL
            ELSE CAST(balancedate AS DATE)
        END
    ) AS balancedate,
    (
        CASE
            WHEN
                CAST(lastmodifiedtimestamp AS DATE) < DATE '1950' THEN NULL
            ELSE CAST(lastmodifiedtimestamp AS TIMESTAMP)
        END
    ) AS lastmodifiedtimestamp,
    CAST(manualtaxamount AS DECIMAL (12, 2)) AS manualtaxamount,
    CAST(returntaxamount AS DECIMAL (12, 2)) AS returntaxamount,
    authenticationtype,
    authdata,
    protectionsource,
    protectionmethod,
    voidedtransactiontype,
    voidedglobaltransactionid,
    CAST(voidedstorenumber AS INT) AS voidedstorenumber,
    CAST(voidedworkstationnumber AS INT) AS voidedworkstationnumber,
    voidedtransactionnumber,
    (
        CASE
            WHEN
                CAST(dtcrequesttransactiondate AS DATE) < DATE '1950' THEN NULL
            ELSE CAST(dtcrequesttransactiondate AS DATE)
        END
    ) AS dtcrequesttransactiondate,
    dtcrequesttransactionnumber,
    CAST(dtcrequestworkstationnumber AS INT) AS dtcrequestworkstationnumber,
    CAST(fulfillingretailstorenumber AS INT) AS fulfillingretailstorenumber,
    CAST(intentstorenumber AS INT) AS intentstorenumber,
    dtcrequesttransactionid,
    fk_tokenized_hvt_cardnumber,
    n_token_cardnumber,
    truncated_cardnumber,
    CURRENT_TIMESTAMP() AS last_updated_time,
    (
        CASE
            WHEN
                CAST(transactiontimestamp AS DATE) < DATE '1950' THEN NULL
            ELSE DATE_FORMAT(transactiontimestamp, "yyyy")
        END
    ) AS year,
    (
        CASE
            WHEN
                CAST(transactiontimestamp AS DATE) < DATE '1950'
                THEN NULL
            ELSE DATE_FORMAT(transactiontimestamp, "MM")
        END
    ) AS month
FROM temp_rps_transactions_csv;
