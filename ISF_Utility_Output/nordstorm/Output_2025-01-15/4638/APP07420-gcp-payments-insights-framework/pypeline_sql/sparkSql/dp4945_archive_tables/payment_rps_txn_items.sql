--Reading Data from landing S3 bucket
CREATE TEMPORARY TABLE temp_rps_txn_items_csv USING CSV
OPTIONS (path "s3://{dp495_s3_bucket}/csv/rps_txn_items/", header "true");

--Writing data to new S3 Path
INSERT INTO TABLE rps_txn_items PARTITION (year, month)
SELECT
    cast(txid AS bigint) AS txid,
    cast(rtm_insdt AS timestamp) AS rtm_insdt,
    cast(businessdate AS timestamp) AS businessdate,
    cast(amount AS decimal (12, 2)) AS amount,
    classid,
    cast(quantity AS int) AS quantity,
    upccode,
    uniqueitemid,
    itemdescription,
    skucode,
    feecode,
    departmentid,
    deptname,
    cast(taxamount AS decimal (12, 2)) AS taxamount,
    fulfillmenttypecode,
    commissionemp,
    employeeid,
    cast(employeediscountpercent AS int) AS employeediscountpercent,
    cast(intentstorenumber AS int) AS intentstorenumber,
    ordertypecode,
    (
        CASE
            WHEN
                cast(originalbusinessdate AS date) < DATE '1950' THEN NULL
            ELSE cast(originalbusinessdate AS date)
        END
    ) AS originalbusinessdate,
    originalretailstoreid,
    (
        CASE
            WHEN
                cast(originaltransactiondate AS date) < DATE '1950' THEN NULL
            ELSE cast(originaltransactiondate AS date)
        END
    ) AS originaltransactiondate,
    originaltransactionidentifier,
    originaltransseqnumber,
    originaltransactionnumber,
    originaltransactiontimestamp,
    originalworkstationid,
    cast(promotionamount AS decimal (12, 2)) AS promotionamount,
    cast(regularprice AS decimal (12, 2)) AS regularprice,
    cast(savingscomparisionprice AS decimal (12, 2)) AS savingscomparisionprice,
    shoppingsystemid,
    itemtype,
    current_timestamp() AS last_updated_time,
    date_format(rtm_insdt, "yyyy") AS year,
    date_format(rtm_insdt, "MM") AS month
FROM temp_rps_txn_items_csv;
