set `mapreduce.input.fileinputformat.input.dir.recursive` = true;

MSCK REPAIR table acp_etl_landing.source_events;

create
temporary view source_events AS
SELECT *
FROM acp_etl_landing.source_events
WHERE TO_DATE(CONCAT_WS('-', CAST(year AS STRING), CAST(month AS STRING), CAST(day AS STRING))) >= {curr_date} - INTERVAL {source_events_date} DAY;

CREATE OR REPLACE TEMPORARY VIEW temp_SaleInvoiced AS
    SELECT
    get_json_object(payload, "$.content.invoiceId") as invoiceId,
    get_json_object(payload, "$.content.invoiceTime") as invoiceTime,
    explode(from_json(get_json_object(payload, "$.content.items"), "array<string>")) AS exploded_items,
    *
    FROM source_events
    where type = "SaleInvoiced" ;

CREATE OR REPLACE TEMPORARY VIEW temp2_SaleInvoiced AS
    SELECT
    case when (invoiceId is null or invoiceId = '')
        then 'N'
        else 'Y'
    end as does_SaleInvoice_Event_Exists,
    get_json_object(exploded_items, "$.orderLineId") as saleInvoiced_orderLineId,
    invoiceId as saleInvoiceId,
    from_utc_timestamp(to_timestamp(invoiceTime / 1000), "America/Los_Angeles") as saleInvoiceTime
    FROM temp_SaleInvoiced ;

CREATE OR REPLACE TEMPORARY VIEW SaleInvoiced AS
    SELECT * from temp2_SaleInvoiced;

CREATE OR REPLACE TEMPORARY VIEW temp_OrderSubmitted AS
SELECT
explode(from_json(get_json_object(payload, '$.content.items'), 'array<string>')) AS exploded_items,
get_json_object(payload, '$.content.invoice.orderNumber') as orderSubmitted_orderNumber,
*
FROM source_events
where type = "OrderSubmitted";

CREATE OR REPLACE TEMPORARY VIEW temp2_OrderSubmitted AS
SELECT
get_json_object(exploded_items, '$.orderLineId') as orderSubmitted_orderLineId,
get_json_object(exploded_items, '$.partnerRelationshipId') as partnerRelationshipId,
orderSubmitted_orderNumber
FROM temp_OrderSubmitted;

CREATE OR REPLACE TEMPORARY VIEW OrderSubmitted AS
select *
from temp2_OrderSubmitted
WHERE length(partnerRelationshipId) > 0
AND orderSubmitted_orderNumber RLIKE '^[0-9]+$';

CREATE OR REPLACE TEMPORARY VIEW temp_Shipped AS
    SELECT
    get_json_object(payload, "$.content.orderLineId") as shipped_orderLineId,
    get_json_object(payload, "$.content.orderNumber") as shipped_orderNumber,
    get_json_object(payload, "$.content.shipmentDate") as shipped_shipmentDate,
    *
    FROM source_events
    where type = "SnapshotDetails"
    and created_at >= {shipped_date};

CREATE OR REPLACE TEMPORARY VIEW temp2_Shipped AS
    select
    case when (shipped_orderLineId is null or shipped_orderLineId = '')
        then 'N'
        else 'Y'
    end as does_Shipped_Event_Exists,
    shipped_orderLineId,
    shipped_orderNumber,
    from_utc_timestamp(shipped_shipmentDate, "America/Los_Angeles") as shipped_shipmentDate
    from temp_Shipped;

CREATE OR REPLACE TEMPORARY VIEW Shipped_v1 AS
    SELECT * FROM temp2_Shipped;

CREATE OR REPLACE TEMPORARY VIEW temp_Settlement AS
    SELECT
    get_json_object(payload, "$.content.transactionId.id") as settlement_transactionId,
    get_json_object(payload, "$.content.eventTime") as settlement_time,
    CASE
        WHEN type = 'SettlementFailedV2' THEN get_json_object(payload, "$.content.failureReason")
        ELSE 'Not Applicable'
    END as failure_reason,
    *
    FROM source_events
    where type = "SettlementSucceededV2"
    or type = "SettlementFailedV2";

CREATE OR REPLACE TEMPORARY VIEW temp2_Settlement AS
    select
    case when (first(settlement_transactionId) is null or first(settlement_transactionId) = '')
        then 'N'
        else 'Y'
    end as does_Settlement_Event_Exists,
    first(settlement_transactionId) as settlement_transactionId,
    first(type) as settlement_type,
    first(from_utc_timestamp(to_timestamp(settlement_time / 1000), "America/Los_Angeles")) as settlement_time,
    first(failure_reason) as failure_reason
    from temp_Settlement
    group by settlement_transactionId
    order by settlement_time desc;

CREATE OR REPLACE TEMPORARY VIEW Settlement AS
    SELECT * FROM temp2_Settlement;

CREATE OR REPLACE TEMPORARY VIEW temp_Artemis AS
    SELECT
    get_json_object(payload, "$.content.invoiceId") as artemis_invoice_id,
    explode(from_json(get_json_object(payload, '$.content.orderLineRevenueDetails'), "array<string>")) AS exploded_items,
    get_json_object(payload, "$.content.eventTime") as artemis_time,
    *
    FROM source_events
    where type = "InvoiceRevenueRecognized";

CREATE OR REPLACE TEMPORARY VIEW temp2_Artemis AS
    select
    case when (first(artemis_invoice_id) is null or first(artemis_invoice_id) = '')
        then 'N'
        else 'Y'
    end as does_Artemis_Event_Exists,
    first(artemis_invoice_id) as artemis_invoice_id,
    first(get_json_object(exploded_items, "$.orderLineId")) as artemis_orderLineId,
    first(from_utc_timestamp(to_timestamp(artemis_time / 1000), "America/Los_Angeles")) as artemis_time
    from temp_Artemis;

CREATE OR REPLACE TEMPORARY VIEW Artemis AS
    select * from temp2_Artemis;

CREATE OR REPLACE TEMPORARY VIEW temp_failed_FRT AS
    select
    "FAILED" as failed_validation_status,
    from_utc_timestamp(creationTime, "America/Los_Angeles") as failed_frt_creationTime,
    financialRetailTransactionRecordId as failed_frt_financialRetailTransactionRecordId,
    explode(merchandiseLineItems) as merchandiseLineItem,
    transactionType as failed_frt_transactionType,
    *
    from temp_frt_failure_topic;

CREATE OR REPLACE TEMPORARY VIEW failed_FRT AS
    select
    case when (first(failed_frt_financialRetailTransactionRecordId) is null or first(failed_frt_financialRetailTransactionRecordId) = '')
        then 'N'
        else 'Y'
    end as does_failed_FinancialRetailTransaction_Exists,
    first(failed_validation_status) as failed_validation_status,
    first(failed_frt_creationTime) as failed_frt_creationTime,
    first(failed_frt_transactionType) as failed_frt_transactionType,
    first(failed_frt_financialRetailTransactionRecordId) as failed_frt_financialRetailTransactionRecordId,
    first(merchandiseLineItem.transactionLineId) as failed_frt_transactionLineId,
    first(customerTransactionId) as failed_customerTransactionId
    from temp_failed_FRT
    where failed_frt_transactionType = 'SALE'
    group by customerTransactionId;


CREATE OR REPLACE TEMPORARY VIEW temp_FRT AS
    select
    "SUCCEEDED" as validation_status,
    explode(merchandiseLineItems) as merchandiseLineItem,
    transactionType as frt_transactionType,
    *
    from temp_frt_success_topic;


CREATE OR REPLACE TEMPORARY VIEW temp2_FRT AS
    select
    case when (financialRetailTransactionRecordId is null or financialRetailTransactionRecordId = '')
        then 'N'
        else 'Y'
    end as does__FinancialRetailTransaction_Exists,
    validation_status as frt_validation_status,
    from_utc_timestamp(creationTime, "America/Los_Angeles") as frt_creationTime,
    frt_transactionType,
    financialRetailTransactionRecordId as frt_financialRetailTransactionRecordId,
    merchandiseLineItem.transactionLineId as frt_transactionLineId,
    customerTransactionId
    from temp_FRT
    where frt_transactionType = 'SALE';


CREATE OR REPLACE TEMPORARY VIEW failed_FRT_Overridden AS
    select
    merchandiseLineItem.transactionLineId as failed_frt_transactionLineId
    from temp_failed_FRT
    where auditActivityDetail.auditActivityType = 'ERROR_OVERRIDDEN';

--Filter out Overridden FRTs from Shipped Dataset
CREATE OR REPLACE TEMPORARY VIEW Shipped AS
    select *
    from Shipped_v1
    left join failed_FRT_Overridden
    on Shipped_v1.shipped_orderLineId = failed_FRT_Overridden.failed_frt_transactionLineId
    where failed_FRT_Overridden.failed_frt_transactionLineId is null;

CREATE OR REPLACE TEMPORARY VIEW FRT AS
    select * from temp2_FRT;

CREATE OR REPLACE TEMPORARY VIEW temp_REV400 AS
select
--Shipped
    shipped_shipmentDate as shipped_Event_Date,

--OrderSubmitted
	partnerRelationshipId,
	orderSubmitted_orderNumber as orderNumber,

--Shipped
    shipped_orderLineId as orderLineId,
--SaleInvoiced
    saleInvoiceId as invoiceId,
    saleInvoiceTime,

--Settlement
    settlement_type,
    settlement_time as settlement_Event_Time,
    failure_reason as settlement_failure_reason,

--Artemis
    artemis_time as artemis_Event_Time,

--Success FRT
    frt_validation_status as success_frt_validation_status,
    frt_creationTime as success_frt_creationTime,
    frt_transactionType as success_frt_transactionType,
    frt_financialRetailTransactionRecordId as success_frt_financialRetailTransactionRecordId,

--failed_FRT
    failed_validation_status,
    failed_frt_creationTime,
    failed_frt_transactionType,
    failed_frt_financialRetailTransactionRecordId
from Shipped sh
inner join OrderSubmitted as os on sh.shipped_orderLineId = os.orderSubmitted_orderLineId
left join SaleInvoiced sa on sh.shipped_orderLineId = sa.saleInvoiced_orderLineId
left join Settlement st on sa.saleInvoiceId = st.settlement_transactionId
left join Artemis ar on sa.saleInvoiceId = ar.artemis_invoice_id
left join FRT on sh.shipped_orderLineId = FRT.frt_transactionLineId
left join failed_FRT on sa.saleInvoiceId = failed_FRT.failed_customerTransactionId;

INSERT into table SALE
SELECT distinct *
FROM temp_REV400
where success_frt_financialRetailTransactionRecordId is null;

