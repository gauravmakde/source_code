-- Read kafka source data
create temporary view kafka_source_stg as
select
    deterministicProfileId as deterministicProfileId,
    customer.classifications.verificationStatus as verificationStatus,
    customer.classifications.partnerPool as partnerPool,
    enterpriseRetailTransaction as enterpriseRetailTransaction,
    financialRetailTransaction as financialRetailTransaction,
    customer.uniqueSourceId as uniqueSourceId,
    cast(cardinality(enterpriseRetailTransaction) as string) as enterpriseRetailTransactionCount,
    cast(cardinality(financialRetailTransaction) as string) as financialRetailTransactionCount,
    element_at(headers, 'TransponderId') as TransponderId,
    element_at(headers, 'EventTime') as eventTime
from kafka_source;

-- Create stage data
create temporary view deterministic_customer_transaction_stg as
select
    deterministicProfileId,
    explode(enterpriseRetailTransaction) as enterpriseRetailTransactionItem,
    TransponderId,
    eventTime
from kafka_source_stg;

create temporary view deterministic_customer_frt_stg as
select
    deterministicProfileId,
    explode(financialRetailTransaction) as financialRetailTransactionItem,
    TransponderId,
    eventTime
from kafka_source_stg;

-- Create final sink data
create temporary view deterministic_customer_classification_final as
select
    deterministicProfileId deterministicprofileid,
    verificationStatus classifications_verificationstatus,
    partnerPool classifications_partnerpool,
    TransponderId header_transponderid,
    eventTime header_eventtime
FROM kafka_source_stg;

create temporary view deterministic_customer_summary_final as
select
    deterministicProfileId as deterministicprofileid,
    uniqueSourceId as uniquesourceid,
    enterpriseRetailTransactionCount as enterpriseretailtransaction_count,
    financialRetailTransactionCount as financialretailtransaction_count,
    TransponderId as header_transponderid,
    eventTime as header_eventtime
from kafka_source_stg;

create temporary view deterministic_customer_transaction_final as
select
    deterministicProfileId as deterministicprofileid,
    cast(enterpriseRetailTransactionItem.salesCollectHeader.globalTransactionID as string) as globaltransactionid,
    cast(enterpriseRetailTransactionItem.salesCollectHeader.businessDayDate as string) as businessdaydate,
    TransponderId as header_transponderid,
    eventTime as header_eventtime
from deterministic_customer_transaction_stg;

create temporary view deterministic_customer_frt_final as
select
    deterministicProfileId as deterministicprofileid,
    financialRetailTransactionItem.financialRetailTransactionRecordId as financialretailtransactionrecordid,
    financialRetailTransactionItem.supplementaryIdentifiers.enterpriseRetailTransactionLegacyId as enterpriseretailtransactionlegacyid,
    cast(financialRetailTransactionItem.businessDate as string) as businessdaydate,
    TransponderId as header_transponderid,
    eventTime as header_eventtime
from deterministic_customer_frt_stg;


-- Sink data to s3 stage path
insert overwrite table deterministic_customer_classification
select *
from deterministic_customer_classification_final;

insert overwrite table deterministic_customer_summary
select *
from deterministic_customer_summary_final;

insert overwrite table deterministic_customer_transaction
select *
from deterministic_customer_transaction_final;

insert overwrite table deterministic_customer_frt
select *
from deterministic_customer_frt_final;
