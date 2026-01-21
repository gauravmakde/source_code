-- noqa: disable=all
CREATE TABLE IF NOT EXISTS {delta_schema_name}.customer_activity_ordersubmitted_session_delta
(
  sessionId string,
  sessionLastUpdatedTime timestamp,
  sessionAlgorithmId string,
  sessionStartTime timestamp,
  sessionEndTime timestamp,
  sessionExperience string,
  sessionChannel struct<channelCountry:string,channelBrand:string,sellingChannel:string>,
  sessionEventName string,
  sessionEventId string,
  eventId string,
  eventHeaderEventTime timestamp,
  eventSystemTime timestamp,
  eventHeaders map<string,binary>,
  invoice struct<ordernumber:string,ordersessionid:string,subtotal:struct<currencycode:string,units:bigint,nanos:int>,discount:struct<currencycode:string,units:bigint,nanos:int>,shipping:struct<currencycode:string,units:bigint,nanos:int>,fees:array<struct<feeid:string,amount:struct<currencycode:string,units:bigint,nanos:int>,feetype:string>>,shippingoverride:struct<originalshipping:struct<currencycode:string,units:bigint,nanos:int>,reason:string>,anniversaryearlyaccessoverride:string,chargedpromise:struct<maxpromisedate:timestamp,promisetype:string,promisemode:string>,giftoptionstotal:struct<currencycode:string,units:bigint,nanos:int>,tax:struct<currencycode:string,units:bigint,nanos:int>,taxadjustmentdetails:struct<taxmodificationtype:string,taxmodificationentrymethod:string,taxmodificationreason:string,amount:struct<currencycode:string,units:bigint,nanos:int>,governmentidentifier:struct<governmentidtype:string,id:struct<value:string,authority:string,strategy:string,dataclassification:string>,expirydate:date>,offlinereturntaxlocation:struct<city:string,state:string,postalcode:binary,salescountry:string>>,total:struct<currencycode:string,units:bigint,nanos:int>,createtime:timestamp,customercontact:struct<email:struct<value:string,authority:string,strategy:string,dataclassification:string>,phone:struct<value:string,authority:string,strategy:string,dataclassification:string>,firstname:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastname:struct<value:string,authority:string,strategy:string,dataclassification:string>>,referredby:string>,
  eventSource struct<channelcountry:string,channel:string,platform:string,feature:string,servicename:string,store:string,register:string>,
  contextid string,
  customer struct<idtype:string,id:string>,
  credentials struct<iconid:string,shopperid:string,tokenizingcreditcardhvt:struct<value:string,authority:string,strategy:string,dataclassification:string>,creditcardhvt:struct<value:string,authority:string,dataclassification:string>,fraudtoken:string,useragent:string,tokenizingipaddress:struct<value:string,authority:string,strategy:string,dataclassification:string>,ipaddress:struct<value:string,authority:string,dataclassification:string>,experimentid:string,adtrackingstatus:string,advertisingid:string,applevendorid:string,firebaseinstanceid:string>,
  byemployee struct<idtype:string,id:string>,
  isinternational boolean,
  serviceticketid string,
  tenderresults array<struct<tender:struct<bankcard:struct<cardtype:struct<cardtype:string,cardsubtype:string>,expirationdate:date,billingaddress:struct<id:string,firstname:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastname:struct<value:string,authority:string,strategy:string,dataclassification:string>,middlename:struct<value:string,authority:string,strategy:string,dataclassification:string>,line1:struct<value:string,authority:string,strategy:string,dataclassification:string>,line2:struct<value:string,authority:string,strategy:string,dataclassification:string>,line3:struct<value:string,authority:string,strategy:string,dataclassification:string>,city:string,state:string,postalcode:struct<value:string,authority:string,strategy:string,dataclassification:string>,coarsepostalcode:string,countrycode:string,maskaddress:boolean>,shippingaddress:struct<id:string,firstname:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastname:struct<value:string,authority:string,strategy:string,dataclassification:string>,middlename:struct<value:string,authority:string,strategy:string,dataclassification:string>,line1:struct<value:string,authority:string,strategy:string,dataclassification:string>,line2:struct<value:string,authority:string,strategy:string,dataclassification:string>,line3:struct<value:string,authority:string,strategy:string,dataclassification:string>,city:string,state:string,postalcode:struct<value:string,authority:string,strategy:string,dataclassification:string>,coarsepostalcode:string,countrycode:string,maskaddress:boolean>,token:struct<token:struct<value:string,authority:string,strategy:string,dataclassification:string>,tokentype:string>,credentialonfile:string,lastfour:string,tendercapturemethod:string,digitalwalletprovider:string>,bankcardtender:struct<bankcard:struct<token:struct<token:struct<value:string,authority:string,strategy:string,dataclassification:string>,tokentype:string>,cardtypeinfo:struct<cardtype:string,cardsubtype:string>,lastfour:struct<value:string,authority:string,strategy:string,dataclassification:string>,expirationdate:struct<month:string,year:binary>,billingaddress:struct<id:string,firstname:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastname:struct<value:string,authority:string,strategy:string,dataclassification:string>,middlename:struct<value:string,authority:string,strategy:string,dataclassification:string>,line1:struct<value:string,authority:string,strategy:string,dataclassification:string>,line2:struct<value:string,authority:string,strategy:string,dataclassification:string>,line3:struct<value:string,authority:string,strategy:string,dataclassification:string>,city:string,state:string,postalcode:struct<value:string,authority:string,strategy:string,dataclassification:string>,coarsepostalcode:string,countrycode:string,maskaddress:boolean>>,credentialonfile:string,tendercapturemethod:string,digitalwalletprovider:string>,afterpayvirtualcard:struct<afterpayvirtualcard:struct<token:struct<token:struct<value:string,authority:string,strategy:string,dataclassification:string>,tokentype:string>,cardtype:struct<cardtype:string,cardsubtype:string>,lastfour:string,expirationdate:date>,afterpayordertoken:string>,afterpayvirtualcardtender:struct<bankcard:struct<token:struct<token:struct<value:string,authority:string,strategy:string,dataclassification:string>,tokentype:string>,cardtypeinfo:struct<cardtype:string,cardsubtype:string>,lastfour:struct<value:string,authority:string,strategy:string,dataclassification:string>,expirationdate:struct<month:string,year:binary>>,afterpayordertoken:string>,giftcardnote:struct<cardtype:struct<cardtype:string,cardsubtype:string>,accountnumber:struct<value:string,authority:string,strategy:string,dataclassification:string>,accounttoken:struct<token:struct<value:string,authority:string,strategy:string,dataclassification:string>,tokentype:string>,accesscode:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastfour:string,tendercapturemethod:string>,paypal:struct<authorizationid:struct<value:string,authority:string,strategy:string,dataclassification:string>,orderid:struct<value:string,authority:string,strategy:string,dataclassification:string>,cartid:string,payerstatus:string,payerprotectioneligibility:string,payerid:struct<value:string,authority:string,strategy:string,dataclassification:string>,payeremail:struct<value:string,authority:string,strategy:string,dataclassification:string>,paymentmethod:string,paymentstatus:string,payeraccountcountry:string,paymentid:string,billingaddress:struct<id:string,firstname:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastname:struct<value:string,authority:string,strategy:string,dataclassification:string>,middlename:struct<value:string,authority:string,strategy:string,dataclassification:string>,line1:struct<value:string,authority:string,strategy:string,dataclassification:string>,line2:struct<value:string,authority:string,strategy:string,dataclassification:string>,line3:struct<value:string,authority:string,strategy:string,dataclassification:string>,city:string,state:string,postalcode:struct<value:string,authority:string,strategy:string,dataclassification:string>,coarsepostalcode:string,countrycode:string,maskaddress:boolean>>,paypalv2:struct<authorizationid:string,orderid:string,cartid:string,invoicenumber:string,payerstatus:string,payerprotectioneligibility:string,payerid:string,payeremail:struct<value:string,authority:string,strategy:string,dataclassification:string>,paymentmethod:string,paymentstatus:string,payeraccountcountry:string,paymentid:string,billingaddress:struct<id:string,firstname:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastname:struct<value:string,authority:string,strategy:string,dataclassification:string>,middlename:struct<value:string,authority:string,strategy:string,dataclassification:string>,line1:struct<value:string,authority:string,strategy:string,dataclassification:string>,line2:struct<value:string,authority:string,strategy:string,dataclassification:string>,line3:struct<value:string,authority:string,strategy:string,dataclassification:string>,city:string,state:string,postalcode:struct<value:string,authority:string,strategy:string,dataclassification:string>,coarsepostalcode:string,countrycode:string,maskaddress:boolean>>,paypalbillingagreement:struct<billingagreementid:struct<value:string,authority:string,strategy:string,dataclassification:string>,authorizationid:string,payeremail:struct<value:string,authority:string,strategy:string,dataclassification:string>,billingaddress:struct<id:string,firstname:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastname:struct<value:string,authority:string,strategy:string,dataclassification:string>,middlename:struct<value:string,authority:string,strategy:string,dataclassification:string>,line1:struct<value:string,authority:string,strategy:string,dataclassification:string>,line2:struct<value:string,authority:string,strategy:string,dataclassification:string>,line3:struct<value:string,authority:string,strategy:string,dataclassification:string>,city:string,state:string,postalcode:struct<value:string,authority:string,strategy:string,dataclassification:string>,coarsepostalcode:string,countrycode:string,maskaddress:boolean>>,total:struct<currencycode:string,units:bigint,nanos:int>,tendertype:string>,bankcardauthorizationinfo:struct<cvvcode:string,avscode:string,authorizationcode:string>,bankcardauthorizationinfov2:struct<cvvresultcode:string,avsresultcode:string,authorizationcode:string>,afterpayvirtualcardauthorizationinfo:struct<cvvresultcode:string,avsresultcode:string,authorizationcode:string>,transactiontime:timestamp,referralflag:boolean>>,
  promotions array<struct<promotionid:string,changetype:string,reason:string,discount:struct<currencycode:string,units:bigint,nanos:int>,percentoff:double,activationcode:string,employee:struct<idtype:string,id:string>>>,
  items array<struct<orderlineid:string,orderlinenumber:string,product:struct<idtype:string,id:string>,productstyle:struct<idtype:string,id:string>,destination:struct<storenumber:string,address:struct<id:string,firstname:struct<value:string,authority:string,strategy:string,dataclassification:string>,lastname:struct<value:string,authority:string,strategy:string,dataclassification:string>,middlename:struct<value:string,authority:string,strategy:string,dataclassification:string>,line1:struct<value:string,authority:string,strategy:string,dataclassification:string>,line2:struct<value:string,authority:string,strategy:string,dataclassification:string>,line3:struct<value:string,authority:string,strategy:string,dataclassification:string>,city:string,state:string,postalcode:struct<value:string,authority:string,strategy:string,dataclassification:string>,coarsepostalcode:string,countrycode:string,maskaddress:boolean>>,price:struct<current:struct<currencycode:string,units:bigint,nanos:int>,ownership:struct<currencycode:string,units:bigint,nanos:int>,regular:struct<currencycode:string,units:bigint,nanos:int>,percentoff:double,discount:struct<currencycode:string,units:bigint,nanos:int>,taxdetail:struct<taxbreakdown:array<struct<amount:struct<currencycode:string,units:bigint,nanos:int>,category:string>>,tax:struct<currencycode:string,units:bigint,nanos:int>>,adjustmentdetail:struct<discount:struct<currencycode:string,units:bigint,nanos:int>,adjustmentreason:string,pricematchurl:string>,total:struct<currencycode:string,units:bigint,nanos:int>>,promotions:array<struct<promotionid:string,changetype:string,reason:string,discount:struct<currencycode:string,units:bigint,nanos:int>,percentoff:double,activationcode:string,employee:struct<idtype:string,id:string>>>,promise:struct<maxpromisedate:timestamp,promisetype:string,promisemode:string>,backorder:boolean,isgwp:boolean,isbeautysample:boolean,isfinalsale:boolean,customizations:map<string,string>,metadata:map<string,string>,wishlistid:string,upc:string,giftoption:struct<type:string,giftoptionindex:string,recipientemail:struct<value:string,authority:string,strategy:string,dataclassification:string>,from:struct<value:string,authority:string,strategy:string,dataclassification:string>,to:struct<value:string,authority:string,strategy:string,dataclassification:string>,message:struct<value:string,authority:string,strategy:string,dataclassification:string>,cost:struct<currencycode:string,units:bigint,nanos:int>>,assistedbyemployee:struct<idtype:string,id:string>>>,
  exchangeditems array<struct<ordernumber:string,orderlineid:string,product:struct<idtype:string,id:string>,price:struct<current:struct<currencycode:string,units:bigint,nanos:int>,ownership:struct<currencycode:string,units:bigint,nanos:int>,regular:struct<currencycode:string,units:bigint,nanos:int>,percentoff:double,discount:struct<currencycode:string,units:bigint,nanos:int>,taxdetail:struct<taxbreakdown:array<struct<amount:struct<currencycode:string,units:bigint,nanos:int>,category:string>>,tax:struct<currencycode:string,units:bigint,nanos:int>>,adjustmentdetail:struct<discount:struct<currencycode:string,units:bigint,nanos:int>,adjustmentreason:string,pricematchurl:string>,total:struct<currencycode:string,units:bigint,nanos:int>>,returntype:string,reasoncode:string>>,
  donations array<struct<campaignid:string,amount:struct<currencycode:string,units:bigint,nanos:int>>>,
  sellingchannel string,
  clientinstanceid string,
  activity_date date,
  batch_id int,
  batch_date date,
  rcd_load_tmstp timestamp,
  rcd_update_tmstp timestamp
)
USING DELTA
PARTITIONED BY (activity_date)
LOCATION 's3://{delta_s3_bucket}/session_event_delta/customer_activity_ordersubmitted_session_delta/';
-- noqa: enable=all

CREATE TEMPORARY VIEW customer_activity_ordersubmitted_dedupe AS
WITH session_dates AS (
    select
        min(cast(sessionEndTime as date)) - 2 AS min_date,
        max(cast(sessionEndTime as date)) + 1 AS max_date
    from {delta_schema_name}.session_analytical_object_flatten_delta
)

SELECT
    cast(element_at(headers, 'Id') as string) as eventId,
    timestamp_millis(cast(cast(element_at(headers, 'EventTime') as string) as bigint)) as eventHeaderEventTime,
    timestamp_millis(cast(cast(element_at(headers, 'SystemTime') as string) as bigint)) as eventSystemTime,
    headers as eventHeaders,
    value.invoice as invoice,
    value.source as eventSource,
    value.contextid as contextid,
    value.customer as customer,
    value.credentials as credentials,
    value.byemployee as byemployee,
    value.isinternational as isinternational,
    value.serviceticketid as serviceticketid,
    value.tenderresults as tenderresults,
    value.promotions as promotions,
    value.items as items,
    value.exchangeditems as exchangeditems,
    value.donations as donations,
    value.sellingchannel as sellingchannel,
    value.clientInstanceId as clientInstanceId,
    row_number() over (
        partition by cast(element_at(headers, 'Id') as string)
        order by timestamp_millis(cast(cast(element_at(headers, 'EventTime') as string) as bigint)) desc
    )
    as rownumber
FROM {event_parquet_schema_name}.customer_activity_order_submitted_parquet
WHERE cast(concat_ws('-', year, month, day) as date) between (select cast(min_date as date) from session_dates) and (select cast(max_date as date) from session_dates);

MERGE INTO {delta_schema_name}.customer_activity_ordersubmitted_session_delta AS TGT
USING (
    SELECT DISTINCT sessionId
    FROM {delta_schema_name}.session_analytical_object_flatten_delta
) AS SRC
    ON TGT.sessionId = SRC.sessionId
WHEN MATCHED THEN DELETE;

CREATE TEMPORARY VIEW customer_activity_ordersubmitted_session_delta_temp AS
WITH ordersubmitted_session AS (
    select *
    from {delta_schema_name}.session_analytical_object_flatten_delta
    where session_event_name = 'com.nordstrom.customer.OrderSubmitted'
),

ordersubmitted_event_dedupe AS (
    select *
    from customer_activity_ordersubmitted_dedupe
    where rownumber = 1
)

SELECT
    a.sessionId,
    a.sessionLastUpdatedTime,
    a.sessionAlgorithmId,
    a.sessionStartTime,
    a.sessionEndTime,
    a.sessionExperience,
    a.sessionChannel,
    a.session_event_name as sessionEventName,
    a.session_event_id as sessionEventId,
    b.eventId,
    b.eventHeaderEventTime,
    b.eventSystemTime,
    b.eventHeaders,
    b.invoice,
    b.eventSource,
    b.contextid,
    b.customer,
    b.credentials,
    b.byemployee,
    b.isinternational,
    b.serviceticketid,
    b.tenderresults,
    b.promotions,
    b.items,
    b.exchangeditems,
    b.donations,
    b.sellingchannel,
    b.clientInstanceId,
    a.activity_date,
    a.batch_id,
    a.batch_date,
    current_timestamp() as rcd_load_tmstp,
    current_timestamp() as rcd_update_tmstp
from ordersubmitted_session AS a
left join ordersubmitted_event_dedupe AS b
    on
        a.session_event_id = b.eventId;

INSERT INTO TABLE {delta_schema_name}.customer_activity_ordersubmitted_session_delta PARTITION (activity_date)
SELECT
    sessionId,
    sessionLastUpdatedTime,
    sessionAlgorithmId,
    sessionStartTime,
    sessionEndTime,
    sessionExperience,
    sessionChannel,
    sessionEventName,
    sessionEventId,
    eventId,
    eventHeaderEventTime,
    eventSystemTime,
    eventHeaders,
    invoice,
    eventSource,
    contextid,
    customer,
    credentials,
    byemployee,
    isinternational,
    serviceticketid,
    tenderresults,
    promotions,
    items,
    exchangeditems,
    donations,
    sellingchannel,
    clientinstanceid,
    activity_date,
    batch_id,
    batch_date,
    rcd_load_tmstp,
    rcd_update_tmstp
from customer_activity_ordersubmitted_session_delta_temp;

VACUUM {delta_schema_name}.customer_activity_ordersubmitted_session_delta;

-- noqa: disable=all
OPTIMIZE {delta_schema_name}.customer_activity_ordersubmitted_session_delta ZORDER BY (sessionid);
-- noqa: enable=all
