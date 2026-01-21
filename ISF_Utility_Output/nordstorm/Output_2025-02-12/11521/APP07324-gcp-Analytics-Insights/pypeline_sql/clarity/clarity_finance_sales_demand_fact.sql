SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_clarity_finance_sales_demand_fact_11521_ACE_ENG;
     Task_Name=clarity_finance_sales_demand_fact;'
     FOR SESSION VOLATILE; 

/*
PRD_NAP_JWN_METRICS_USR_VWS.finance_sales_demand_fact
Description - Creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) table of daily sales AND demand split by store, zip, platform (device), tender (payment type), merch categories, fulfillment categories, and customer categories
Full documenation: https://confluence.nordstrom.com/x/M4fITg
Contacts: Matthew Bond, Analytics

SLA:
source data (JWN_CLARITY_TRANSACTION_FACT) finishes around 3-4am, this job starts at 3:05am
SLA is 5:30am (so that finance_sales_demand_margin_traffic_fact can finish by 6am)
*/


--daily refresh schedule, needs to pull last 40 days to cover any changes made by audit of last FM AND demand cancels changing over time (if that doesn't bog down the system)


--index and transform this table one time to de-skew the joins later
CREATE MULTISET VOLATILE TABLE buyerflow AS (
SELECT
case when channel = '1) Nordstrom Stores' then 'FULL LINE'
    when channel = '2) Nordstrom.com' then 'N.COM'
    when channel = '3) Rack Stores' then 'RACK'
    when channel = '4) Rack.com' then 'OFFPRICE ONLINE' else null end as business_unit_desc
,right(fiscal_year_shopped,2)+2000 as year_num
,acp_id
,buyer_flow as buyerflow_code
,case when AARE_acquired = 1 then upper('Y') else upper('N') end as AARE_acquired_ind
,case when AARE_activated = 1 then upper('Y') else upper('N') end as AARE_activated_ind
,case when AARE_retained = 1 then upper('Y') else upper('N') end as AARE_retained_ind
,case when AARE_engaged = 1 then upper('Y') else upper('N') end as AARE_engaged_ind
FROM T2DL_DAS_STRATEGY.cco_buyer_flow_fy
where 1=1
and channel in ('1) Nordstrom Stores', '2) Nordstrom.com', '3) Rack Stores', '4) Rack.com') --others are nordstrom banner, rack banner, jwn
and year_num in (select distinct year_num from PRD_NAP_BASE_VWS.DAY_CAL where day_date between {start_date} AND {end_date})
)
WITH DATA PRIMARY INDEX(acp_id, year_num, business_unit_desc) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id, year_num, business_unit_desc) ON buyerflow;


--index this table one time to de-skew the joins later
CREATE MULTISET VOLATILE TABLE aec AS (
SELECT
acp_id
,execution_qtr
,engagement_cohort
FROM T2DL_DAS_AEC.audience_engagement_cohorts
where 1=1
and execution_qtr_end_dt >= {start_date} and execution_qtr_start_dt <= {end_date}
)
WITH DATA PRIMARY INDEX(acp_id, execution_qtr) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id, execution_qtr) ON aec;


--aggregate tender table (which has multiple rows per order) for later join
CREATE MULTISET VOLATILE TABLE tender AS (
select
transaction_id as purchase_id
,sum(case when tender_type = 'Cash' then 1 else 0 end) as tran_tender_cash_flag
,sum(case when tender_type = 'Check' then 1 else 0 end) as tran_tender_check_flag
,sum(case when tender_type = 'Credit Card' and tender_subtype like 'Nordstrom%' then 1 else 0 end) as tran_tender_nordstrom_card_flag
,sum(case when tender_type = 'Credit Card' and tender_subtype not like 'Nordstrom%' then 1 else 0 end) as tran_tender_non_nordstrom_credit_flag
,sum(case when tender_type = 'Debit Card' then 1 else 0 end) as tran_tender_non_nordstrom_debit_flag
,sum(case when tender_type = 'Gift Card' then 1 else 0 end) as tran_tender_nordstrom_gift_card_flag
,sum(case when tender_type = 'Nordstrom Note' then 1 else 0 end) as tran_tender_nordstrom_note_flag
,sum(case when tender_type = 'PayPal' then 1 else 0 end) as tran_tender_paypal_flag
from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_CLARITY_TRAN_TENDER_TYPE_FACT as jctttf
where business_day_date between {start_date} and {end_date}
and business_day_date between '2021-01-31' and current_date-1
group by 1
)
WITH DATA PRIMARY INDEX(purchase_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(purchase_id) ON tender;


--make intermediate table to add marketing channel for orders by acp_id x day x bu x platform
CREATE MULTISET VOLATILE TABLE pre_sessions AS (
select
activity_date_pacific
,mrkt_type
,finance_rollup
,finance_detail
,session_id
FROM T2DL_DAS_SESSIONS.dior_session_fact as dsfd
where activity_date_pacific between {start_date}-30 and {end_date} --need to account for lag between order and sale (for opgmv and margin)
and activity_date_pacific >= '2022-01-31' --first date of sessions data.
and experience in ('ANDROID_APP','DESKTOP_WEB','IOS_APP','MOBILE_WEB') --POS and CSR are not relevant for marketing channel
-- DO NOT exclude inactive sessions because they can place orders and include marketing channel information
and (web_orders >= 1 or activity_date_pacific < '2022-02-09') -- ordering sessions is empty until 2022-02-09
)
WITH DATA PRIMARY INDEX(session_id,activity_date_pacific) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(session_id,activity_date_pacific) ON pre_sessions;


--make intermediate table to add marketing channel for orders by acp_id x day x bu x platform
CREATE MULTISET VOLATILE TABLE sess_order_map AS (
select
activity_date
,order_id
,session_id
from PRD_NAP_BASE_VWS.CUSTOMER_SESSION_ORDER_FACT
where activity_date between {start_date}-30 and {end_date}
and activity_date >= '2022-01-31' --first date of sessions data.
)
WITH DATA PRIMARY INDEX(session_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(session_id) ON sess_order_map;


--make intermediate table to add marketing channel for orders 
CREATE MULTISET VOLATILE TABLE channel AS (
select
activity_date_pacific
,order_id
,mrkt_type
,finance_rollup
,finance_detail
FROM pre_sessions as ps
inner join sess_order_map as som on som.session_id = ps.session_id
)
WITH DATA PRIMARY INDEX(order_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(order_id) ON channel;


CREATE MULTISET VOLATILE TABLE jdmv,
NO FALLBACK, 
NO BEFORE JOURNAL, 
NO AFTER JOURNAL,
CHECKSUM = DEFAULT
(
demand_date               DATE FORMAT 'YYYY-MM-DD' NOT NULL,
purchase_id               VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC,
year_num                  INTEGER,
quarter_num               INTEGER,
business_unit_desc        VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA'),
intent_store_num          INTEGER,
ringing_store_num         INTEGER,
bill_zip_code             VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('97250','92130','92677','98101','60614','98004','92660','92618','92694','98033','19720','92886','98012','85255','94010','NOT_APPLICABLE'),
line_item_currency_code   VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD'),
inventory_business_model    VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Wholesale','Wholesession','Drop ship','Concession','NOT_APPLICABLE','UNKNOWN_VALUE'),
division_name               VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('APPAREL','ACCESSORIES','BEAUTY','SHOES','HOME','DESIGNER','MERCH PROJECTS','LAST CHANCE','ALTERNATE MODELS','LEASED', 'LEASED BOUTIQUES', 'OTHER' ,'OTHER/NON-MERCH', 'RESTAURANT'),
subdivision_name            VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('WOMENS APPAREL','WOMENS SPECIALIZED','WOMENS SHOES','MENS APPAREL','KIDS APPAREL','MENS SPECIALIZED','MENS SHOES','KIDS SHOES'),
price_type                  VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Regular Price','Clearance','Promotion','NOT_APPLICABLE','UNKNOWN_VALUE'),
record_source               CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
platform_code               VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('WEB','IOS','MOW','ANDROID','POS','CSR_PHONE','THIRD_PARTY_VENDOR','UNKNOWN_VALUE','NOT_APPLICABLE'),
remote_selling_ind              VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Remote Selling','Self-Service'),
fulfilled_from_location_type    VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('DC','FC','Store','Vendor (Drop Ship)','Trunk Club','UNKNOWN_VALUE','NOT_APPLICABLE'),
delivery_method                 VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('BOPUS','Ship to Store','Store Take','Ship to Customer/Other','NOT_APPLICABLE','StorePickup', 'Charge Send'),
delivery_method_subtype         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Charge send', 'Free 2 Day (F2DD)', 'Next Day Delivery', 'Next Day Pickup', 'NOT_APPLICABLE', 'Other', 'Paid Expedited', 'Same Day Pickup', 'Standard Shipping', 'Store Take'),
loyalty_status               VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('MEMBER','INSIDER','AMBASSADOR','INFLUENCER','ICON','UNKNOWN_VALUE','NOT_APPLICABLE'),
rms_sku_num                  VARCHAR(16),
demand_tmstp_pacific         TIMESTAMP WITH TIME ZONE,
acp_id                       VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
cancel_reason_code           VARCHAR(100),
fulfillment_status           VARCHAR(20),
order_num                    VARCHAR(40),
jwn_reported_demand_ind      CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
employee_discount_ind        CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
jwn_gross_demand_usd_amt     DECIMAL(20,2) COMPRESS 0.00,
demand_units                 INTEGER COMPRESS (0 ,1 ),
jwn_reported_demand_usd_amt  DECIMAL(20,2) COMPRESS 0.00,
employee_discount_usd_amt    DECIMAL(20,2) COMPRESS 0.00
)
NO PRIMARY INDEX 
INDEX(purchase_id) ON COMMIT PRESERVE ROWS;


 INSERT INTO jdmv  
SELECT
demand_date
,purchase_id
,dc.year_num
,dc.quarter_num
,business_unit_desc
,intent_store_num
,ringing_store_num
,bill_zip_code
,line_item_currency_code
,inventory_business_model
,case when division_name like 'INACT%' then 'OTHER'
    when division_name in ('DIRECT', 'DISCONTINUED DEPTS', 'INVENTORY INTEGRITY TEST', 'NPG NEW BUSINESS', 'MERCH PROJECTS') then 'OTHER'
    when division_name = 'CORPORATE' then 'OTHER/NON-MERCH'
    when division_name is null then 'OTHER/NON-MERCH'
    else division_name end as division_name
--break out the 2 largest divisions (Apparel + Shoes) into more helpful chunks (Women/Men/Kid Apparel, Women/Men Specialized, Women/Men/Kid Shoes)
,case when subdivision_num in (770, 790, 775, 780, 710, 705, 700, 785) then
    case when subdivision_name = 'KIDS WEAR' then 'KIDS APPAREL' else subdivision_name end
    else null end as subdivision_name
,price_type
,record_source
,case when order_platform_type = 'Mobile App' then
            case when platform_subtype in ('UNKNOWN','UNKNOWN: Price Adjustment') then 'Other'
            else platform_subtype end
        when order_platform_type in ('BorderFree','Phone (Customer Care)','UNKNOWN','UNKNOWN: Price Adjustment', 'UNKNOWN_VALUE', 'UNKNOWN_VALUE: Platform=UNKNOW') then 'Other'
        else order_platform_type end as platform_code
,remote_selling_ind
,fulfilled_from_location_type
,delivery_method
,case when delivery_method_subtype = 'UNKNOWN_VALUE' then 'Other' else delivery_method_subtype end as delivery_method_subtype
,loyalty_status
,rms_sku_num
,demand_tmstp_pacific
,acp_id
,cancel_reason_code
,fulfillment_status
,order_num
,jwn_reported_demand_ind
,employee_discount_ind
,jwn_gross_demand_usd_amt
,demand_units
,jwn_reported_demand_usd_amt
,employee_discount_usd_amt
from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_DEMAND_METRIC_VW jdmv
inner join PRD_NAP_BASE_VWS.DAY_CAL dc on dc.day_date = jdmv.demand_date
where 1=1
and demand_date between {start_date} AND {end_date}
and demand_date between '2021-01-31' and current_date-1
and zero_value_unit_ind ='N' --remove Gift with Purchase and Beauty (Smart) Sample
and not (business_unit_country='CA' and business_day_date > '2023-02-25')-- fiscal month 202301 and prior
    hash by random;

collect statistics column (purchase_id) on jdmv;


--make intermediate table for canceled items in demand calculations
CREATE MULTISET VOLATILE TABLE price_type AS (
SELECT
rms_sku_num
, (eff_begin_tmstp + INTERVAL '1' SECOND) as eff_begin_tmstp --add 1 second to prevent duplication of items that were ordered exactly when the price type changed. yes, it can happen
, eff_end_tmstp
,case when ownership_retail_price_type_code = 'CLEARANCE' then 'Clearance'
    when selling_retail_price_type_code = 'CLEARANCE' then 'Clearance'
    when selling_retail_price_type_code = 'PROMOTION' then 'Promotion'
    when selling_retail_price_type_code = 'REGULAR' then 'Regular Price'
    else null end as price_type
,store_num
,case when ppd.channel_brand= 'NORDSTROM_RACK' and ppd.selling_channel = 'STORE' and ppd.channel_country = 'CA' THEN 'RACK CANADA'
    when ppd.channel_brand= 'NORDSTROM_RACK' and ppd.selling_channel = 'STORE' and ppd.channel_country = 'US' THEN 'RACK'
    when ppd.channel_brand= 'NORDSTROM_RACK' and ppd.selling_channel = 'ONLINE' and ppd.channel_country = 'US' THEN 'OFFPRICE ONLINE'
    when ppd.channel_brand= 'NORDSTROM' and ppd.selling_channel = 'STORE' and ppd.channel_country = 'CA' THEN 'FULL LINE CANADA'
    when ppd.channel_brand= 'NORDSTROM' and ppd.selling_channel = 'STORE' and ppd.channel_country = 'US' THEN 'FULL LINE'
    when ppd.channel_brand= 'NORDSTROM' and ppd.selling_channel = 'ONLINE' and ppd.channel_country = 'CA' THEN 'N.CA'
    when ppd.channel_brand= 'NORDSTROM' and ppd.selling_channel = 'ONLINE' and ppd.channel_country = 'US' THEN 'N.COM'
    else null end as business_unit_desc
from PRD_NAP_BASE_VWS.PRODUCT_PRICE_TIMELINE_DIM ppd
where eff_end_tmstp >= cast({start_date} as date) and eff_begin_tmstp <= cast({end_date} as date)
and (ownership_retail_price_type_code = 'CLEARANCE' or selling_retail_price_type_code in ('CLEARANCE','PROMOTION'))
and rms_sku_num in (select distinct rms_sku_num from jdmv where jdmv.price_type = 'NOT_APPLICABLE')
)
WITH DATA PRIMARY INDEX(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON price_type;


CREATE MULTISET VOLATILE TABLE jdmv_tender AS (
SELECT
jdmv.demand_date
,jdmv.year_num
,jdmv.quarter_num
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,cast(case when jdmv.price_type <> 'NOT_APPLICABLE' then jdmv.price_type
    when ptc.price_type is not null then ptc.price_type
    else 'Regular Price' end as varchar(20)) AS price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,case when tran_tender_cash_flag >= 1 then 'Y' else 'N' end as tran_tender_cash_flag
,case when tran_tender_check_flag >= 1 then 'Y' else 'N' end as tran_tender_check_flag
,case when tran_tender_nordstrom_card_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_card_flag
,case when tran_tender_non_nordstrom_credit_flag >= 1 then 'Y' else 'N' end as tran_tender_non_nordstrom_credit_flag
,case when tran_tender_non_nordstrom_debit_flag >= 1 then 'Y' else 'N' end as tran_tender_non_nordstrom_debit_flag
,case when tran_tender_nordstrom_gift_card_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_gift_card_flag
,case when tran_tender_nordstrom_note_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_note_flag
,case when tran_tender_paypal_flag >= 1 then 'Y' else 'N' end as tran_tender_paypal_flag
,jdmv.acp_id
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv
left join tender on tender.purchase_id = jdmv.purchase_id
left join price_type AS ptc
    on jdmv.rms_sku_num = ptc.rms_sku_num and jdmv.business_unit_desc = ptc.business_unit_desc and jdmv.demand_tmstp_pacific between ptc.eff_begin_tmstp and ptc.eff_end_tmstp and jdmv.price_type = 'NOT_APPLICABLE'
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;




CREATE MULTISET VOLATILE TABLE jdmv_tender_order_null AS (
SELECT
jdmv.demand_date
,jdmv.year_num
,jdmv.quarter_num
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,jdmv.acp_id
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv_tender as jdmv 
where order_num is null
)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE jdmv_tender_order_not_null AS (
SELECT
jdmv.demand_date
,jdmv.year_num
,jdmv.quarter_num
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,jdmv.acp_id
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv_tender as jdmv
where order_num is not null
)
WITH DATA PRIMARY INDEX(order_num,demand_date) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(order_num,demand_date) ON jdmv_tender_order_not_null;


CREATE MULTISET VOLATILE TABLE jdmv_channel AS (
SELECT
jdmv.demand_date
,jdmv.year_num
,jdmv.quarter_num
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,mc.mrkt_type as mrtk_chnl_type_code
,mc.finance_rollup as mrtk_chnl_finance_rollup_code
,mc.finance_detail as mrtk_chnl_finance_detail_code
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,jdmv.acp_id
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv_tender_order_not_null as jdmv
left join channel as mc on jdmv.order_num = mc.order_id and jdmv.demand_date = mc.activity_date_pacific
  UNION ALL
SELECT
jdmv.demand_date
,jdmv.year_num
,jdmv.quarter_num
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,cast(null as varchar(40)) mrtk_chnl_type_code
,cast(null as varchar(40)) mrtk_chnl_finance_rollup_code
,cast(null as varchar(40)) mrtk_chnl_finance_detail_code
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,jdmv.acp_id
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv_tender_order_null as jdmv
)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


collect statistics column (acp_id, year_num, business_unit_desc) on jdmv_channel;


CREATE MULTISET VOLATILE TABLE jdmv_acp_null AS (
SELECT
jdmv.demand_date
,jdmv.year_num
,jdmv.quarter_num
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,jdmv.acp_id
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv_channel as jdmv
where acp_id is null)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jdmv_acp_not_null AS (
SELECT
jdmv.demand_date
,jdmv.year_num
,jdmv.quarter_num
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,jdmv.acp_id
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv_channel as jdmv
where acp_id is not null)
WITH DATA PRIMARY INDEX(acp_id, quarter_num) ON COMMIT PRESERVE ROWS;

collect statistics column (acp_id, quarter_num) on jdmv_acp_not_null;


CREATE MULTISET VOLATILE TABLE jdmv_aec AS (
SELECT
jdmv.demand_date
,jdmv.year_num
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,aec.engagement_cohort
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,jdmv.acp_id
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv_acp_not_null as jdmv
left join aec on jdmv.acp_id = aec.acp_id and jdmv.quarter_num = aec.execution_qtr
)
WITH DATA PRIMARY INDEX(acp_id, year_num, business_unit_desc) ON COMMIT PRESERVE ROWS;

collect statistics column (acp_id, year_num, business_unit_desc) on jdmv_aec;


CREATE MULTISET VOLATILE TABLE jdmv_buyerflow AS (
SELECT
jdmv.demand_date
,jdmv.business_unit_desc
,jdmv.intent_store_num
,jdmv.ringing_store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,jdmv.engagement_cohort
,bf.buyerflow_code
,bf.AARE_acquired_ind
,bf.AARE_activated_ind
,bf.AARE_retained_ind
,bf.AARE_engaged_ind
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,jdmv.cancel_reason_code
,jdmv.fulfillment_status
,jdmv.order_num
,jdmv.jwn_reported_demand_ind
,jdmv.employee_discount_ind
,jdmv.jwn_gross_demand_usd_amt
,jdmv.demand_units
,jdmv.jwn_reported_demand_usd_amt
,jdmv.employee_discount_usd_amt
from jdmv_aec as jdmv
left join buyerflow as bf on jdmv.business_unit_desc = bf.business_unit_desc and jdmv.year_num = bf.year_num and jdmv.acp_id = bf.acp_id
)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE demand,
NO FALLBACK, 
NO BEFORE JOURNAL, 
NO AFTER JOURNAL,
CHECKSUM = DEFAULT
(
demand_date               DATE FORMAT 'YYYY-MM-DD' NOT NULL,
business_unit_desc        VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA'),
store_num                 INTEGER,
bill_zip_code             VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('97250','92130','92677','98101','60614','98004','92660','92618','92694','98033','19720','92886','98012','85255','94010','NOT_APPLICABLE'),
line_item_currency_code   VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD'),
inventory_business_model    VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Wholesale','Wholesession','Drop ship','Concession','NOT_APPLICABLE','UNKNOWN_VALUE'),
division_name               VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('APPAREL','ACCESSORIES','BEAUTY','SHOES','HOME','DESIGNER','MERCH PROJECTS','LAST CHANCE','ALTERNATE MODELS','LEASED', 'LEASED BOUTIQUES', 'OTHER' ,'OTHER/NON-MERCH', 'RESTAURANT'),
subdivision_name            VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('WOMENS APPAREL','WOMENS SPECIALIZED','WOMENS SHOES','MENS APPAREL','KIDS APPAREL','MENS SPECIALIZED','MENS SHOES','KIDS SHOES'),
price_type                  VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Regular Price','Clearance','Promotion','NOT_APPLICABLE','UNKNOWN_VALUE'),
record_source               CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
platform_code               VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('WEB','IOS','MOW','ANDROID','POS','CSR_PHONE','THIRD_PARTY_VENDOR','UNKNOWN_VALUE','NOT_APPLICABLE'),
remote_selling_ind              VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Remote Selling','Self-Service'),
fulfilled_from_location_type    VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('DC','FC','Store','Vendor (Drop Ship)','Trunk Club','UNKNOWN_VALUE','NOT_APPLICABLE'),
delivery_method                 VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('BOPUS','Ship to Store','Store Take','Ship to Customer/Other','NOT_APPLICABLE','StorePickup', 'Charge Send'),
delivery_method_subtype         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Charge send', 'Free 2 Day (F2DD)', 'Next Day Delivery', 'Next Day Pickup', 'NOT_APPLICABLE', 'Other', 'Paid Expedited', 'Same Day Pickup', 'Standard Shipping', 'Store Take'),
loyalty_status              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('MEMBER','INSIDER','AMBASSADOR','INFLUENCER','ICON','UNKNOWN_VALUE','NOT_APPLICABLE'),
buyerflow_code              VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('1) New-to-JWN', '2) New-to-Channel (not JWN)', '3) Retained-to-Channel', '4) Reactivated-to-Channel'),
AARE_acquired_ind                     CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
AARE_activated_ind                    CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
AARE_retained_ind                     CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
AARE_engaged_ind                      CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
mrtk_chnl_type_code                   VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('BASE', 'PAID', 'UNATTRIBUTED', 'UNPAID'),
mrtk_chnl_finance_rollup_code         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('AFFILIATES', 'BASE', 'DISPLAY', 'EMAIL', 'NOT_APPLICABLE', 'PAID_OTHER', 'PAID_SEARCH', 'SEO', 'SHOPPING', 'SOCIAL', 'UNATTRIBUTED', 'UNPAID_OTHER'),
mrtk_chnl_finance_detail_code         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('AFFILIATES', 'BASE', 'DISPLAY', 'EMAIL_TRANSACT', 'EMAIL_MARKETING', 'APP_PUSH_PLANNED', 'EMAIL_TRIGGER', 'APP_PUSH_TRANSACTIONAL', 'VIDEO', 'PAID_SEARCH_BRANDED', 'PAID_SEARCH_UNBRANDED', 'SEO_SHOPPING', 'SEO_LOCAL', 'SEO_SEARCH', 'SHOPPING', 'SOCIAL_PAID', 'UNATTRIBUTED', 'SOCIAL_ORGANIC'),
engagement_cohort                     VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Acquire & Activate', 'Acquired Mid-Qtr', 'Highly-Engaged', 'Lightly-Engaged', 'Moderately-Engaged'),
tran_tender_cash_flag                 CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_check_flag                CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_nordstrom_card_flag       CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_non_nordstrom_credit_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_non_nordstrom_debit_flag  CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_nordstrom_gift_card_flag  CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_nordstrom_note_flag       CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_paypal_flag               CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
gross_demand_usd_amt                    DECIMAL(20,2) COMPRESS 0.00,
gross_demand_units                      INTEGER COMPRESS (0 ,1 ),
canceled_gross_demand_usd_amt           DECIMAL(20,2) COMPRESS 0.00,
canceled_gross_demand_units             INTEGER COMPRESS (0 ,1 ),
reported_demand_usd_amt                 DECIMAL(20,2) COMPRESS 0.00,
reported_demand_units                   INTEGER COMPRESS (0 ,1 ),
canceled_reported_demand_usd_amt        DECIMAL(20,2) COMPRESS 0.00,
canceled_reported_demand_units          INTEGER COMPRESS (0 ,1 ),
emp_disc_gross_demand_usd_amt           DECIMAL(20,2) COMPRESS 0.00,
emp_disc_gross_demand_units             INTEGER COMPRESS (0 ,1 ),
emp_disc_reported_demand_usd_amt        DECIMAL(20,2) COMPRESS 0.00,
emp_disc_reported_demand_units          INTEGER COMPRESS (0 ,1 )

)
NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


 INSERT INTO demand 
SELECT
jdmv.demand_date
,jdmv.business_unit_desc
,jdmv.intent_store_num as store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,jdmv.buyerflow_code
,jdmv.AARE_acquired_ind
,jdmv.AARE_activated_ind
,jdmv.AARE_retained_ind
,jdmv.AARE_engaged_ind
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,jdmv.engagement_cohort
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,sum(jdmv.jwn_gross_demand_usd_amt) as gross_demand_usd_amt
,sum(jdmv.demand_units) as gross_demand_units
,sum(case when jdmv.cancel_reason_code is not null and jdmv.cancel_reason_code <> 'UNKNOWN_VALUE' then jdmv.jwn_gross_demand_usd_amt else 0 end) as canceled_gross_demand_usd_amt
,sum(case when jdmv.cancel_reason_code is not null and jdmv.cancel_reason_code <> 'UNKNOWN_VALUE' then jdmv.demand_units else 0 end) as canceled_gross_demand_units
,sum(jdmv.jwn_reported_demand_usd_amt) as reported_demand_usd_amt
,sum(case when jwn_reported_demand_ind = 'Y' then jdmv.demand_units else 0 end) as reported_demand_units
,sum(case when jdmv.cancel_reason_code is not null and jdmv.cancel_reason_code <> 'UNKNOWN_VALUE' then jdmv.jwn_reported_demand_usd_amt else 0 end) as canceled_reported_demand_usd_amt
,sum(case when jdmv.cancel_reason_code is not null and jdmv.cancel_reason_code <> 'UNKNOWN_VALUE' and coalesce(jwn_reported_demand_ind,'N') = 'Y' then jdmv.demand_units else 0 end) as canceled_reported_demand_units
,sum(employee_discount_usd_amt) as emp_disc_gross_demand_usd_amt
,sum(case when employee_discount_ind = 'Y' then jdmv.demand_units else 0 end) as emp_disc_gross_demand_units
,sum(case when jwn_reported_demand_ind = 'Y' then employee_discount_usd_amt else 0 end) as emp_disc_reported_demand_usd_amt
,sum(case when jwn_reported_demand_ind = 'Y' and employee_discount_ind = 'Y' then jdmv.demand_units else 0 end) as emp_disc_reported_demand_units
from jdmv_buyerflow as jdmv
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
    UNION ALL
SELECT
jdmv.demand_date
,jdmv.business_unit_desc
,jdmv.intent_store_num as store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code
,jdmv.inventory_business_model
,jdmv.division_name
,jdmv.subdivision_name
,jdmv.price_type
,jdmv.record_source
,jdmv.platform_code
,jdmv.remote_selling_ind
,jdmv.fulfilled_from_location_type
,jdmv.delivery_method
,jdmv.delivery_method_subtype
,jdmv.loyalty_status
,cast(null as varchar(27)) as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,cast(null as varchar(30)) as engagement_cohort
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,sum(jdmv.jwn_gross_demand_usd_amt) as gross_demand_usd_amt
,sum(jdmv.demand_units) as gross_demand_units
,sum(case when jdmv.cancel_reason_code is not null and jdmv.cancel_reason_code <> 'UNKNOWN_VALUE' then jdmv.jwn_gross_demand_usd_amt else 0 end) as canceled_gross_demand_usd_amt
,sum(case when jdmv.cancel_reason_code is not null and jdmv.cancel_reason_code <> 'UNKNOWN_VALUE' then jdmv.demand_units else 0 end) as canceled_gross_demand_units
,sum(jdmv.jwn_reported_demand_usd_amt) as reported_demand_usd_amt
,sum(case when jwn_reported_demand_ind = 'Y' then jdmv.demand_units else 0 end) as reported_demand_units
,sum(case when jdmv.cancel_reason_code is not null and jdmv.cancel_reason_code <> 'UNKNOWN_VALUE' then jdmv.jwn_reported_demand_usd_amt else 0 end) as canceled_reported_demand_usd_amt
,sum(case when jdmv.cancel_reason_code is not null and jdmv.cancel_reason_code <> 'UNKNOWN_VALUE' and coalesce(jwn_reported_demand_ind,'N') = 'Y' then jdmv.demand_units else 0 end) as canceled_reported_demand_units
,sum(employee_discount_usd_amt) as emp_disc_gross_demand_usd_amt
,sum(case when employee_discount_ind = 'Y' then jdmv.demand_units else 0 end) as emp_disc_gross_demand_units
,sum(case when jwn_reported_demand_ind = 'Y' then employee_discount_usd_amt else 0 end) as emp_disc_reported_demand_usd_amt
,sum(case when jwn_reported_demand_ind = 'Y' and employee_discount_ind = 'Y' then jdmv.demand_units else 0 end) as emp_disc_reported_demand_units
from jdmv_acp_null as jdmv
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
    hash by random;
    

CREATE MULTISET VOLATILE TABLE preorders_buyerflow AS (
SELECT
jdmv.demand_date
,case when store_num = 808 then 'N.COM'
    when store_num = 828 then 'OFFPRICE ONLINE' else null end as business_unit_desc_ringing
,case when jdmv.platform_code = 'Direct to Customer (DTC)' then 808 else jdmv.ringing_store_num end as store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code --check this for orders when canada was open
,jdmv.platform_code
,jdmv.loyalty_status
,bf.buyerflow_code
,bf.AARE_acquired_ind
,bf.AARE_activated_ind
,bf.AARE_retained_ind
,bf.AARE_engaged_ind
,jdmv.engagement_cohort
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,fulfillment_status
,jdmv.acp_id
,cancel_reason_code
,employee_discount_ind
,order_num
from jdmv_aec as jdmv
left join buyerflow as bf on business_unit_desc_ringing = bf.business_unit_desc and jdmv.year_num = bf.year_num and jdmv.acp_id = bf.acp_id
where 1=1
and record_source = 'O' --remove records from other sources which can have incomplete information for certain lines and mess up the aggregation below
and order_num is not null
    UNION ALL
SELECT
jdmv.demand_date
,case when store_num = 808 then 'N.COM'
    when store_num = 828 then 'OFFPRICE ONLINE' else null end as business_unit_desc_ringing
,case when jdmv.platform_code = 'Direct to Customer (DTC)' then 808 else jdmv.ringing_store_num end as store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code --check this for orders when canada was open
,jdmv.platform_code
,jdmv.loyalty_status
,cast(null as varchar(27)) as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,cast(null as varchar(30)) as engagement_cohort
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,fulfillment_status
,acp_id
,cancel_reason_code
,employee_discount_ind
,order_num
from jdmv_acp_null as jdmv
where 1=1
and record_source = 'O' --remove records from other sources which can have incomplete information for certain lines and mess up the aggregation below
and order_num is not null
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE preorders AS (
SELECT
jdmv.demand_date
,business_unit_desc_ringing
,store_num
,jdmv.bill_zip_code
,jdmv.line_item_currency_code --check this for orders when canada was open
,jdmv.platform_code
,jdmv.loyalty_status
,jdmv.buyerflow_code
,jdmv.AARE_acquired_ind
,jdmv.AARE_activated_ind
,jdmv.AARE_retained_ind
,jdmv.AARE_engaged_ind
,jdmv.mrtk_chnl_type_code
,jdmv.mrtk_chnl_finance_rollup_code
,jdmv.mrtk_chnl_finance_detail_code
,jdmv.engagement_cohort
,jdmv.tran_tender_cash_flag
,jdmv.tran_tender_check_flag
,jdmv.tran_tender_nordstrom_card_flag
,jdmv.tran_tender_non_nordstrom_credit_flag
,jdmv.tran_tender_non_nordstrom_debit_flag
,jdmv.tran_tender_nordstrom_gift_card_flag
,jdmv.tran_tender_nordstrom_note_flag
,jdmv.tran_tender_paypal_flag
,cancel_reason_code
,employee_discount_ind
,order_num
,rank () over (partition by order_num, business_unit_desc_ringing order by fulfillment_status desc, jdmv.acp_id desc, loyalty_status) as rank_col --cancel_reason_code,
--this is essential to remove units that have incomplete/inaccurate acp_id info (sometimes from canceled units, other times I'm not sure why)
from preorders_buyerflow as jdmv)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE orders AS (
select
demand_date as tran_date
,business_unit_desc_ringing as business_unit_desc
,store_num
,bill_zip_code
,line_item_currency_code
,platform_code
,loyalty_status
,buyerflow_code
,AARE_acquired_ind
,AARE_activated_ind
,AARE_retained_ind
,AARE_engaged_ind
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,count(distinct order_num) as orders_count
,count(distinct case when cancel_reason_code = 'Fraud Cancel' then order_num else null end) as canceled_fraud_orders_count
,count(distinct case when employee_discount_ind = 'Y' then order_num else null end) as emp_disc_orders_count
where rank_col = 1
from preorders group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE jogmv AS (
SELECT
business_day_date
,demand_date
,purchase_id
,dc.year_num
,dc.quarter_num
,business_unit_desc
,intent_store_num
,bill_zip_code
,line_item_currency_code
,inventory_business_model
,case when division_name like 'INACT%' then 'OTHER'
    when division_name in ('DIRECT', 'DISCONTINUED DEPTS', 'INVENTORY INTEGRITY TEST', 'NPG NEW BUSINESS', 'MERCH PROJECTS') then 'OTHER'
    when division_name = 'CORPORATE' then 'OTHER/NON-MERCH'
    when division_name is null then 'OTHER/NON-MERCH'
    else division_name end as division_name
--break out the 2 largest divisions (Apparel + Shoes) into more helpful chunks (Women/Men/Kid Apparel, Women/Men Specialized, Women/Men/Kid Shoes)
,case when subdivision_num in (770, 790, 775, 780, 710, 705, 700, 785) then
    case when subdivision_name = 'KIDS WEAR' then 'KIDS APPAREL' else subdivision_name end
    else null end as subdivision_name
,price_type
,record_source
,case when order_platform_type = 'Mobile App' then
            case when platform_subtype in ('UNKNOWN','UNKNOWN: Price Adjustment') then 'Other'
            else platform_subtype end
        when order_platform_type in ('BorderFree','Phone (Customer Care)','UNKNOWN','UNKNOWN: Price Adjustment', 'UNKNOWN_VALUE', 'UNKNOWN_VALUE: Platform=UNKNOW') then 'Other'
        else order_platform_type end as platform_code
,remote_selling_ind
,fulfilled_from_location_type
,delivery_method
,case when delivery_method_subtype = 'UNKNOWN_VALUE' then 'Other' else delivery_method_subtype end as delivery_method_subtype
,loyalty_status
,acp_id
,order_num
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_OPERATIONAL_GMV_METRIC_VW jogmv
inner join PRD_NAP_BASE_VWS.DAY_CAL dc on dc.day_date = jogmv.business_day_date
where 1=1
and business_day_date between {start_date} AND {end_date}
and business_day_date between '2021-01-31' and current_date-1
and zero_value_unit_ind ='N' --remove Gift with Purchase and Beauty (Smart) Sample
and not (business_unit_country='CA' and business_day_date > '2023-02-25')-- fiscal month 202301 and prior
)
WITH DATA NO PRIMARY INDEX
INDEX(purchase_id) ON COMMIT PRESERVE ROWS;

collect statistics column (purchase_id) on jogmv;


CREATE MULTISET VOLATILE TABLE gmv_tender AS (
SELECT
jogmv.business_day_date
,jogmv.demand_date
,jogmv.year_num
,jogmv.quarter_num
,jogmv.business_unit_desc
,jogmv.intent_store_num as store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.acp_id
,jogmv.order_num
,case when tran_tender_cash_flag >= 1 then 'Y' else 'N' end as tran_tender_cash_flag
,case when tran_tender_check_flag >= 1 then 'Y' else 'N' end as tran_tender_check_flag
,case when tran_tender_nordstrom_card_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_card_flag
,case when tran_tender_non_nordstrom_credit_flag >= 1 then 'Y' else 'N' end as tran_tender_non_nordstrom_credit_flag
,case when tran_tender_non_nordstrom_debit_flag >= 1 then 'Y' else 'N' end as tran_tender_non_nordstrom_debit_flag
,case when tran_tender_nordstrom_gift_card_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_gift_card_flag
,case when tran_tender_nordstrom_note_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_note_flag
,case when tran_tender_paypal_flag >= 1 then 'Y' else 'N' end as tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from jogmv
left join tender on tender.purchase_id = jogmv.purchase_id
)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE gmv_tender_order_null AS (
SELECT
jogmv.business_day_date
,jogmv.year_num
,jogmv.quarter_num
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.acp_id
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from gmv_tender as jogmv
where order_num is null
)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE gmv_tender_order_not_null AS (
SELECT
jogmv.business_day_date
,jogmv.demand_date
,jogmv.year_num
,jogmv.quarter_num
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.acp_id
,jogmv.order_num
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from gmv_tender as jogmv
where order_num is not null
)
WITH DATA PRIMARY INDEX(order_num,demand_date) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(order_num,demand_date) ON gmv_tender_order_not_null;


CREATE MULTISET VOLATILE TABLE gmv_channel AS (
SELECT
jogmv.business_day_date
,jogmv.year_num
,jogmv.quarter_num
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.acp_id
,mc.mrkt_type as mrtk_chnl_type_code
,mc.finance_rollup as mrtk_chnl_finance_rollup_code
,mc.finance_detail as mrtk_chnl_finance_detail_code
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from gmv_tender_order_not_null as jogmv
left join channel as mc on jogmv.order_num = mc.order_id and jogmv.demand_date = mc.activity_date_pacific
  UNION ALL
SELECT
jogmv.business_day_date
,jogmv.year_num
,jogmv.quarter_num
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.acp_id
,cast(null as varchar(40)) as mrtk_chnl_type_code
,cast(null as varchar(40)) as mrtk_chnl_finance_rollup_code
,cast(null as varchar(40)) as mrtk_chnl_finance_detail_code
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from gmv_tender_order_null as jogmv
)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE gmv_acp_null AS (
SELECT
jogmv.business_day_date
,jogmv.year_num
,jogmv.quarter_num
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.acp_id
,jogmv.mrtk_chnl_type_code
,jogmv.mrtk_chnl_finance_rollup_code
,jogmv.mrtk_chnl_finance_detail_code
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from gmv_channel as jogmv
where acp_id is null
)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE gmv_acp_not_null AS (
SELECT
jogmv.business_day_date
,jogmv.year_num
,jogmv.quarter_num
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.acp_id
,jogmv.mrtk_chnl_type_code
,jogmv.mrtk_chnl_finance_rollup_code
,jogmv.mrtk_chnl_finance_detail_code
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from gmv_channel as jogmv
where acp_id is not null
)
WITH DATA PRIMARY INDEX(acp_id, year_num, business_unit_desc) ON COMMIT PRESERVE ROWS;

collect statistics column (acp_id, year_num, business_unit_desc) on gmv_acp_not_null;


CREATE MULTISET VOLATILE TABLE gmv_buyerflow AS (
SELECT
jogmv.business_day_date
,jogmv.quarter_num
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.acp_id
,bf.buyerflow_code
,bf.AARE_acquired_ind
,bf.AARE_activated_ind
,bf.AARE_retained_ind
,bf.AARE_engaged_ind
,jogmv.mrtk_chnl_type_code
,jogmv.mrtk_chnl_finance_rollup_code
,jogmv.mrtk_chnl_finance_detail_code
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from gmv_acp_not_null as jogmv
left join buyerflow as bf on jogmv.business_unit_desc = bf.business_unit_desc and jogmv.year_num = bf.year_num and jogmv.acp_id = bf.acp_id
)
WITH DATA PRIMARY INDEX(acp_id, quarter_num) ON COMMIT PRESERVE ROWS;

collect statistics column (acp_id, quarter_num) on gmv_buyerflow;


CREATE MULTISET VOLATILE TABLE gmv_aec AS (
SELECT
jogmv.business_day_date
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.buyerflow_code
,jogmv.AARE_acquired_ind
,jogmv.AARE_activated_ind
,jogmv.AARE_retained_ind
,jogmv.AARE_engaged_ind
,aec.engagement_cohort
,jogmv.mrtk_chnl_type_code
,jogmv.mrtk_chnl_finance_rollup_code
,jogmv.mrtk_chnl_finance_detail_code
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,employee_discount_ind
,jwn_fulfilled_demand_ind
,jwn_operational_gmv_ind
,price_adjustment_ind
,product_return_ind
,same_day_store_return_ind
,service_type
,employee_discount_usd_amt
,jwn_fulfilled_demand_usd_amt
,line_item_quantity
,operational_gmv_usd_amt
,operational_gmv_units
from gmv_buyerflow as jogmv
left join aec on jogmv.acp_id = aec.acp_id and jogmv.quarter_num = aec.execution_qtr
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;



create multiset volatile table gmv,
no fallback, 
no before journal, 
no after journal,
checksum =default
(
business_day_date         DATE FORMAT 'YYYY-MM-DD' NOT NULL,
business_unit_desc        VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA'),
store_num                 INTEGER,
bill_zip_code             VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('97250','92130','92677','98101','60614','98004','92660','92618','92694','98033','19720','92886','98012','85255','94010','NOT_APPLICABLE'),
line_item_currency_code   VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD'),
inventory_business_model    VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Wholesale','Wholesession','Drop ship','Concession','NOT_APPLICABLE','UNKNOWN_VALUE'),
division_name               VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('APPAREL','ACCESSORIES','BEAUTY','SHOES','HOME','DESIGNER','MERCH PROJECTS','LAST CHANCE','ALTERNATE MODELS','LEASED', 'LEASED BOUTIQUES', 'OTHER' ,'OTHER/NON-MERCH', 'RESTAURANT'),
subdivision_name            VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('WOMENS APPAREL','WOMENS SPECIALIZED','WOMENS SHOES','MENS APPAREL','KIDS APPAREL','MENS SPECIALIZED','MENS SHOES','KIDS SHOES'),
price_type                  VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Regular Price','Clearance','Promotion','NOT_APPLICABLE','UNKNOWN_VALUE'),
record_source               CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
platform_code               VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('WEB','IOS','MOW','ANDROID','POS','CSR_PHONE','THIRD_PARTY_VENDOR','UNKNOWN_VALUE','NOT_APPLICABLE'),
remote_selling_ind              VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Remote Selling','Self-Service'),
fulfilled_from_location_type    VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('DC','FC','Store','Vendor (Drop Ship)','Trunk Club','UNKNOWN_VALUE','NOT_APPLICABLE'),
delivery_method                 VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('BOPUS','Ship to Store','Store Take','Ship to Customer/Other','NOT_APPLICABLE','StorePickup', 'Charge Send'),
delivery_method_subtype         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Charge send', 'Free 2 Day (F2DD)', 'Next Day Delivery', 'Next Day Pickup', 'NOT_APPLICABLE', 'Other', 'Paid Expedited', 'Same Day Pickup', 'Standard Shipping', 'Store Take'),
loyalty_status              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('MEMBER','INSIDER','AMBASSADOR','INFLUENCER','ICON','UNKNOWN_VALUE','NOT_APPLICABLE'),
buyerflow_code              VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('1) New-to-JWN', '2) New-to-Channel (not JWN)', '3) Retained-to-Channel', '4) Reactivated-to-Channel'),
AARE_acquired_ind                     CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
AARE_activated_ind                    CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
AARE_retained_ind                     CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
AARE_engaged_ind                      CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
engagement_cohort                     VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Acquire & Activate', 'Acquired Mid-Qtr', 'Highly-Engaged', 'Lightly-Engaged', 'Moderately-Engaged'),
mrtk_chnl_type_code                   VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('BASE', 'PAID', 'UNATTRIBUTED', 'UNPAID'),
mrtk_chnl_finance_rollup_code         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('AFFILIATES', 'BASE', 'DISPLAY', 'EMAIL', 'NOT_APPLICABLE', 'PAID_OTHER', 'PAID_SEARCH', 'SEO', 'SHOPPING', 'SOCIAL', 'UNATTRIBUTED', 'UNPAID_OTHER'),
mrtk_chnl_finance_detail_code         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('AFFILIATES', 'BASE', 'DISPLAY', 'EMAIL_TRANSACT', 'EMAIL_MARKETING', 'APP_PUSH_PLANNED', 'EMAIL_TRIGGER', 'APP_PUSH_TRANSACTIONAL', 'VIDEO', 'PAID_SEARCH_BRANDED', 'PAID_SEARCH_UNBRANDED', 'SEO_SHOPPING', 'SEO_LOCAL', 'SEO_SEARCH', 'SHOPPING', 'SOCIAL_PAID', 'UNATTRIBUTED', 'SOCIAL_ORGANIC'),
tran_tender_cash_flag                 CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_check_flag                CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_nordstrom_card_flag       CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_non_nordstrom_credit_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_non_nordstrom_debit_flag  CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_nordstrom_gift_card_flag  CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_nordstrom_note_flag       CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
tran_tender_paypal_flag               CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
emp_disc_fulfilled_demand_usd_amt       DECIMAL(20,2) COMPRESS 0.00,
emp_disc_fulfilled_demand_units         INTEGER  COMPRESS (0 ,1 ),
emp_disc_op_gmv_usd_amt                 DECIMAL(20,2) COMPRESS 0.00,
emp_disc_op_gmv_units                   INTEGER  COMPRESS (0 ,1 ),
fulfilled_demand_usd_amt                DECIMAL(20,2) COMPRESS 0.00,
fulfilled_demand_units                  INTEGER  COMPRESS (0 ,1 ),
post_fulfill_price_adj_usd_amt          DECIMAL(20,2) COMPRESS 0.00,
same_day_store_return_usd_amt           DECIMAL(20,2) COMPRESS 0.00,
same_day_store_return_units             INTEGER  COMPRESS (0 ,1 ),
last_chance_usd_amt                     DECIMAL(20,2) COMPRESS 0.00,
last_chance_units                       INTEGER  COMPRESS (0 ,1 ),
actual_product_returns_usd_amt          DECIMAL(20,2) COMPRESS 0.00,
actual_product_returns_units            INTEGER  COMPRESS (0 ,1 ),
op_gmv_usd_amt                          DECIMAL(20,2) COMPRESS 0.00,
op_gmv_units                            INTEGER  COMPRESS (0 ,1 )
)
NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;
 
 
 INSERT INTO gmv  
SELECT
jogmv.business_day_date
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,jogmv.buyerflow_code
,jogmv.AARE_acquired_ind
,jogmv.AARE_activated_ind
,jogmv.AARE_retained_ind
,jogmv.AARE_engaged_ind
,jogmv.engagement_cohort
,jogmv.mrtk_chnl_type_code
,jogmv.mrtk_chnl_finance_rollup_code
,jogmv.mrtk_chnl_finance_detail_code
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,sum(case when jwn_fulfilled_demand_ind = 'Y' then jogmv.employee_discount_usd_amt else 0 end) as emp_disc_fulfilled_demand_usd_amt
,sum(case when jwn_fulfilled_demand_ind = 'Y' and employee_discount_ind = 'Y' then jogmv.line_item_quantity else 0 end) as emp_disc_fulfilled_demand_units
,sum(case when jwn_operational_gmv_ind = 'Y' then jogmv.employee_discount_usd_amt else 0 end) as emp_disc_op_gmv_usd_amt
,sum(case when jwn_operational_gmv_ind = 'Y' and employee_discount_ind = 'Y' then jogmv.line_item_quantity else 0 end) as emp_disc_op_gmv_units
,sum(case when jwn_fulfilled_demand_ind = 'Y' then jogmv.jwn_fulfilled_demand_usd_amt else 0 end) as fulfilled_demand_usd_amt
,sum(case when jwn_fulfilled_demand_ind = 'Y' then jogmv.line_item_quantity else 0 end) as fulfilled_demand_units
,sum(case when price_adjustment_ind = 'Y' and product_return_ind = 'N' then operational_gmv_usd_amt - jwn_fulfilled_demand_usd_amt else 0 end) as post_fulfill_price_adj_usd_amt
,sum(case when same_day_store_return_ind = 'Y' and service_type <> 'Last Chance' then operational_gmv_usd_amt else 0 end) as same_day_store_return_usd_amt
,sum(case when same_day_store_return_ind = 'Y' and service_type <> 'Last Chance' then line_item_quantity else 0 end) as same_day_store_return_units
,sum(case when service_type = 'Last Chance' then operational_gmv_usd_amt else 0 end) as last_chance_usd_amt
,sum(case when service_type = 'Last Chance' then line_item_quantity else 0 end) as last_chance_units
,sum(case when product_return_ind = 'Y' and service_type <> 'Last Chance' then operational_gmv_usd_amt else 0 end) as actual_product_returns_usd_amt
,sum(case when product_return_ind = 'Y' and service_type <> 'Last Chance' then line_item_quantity else 0 end) as actual_product_returns_units
,sum(case when jogmv.jwn_operational_gmv_ind = 'Y' then jogmv.operational_gmv_usd_amt else 0 end) as op_gmv_usd_amt
,sum(case when jogmv.jwn_operational_gmv_ind = 'Y' then jogmv.operational_gmv_units else 0 end) as op_gmv_units
from gmv_aec as jogmv
where 1=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
  UNION ALL
SELECT
jogmv.business_day_date
,jogmv.business_unit_desc
,jogmv.store_num
,jogmv.bill_zip_code
,jogmv.line_item_currency_code
,jogmv.inventory_business_model
,jogmv.division_name
,jogmv.subdivision_name
,jogmv.price_type
,jogmv.record_source
,jogmv.platform_code
,jogmv.remote_selling_ind
,jogmv.fulfilled_from_location_type
,jogmv.delivery_method
,jogmv.delivery_method_subtype
,jogmv.loyalty_status
,cast(null as varchar(27)) as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,cast(null as varchar(230)) as engagement_cohort
,jogmv.mrtk_chnl_type_code
,jogmv.mrtk_chnl_finance_rollup_code
,jogmv.mrtk_chnl_finance_detail_code
,jogmv.tran_tender_cash_flag
,jogmv.tran_tender_check_flag
,jogmv.tran_tender_nordstrom_card_flag
,jogmv.tran_tender_non_nordstrom_credit_flag
,jogmv.tran_tender_non_nordstrom_debit_flag
,jogmv.tran_tender_nordstrom_gift_card_flag
,jogmv.tran_tender_nordstrom_note_flag
,jogmv.tran_tender_paypal_flag
,sum(case when jwn_fulfilled_demand_ind = 'Y' then jogmv.employee_discount_usd_amt else 0 end) as emp_disc_fulfilled_demand_usd_amt
,sum(case when jwn_fulfilled_demand_ind = 'Y' and employee_discount_ind = 'Y' then jogmv.line_item_quantity else 0 end) as emp_disc_fulfilled_demand_units
,sum(case when jwn_operational_gmv_ind = 'Y' then jogmv.employee_discount_usd_amt else 0 end) as emp_disc_op_gmv_usd_amt
,sum(case when jwn_operational_gmv_ind = 'Y' and employee_discount_ind = 'Y' then jogmv.line_item_quantity else 0 end) as emp_disc_op_gmv_units
,sum(case when jwn_fulfilled_demand_ind = 'Y' then jogmv.jwn_fulfilled_demand_usd_amt else 0 end) as fulfilled_demand_usd_amt
,sum(case when jwn_fulfilled_demand_ind = 'Y' then jogmv.line_item_quantity else 0 end) as fulfilled_demand_units
,sum(case when price_adjustment_ind = 'Y' and product_return_ind = 'N' then operational_gmv_usd_amt - jwn_fulfilled_demand_usd_amt else 0 end) as post_fulfill_price_adj_usd_amt
,sum(case when same_day_store_return_ind = 'Y' and service_type <> 'Last Chance' then operational_gmv_usd_amt else 0 end) as same_day_store_return_usd_amt
,sum(case when same_day_store_return_ind = 'Y' and service_type <> 'Last Chance' then line_item_quantity else 0 end) as same_day_store_return_units
,sum(case when service_type = 'Last Chance' then operational_gmv_usd_amt else 0 end) as last_chance_usd_amt
,sum(case when service_type = 'Last Chance' then line_item_quantity else 0 end) as last_chance_units
,sum(case when product_return_ind = 'Y' and service_type <> 'Last Chance' then operational_gmv_usd_amt else 0 end) as actual_product_returns_usd_amt
,sum(case when product_return_ind = 'Y' and service_type <> 'Last Chance' then line_item_quantity else 0 end) as actual_product_returns_units
,sum(case when jogmv.jwn_operational_gmv_ind = 'Y' then jogmv.operational_gmv_usd_amt else 0 end) as op_gmv_usd_amt
,sum(case when jogmv.jwn_operational_gmv_ind = 'Y' then jogmv.operational_gmv_units else 0 end) as op_gmv_units
from gmv_acp_null as jogmv
where 1=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
    hash by random;
    


CREATE MULTISET VOLATILE TABLE combo_sum AS ( 
with combo as (
select
demand_date as tran_date
,business_unit_desc
,store_num
,bill_zip_code
,line_item_currency_code
,inventory_business_model
,division_name
,subdivision_name
,price_type
,record_source
,platform_code
,remote_selling_ind
,fulfilled_from_location_type
,delivery_method
,delivery_method_subtype
,loyalty_status
,buyerflow_code
,AARE_acquired_ind
,AARE_activated_ind
,AARE_retained_ind
,AARE_engaged_ind
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,gross_demand_usd_amt
,gross_demand_units
,canceled_gross_demand_usd_amt
,canceled_gross_demand_units
,reported_demand_usd_amt
,reported_demand_units
,canceled_reported_demand_usd_amt
,canceled_reported_demand_units
,emp_disc_gross_demand_usd_amt
,emp_disc_gross_demand_units
,emp_disc_reported_demand_usd_amt
,emp_disc_reported_demand_units
,cast(null as decimal (32,8)) as emp_disc_fulfilled_demand_usd_amt
,cast(null as integer) as emp_disc_fulfilled_demand_units
,cast(null as decimal (32,8)) as emp_disc_op_gmv_usd_amt
,cast(null as integer) as emp_disc_op_gmv_units
,cast(null as decimal (32,8)) as fulfilled_demand_usd_amt
,cast(null as integer) as fulfilled_demand_units
,cast(null as decimal (32,8)) as post_fulfill_price_adj_usd_amt
,cast(null as decimal (32,8)) as same_day_store_return_usd_amt
,cast(null as integer) as same_day_store_return_units
,cast(null as decimal (32,8)) as last_chance_usd_amt
,cast(null as integer) as last_chance_units
,cast(null as decimal (32,8)) as actual_product_returns_usd_amt
,cast(null as integer) as actual_product_returns_units
,cast(null as decimal (32,8)) as op_gmv_usd_amt
,cast(null as integer) as op_gmv_units
,cast(null as integer) as orders_count
,cast(null as integer) as canceled_fraud_orders_count
,cast(null as integer) as emp_disc_orders_count
from demand
    UNION ALL
select
business_day_date as tran_date
,business_unit_desc
,store_num
,bill_zip_code
,line_item_currency_code
,inventory_business_model
,division_name
,subdivision_name
,price_type
,record_source
,platform_code
,remote_selling_ind
,fulfilled_from_location_type
,delivery_method
,delivery_method_subtype
,loyalty_status
,buyerflow_code
,AARE_acquired_ind
,AARE_activated_ind
,AARE_retained_ind
,AARE_engaged_ind
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,null as gross_demand_usd_amt
,null as gross_demand_units
,null as canceled_gross_demand_usd_amt
,null as canceled_gross_demand_units
,null as reported_demand_usd_amt
,null as reported_demand_units
,null as canceled_reported_demand_usd_amt
,null as canceled_reported_demand_units
,null as emp_disc_gross_demand_usd_amt
,null as emp_disc_gross_demand_units
,null as emp_disc_reported_demand_usd_amt
,null as emp_disc_reported_demand_units
,emp_disc_fulfilled_demand_usd_amt
,emp_disc_fulfilled_demand_units
,emp_disc_op_gmv_usd_amt
,emp_disc_op_gmv_units
,fulfilled_demand_usd_amt
,fulfilled_demand_units
,post_fulfill_price_adj_usd_amt
,same_day_store_return_usd_amt
,same_day_store_return_units
,last_chance_usd_amt
,last_chance_units
,actual_product_returns_usd_amt
,actual_product_returns_units
,op_gmv_usd_amt
,op_gmv_units
,null as orders_count
,null as canceled_fraud_orders_count
,null as emp_disc_orders_count
from gmv
    UNION ALL
select
tran_date
,business_unit_desc
,store_num
,bill_zip_code
,line_item_currency_code
,cast(null as varchar(4)) as inventory_business_model
,cast(null as varchar(4)) as division_name
,cast(null as varchar(4)) as subdivision_name
,cast(null as varchar(4)) as price_type
,'O' as record_source
,platform_code
,cast(null as varchar(4)) as remote_selling_ind
,cast(null as varchar(4)) as fulfilled_from_location_type
,cast(null as varchar(4)) as delivery_method
,cast(null as varchar(4)) as delivery_method_subtype
,loyalty_status
,buyerflow_code
,AARE_acquired_ind
,AARE_activated_ind
,AARE_retained_ind
,AARE_engaged_ind
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,null as gross_demand_usd_amt
,null as gross_demand_units
,null as canceled_gross_demand_usd_amt
,null as canceled_gross_demand_units
,null as reported_demand_usd_amt
,null as reported_demand_units
,null as canceled_reported_demand_usd_amt
,null as canceled_reported_demand_units
,null as emp_disc_gross_demand_usd_amt
,null as emp_disc_gross_demand_units
,null as emp_disc_reported_demand_usd_amt
,null as emp_disc_reported_demand_units
,null as emp_disc_fulfilled_demand_usd_amt
,null as emp_disc_fulfilled_demand_units
,null as emp_disc_op_gmv_usd_amt
,null as emp_disc_op_gmv_units
,null as fulfilled_demand_usd_amt
,null as fulfilled_demand_units
,null as post_fulfill_price_adj_usd_amt
,null as same_day_store_return_usd_amt
,null as same_day_store_return_units
,null as last_chance_usd_amt
,null as last_chance_units
,null as actual_product_returns_usd_amt
,null as actual_product_returns_units
,null as op_gmv_usd_amt
,null as op_gmv_units
,orders_count
,canceled_fraud_orders_count
,emp_disc_orders_count
from orders
)
select
tran_date
,business_unit_desc
,store_num
,case when bill_zip_code = 'UNKNOWN_VALUE' then 'other'
    when substring(bill_zip_code, 6, 1) = '-' then left(bill_zip_code,5) --zip 9, change to zip 5
    when bill_zip_code like '%-%' then 'other' --remove international postal codes or errors
    when REGEXP_SIMILAR(bill_zip_code,'^[0-9]+$','c')=1 and left(bill_zip_code,5) between 00501 and 99950 then left(bill_zip_code,5) --format US zip codes
       else 'other' end as bill_zip_code --remove international postal codes or errors
,line_item_currency_code
,inventory_business_model
,division_name
,subdivision_name
,price_type
,record_source
,platform_code
,remote_selling_ind
,fulfilled_from_location_type-- as line_item_fulfillment_type
,delivery_method
,delivery_method_subtype
,null as fulfillment_journey
,loyalty_status
,buyerflow_code
,AARE_acquired_ind
,AARE_activated_ind
,AARE_retained_ind
,AARE_engaged_ind
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,sum(gross_demand_usd_amt) as gross_demand_usd_amt
,sum(gross_demand_units) as gross_demand_units
,sum(canceled_gross_demand_usd_amt) as canceled_gross_demand_usd_amt
,sum(canceled_gross_demand_units) as canceled_gross_demand_units
,sum(reported_demand_usd_amt) as reported_demand_usd_amt
,sum(reported_demand_units) as reported_demand_units
,sum(canceled_reported_demand_usd_amt) as canceled_reported_demand_usd_amt
,sum(canceled_reported_demand_units) as canceled_reported_demand_units
,sum(emp_disc_gross_demand_usd_amt) as emp_disc_gross_demand_usd_amt
,sum(emp_disc_gross_demand_units) as emp_disc_gross_demand_units
,sum(emp_disc_reported_demand_usd_amt) as emp_disc_reported_demand_usd_amt
,sum(emp_disc_reported_demand_units) as emp_disc_reported_demand_units
,sum(emp_disc_fulfilled_demand_usd_amt) as emp_disc_fulfilled_demand_usd_amt
,sum(emp_disc_fulfilled_demand_units) as emp_disc_fulfilled_demand_units
,sum(emp_disc_op_gmv_usd_amt) as emp_disc_op_gmv_usd_amt
,sum(emp_disc_op_gmv_units) as emp_disc_op_gmv_units
,sum(fulfilled_demand_usd_amt) as fulfilled_demand_usd_amt
,sum(fulfilled_demand_units) as fulfilled_demand_units
,sum(post_fulfill_price_adj_usd_amt) as post_fulfill_price_adj_usd_amt
,sum(same_day_store_return_usd_amt) as same_day_store_return_usd_amt
,sum(same_day_store_return_units) as same_day_store_return_units
,sum(last_chance_usd_amt) as last_chance_usd_amt
,sum(last_chance_units) as last_chance_units
,sum(actual_product_returns_usd_amt) as actual_product_returns_usd_amt
,sum(actual_product_returns_units) as actual_product_returns_units
,sum(op_gmv_usd_amt) as op_gmv_usd_amt
,sum(op_gmv_units) as op_gmv_units
,sum(orders_count) as orders_count
,sum(canceled_fraud_orders_count) as canceled_fraud_orders_count
,sum(emp_disc_orders_count) as emp_disc_orders_count
,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from combo
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) 
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;

DELETE FROM {clarity_schema}.finance_sales_demand_fact WHERE tran_date BETWEEN {start_date} AND {end_date};

INSERT INTO {clarity_schema}.finance_sales_demand_fact
(
  tran_date  
  ,business_unit_desc
  ,store_num
  ,bill_zip_code
  ,line_item_currency_code
  ,inventory_business_model
  ,division_name
  ,subdivision_name
  ,price_type
  ,record_source
  ,platform_code
  ,remote_selling_ind
  ,fulfilled_from_location_type
  ,delivery_method
  ,delivery_method_subtype
  ,fulfillment_journey
  ,loyalty_status
  ,buyerflow_code
  ,AARE_acquired_ind
  ,AARE_activated_ind
  ,AARE_retained_ind
  ,AARE_engaged_ind
  ,engagement_cohort
  ,mrtk_chnl_type_code
  ,mrtk_chnl_finance_rollup_code
  ,mrtk_chnl_finance_detail_code
  ,tran_tender_cash_flag
  ,tran_tender_check_flag
  ,tran_tender_nordstrom_card_flag
  ,tran_tender_non_nordstrom_credit_flag
  ,tran_tender_non_nordstrom_debit_flag
  ,tran_tender_nordstrom_gift_card_flag
  ,tran_tender_nordstrom_note_flag
  ,tran_tender_paypal_flag
  ,gross_demand_usd_amt
  ,gross_demand_units
  ,canceled_gross_demand_usd_amt
  ,canceled_gross_demand_units
  ,reported_demand_usd_amt
  ,reported_demand_units
  ,canceled_reported_demand_usd_amt
  ,canceled_reported_demand_units
  ,emp_disc_gross_demand_usd_amt
  ,emp_disc_gross_demand_units
  ,emp_disc_reported_demand_usd_amt
  ,emp_disc_reported_demand_units
  ,emp_disc_fulfilled_demand_usd_amt
  ,emp_disc_fulfilled_demand_units
  ,emp_disc_op_gmv_usd_amt
  ,emp_disc_op_gmv_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,post_fulfill_price_adj_usd_amt
  ,same_day_store_return_usd_amt
  ,same_day_store_return_units
  ,last_chance_usd_amt
  ,last_chance_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,orders_count
  ,canceled_fraud_orders_count
  ,emp_disc_orders_count
  ,dw_sys_load_tmstp
  )
  SELECT
  tran_date  
  ,business_unit_desc
  ,store_num
  ,bill_zip_code
  ,line_item_currency_code
  ,inventory_business_model
  ,division_name
  ,subdivision_name
  ,price_type
  ,record_source
  ,platform_code
  ,remote_selling_ind 
  ,fulfilled_from_location_type
  ,delivery_method
  ,delivery_method_subtype
  ,fulfillment_journey
  ,loyalty_status
  ,buyerflow_code
  ,AARE_acquired_ind
  ,AARE_activated_ind
  ,AARE_retained_ind
  ,AARE_engaged_ind
  ,engagement_cohort
  ,mrtk_chnl_type_code
  ,mrtk_chnl_finance_rollup_code
  ,mrtk_chnl_finance_detail_code
  ,tran_tender_cash_flag
  ,tran_tender_check_flag
  ,tran_tender_nordstrom_card_flag
  ,tran_tender_non_nordstrom_credit_flag
  ,tran_tender_non_nordstrom_debit_flag
  ,tran_tender_nordstrom_gift_card_flag
  ,tran_tender_nordstrom_note_flag
  ,tran_tender_paypal_flag
  ,gross_demand_usd_amt
  ,gross_demand_units
  ,canceled_gross_demand_usd_amt
  ,canceled_gross_demand_units
  ,reported_demand_usd_amt
  ,reported_demand_units
  ,canceled_reported_demand_usd_amt
  ,canceled_reported_demand_units
  ,emp_disc_gross_demand_usd_amt
  ,emp_disc_gross_demand_units
  ,emp_disc_reported_demand_usd_amt
  ,emp_disc_reported_demand_units
  ,emp_disc_fulfilled_demand_usd_amt
  ,emp_disc_fulfilled_demand_units
  ,emp_disc_op_gmv_usd_amt
  ,emp_disc_op_gmv_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,post_fulfill_price_adj_usd_amt
  ,same_day_store_return_usd_amt
  ,same_day_store_return_units
  ,last_chance_usd_amt
  ,last_chance_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,orders_count
  ,canceled_fraud_orders_count
  ,emp_disc_orders_count
  ,dw_sys_load_tmstp
 FROM 
COMBO_sum;


SET QUERY_BAND = NONE FOR SESSION;