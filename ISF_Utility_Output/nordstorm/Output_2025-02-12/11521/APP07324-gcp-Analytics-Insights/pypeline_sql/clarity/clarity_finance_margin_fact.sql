SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_clarity_finance_margin_fact_11521_ACE_ENG;
     Task_Name=clarity_finance_margin_fact;'
     FOR SESSION VOLATILE; 

/*
PRD_NAP_JWN_METRICS_USR_VWS.finance_margin_fact
Description - This ddl creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) 
table of margin split by store, zip, platform (device), tender (payment type), merch categories, fulfillment categories, and customer categories
Full documenation: https://confluence.nordstrom.com/x/M4fITg
Contacts: Matthew Bond, Analytics

SLA:
source data (JWN_CLARITY_TRANSACTION_FACT) finishes around 3-5am
SLA is 6am
*/

--daily refresh schedule, needs to pull last 40 days to cover any changes made by audit of last FM AND demand cancels changing over time (if that doesn't bog down the system)

--index and transform this table one time to de-skew the joins later
CREATE MULTISET VOLATILE TABLE buyerflow AS (
SELECT
case when channel = '1) Nordstrom Stores' then 'FULL LINE'
    when channel = '2) Nordstrom.com' then 'N.COM'
    when channel = '3) Rack Stores' then 'RACK'
    when channel = '4) Rack.com' then 'OFFPRICE ONLINE' else null end as business_unit_desc
,right(fiscal_year_shopped,2)+2000 year_num
,acp_id
,buyer_flow as buyerflow_code
,case when AARE_acquired = 1 then upper('Y') else upper('N') end as AARE_acquired_ind
,case when AARE_activated = 1 then upper('Y') else upper('N') end as AARE_activated_ind
,case when AARE_retained = 1 then upper('Y') else upper('N') end as AARE_retained_ind
,case when AARE_engaged = 1 then upper('Y') else upper('N') end as AARE_engaged_ind
FROM T2DL_DAS_STRATEGY.cco_buyer_flow_fy
where 1=1
and channel in ('1) Nordstrom Stores', '2) Nordstrom.com', '3) Rack Stores', '4) Rack.com') --others are nordstrom banner, rack banner, jwn
and year_num in (select distinct year_num from PRD_NAP_BASE_VWS.DAY_CAL where day_date between {start_date} and {end_date})
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
--and execution_qtr_end_dt >= current_Date-70 and execution_qtr_start_dt <= current_Date
)
WITH DATA PRIMARY INDEX(acp_id, execution_qtr) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id, execution_qtr) ON aec;


--aggregate tender table (which has multiple rows per order) for later join
CREATE MULTISET VOLATILE TABLE tender AS (
select
transaction_id as purchase_id
,min(business_day_date) as business_day_date
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
WITH DATA PRIMARY INDEX(session_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(session_id) ON pre_sessions;


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
WITH DATA PRIMARY INDEX(order_id,activity_date_pacific) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(order_id,activity_date_pacific) ON channel;


CREATE MULTISET VOLATILE TABLE jcmmv,
NO FALLBACK, 
NO BEFORE JOURNAL, 
NO AFTER JOURNAL,
CHECKSUM = DEFAULT
(
business_day_date           DATE FORMAT 'YYYY-MM-DD' NOT NULL,
demand_date                 DATE FORMAT 'YYYY-MM-DD',
purchase_id                 VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC,
business_unit_desc          VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA'),
intent_store_num            INTEGER,
bill_zip_code               VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('97250','92130','92677','98101','60614','98004','92660','92618','92694','98033','19720','92886','98012','85255','94010','NOT_APPLICABLE'),
line_item_currency_code     VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD'),
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
acp_id                      VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
order_num                    VARCHAR(40),
jwn_reported_gmv_ind        CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
jwn_merch_net_sales_ind     CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
jwn_reported_net_sales_ind  CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N','Y'),
line_item_activity_type_code CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS 'S',
jwn_reported_gmv_usd_amt    DECIMAL(25,5) COMPRESS,
line_item_quantity          INTEGER COMPRESS (0,1),
jwn_merch_net_sales_usd_amt DECIMAL(25,5) COMPRESS,
jwn_reported_net_sales_usd_amt  DECIMAL(25,5) COMPRESS,
jwn_variable_expense_usd_amt  DECIMAL(25,5) COMPRESS,
jwn_contribution_margin_usd_amt  DECIMAL(25,5) COMPRESS
)
NO PRIMARY INDEX
INDEX(purchase_id) ON COMMIT PRESERVE ROWS;
 
 
 INSERT INTO jcmmv  
SELECT
business_day_date
,demand_date
,transaction_id as purchase_id
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
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_CONTRIBUTION_MARGIN_METRIC_VW jcmmv
where 1=1
and business_day_date between {start_date} and {end_date}
and business_day_date between '2021-01-31' and current_date-1
and zero_value_unit_ind ='N' --remove Gift with Purchase and Beauty (Smart) Sample
and ((business_unit_desc in ('FULL LINE','N.COM','RACK','OFFPRICE ONLINE')) or (business_unit_desc in ('FULL LINE CANADA', 'N.CA','RACK CANADA') and business_day_date <= '2023-02-25'))-- fiscal month 202301 and prior
--special filter for this table to exclude other BUs that are only in the contribution margin table
and record_source <> 'O'
    hash by random;

collect statistics column (purchase_id) on jcmmv;


CREATE MULTISET VOLATILE TABLE margin_tender AS (
SELECT
jcmmv.business_day_date
,jcmmv.demand_date
,dc.year_num
,dc.quarter_num
,jcmmv.business_unit_desc
,jcmmv.intent_store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.acp_id
,jcmmv.order_num
,case when tran_tender_cash_flag >= 1 then 'Y' else 'N' end as tran_tender_cash_flag
,case when tran_tender_check_flag >= 1 then 'Y' else 'N' end as tran_tender_check_flag
,case when tran_tender_nordstrom_card_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_card_flag
,case when tran_tender_non_nordstrom_credit_flag >= 1 then 'Y' else 'N' end as tran_tender_non_nordstrom_credit_flag
,case when tran_tender_non_nordstrom_debit_flag >= 1 then 'Y' else 'N' end as tran_tender_non_nordstrom_debit_flag
,case when tran_tender_nordstrom_gift_card_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_gift_card_flag
,case when tran_tender_nordstrom_note_flag >= 1 then 'Y' else 'N' end as tran_tender_nordstrom_note_flag
,case when tran_tender_paypal_flag >= 1 then 'Y' else 'N' end as tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from jcmmv
inner join PRD_NAP_BASE_VWS.DAY_CAL dc on dc.day_date = jcmmv.business_day_date
left join tender on tender.purchase_id = jcmmv.purchase_id
where 1=1)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE margin_tender_order_null AS (
SELECT
jcmmv.business_day_date
,jcmmv.year_num
,jcmmv.quarter_num
,jcmmv.business_unit_desc
,jcmmv.intent_store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.acp_id
,jcmmv.order_num
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from margin_tender as jcmmv
where order_num is null)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE margin_tender_order_not_null AS (
SELECT
jcmmv.business_day_date
,jcmmv.demand_date
,jcmmv.year_num
,jcmmv.quarter_num
,jcmmv.business_unit_desc
,jcmmv.intent_store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.acp_id
,jcmmv.order_num
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from margin_tender as jcmmv
where order_num is not null)
WITH DATA PRIMARY INDEX(order_num,demand_date) ON COMMIT PRESERVE ROWS;

collect statistics column (order_num,demand_date) on margin_tender_order_not_null;


CREATE MULTISET VOLATILE TABLE margin_channel AS (
SELECT
jcmmv.business_day_date
,jcmmv.year_num
,jcmmv.quarter_num
,jcmmv.business_unit_desc
,jcmmv.intent_store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.acp_id
,mc.mrkt_type as mrtk_chnl_type_code
,mc.finance_rollup as mrtk_chnl_finance_rollup_code
,mc.finance_detail as mrtk_chnl_finance_detail_code
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from margin_tender_order_not_null as jcmmv
left join channel as mc on jcmmv.order_num = mc.order_id and jcmmv.demand_date = mc.activity_date_pacific
  UNION ALL
SELECT
jcmmv.business_day_date
,jcmmv.year_num
,jcmmv.quarter_num
,jcmmv.business_unit_desc
,jcmmv.intent_store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.acp_id
,cast(null as varchar(40)) as mrtk_chnl_type_code
,cast(null as varchar(40)) as mrtk_chnl_finance_rollup_code
,cast(null as varchar(40)) as mrtk_chnl_finance_detail_code
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from margin_tender_order_null as jcmmv
)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE margin_acp_null AS (
SELECT
jcmmv.business_day_date
,jcmmv.year_num
,jcmmv.quarter_num
,jcmmv.business_unit_desc
,jcmmv.intent_store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.acp_id
,jcmmv.mrtk_chnl_type_code
,jcmmv.mrtk_chnl_finance_rollup_code
,jcmmv.mrtk_chnl_finance_detail_code
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from margin_channel as jcmmv
where acp_id is null)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE margin_acp_not_null AS (
SELECT
jcmmv.business_day_date
,jcmmv.year_num
,jcmmv.quarter_num
,jcmmv.business_unit_desc
,jcmmv.intent_store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.acp_id
,jcmmv.mrtk_chnl_type_code
,jcmmv.mrtk_chnl_finance_rollup_code
,jcmmv.mrtk_chnl_finance_detail_code
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from margin_channel as jcmmv
where acp_id is not null)
WITH DATA PRIMARY INDEX(acp_id, year_num, business_unit_desc) ON COMMIT PRESERVE ROWS;

collect statistics column (acp_id, year_num, business_unit_desc) on margin_acp_not_null;


CREATE MULTISET VOLATILE TABLE margin_buyerflow AS (
SELECT
jcmmv.business_day_date
,jcmmv.business_unit_desc
,jcmmv.intent_store_num as store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.quarter_num
,bf.buyerflow_code
,bf.AARE_acquired_ind
,bf.AARE_activated_ind
,bf.AARE_retained_ind
,bf.AARE_engaged_ind
,jcmmv.acp_id
,jcmmv.mrtk_chnl_type_code
,jcmmv.mrtk_chnl_finance_rollup_code
,jcmmv.mrtk_chnl_finance_detail_code
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from margin_acp_not_null as jcmmv
left join buyerflow as bf on jcmmv.business_unit_desc = bf.business_unit_desc and jcmmv.year_num = bf.year_num and jcmmv.acp_id = bf.acp_id
where 1=1)
WITH DATA PRIMARY INDEX(acp_id, quarter_num) ON COMMIT PRESERVE ROWS;

collect statistics column (acp_id, quarter_num) on margin_buyerflow;


--drop table margin_aec;
CREATE MULTISET VOLATILE TABLE margin_aec AS (
SELECT
jcmmv.business_day_date
,jcmmv.business_unit_desc
,jcmmv.store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,jcmmv.loyalty_status
,jcmmv.buyerflow_code
,jcmmv.AARE_acquired_ind
,jcmmv.AARE_activated_ind
,jcmmv.AARE_retained_ind
,jcmmv.AARE_engaged_ind
,jcmmv.mrtk_chnl_type_code
,jcmmv.mrtk_chnl_finance_rollup_code
,jcmmv.mrtk_chnl_finance_detail_code
,aec.engagement_cohort
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,jwn_reported_gmv_ind
,jwn_merch_net_sales_ind
,jwn_reported_net_sales_ind
,line_item_activity_type_code
,jwn_reported_gmv_usd_amt
,line_item_quantity
,jwn_merch_net_sales_usd_amt
,jwn_reported_net_sales_usd_amt
,jwn_variable_expense_usd_amt
,jwn_contribution_margin_usd_amt
from margin_buyerflow as jcmmv
left join aec on jcmmv.acp_id = aec.acp_id and jcmmv.quarter_num = aec.execution_qtr
where 1=1)
WITH DATA  NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE margin,
NO FALLBACK, 
NO BEFORE JOURNAL, 
NO AFTER JOURNAL,
CHECKSUM = DEFAULT
(
business_day_date                     DATE FORMAT 'YYYY-MM-DD' NOT NULL,
business_unit_desc                    VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA'),
store_num                             INTEGER,
bill_zip_code                         VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('97250','92130','92677','98101','60614','98004','92660','92618','92694','98033','19720','92886','98012','85255','94010','NOT_APPLICABLE'),
line_item_currency_code               VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD'),
inventory_business_model              VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Wholesale','Wholesession','Drop ship','Concession','NOT_APPLICABLE','UNKNOWN_VALUE'),
division_name                         VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('APPAREL','ACCESSORIES','BEAUTY','SHOES','HOME','DESIGNER','MERCH PROJECTS','LAST CHANCE','ALTERNATE MODELS','LEASED', 'LEASED BOUTIQUES', 'OTHER' ,'OTHER/NON-MERCH', 'RESTAURANT'),
subdivision_name            VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('WOMENS APPAREL','WOMENS SPECIALIZED','WOMENS SHOES','MENS APPAREL','KIDS APPAREL','MENS SPECIALIZED','MENS SHOES','KIDS SHOES'),
price_type                  VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Regular Price','Clearance','Promotion','NOT_APPLICABLE','UNKNOWN_VALUE'),
record_source               CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
platform_code               VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('WEB','IOS','MOW','ANDROID','POS','CSR_PHONE','THIRD_PARTY_VENDOR','UNKNOWN_VALUE','NOT_APPLICABLE'),
remote_selling_ind              VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Remote Selling','Self-Service'),
fulfilled_from_location_type    VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('DC','FC','Store','Vendor (Drop Ship)','Trunk Club','UNKNOWN_VALUE','NOT_APPLICABLE'),
delivery_method                 VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('BOPUS','Ship to Store','Store Take','Ship to Customer/Other','NOT_APPLICABLE','StorePickup', 'Charge Send'),
delivery_method_subtype         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Charge send', 'Free 2 Day (F2DD)', 'Next Day Delivery', 'Next Day Pickup', 'NOT_APPLICABLE', 'Other', 'Paid Expedited', 'Same Day Pickup', 'Standard Shipping', 'Store Take'),
fulfillment_journey                    VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Charge send', 'Free 2 Day (F2DD)', 'Next Day Delivery', 'Next Day Pickup', 'NOT_APPLICABLE', 'Other', 'Paid Expedited', 'Same Day Pickup', 'Standard Shipping', 'Store Take'),
loyalty_status                        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('MEMBER','INSIDER','AMBASSADOR','INFLUENCER','ICON','UNKNOWN_VALUE','NOT_APPLICABLE'),
buyerflow_code                        VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC  COMPRESS ('1) New-to-JWN', '2) New-to-Channel (not JWN)', '3) Retained-to-Channel', '4) Reactivated-to-Channel'),
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
reported_gmv_usd_amt                  DECIMAL(25,5) COMPRESS 0.00,
reported_gmv_units                    INTEGER COMPRESS (0 ,1 ),
merch_net_sales_usd_amt               DECIMAL(25,5) COMPRESS 0.00,
merch_net_sales_units                 INTEGER COMPRESS (0 ,1 ),
reported_net_sales_usd_amt            DECIMAL(25,5) COMPRESS 0.00,
reported_net_sales_units              INTEGER COMPRESS (0 ,1 ),
variable_cost_usd_amt                 DECIMAL(25,5) COMPRESS 0.00,
gross_contribution_margin_usd_amt     DECIMAL(25,5) COMPRESS 0.00,
net_contribution_margin_usd_amt       DECIMAL(25,5) COMPRESS 0.00,
dw_sys_load_tmstp                     TIMESTAMP(6) NOT NULL 
)
NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;
 
 
 INSERT INTO margin  
SELECT
jcmmv.business_day_date
,jcmmv.business_unit_desc
,jcmmv.store_num
,jcmmv.bill_zip_code
,jcmmv.line_item_currency_code
,jcmmv.inventory_business_model
,jcmmv.division_name
,jcmmv.subdivision_name
,jcmmv.price_type
,jcmmv.record_source
,jcmmv.platform_code
,jcmmv.remote_selling_ind
,jcmmv.fulfilled_from_location_type
,jcmmv.delivery_method
,jcmmv.delivery_method_subtype
,null as fulfillment_journey
,jcmmv.loyalty_status
,jcmmv.buyerflow_code
,jcmmv.AARE_acquired_ind
,jcmmv.AARE_activated_ind
,jcmmv.AARE_retained_ind
,jcmmv.AARE_engaged_ind
,jcmmv.mrtk_chnl_type_code
,jcmmv.mrtk_chnl_finance_rollup_code
,jcmmv.mrtk_chnl_finance_detail_code
,jcmmv.engagement_cohort
,jcmmv.tran_tender_cash_flag
,jcmmv.tran_tender_check_flag
,jcmmv.tran_tender_nordstrom_card_flag
,jcmmv.tran_tender_non_nordstrom_credit_flag
,jcmmv.tran_tender_non_nordstrom_debit_flag
,jcmmv.tran_tender_nordstrom_gift_card_flag
,jcmmv.tran_tender_nordstrom_note_flag
,jcmmv.tran_tender_paypal_flag
,sum(case when jwn_reported_gmv_ind = 'Y' then jcmmv.jwn_reported_gmv_usd_amt else 0 end) as reported_gmv_usd_amt
,sum(case when jwn_reported_gmv_ind = 'Y' then jcmmv.line_item_quantity else 0 end) as reported_gmv_units
,sum(case when jwn_merch_net_sales_ind = 'Y' then jcmmv.jwn_merch_net_sales_usd_amt else 0 end) as merch_net_sales_usd_amt
,sum(case when jwn_merch_net_sales_ind = 'Y' then jcmmv.line_item_quantity else 0 end) as merch_net_sales_units
,sum(case when jwn_reported_net_sales_ind = 'Y' then jcmmv.jwn_reported_net_sales_usd_amt else 0 end) as reported_net_sales_usd_amt
,sum(case when jwn_reported_net_sales_ind = 'Y' then jcmmv.line_item_quantity else 0 end) as reported_net_sales_units
,sum(jwn_variable_expense_usd_amt) as variable_cost_usd_amt
,sum(case when jcmmv.line_item_activity_type_code='S' then jcmmv.jwn_contribution_margin_usd_amt else 0 end) as gross_contribution_margin_usd_amt
,sum(jcmmv.jwn_contribution_margin_usd_amt) as net_contribution_margin_usd_amt
,current_timestamp as dw_sys_load_tmstp
from margin_aec as jcmmv
where 1=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
  UNION ALL
SELECT
man.business_day_date
,man.business_unit_desc
,man.intent_store_num as store_num
,man.bill_zip_code
,man.line_item_currency_code
,man.inventory_business_model
,man.division_name
,man.subdivision_name
,man.price_type
,man.record_source
,man.platform_code
,man.remote_selling_ind
,man.fulfilled_from_location_type
,man.delivery_method
,man.delivery_method_subtype
,null as fulfillment_journey
,man.loyalty_status
,cast(null as varchar(27)) as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,man.mrtk_chnl_type_code
,man.mrtk_chnl_finance_rollup_code
,man.mrtk_chnl_finance_detail_code
,cast(null as varchar(30)) as engagement_cohort
,man.tran_tender_cash_flag
,man.tran_tender_check_flag
,man.tran_tender_nordstrom_card_flag
,man.tran_tender_non_nordstrom_credit_flag
,man.tran_tender_non_nordstrom_debit_flag
,man.tran_tender_nordstrom_gift_card_flag
,man.tran_tender_nordstrom_note_flag
,man.tran_tender_paypal_flag
,sum(case when jwn_reported_gmv_ind = 'Y' then man.jwn_reported_gmv_usd_amt else 0 end) as reported_gmv_usd_amt
,sum(case when jwn_reported_gmv_ind = 'Y' then man.line_item_quantity else 0 end) as reported_gmv_units
,sum(case when jwn_merch_net_sales_ind = 'Y' then man.jwn_merch_net_sales_usd_amt else 0 end) as merch_net_sales_usd_amt
,sum(case when jwn_merch_net_sales_ind = 'Y' then man.line_item_quantity else 0 end) as merch_net_sales_units
,sum(case when jwn_reported_net_sales_ind = 'Y' then man.jwn_reported_net_sales_usd_amt else 0 end) as reported_net_sales_usd_amt
,sum(case when jwn_reported_net_sales_ind = 'Y' then man.line_item_quantity else 0 end) as reported_net_sales_units
,sum(jwn_variable_expense_usd_amt) as variable_cost_usd_amt
,sum(case when man.line_item_activity_type_code='S' then man.jwn_contribution_margin_usd_amt else 0 end) as gross_contribution_margin_usd_amt
,sum(man.jwn_contribution_margin_usd_amt) as net_contribution_margin_usd_amt
,current_timestamp as dw_sys_load_tmstp
from margin_acp_null as man
where 1=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
    hash by random;


DELETE FROM {clarity_schema}.finance_margin_fact WHERE business_day_date BETWEEN {start_date} AND {end_date};


INSERT INTO {clarity_schema}.finance_margin_fact
(
  business_day_date
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
  ,reported_gmv_usd_amt
  ,reported_gmv_units
  ,merch_net_sales_usd_amt
  ,merch_net_sales_units
  ,reported_net_sales_usd_amt
  ,reported_net_sales_units
  ,variable_cost_usd_amt
  ,gross_contribution_margin_usd_amt
  ,net_contribution_margin_usd_amt
  ,dw_sys_load_tmstp
  )
  SELECT
  business_day_date
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
  ,reported_gmv_usd_amt
  ,reported_gmv_units
  ,merch_net_sales_usd_amt
  ,merch_net_sales_units
  ,reported_net_sales_usd_amt
  ,reported_net_sales_units
  ,variable_cost_usd_amt
  ,gross_contribution_margin_usd_amt
  ,net_contribution_margin_usd_amt
  ,dw_sys_load_tmstp
 FROM 
margin;


SET QUERY_BAND = NONE FOR SESSION;