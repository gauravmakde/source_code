SET QUERY_BAND = 'App_ID=APP08629;
     DAG_ID=mta_publisher_daily_11521_ACE_ENG;
     Task_Name=mta_publisher_daily;'
     FOR SESSION VOLATILE;


--T2/Table Name: {apd_t2_schema}.mta_publisher_daily
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2023-09-27
--Note:
-- This table supports the 4 BOX Affiliates Performance Dashboard-Consolidated Version.

CREATE Multiset VOLATILE TABLE start_varibale
AS
(SELECT MIN(day_date) AS start_date
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE week_idnt = (SELECT DISTINCT week_idnt
                   FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
                   WHERE day_date = {start_date}))
WITH data PRIMARY INDEX (start_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE end_varibale
AS
(SELECT max(day_date) AS end_date
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE week_idnt = (SELECT DISTINCT week_idnt
                   FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
                   WHERE day_date = {end_date}))
WITH data PRIMARY INDEX (end_date) ON COMMIT PRESERVE ROWS;

CREATE Multiset VOLATILE TABLE _variables
AS
( select start_date , end_date from start_varibale, end_varibale)
WITH data PRIMARY INDEX (start_date) ON COMMIT PRESERVE ROWS;

CREATE volatile multiset TABLE date_lookup
AS
(SELECT DISTINCT day_date AS DAY_DT,
	   last_year_day_date as ly_day_dt,
       MIN(CASE WHEN last_year_day_date_realigned IS NULL THEN (day_date -364) ELSE last_year_day_date_realigned END) -1 AS LY_start_dt,
       MIN(day_date) -1 AS LY_end_dt,
       MAX(day_date) AS TY_end_dt
FROM prd_nap_usr_vws.day_cal,
     _variables
WHERE day_date BETWEEN start_date AND end_date
GROUP BY 1,2
	  ) WITH data PRIMARY INDEX (DAY_DT) ON COMMIT preserve rows;


CREATE volatile multiset TABLE Acquired_cust
AS
(SELECT DISTINCT a.acp_id,
       a.aare_status_date AS day_date,
       a.aare_chnl_code AS box,
       'NTN' AS aare_status_code
FROM PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT a,
     _variables
WHERE a.aare_status_date BETWEEN start_date AND end_date)
WITH data PRIMARY INDEX (acp_id,day_date,box) ON COMMIT preserve rows;

CREATE volatile multiset TABLE Activated_cust
AS
(SELECT DISTINCT a.acp_id,
       a.activated_date AS day_date,
       a.activated_channel AS box,
       'A' AS aare_status_code
FROM dl_cma_cmbr.customer_activation a,
     _variables
WHERE a.activated_date BETWEEN start_date AND end_date)
WITH data PRIMARY INDEX (acp_id,day_date,box) ON COMMIT preserve rows;

CREATE multiset volatile TABLE ly_stg_1
AS
(SELECT DISTINCT acp_id,
       dl.ly_start_dt AS ly_dl_start_dt,
       dl.ly_end_dt AS ly_dl_end_dt,
       dl.Ty_end_dt AS Ty_dl_end_dt,
       COALESCE(dtl.order_date,dtl.tran_date) AS sale_date,
       CASE
         WHEN str.business_unit_desc IN ('FULL LINE','FULL LINE CANADA') THEN 'FLS'
         WHEN str.business_unit_desc IN ('N.CA','N.COM') THEN 'NCOM'
         WHEN str.business_unit_desc IN ('RACK','RACK CANADA') THEN 'RACK'
         WHEN str.business_unit_desc IN ('OFFPRICE ONLINE') THEN 'NRHL'
         ELSE NULL
       END AS box
FROM prd_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
  INNER JOIN prd_nap_usr_vws.store_dim str ON dtl.intent_store_num = str.store_num
  LEFT JOIN date_lookup dl ON COALESCE (dtl.order_date,dtl.tran_date) = dl.day_dt
WHERE COALESCE(dtl.order_date,dtl.tran_date) BETWEEN (SELECT MIN(LY_start_dt) FROM date_lookup) AND (SELECT MAX(TY_end_dt) FROM date_lookup)
AND   dtl.line_item_net_amt_currency_code IN ('USD')
AND   dtl.line_net_usd_amt > 0
AND   NOT dtl.acp_id IS NULL
AND   str.business_unit_desc IN ('FULL LINE','N.COM','OFFPRICE ONLINE','RACK')
AND   1 = 1)
WITH data PRIMARY INDEX (acp_id,sale_date,box) ON COMMIT preserve rows;


CREATE multiset volatile TABLE ly_test_mapping
AS
(SELECT acp_id,
       MIN(ly_dl_start_dt) AS ly_dl_start_dt
FROM ly_stg_1,
     _variables
WHERE sale_date BETWEEN start_date AND end_date
GROUP BY 1) WITH data PRIMARY INDEX (acp_id,ly_dl_start_dt) ON COMMIT preserve rows;


CREATE multiset volatile TABLE ly_stg_2
AS
(SELECT DISTINCT dtl.acp_id,
       dtl.ly_dl_start_dt,
       dtl.ly_dl_end_dt,
       dtl.Ty_dl_end_dt,
       dtl.sale_date,
       dtl.box
FROM ly_stg_1 dtl
  INNER JOIN ly_test_mapping dl
          ON dl.acp_id = dtl.acp_id
         AND dtl.sale_date >= dl.ly_dl_start_dt
WHERE 1 = 1)
WITH data PRIMARY INDEX (acp_id,sale_date,box) ON COMMIT preserve rows;


CREATE multiset volatile TABLE ly_stg_3
AS
(SELECT b.acp_id,
       b.sale_date,
       b.box,
       CASE
         WHEN b.box_count2 > 0 OR b.box_count1 > 0 THEN 1
         ELSE 0
       END AS Ret_fl,
	   case when (b.box_count1-b.box_count2)<>0 then 1 else 0 end as Engaged_fl
FROM (SELECT a.acp_id,
             a.sale_date,
             a.box,
             COUNT(box) OVER (PARTITION BY acp_id,box ORDER BY sale_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) -1 AS box_count1,
             COUNT(box) OVER (PARTITION BY acp_id ORDER BY sale_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) -1 AS box_count2
      FROM ly_stg_2 a
      GROUP BY 1,
               2,
               3) b
WHERE b.sale_date between (SELECT start_date FROM _variables) and (SELECT end_date FROM _variables))
WITH data PRIMARY INDEX (acp_id,sale_date,box) ON COMMIT preserve rows;

create multiset volatile table ly_stg_4
as
(select distinct a.acp_id,a.sale_date,a.box,case when a.acp_id=b.acp_id and a.sale_date=b.day_date and a.box=b.box then 0 else 1 end as Reactivated_fl
from
ly_stg_3 a
left join (select distinct acp_id,day_date,box from acquired_cust) b
on a.acp_id=b.acp_id and a.sale_date=b.day_date and a.box=b.box
where a.ret_fl=0 and a.engaged_fl=0 and 1=1)
WITH data PRIMARY INDEX (acp_id,sale_date,box) ON COMMIT preserve rows;
		 

CREATE multiset volatile TABLE cust_aare
AS
(SELECT acp_id,
       day_date AS DAY_DT,
       CASE
         WHEN box IN ('FLS','NCOM') THEN 'NORDSTROM'
         WHEN box IN ('RACK','NRHL') THEN 'NORDSTROM RACK'
         ELSE NULL
       END AS BANNER,
       aare_status_code AS AARE_STATUS_CODE
FROM acquired_cust
UNION
SELECT acp_id,
       day_date AS DAY_DT,
       CASE
         WHEN box IN ('FLS','NCOM') THEN 'NORDSTROM'
         WHEN box IN ('RACK','NRHL') THEN 'NORDSTROM RACK'
         ELSE NULL
       END AS BANNER,
       aare_status_code AS AARE_STATUS_CODE
FROM activated_cust
UNION
SELECT acp_id,
       sale_date AS DAY_DT,
       CASE
         WHEN box IN ('FLS','NCOM') THEN 'NORDSTROM'
         WHEN box IN ('RACK','NRHL') THEN 'NORDSTROM RACK'
         ELSE NULL
       END AS BANNER,
       CASE
         WHEN acp_id IS NOT NULL AND ret_fl = 1 THEN 'R'
         ELSE NULL
       END AS AARE_STATUS_CODE
FROM ly_stg_3
WHERE Ret_fl = 1
UNION
SELECT acp_id,
       sale_date AS DAY_DT,
       CASE
         WHEN box IN ('FLS','NCOM') THEN 'NORDSTROM'
         WHEN box IN ('RACK','NRHL') THEN 'NORDSTROM RACK'
         ELSE NULL
       END AS BANNER,
       CASE
         WHEN acp_id IS NOT NULL AND ret_fl = 1 AND engaged_fl = 1 THEN 'E'
         ELSE NULL
       END AS AARE_STATUS_CODE
FROM ly_stg_3
WHERE Ret_fl = 1
AND   engaged_fl = 1
UNION
SELECT acp_id,
       sale_date AS DAY_DT,
       CASE
         WHEN box IN ('FLS','NCOM') THEN 'NORDSTROM'
         WHEN box IN ('RACK','NRHL') THEN 'NORDSTROM RACK'
         ELSE NULL
       END AS BANNER,
       CASE
         WHEN acp_id IS NOT NULL AND Reactivated_fl = 1 THEN 'RA'
         ELSE NULL
       END AS AARE_STATUS_CODE
FROM ly_stg_4
WHERE Reactivated_fl = 1
GROUP BY 1,
         2,
         3,
         4) WITH data PRIMARY INDEX (acp_id,DAY_DT,BANNER,AARE_STATUS_CODE) ON COMMIT PRESERVE ROWS;
		 

CREATE multiset volatile TABLE cust_aare_weightage
AS
(SELECT acp_id,
       DAY_DT,
       BANNER,
       AARE_STATUS_CODE,
       1 / CAST(COUNT(*) OVER (PARTITION BY acp_id,DAY_DT,BANNER) AS DECIMAL(8,7)) AS WEIGHTAGE
FROM cust_aare) WITH data PRIMARY INDEX (acp_id,DAY_DT,BANNER,AARE_STATUS_CODE) ON COMMIT PRESERVE ROWS;
		 		

CREATE MULTISET VOLATILE TABLE mta_staging_tbl1
AS
(SELECT a.order_date_pacific AS DAY_DT,
       a.acp_id,
       a.order_number AS ORDER_NUMBER,
       a.sku_id AS SKU_NUM,
       UPPER(a.UTM_CAMPAIGN) AS UTM_CAMPAIGN,
       f.PUBLISHER,
       f.PUBLISHER_GROUP,
       f.PUBLISHER_SUBGROUP,
       b.nordy_level AS LOYALTY_STATUS,
       CASE
         WHEN arrived_channel = 'RACK' THEN 'NORDSTROM RACK'
         WHEN arrived_channel = 'FULL_LINE' THEN 'NORDSTROM'
         ELSE NULL
       END AS BANNER,
       c.div_desc AS DIVISION,
       c.grp_desc AS SUB_DIVISION,
       c.dept_desc AS DEPARTMENT,
       c.brand_name AS BRAND_NAME,
       c.npg_ind AS NPG_CODE,
       e.PRICE_TYPE,
       SUM(a.attributed_demand) AS ATTRIBUTED_DEMAND,
       SUM(a.attributed_orders) AS ATTRIBUTED_ORDERS,
       SUM(a.attributed_pred_net) ATTRIBUTED_NET_SALES
FROM T2DL_DAS_MTA.mta_acp_scoring_fact AS a
  LEFT JOIN (SELECT DISTINCT acp_id,
                    nordy_level
             FROM prd_nap_usr_vws.analytical_customer
             GROUP BY 1,
                      2) AS b ON a.acp_id = b.acp_id
  LEFT JOIN (SELECT rms_sku_num,
                    div_desc,
                    grp_desc,
                    dept_desc,
                    brand_name,
                    npg_ind
             FROM prd_nap_usr_vws.product_sku_dim_vw
             WHERE channel_country = 'US'
             GROUP BY 1,
                      2,
                      3,
                      4,
                      5,
                      6) AS c ON a.sku_id = c.rms_sku_num
  LEFT JOIN (SELECT order_num AS ORDER_NUMBER,
                    SKU_NUM,
                    SUM(order_line_current_amount_usd) AS AMOUNT
             FROM prd_nap_usr_vws.ORDER_LINE_DETAIL_FACT,
                  _variables
             WHERE ORDER_DATE_PACIFIC BETWEEN start_date AND end_date
             GROUP BY 1,
                      2) AS d
         ON a.ORDER_NUMBER = d.ORDER_NUMBER
        AND a.sku_id = d.SKU_NUM
  LEFT JOIN (SELECT DISTINCT order_num AS ORDER_NUMBER,
                    SKU_NUM,
                    CASE
                      WHEN PRICE_TYPE = 'R' THEN 'REGULAR'
                      WHEN PRICE_TYPE = 'C' THEN 'CLEARANCE'
                      WHEN PRICE_TYPE = 'P' THEN 'PROMOTIONAL'
                      ELSE 'UNKNOWN'
                    END AS PRICE_TYPE,
                    TRANSACTION_PRICE_AMT AS AMOUNT
             FROM T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT
             WHERE GLOBAL_TRAN_ID IS NOT NULL) AS e
         ON d.ORDER_NUMBER = e.ORDER_NUMBER
        AND d.SKU_NUM = e.SKU_NUM
        AND d.AMOUNT = e.AMOUNT
  LEFT JOIN (SELECT DISTINCT UPPER(ENCRYPTED_ID) AS ENCRYPTED_ID,
                    UPPER(PUBLISHER_NAME) PUBLISHER,
                    UPPER(PUBLISHER_GROUP) PUBLISHER_GROUP,
                    UPPER(PUBLISHER_SUBGROUP) PUBLISHER_SUBGROUP
             FROM T2DL_DAS_FUNNEL_IO.affiliate_publisher_mapping) f
         ON UPPER (a.UTM_CAMPAIGN) = UPPER (f.ENCRYPTED_ID)
        AND f.ENCRYPTED_ID IS NOT NULL
  ,_variables 
WHERE CAST(a.order_date_pacific AS DATE) BETWEEN start_date AND end_date
AND   a.arrived_channel IN ('RACK','FULL_LINE')
AND   a.utm_source LIKE '%rakuten%'
AND   a.finance_detail LIKE '%affiliates%'
AND   1 = 1
and   a.order_channelcountry='US'
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14,
         15,
         16
		) WITH data PRIMARY INDEX (DAY_DT,acp_id,ORDER_NUMBER,SKU_NUM,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP,LOYALTY_STATUS,BANNER,DIVISION,SUB_DIVISION,DEPARTMENT,BRAND_NAME,NPG_CODE,PRICE_TYPE) ON COMMIT PRESERVE ROWS;

	
CREATE MULTISET VOLATILE TABLE mta_staging_tbl2
AS
(SELECT a.DAY_DT,
	SKU_NUM,
       UTM_CAMPAIGN,
       PUBLISHER,
       PUBLISHER_GROUP,
       PUBLISHER_SUBGROUP,
	AARE_STATUS_CODE,
       LOYALTY_STATUS,
       a.BANNER,
       DIVISION,
       SUB_DIVISION,
       DEPARTMENT,
       BRAND_NAME,
       NPG_CODE,
       PRICE_TYPE,
       SUM(ATTRIBUTED_DEMAND*COALESCE(WEIGHTAGE,1)) AS ATTRIBUTED_DEMAND,
       SUM(ATTRIBUTED_ORDERS*COALESCE(WEIGHTAGE,1)) AS ATTRIBUTED_ORDERS,
       SUM(ATTRIBUTED_NET_SALES*COALESCE(WEIGHTAGE,1)) ATTRIBUTED_NET_SALES,
	SUM(CASE WHEN AARE_STATUS_CODE = 'R' THEN ATTRIBUTED_ORDERS*COALESCE(WEIGHTAGE,1) ELSE 0 END) AS RETAINED_ATTRIBUTED_ORDERS,
       SUM(CASE WHEN AARE_STATUS_CODE = 'NTN' THEN ATTRIBUTED_ORDERS*COALESCE(WEIGHTAGE,1) ELSE 0 END) AS ACQUIRED_ATTRIBUTED_ORDERS,
       SUM(CASE WHEN AARE_STATUS_CODE = 'NTN' THEN ATTRIBUTED_DEMAND*COALESCE(WEIGHTAGE,1) ELSE 0 END) AS ACQUIRED_ATTRIBUTED_DEMAND
FROM mta_staging_tbl1 AS a
  LEFT JOIN cust_aare_weightage AS b
         ON a.acp_id = b.acp_id
        AND a.DAY_DT = b.DAY_DT
        AND a.BANNER = b.BANNER	
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14,
         15
		 ) WITH data PRIMARY INDEX (DAY_DT,SKU_NUM,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP,AARE_STATUS_CODE,LOYALTY_STATUS,BANNER,DIVISION,SUB_DIVISION,DEPARTMENT,BRAND_NAME,NPG_CODE,PRICE_TYPE) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE apd_4_box_mta
AS
(SELECT a.DAY_DT,
	   a.BANNER,
       a.UTM_CAMPAIGN,
       a.PUBLISHER,
       a.PUBLISHER_GROUP,
       a.PUBLISHER_SUBGROUP,
	   a.AARE_STATUS_CODE,
       a.LOYALTY_STATUS,  
	   a.DIVISION,
       a.SUB_DIVISION,
       a.DEPARTMENT,
       a.BRAND_NAME,
       a.NPG_CODE,
	   a.PRICE_TYPE,
       SUM(a.ATTRIBUTED_DEMAND) AS ATTRIBUTED_DEMAND,
       SUM(a.ATTRIBUTED_ORDERS) AS ATTRIBUTED_ORDERS,
       SUM(a.ATTRIBUTED_NET_SALES) AS ATTRIBUTED_NET_SALES,
       SUM(a.RETAINED_ATTRIBUTED_ORDERS) AS RETAINED_ATTRIBUTED_ORDERS,
       SUM(a.ACQUIRED_ATTRIBUTED_ORDERS) AS ACQUIRED_ATTRIBUTED_ORDERS,
       SUM(a.ACQUIRED_ATTRIBUTED_DEMAND) AS ACQUIRED_ATTRIBUTED_DEMAND       
FROM mta_staging_tbl2 a
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14
         ) WITH DATA PRIMARY INDEX (DAY_DT,BANNER,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP,AARE_STATUS_CODE,LOYALTY_STATUS,DIVISION,SUB_DIVISION,DEPARTMENT,BRAND_NAME,NPG_CODE,PRICE_TYPE) ON COMMIT PRESERVE ROWS;	

DELETE 
FROM    {apd_t2_schema}.mta_publisher_daily
where day_dt between (select start_date from _variables) and (select end_date from _variables);


INSERT INTO {apd_t2_schema}.mta_publisher_daily
select a.*,CURRENT_TIMESTAMP as dw_sys_load_tmstp from apd_4_box_mta a;

COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (DAY_DT,BANNER,UTM_CAMPAIGN,PUBLISHER,PUBLISHER_GROUP,PUBLISHER_SUBGROUP,AARE_STATUS_CODE,LOYALTY_STATUS,DIVISION,SUB_DIVISION,DEPARTMENT,BRAND_NAME,NPG_CODE,PRICE_TYPE), 
                    COLUMN (DAY_DT,BANNER)  
on {apd_t2_schema}.mta_publisher_daily;


SET QUERY_BAND = NONE FOR SESSION;		 
