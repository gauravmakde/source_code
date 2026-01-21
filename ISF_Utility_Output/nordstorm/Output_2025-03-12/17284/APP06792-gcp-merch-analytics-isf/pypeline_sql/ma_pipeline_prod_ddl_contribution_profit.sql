/*
Contribution Profit DDL
Authors: Sara Riker and KP Tryon
Datalab: t2dl_das_contribution_margin

2022-01-31: Create DDLs for Contribution Profit

Creates Tables:
    complete:
        - t2dl_das_contribution_margin.commission_rates
        - t2dl_das_contribution_margin.transaction_base
        - t2dl_das_contribution_margin.cost_selling_store_labor

    in progress:
        - t2dl_das_contribution_margin.cost_marketing
        - t2dl_das_contribution_margin.cost_loyalty
        - t2dl_das_contribution_margin.cost_distribution_fulfillment
        - t2dl_das_contribution_margin.cost_shipping_revenue
        - t2dl_das_contribution_margin.cost_contra_cogs
        - t2dl_das_contribution_margin.cost_base
*/


/*
================
COMMISSION RATES
================
Commission rate lookup table based on dept_idnt

-- 2022-01-31: Create DDL for Commissin Rates by dept_idnt

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_contribution_margin', 'commission_rates', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_contribution_margin.commission_rates, 
    FALLBACK, 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     dept_idnt INTEGER 
    ,commission_rate DECIMAL(4,4)
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(dept_idnt)
;

GRANT SELECT ON t2dl_das_contribution_margin.commission_rates TO public;

/*
======================
TRANSACTION BASE TABLE
======================
Holds transaction details to map costs to an individual item 

    - 2022-01-31: Create DDL for base transaction table
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_contribution_margin', 'transaction_base', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_contribution_margin.transaction_base, 
    FALLBACK, 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     business_day_date DATE FORMAT 'YYYY-MM-DD' NOT NULL
    ,tran_date DATE FORMAT 'YYYY-MM-DD'
    ,tran_time TIMESTAMP(6)
    ,order_num VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,global_tran_id BIGINT NOT NULL
    ,line_item_seq_num SMALLINT NOT NULL
    ,tran_type_code CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('N','R','S')
    ,tran_type VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('SALE','RETURN','N','NA')
    ,line_item_order_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,line_item_fulfillment_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,ringing_store_num INTEGER
    ,fulfilling_store_num INTEGER 
    ,intent_store_num INTEGER 
    ,business_unit_desc VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,worker_number VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,commissionable_flag INTEGER COMPRESS (0,1)
    ,shipping_flag INTEGER COMPRESS (0,1)
    ,merch_ind INTEGER COMPRESS (0,1)
    ,nonmerch_fee_code VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('150','6666','140')
    ,merch_dept_num VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('113','166','5','164','222','22','267','167','84','46','143','172','209','574','573','213','109','230','536','567','28','525','173','560','36','161','163','77','26','193','162','597','275','16','584','256','215','169','67','54','258','218','287','14','97','192','160','586','53','122','312','24','216','3','62','75','100','220','543','590','191','251','37','526','178','229','78','9','618','490','557','85','240','537','92','467','15','464','242','596','103','221','593','321','64','61','40','545','30','277','521','59','91','522','594','524','592','266','179','346','470','572','294','243','101','183','83','149','219','174','622','588','127','493','478','585','58','8','11','566','4','579','463','544','226','238','341','624','561','82','214','60','625','123','80','42','199','639','264','569','449','595','250','559','276','549','98','291','20','2','693','225','285','542','554','17','76','615','626','43','591','44','197','607','217','227','165','484','447','232','134','556','446','194','649','188','476','68','479','198','329','489','86','623','111','558','568','438','609','39','298','617','541','472','575','631','492','523','534','562','482','456','587','56','107','638','32','520','547','627','25','500','546','589','79','57','471','129','491','342','497','555','87','535','132','533','308','176','604','498','330','516','707','629','552','114','527','95','598','89','495','619','630','452','515','177','628','38','576','695','578','645','180','528','571')
    ,sku_num VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS 
    ,line_net_usd_amt DECIMAL(12,2)
    ,line_net_amt DECIMAL(12,2)
    ,line_item_net_amt_currency_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,line_item_regular_price DECIMAL(12,2)
    ,line_item_quantity DECIMAL(8,0) 
    ,price_adj_code VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS 'N'
    ,commission_rate DECIMAL(4,4)
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(business_day_date, global_tran_id, line_item_seq_num)
PARTITION BY RANGE_N(business_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

GRANT SELECT ON t2dl_das_contribution_margin.transaction_base TO public;

/*
============================
STORE AND SELLING LABOR COST
============================
Calculates commission dollars paid per item and labor costs associated with shipping an item

    - 2022-01-31: Create DDL for selling and labor costs
        - commissionable_flag: is the item/selling employee elegeble for commission
        - shipped_flag: was this item shipped and will incur labor costs to fulfill the order
        - commission_dollars: how much was paid to the selling employee (or recupped in the case of a return) 
        - non_sell_labor_dollars: How much was paid to fulfill the item in labor costs (fulfill, package, ship...)--currently a placeholder

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_contribution_margin', 'cost_selling_store_labor', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_contribution_margin.cost_selling_store_labor, 
    FALLBACK, 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     business_day_date DATE FORMAT 'YYYY-MM-DD' NOT NULL
    ,tran_date DATE FORMAT 'YYYY-MM-DD'
    ,order_num VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,global_tran_id BIGINT NOT NULL
    ,line_item_seq_num SMALLINT NOT NULL 
    ,commissionable INTEGER
    ,shipping INTEGER 
    ,employee_commission_rate DECIMAL(4,4)
    ,commission_dollars DECIMAL(12,2)
    ,non_sell_lobar_dollars DECIMAL(12,2)
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(business_day_date, global_tran_id, line_item_seq_num)
PARTITION BY RANGE_N(business_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

GRANT SELECT ON t2dl_das_contribution_margin.cost_selling_store_labor TO public;

/*
================
MARKETING
Author: KP Tryon
================
Attributes marketing dollars to transactions by:
    - Email
    - Digital Affiliates
    - Digital Display
    - Digital Paid Search Branded
    - Digital Paid Search Unbranded
    - Digital Shopping
    - Digital Social Paid
    - Digital Video

    - 2022-01-31: Create DDL for Marketing Costs by Day and by transaction

*/

/*
-- 2021-12-10: DDL Creation for the CRM Cost staging dataset
    - Salesfore data loaded from s3? 

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_contribution_margin', 'cost_ref_crm_cost_stg', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_contribution_margin.cost_ref_crm_cost_stg, 
    FALLBACK, 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,business_day_date DATE FORMAT 'YYYY-MM-DD'
    ,channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,channel_01 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,channel_02 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,cost_value VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,contract_start VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,contract_end VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
    ,process_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX ( platform )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY )
;
*/

/*
MARKETING: EMAIL

    - 2022-01-31: Creation of base dataset for determining transactions that were tied to a Marketing Touchpoint
	    - At the moment, this includes only the basic information needed for cost calculation

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_contribution_margin', 'cost_mktg_email', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_contribution_margin.cost_mktg_email, 
    FALLBACK, 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     global_tran_id BIGINT
    ,business_day_date DATE FORMAT 'YYYY-MM-DD'
    ,tran_type_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,deploy_date DATE FORMAT 'YYYY-MM-DD'
    ,program_type VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC
    ,intent_store_num INTEGER
    ,order_num VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
    ,process_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX ( global_tran_id )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY )
;

GRANT SELECT ON t2dl_das_contribution_margin.cost_mktg_email TO public;


/*
Contribution Profit Main
Authors: Sara Riker and KP Tryon
Datalab: t2dl_das_contribution_margin

2022-01-31: Create main scripts for commssion rates, transaction base, selling labor, marketing email costs

Updates Tables:
    complete:
        - t2dl_das_contribution_margin.commission_rates

    in progress:
        - t2dl_das_contribution_margin.transaction_base
        - t2dl_das_contribution_margin.cost_selling_store_labor
        - t2dl_das_contribution_margin.cost_marketing
        - t2dl_das_contribution_margin.cost_loyalty
        - t2dl_das_contribution_margin.cost_distribution_fulfillment
        - t2dl_das_contribution_margin.cost_shipping_revenue
        - t2dl_das_contribution_margin.cost_contra_cogs
        - t2dl_das_contribution_margin.cost_base


-- 2022-01-31: Commission Rates only needs to be updated once

DELETE FROM t2dl_das_contribution_margin.commission_rates ALL;
INSERT INTO t2dl_das_contribution_margin.commission_rates 
SELECT
     dept_idnt
    ,commission_rate
    ,current_timestamp AS update_timestamp
FROM t3dl_ace_mch.commission_rates
;
*/

CREATE MULTISET VOLATILE TABLE dates AS (
    SELECT
        --  current_date - 7 AS start_date -- daily update
         '2021-02-01' AS start_date -- backfill
        ,current_date AS end_date 
)
WITH DATA
ON COMMIT PRESERVE ROWS
;

-- 2022-01-31: Create base transaction table

DELETE FROM t2dl_das_contribution_margin.transaction_base WHERE business_day_date BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates);
INSERT INTO t2dl_das_contribution_margin.transaction_base
SELECT 
     business_day_date
    ,tran_date
    ,tran_time
    ,order_num
    ,global_tran_id
    ,line_item_seq_num
    ,line_item_activity_type_code AS tran_type_code
    ,line_item_activity_type_desc AS tran_type
    ,line_item_order_type
    ,line_item_fulfillment_type
    ,ringing_store_num
    ,fulfilling_store_num
    ,intent_store_num
    ,business_unit_desc
    ,commission_slsprsn_num AS worker_number
    ,CASE WHEN business_unit_desc IN ('FULL LINE', 'FULL LINE CANADA')      -- Full Line Stores Only
           AND commission_slsprsn_num <> 2079333                            -- N.com dummy employee number
           AND line_item_merch_nonmerch_ind = 'MERCH'                       -- Merchandise only
          THEN 1 
          ELSE 0 
          END AS commissionable_flag
    ,CASE WHEN line_item_fulfillment_type = 'StoreTake' 
            OR line_item_fulfillment_type IS NULL 
          THEN 0 
          ELSE 1 
          END AS shipping_flag
    ,CASE WHEN line_item_merch_nonmerch_ind = 'MERCH' THEN 1 
          ELSE 0 
          END AS merch_ind
    ,nonmerch_fee_code
    ,merch_dept_num
    ,sku_num
    ,line_net_usd_amt
    ,line_net_amt 
    ,line_item_net_amt_currency_code
    ,line_item_regular_price
    ,line_item_quantity
    ,price_adj_code
    ,commission_rate
    ,current_timestamp AS update_timestamp
FROM prd_nap_usr_vws.retail_tran_detail_fact_vw trn
LEFT JOIN t2dl_das_contribution_margin.commission_rates cr
  ON trn.merch_dept_num = cr.dept_idnt
JOIN prd_nap_usr_vws.store st
  ON trn.intent_store_num = st.store_num 
WHERE business_day_date BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates)
;

COLLECT STATS 
     PRIMARY INDEX (business_day_date, global_tran_id, line_item_seq_num)
    ,COLUMN (business_day_date, global_tran_id, line_item_seq_num)
    ,COLUMN (business_day_date)
    ,COLUMN (tran_date)
    ,COLUMN (tran_time)
    ,COLUMN (worker_number)
ON t2dl_das_contribution_margin.transaction_base;

/*
=======================
SELLING AND LABOR COSTS
Author: Sara Riker
=======================
*/

-- 2022-01-31: Get commission plan information from nap hr database

-- DROP TABLE worker_commission;
CREATE MULTISET VOLATILE TABLE worker_commission AS (
SELECT DISTINCT
     worker_number
    ,beauty_line_assignment
    ,cosmetic_line_assignment
    ,other_line_assignment
    ,line_assignment
    ,job_family
    ,job_family_group
    ,job_title
    ,CASE WHEN commission_plan_name = 'N/A' THEN NULL ELSE commission_plan_name END AS commission_plan_name
    ,pay_rate_type
    ,cost_center
    ,cost_center_name
    ,payroll_department
    ,payroll_store
    ,hire_date
    ,CASE WHEN commission_plan_name IN ('Entry Comm RD') THEN -0.01     -- Entry-level Salespeople receive 1.00% less
          WHEN payroll_department = 1196 THEN 0.02
          WHEN job_title = 'Personal Stylist' THEN 0.01                 -- Personal Stylist receive 1.00% more
          ELSE 0 END AS altered_commission
    ,eff_begin_date AS begin_date
    ,eff_end_date AS end_date
FROM prd_nap_hr_usr_vws.hr_worker_dim
) WITH DATA
PRIMARY INDEX (worker_number, begin_date, end_date) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (worker_number, begin_date, end_date)
    ON worker_commission;

/*
=======================================================
2022-01-31: FULFILLMENT LABOR COSTS NEEDS MORE RESEARCH
=======================================================

CREATE MULTISET VOLATILE TABLE fulfillment_activity AS (
SELECT 
     order_num AS order_num
    ,order_line_num AS order_line_num
    ,order_date_pacific AS order_date_pacific
    ,event_date_pacific AS event_date_pacific
    ,event_code AS event_code
    ,rms_sku_num AS sku_idnt
    ,action_employee_num AS action_employee_num
FROM prd_nap_usr_vws.order_line_lifecycle_fact
WHERE order_date_pacific BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates)
  AND action_employee_num IS NOT NULL
) WITH DATA
PRIMARY INDEX (order_num, order_line_num, order_date_pacific) 
ON COMMIT PRESERVE ROWS
;
*/

-- 2022-01-31: Insert into selling and labor cost table

DELETE FROM t2dl_das_contribution_margin.cost_selling_store_labor WHERE business_day_date BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates);
INSERT INTO t2dl_das_contribution_margin.cost_selling_store_labor
SELECT 
     t.business_day_date
    ,t.tran_date 
    ,t.order_num 
    ,t.global_tran_id 
    ,t.line_item_seq_num 
    ,t.commissionable_flag
    ,t.shipping_flag
    ,CASE WHEN LOWER(commission_plan_name) LIKE '%cosm%' OR LOWER(commission_plan_name) LIKE '%spa%' THEN 0.03        -- Beauty receive 3% regardless of department
          WHEN commission_plan_name LIKE 'NC%' THEN 0
          WHEN commission_plan_name IS NOT NULL AND commissionable_flag = 1 THEN altered_commission + commission_rate -- Updated commission rate base on employee's commission plan
          ELSE 0
          END AS employee_commission_rate
    ,CASE WHEN commissionable_flag = 1 AND commission_plan_name IS NOT NULL THEN line_net_usd_amt * employee_commission_rate
          ELSE 0 
          END AS commission_dollars
    ,NULL AS non_sell_lobar_dollars                                                                                   -- Temporary NULL untill pay rates are in NAP
    ,current_timestamp AS update_timestamp
FROM t2dl_das_contribution_margin.transaction_base t
LEFT JOIN worker_commission w
  ON t.worker_number = w.worker_number
 AND t.tran_time BETWEEN w.begin_date AND w.end_date
;

COLLECT STATS 
    PRIMARY INDEX (business_day_date, global_tran_id, line_item_seq_num)
    ON t2dl_das_contribution_margin.cost_selling_store_labor;
    