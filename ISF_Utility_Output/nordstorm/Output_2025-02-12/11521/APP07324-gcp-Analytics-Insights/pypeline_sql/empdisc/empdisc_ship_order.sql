SET QUERY_BAND = 'App_ID=APP02602;
   DAG_ID=empdisc_ship_order_11521_ACE_ENG;
   Task_Name=empdisc_ship_order;'
   FOR SESSION VOLATILE;

/*
T2/Table Name: empdisc_ship_order
Team/Owner: Rujira Achawanantakun (rujira.achawanantakun@nordstrom.com)
Date Created/Modified: 1/4/2024

Note:
-- Purpose of the table: to capture shipping order's profile associated to using
employee discount indicating dropshipping or compromised account.
-- Update Cadence: Daily
*/
INSERT INTO {empdisc_t2_schema}.empdisc_ship_order
select d.*
from (select dtl.employee_discount_num   emp_number
     , hdf.global_tran_id
     , hdf.order_num
     , hdf.business_day_date
     , cal.week_num
     , cal.month_num
     , cal.year_num
     , hdf.total_usd_amt     order_usd_amt
     , dtl.sum_employee_discount_usd_amt tran_emp_disc_amt
     , dtl.sum_line_net_usd_amt    tran_net_amt
     , hdf.data_source_code --POS, RPOS, COM
     , hdf.register_num
     , hdf.tran_num
     , hdf.online_shopper_id
     , hdf.acp_id
     , hdf.deterministic_profile_id
     , hdf.receipt_delivery_method_code
     , dtl.is_bopus
     , hdf.ringing_store_num
     , hdf.fulfilling_store_num
     , case
     when a1.store_type_code = 'NL' then 'NL'
     when a1.business_unit_desc in ('FULL LINE', 'FULL LINE CANADA') then 'FL'
     when a1.business_unit_desc in ('N.CA', 'N.COM', 'TRUNK CLUB') then 'NCOM'
     when a1.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
     when a1.business_unit_desc in ('OFFPRICE ONLINE') then 'RCOM'
     else null end as    ring_channel
     , case
     when a2.store_type_code = 'NL' then 'NL'
     when a2.business_unit_desc in ('FULL LINE', 'FULL LINE CANADA') then 'FL'
     when a2.business_unit_desc in ('N.CA', 'N.COM', 'TRUNK CLUB') then 'NCOM'
     when a2.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
     when a2.business_unit_desc in ('OFFPRICE ONLINE') then 'RCOM'
     else null end as    fullfill_channel
     , hdf.shipment_addressed_to_name  ship_to_name
     , hdf.ship_to_city_name     ship_to_city
     , hdf.ship_to_state_name    ship_to_state
     , hdf.ship_to_zip_code    ship_to_zip
     , hdf.ship_to_address     ship_to_address
     , CURRENT_TIMESTAMP as    dw_sys_load_tmstp
  from PRD_NAP_USR_VWS.RETAIL_TRAN_HDR_FACT hdf
     left join prd_nap_usr_vws.day_cal cal
       on hdf.business_day_date = cal.day_date
     left join prd_nap_usr_vws.store_dim a1
       on a1.store_num = hdf.ringing_store_num
     left join prd_nap_usr_vws.store_dim a2
       on a2.store_num = hdf.fulfilling_store_num
     right join (select global_tran_id
        , order_num
        , business_day_date
        , employee_discount_num
        , (CASE
           WHEN tran_type_code = 'SALE' AND (line_item_order_type LIKE 'CustInit%' AND
                   line_item_fulfillment_type = 'StorePickUp' AND
                   data_source_code = 'COM') THEN 1
           ELSE 0 END)     is_bopus
        , sum(employee_discount_usd_amt) sum_employee_discount_usd_amt
        , sum(line_net_usd_amt)    sum_line_net_usd_amt
         from prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW
         WHERE business_day_date between {start_date} and {end_date}
         and employee_discount_flag = 1
         and order_num IS NOT NULL
         and line_item_order_type <> 'RPOS'   --exclude restaurant transactions
         and tran_type_code = 'SALE'
         and line_item_merch_nonmerch_ind = 'MERCH' --MERCH, NMERCH
         group by global_tran_id, order_num, business_day_date, employee_discount_num,
          is_bopus) AS dtl
        on hdf.business_day_date = dtl.business_day_date
        and hdf.global_tran_id = dtl.global_tran_id
        and hdf.order_num = dtl.order_num
  WHERE hdf.business_day_date between {start_date} AND {end_date}
  and hdf.order_num IS NOT NULL
  and hdf.transaction_identifier_source <> 'RPOS'
  and hdf.tran_type_code = 'SALE'
  and NOT EXISTS (SELECT 1
      FROM {empdisc_t2_schema}.empdisc_ship_order t
      WHERE t.emp_number = CAST(dtl.employee_discount_num AS BIGINT)
      and t.global_tran_id = CAST(hdf.global_tran_id AS BIGINT)
      and t.order_num = CAST(hdf.order_num AS BIGINT)
    )
  ) d
;

COLLECT STATISTICS  COLUMN (emp_number),
      COLUMN (global_tran_id),
      COLUMN (order_num),
      COLUMN (online_shopper_id),
      COLUMN (acp_id),
      COLUMN (deterministic_profile_id),
      COLUMN (ringing_store_num),
      COLUMN (fulfilling_store_num)
on {empdisc_t2_schema}.empdisc_ship_order;

/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
