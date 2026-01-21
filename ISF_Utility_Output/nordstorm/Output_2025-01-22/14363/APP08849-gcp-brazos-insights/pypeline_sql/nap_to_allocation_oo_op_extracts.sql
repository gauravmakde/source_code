SET QUERY_BAND = '
App_ID=APP08849;
DAG_ID=nap_to_allocation_onorder_extracts;
Task_Name=oo_op_extracts;'
FOR SESSION VOLATILE;

--- Read the calender data 
create temporary view day_cal_454_dim_vw as 
select * from day_cal_454_dim;

--- Read the onorder data 
create temporary view merch_on_order_fact_view as 
select * from default.merch_on_order_fact;

--- Read the store data 
create temporary view store_dim_vw as
select * from store_dim;
 

-- generating the extract Data
create temporary view oo_op_extracts as 
select oo.store_num,	
       oo.rms_sku_num,
       oo.purchase_order_number,
       case when oo.week_num<=(select DISTINCT week_idnt from day_cal_454_dim_vw WHERE day_date=(current_date()-1)+49) then oo.quantity_open else 0 end as imminent_quantity_open,
       case when cal.wk_nbr = 1 then oo.quantity_open else 0 end as imminent_quantity_open_0,
       case when cal.wk_nbr = 2 then oo.quantity_open else 0 end as imminent_quantity_open_1,
       case when cal.wk_nbr = 3 then oo.quantity_open else 0 end as imminent_quantity_open_2,
       case when cal.wk_nbr = 4 then oo.quantity_open else 0 end as imminent_quantity_open_3,
       case when cal.wk_nbr = 5 then oo.quantity_open else 0 end as imminent_quantity_open_4,
       case when cal.wk_nbr = 5 then oo.quantity_open else 0 end as imminent_quantity_open_5,
       case when cal.wk_nbr = 7 then oo.quantity_open else 0 end as imminent_quantity_open_6,
       case when cal.wk_nbr = 8 then oo.quantity_open else 0 end as imminent_quantity_open_7,
       case when cal.wk_nbr = 9 then oo.quantity_open else 0 end as imminent_quantity_open_8,
       case when cal.wk_nbr = 10 then oo.quantity_open else 0 end as imminent_quantity_open_9,
       case when cal.wk_nbr = 11 then oo.quantity_open else 0 end as imminent_quantity_open_10,
       case when cal.wk_nbr = 12 then oo.quantity_open else 0 end as imminent_quantity_open_11,
       case when cal.wk_nbr = 13 then oo.quantity_open else 0 end as imminent_quantity_open_12,
       case when cal.wk_nbr = 14 then oo.quantity_open else 0 end as imminent_quantity_open_13,
       case when cal.wk_nbr = 15 then oo.quantity_open else 0 end as imminent_quantity_open_14,
       case when cal.wk_nbr = 16 then oo.quantity_open else 0 end as imminent_quantity_open_15,
       case when cal.wk_nbr = 17 then oo.quantity_open else 0 end as imminent_quantity_open_16,
       case when cal.wk_nbr = 18 then oo.quantity_open else 0 end as imminent_quantity_open_17,
       case when cal.wk_nbr = 19 then oo.quantity_open else 0 end as imminent_quantity_open_18,
       case when cal.wk_nbr = 20 then oo.quantity_open else 0 end as imminent_quantity_open_19,
       case when cal.wk_nbr = 21 then oo.quantity_open else 0 end as imminent_quantity_open_20,
       case when cal.wk_nbr = 22 then oo.quantity_open else 0 end as imminent_quantity_open_21,
       case when cal.wk_nbr = 23 then oo.quantity_open else 0 end as imminent_quantity_open_22,
       case when cal.wk_nbr = 24 then oo.quantity_open else 0 end as imminent_quantity_open_23,
       case when cal.wk_nbr = 25 then oo.quantity_open else 0 end as imminent_quantity_open_24,
       case when cal.wk_nbr = 26 then oo.quantity_open else 0 end as imminent_quantity_open_25,
       case when cal.wk_nbr = 27 then oo.quantity_open else 0 end as imminent_quantity_open_26,
       case when cal.wk_nbr = 28 then oo.quantity_open else 0 end as imminent_quantity_open_27,
       case when cal.wk_nbr = 29 then oo.quantity_open else 0 end as imminent_quantity_open_28,
       case when cal.wk_nbr = 30 then oo.quantity_open else 0 end as imminent_quantity_open_29,
       case when cal.wk_nbr = 31 then oo.quantity_open else 0 end as imminent_quantity_open_30,
       case when cal.wk_nbr = 32 then oo.quantity_open else 0 end as imminent_quantity_open_31,
       case when cal.wk_nbr = 33 then oo.quantity_open else 0 end as imminent_quantity_open_32,
       case when cal.wk_nbr = 34 then oo.quantity_open else 0 end as imminent_quantity_open_33,
       case when cal.wk_nbr = 35 then oo.quantity_open else 0 end as imminent_quantity_open_34,
       case when cal.wk_nbr = 36 then oo.quantity_open else 0 end as imminent_quantity_open_35,
       case when cal.wk_nbr = 37 then oo.quantity_open else 0 end as imminent_quantity_open_36,
       case when cal.wk_nbr = 38 then oo.quantity_open else 0 end as imminent_quantity_open_37,
       case when cal.wk_nbr = 39 then oo.quantity_open else 0 end as imminent_quantity_open_38,
       case when cal.wk_nbr = 40 then oo.quantity_open else 0 end as imminent_quantity_open_39,
       case when cal.wk_nbr = 41 then oo.quantity_open else 0 end as imminent_quantity_open_40,
       case when cal.wk_nbr = 42 then oo.quantity_open else 0 end as imminent_quantity_open_41,
       case when cal.wk_nbr = 43 then oo.quantity_open else 0 end as imminent_quantity_open_42,
       case when cal.wk_nbr = 44 then oo.quantity_open else 0 end as imminent_quantity_open_43,
       case when cal.wk_nbr = 45 then oo.quantity_open else 0 end as imminent_quantity_open_44,
       case when cal.wk_nbr = 46 then oo.quantity_open else 0 end as imminent_quantity_open_45,
       case when cal.wk_nbr = 47 then oo.quantity_open else 0 end as imminent_quantity_open_46,
       case when cal.wk_nbr = 48 then oo.quantity_open else 0 end as imminent_quantity_open_47,
       case when cal.wk_nbr = 49 then oo.quantity_open else 0 end as imminent_quantity_open_48,
       case when cal.wk_nbr = 50 then oo.quantity_open else 0 end as imminent_quantity_open_49,
       case when cal.wk_nbr = 51 then oo.quantity_open else 0 end as imminent_quantity_open_50,
       case when cal.wk_nbr = 52 then oo.quantity_open else 0 end as imminent_quantity_open_51,
       case when cal.wk_nbr = 53 then oo.quantity_open else 0 end as imminent_quantity_open_52,
       case when cal.wk_nbr = 54 then oo.quantity_open else 0 end as imminent_quantity_open_53,
       oo.quantity_open
  FROM merch_on_order_fact_view oo
  JOIN store_dim_vw sd
    ON oo.store_num = sd.store_num
  JOIN (select week_start_day_date,
               week_end_day_date,
               week_idnt,
               ROW_NUMBER() OVER (order by week_idnt asc) as wk_nbr
          from ( select distinct week_start_day_date,
                        week_end_day_date,
                        week_idnt
                   from day_cal_454_dim_vw
                  WHERE day_date >= current_date()
                    and day_date <= current_date()+ 371) as tmp) cal
    ON (oo.week_num = cal.week_idnt)
 WHERE oo.quantity_open>0 
   AND oo.status ='APPROVED'
   AND sd.business_unit_num IN ('2000','5000');


--- Writing Data to s3 on a single partition
insert overwrite table oo_op_tab
select /*+ COALESCE(1) */ * from  oo_op_extracts ;

insert overwrite table oo_op_audit select concat(date_format(from_utc_timestamp(current_timestamp,'PST'),'yyyyMMddHHmmss'),' ',format_number(count(1),'000000000000')) from oo_op_extracts;
 