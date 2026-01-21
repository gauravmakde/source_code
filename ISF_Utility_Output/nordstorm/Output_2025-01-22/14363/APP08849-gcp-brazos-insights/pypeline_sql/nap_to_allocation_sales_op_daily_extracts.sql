SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=nap_to_allocation_sales_extracts_daily;
Task_Name=sales_inv_oo_extracts;'
FOR SESSION VOLATILE;

--- Read the store data
create temporary view store_dim_vw as
select * from store_dim;

create temporary view etl_batch_dt_lkup_vw as
select * from etl_batch_dt_lkup;


-- generating the extract Data
create temporary view sales_op_extracts as 
SELECT
            alloc.store_num
            ,alloc.rms_sku_num
            ,alloc.week_num
            ,SUM(COALESCE(onordr.imminent_quantity_open,0)) AS ON_ORDER_QUANTITY
            ,SUM(COALESCE(onordr.imminent_quantity_open_0,0)) AS ON_ORDER_QUANTITY_0
            ,SUM(COALESCE(onordr.imminent_quantity_open_1,0)) AS ON_ORDER_QUANTITY_1
            ,SUM(COALESCE(onordr.imminent_quantity_open_2,0)) AS ON_ORDER_QUANTITY_2
            ,SUM(COALESCE(onordr.imminent_quantity_open_3,0)) AS ON_ORDER_QUANTITY_3
            ,SUM(COALESCE(onordr.imminent_quantity_open_4,0)) AS ON_ORDER_QUANTITY_4
            ,SUM(COALESCE(onordr.imminent_quantity_open_5,0)) AS ON_ORDER_QUANTITY_5
            ,SUM(COALESCE(onordr.imminent_quantity_open_6,0)) AS ON_ORDER_QUANTITY_6
            ,SUM(COALESCE(onordr.imminent_quantity_open_7,0)) AS ON_ORDER_QUANTITY_7
            ,SUM(COALESCE(onordr.imminent_quantity_open_8,0)) AS ON_ORDER_QUANTITY_8
            ,SUM(COALESCE(onordr.imminent_quantity_open_9,0)) AS ON_ORDER_QUANTITY_9
            ,SUM(COALESCE(onordr.imminent_quantity_open_10,0)) AS ON_ORDER_QUANTITY_10
            ,SUM(COALESCE(onordr.imminent_quantity_open_11,0)) AS ON_ORDER_QUANTITY_11
            ,SUM(COALESCE(onordr.imminent_quantity_open_12,0)) AS ON_ORDER_QUANTITY_12
            ,SUM(COALESCE(onordr.imminent_quantity_open_13,0)) AS ON_ORDER_QUANTITY_13
            ,SUM(COALESCE(onordr.imminent_quantity_open_14,0)) AS ON_ORDER_QUANTITY_14
            ,SUM(COALESCE(onordr.imminent_quantity_open_15,0)) AS ON_ORDER_QUANTITY_15
            ,SUM(COALESCE(onordr.imminent_quantity_open_16,0)) AS ON_ORDER_QUANTITY_16
            ,SUM(COALESCE(onordr.imminent_quantity_open_17,0)) AS ON_ORDER_QUANTITY_17
            ,SUM(COALESCE(onordr.imminent_quantity_open_18,0)) AS ON_ORDER_QUANTITY_18
            ,SUM(COALESCE(onordr.imminent_quantity_open_19,0)) AS ON_ORDER_QUANTITY_19
            ,SUM(COALESCE(onordr.imminent_quantity_open_20,0)) AS ON_ORDER_QUANTITY_20
            ,SUM(COALESCE(onordr.imminent_quantity_open_21,0)) AS ON_ORDER_QUANTITY_21
            ,SUM(COALESCE(onordr.imminent_quantity_open_22,0)) AS ON_ORDER_QUANTITY_22
            ,SUM(COALESCE(onordr.imminent_quantity_open_23,0)) AS ON_ORDER_QUANTITY_23
            ,SUM(COALESCE(onordr.imminent_quantity_open_24,0)) AS ON_ORDER_QUANTITY_24
            ,SUM(COALESCE(onordr.imminent_quantity_open_25,0)) AS ON_ORDER_QUANTITY_25
            ,SUM(COALESCE(onordr.imminent_quantity_open_26,0)) AS ON_ORDER_QUANTITY_26
            ,SUM(COALESCE(onordr.imminent_quantity_open_27,0)) AS ON_ORDER_QUANTITY_27
            ,SUM(COALESCE(onordr.imminent_quantity_open_28,0)) AS ON_ORDER_QUANTITY_28
            ,SUM(COALESCE(onordr.imminent_quantity_open_29,0)) AS ON_ORDER_QUANTITY_29
            ,SUM(COALESCE(onordr.imminent_quantity_open_30,0)) AS ON_ORDER_QUANTITY_30
            ,SUM(COALESCE(onordr.imminent_quantity_open_31,0)) AS ON_ORDER_QUANTITY_31
            ,SUM(COALESCE(onordr.imminent_quantity_open_32,0)) AS ON_ORDER_QUANTITY_32
            ,SUM(COALESCE(onordr.imminent_quantity_open_33,0)) AS ON_ORDER_QUANTITY_33
            ,SUM(COALESCE(onordr.imminent_quantity_open_34,0)) AS ON_ORDER_QUANTITY_34
            ,SUM(COALESCE(onordr.imminent_quantity_open_35,0)) AS ON_ORDER_QUANTITY_35
            ,SUM(COALESCE(onordr.imminent_quantity_open_36,0)) AS ON_ORDER_QUANTITY_36
            ,SUM(COALESCE(onordr.imminent_quantity_open_37,0)) AS ON_ORDER_QUANTITY_37
            ,SUM(COALESCE(onordr.imminent_quantity_open_38,0)) AS ON_ORDER_QUANTITY_38
            ,SUM(COALESCE(onordr.imminent_quantity_open_39,0)) AS ON_ORDER_QUANTITY_39
            ,SUM(COALESCE(onordr.imminent_quantity_open_40,0)) AS ON_ORDER_QUANTITY_40
            ,SUM(COALESCE(onordr.imminent_quantity_open_41,0)) AS ON_ORDER_QUANTITY_41
            ,SUM(COALESCE(onordr.imminent_quantity_open_42,0)) AS ON_ORDER_QUANTITY_42
            ,SUM(COALESCE(onordr.imminent_quantity_open_43,0)) AS ON_ORDER_QUANTITY_43
            ,SUM(COALESCE(onordr.imminent_quantity_open_44,0)) AS ON_ORDER_QUANTITY_44
            ,SUM(COALESCE(onordr.imminent_quantity_open_45,0)) AS ON_ORDER_QUANTITY_45
            ,SUM(COALESCE(onordr.imminent_quantity_open_46,0)) AS ON_ORDER_QUANTITY_46
            ,SUM(COALESCE(onordr.imminent_quantity_open_47,0)) AS ON_ORDER_QUANTITY_47
            ,SUM(COALESCE(onordr.imminent_quantity_open_48,0)) AS ON_ORDER_QUANTITY_48
            ,SUM(COALESCE(onordr.imminent_quantity_open_49,0)) AS ON_ORDER_QUANTITY_49
            ,SUM(COALESCE(onordr.imminent_quantity_open_50,0)) AS ON_ORDER_QUANTITY_50
            ,SUM(COALESCE(onordr.imminent_quantity_open_51,0)) AS ON_ORDER_QUANTITY_51
            ,SUM(COALESCE(onordr.imminent_quantity_open_52,0)) AS ON_ORDER_QUANTITY_52
            ,SUM(COALESCE(onordr.imminent_quantity_open_53,0)) AS ON_ORDER_QUANTITY_53
            ,SUM(COALESCE(onordr.quantity_open,0)) AS ON_ORDER_TOTAL
            ,MAX(CAST(GREATEST(COALESCE(inv_curr.in_transit_qty,0),0) + GREATEST(COALESCE(inv_curr.store_transfer_expected_qty,0),0) AS INT)) AS IN_TRANSIT_QUANTITY
            ,SUM(CAST(COALESCE(opgmv.line_item_quantity,0) AS INT)) AS SALES_QUANTITY
            ,SUM(CASE WHEN opgmv.price_type IN ('Regular Price','Promotion')  
                 THEN CAST(COALESCE(opgmv.line_item_quantity,0) AS INT)
                 ELSE CAST(0 AS INT)
                END) AS REGPROMO_SLS_QUANTITY 
            ,MAX(CAST(GREATEST(COALESCE( inv_curr.stock_on_hand_qty,0),0) - GREATEST(COALESCE(inv_curr.store_transfer_reserved_qty,0 ),0) - GREATEST(COALESCE(inv_curr.return_to_vendor_qty,0 ),0) -
                 COALESCE((GREATEST(COALESCE(inv_curr.unavailable_qty,0 ),0) + GREATEST(COALESCE(inv_curr.pre_inspect_customer_return,0 ),0) + GREATEST(COALESCE(inv_curr.unusable_qty,0 ),0)+ GREATEST(COALESCE(inv_curr.rtv_reqst_resrv,0 ),0)+
                           GREATEST(COALESCE(inv_curr.met_reqst_resrv,0 ),0) + GREATEST(COALESCE(inv_curr.tc_preview,0 ),0) + GREATEST(COALESCE(inv_curr.tc_clubhouse,0 ),0) + GREATEST(COALESCE(inv_curr.tc_mini_1,0 ),0) + GREATEST(COALESCE(inv_curr.tc_mini_2,0 ),0) +GREATEST(COALESCE(inv_curr.tc_mini_3,0 ),0)+ 
                           GREATEST(COALESCE(inv_curr.nd_unavailable_qty,0 ),0) + GREATEST(COALESCE(inv_curr.pb_holds_qty,0 ),0) + GREATEST(COALESCE(inv_curr.store_reserve,0 ),0)),0) AS INT)) AS ON_HAND_QUANTITY  
FROM default.merch_allocation_fact_sku_store_wrk alloc
LEFT OUTER JOIN (select a.purchase_id, a.business_day_date, a.dw_batch_date, a.record_source, a.jwn_operational_gmv_ind,
             a.demand_date, a.rms_sku_num, a.intent_store_num, a.line_item_quantity, a.price_type,
             b.week_idnt AS week_num
        from default.jwn_demand_lifecycle_fact a
        JOIN {hive_schema}.by_allocation_daily_calendar b 
          ON a.business_day_date = b.day_date
        where demand_date BETWEEN (select dw_batch_dt-60 from etl_batch_dt_lkup_vw where interface_code='MERCH_ALLOCATION_DLY')
                              AND (select dw_batch_dt from etl_batch_dt_lkup_vw where interface_code='MERCH_ALLOCATION_DLY')
          and a.dw_batch_date=(select dw_batch_dt+1 from etl_batch_dt_lkup_vw where interface_code='MERCH_ALLOCATION_DLY') 
          and a.record_source = 'S' 
          and a.jwn_operational_gmv_ind = 'Y') opgmv
   ON opgmv.rms_sku_num = alloc.rms_sku_num
  AND opgmv.intent_store_num = alloc.store_num
  AND opgmv.week_num = alloc.week_num          
LEFT OUTER JOIN (select inv.* 
        from default.inventory_stock_quantity_by_day_logical_fact inv
        JOIN store_dim_vw sd
          ON inv.location_id = sd.store_num
       WHERE inv.snapshot_date= (select dw_batch_dt from etl_batch_dt_lkup_vw where interface_code='MERCH_ALLOCATION_DLY')
         AND sd.business_unit_num IN ('2000','5000')) inv_curr
  ON inv_curr.rms_sku_id = alloc.rms_sku_num
 AND inv_curr.location_id = alloc.store_num     
LEFT OUTER JOIN (select oo.store_num,	
                        oo.rms_sku_num,
                       (SELECT week_idnt 
                          FROM {hive_schema}.by_allocation_daily_calendar dcd
                          JOIN etl_batch_dt_lkup_vw etl
                            ON dcd.day_date = etl.dw_batch_dt
                         WHERE interface_code='MERCH_ALLOCATION_DLY') AS week_num,
                        case when oo.week_num<=(select DISTINCT week_idnt from {hive_schema}.by_allocation_daily_calendar WHERE day_date=(current_date()-1)+49) then oo.quantity_open else 0 end as imminent_quantity_open,
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
                  FROM default.merch_on_order_fact oo
                  JOIN store_dim_vw sd
                    ON oo.store_num = sd.store_num
                  JOIN (select week_idnt,
                              ROW_NUMBER() OVER (order by week_idnt asc) as wk_nbr
                        from ( select distinct week_idnt
                                from {hive_schema}.by_allocation_daily_calendar
                                WHERE day_date >= current_date()
                                and day_date <= current_date()+ 371) as tmp) cal
                     ON (oo.week_num = cal.week_idnt)
                 WHERE oo.quantity_open>0 
                   AND oo.status ='APPROVED') onordr 
      ON onordr.rms_sku_num = alloc.rms_sku_num
     AND onordr.store_num = alloc.store_num
     AND onordr.week_num = alloc.week_num  
   WHERE alloc.business_unit_num IN ('2000','5000') 
GROUP BY alloc.store_num,
         alloc.rms_sku_num,
         alloc.week_num;

--- Writing Data to s3 on a single partition
insert overwrite table sales_op_tab
select /*+ COALESCE(1) */ * from  sales_op_extracts ;

insert overwrite table sales_op_audit 
select concat(date_format(from_utc_timestamp(current_timestamp,'PST'),'yyyyMMddHHmmss'),' ',format_number(count(1),'000000000000')) from sales_op_extracts;

 