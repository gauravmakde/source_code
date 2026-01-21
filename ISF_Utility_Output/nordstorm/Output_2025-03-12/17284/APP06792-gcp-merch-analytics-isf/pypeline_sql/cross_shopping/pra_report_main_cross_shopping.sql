/* Cross Shopping Monthly : Used to report on basket insights for NMG
  Notes:
    - Hierarchy is as-was and is not fully restated every month for historicals
    - Has bado dependency on cateogry mapping table that needs to be replaced
    - Each run refreshes the prior completed fiscal month to the end date passed through
    - FOR BACKFILLS: Will not run as normal when loops autimatically, need to go month by month
*/


/* Grab most recent completed fiscal month based on run date */
create multiset volatile table run_dates as (
    select
        month_start_day_date
        , month_end_day_date
    from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW
    where day_date <= current_date -1
        and month_end_day_date <= current_date - 1
    qualify row_number() over(order by month_end_day_date desc) = 1
) with data on commit preserve rows
;

/* Pull in all transactions plus merch data */
create volatile multiset table trip_summary
as (
    select
        trim(channel_num) || ', ' || channel_desc as channel,
        hdr.acp_id||coalesce(dtl.business_day_date,dtl.tran_date)||channel as trip_id,
        month_num,
        year_num,
        trim(div_num) || ', ' || div_desc as division,
        trim(dept_num) || ', ' || dept_desc as dept,
        coalesce(category, 'UNKNOWN') as category,
        brand_name,
        max(case when coalesce(dtl.tran_date,dtl.business_day_date) = acq_dt then 1 else 0 end) as ntn,
        sum(line_net_usd_amt) as sales,
        sum(line_item_quantity) as quantity
    from prd_nap_usr_vws.retail_tran_detail_fact as dtl
    join prd_nap_usr_vws.retail_tran_hdr_fact hdr
        on dtl.global_tran_id = hdr.global_tran_id
        and dtl.business_day_date = hdr.business_day_date
    left join prd_nap_usr_vws.store_dim str
        on dtl.intent_store_num = str.store_num
    inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on coalesce(dtl.sku_num, dtl.hl_sku_num) = sku.rms_sku_num and str.store_country_code = sku.channel_country
    left join (
        select
              distinct a.dept_num as dept_idnt
              , a.class_num as cls_idnt
              , case when a.sbclass_num = '-1' then b.sbclass_num else a.sbclass_num end as scls_idnt
              , b.sbclass_num
              , a.category
        from prd_nap_usr_vws.catg_subclass_map_dim a
        inner join prd_nap_usr_vws.product_sku_dim_vw b
          on a.dept_num = b.dept_num
          and a.class_num = b.class_num
        where a.sbclass_num = '-1' or (a.sbclass_num = b.sbclass_num)
    ) cat
        on sku.dept_num = cat.dept_idnt
        and sku.class_num = cat.cls_idnt
        and sku.sbclass_num = cat.scls_idnt
    left join (
          select
              acp_id,
              max(cast(trans_utc_tmstp as date)) as acq_dt
          from prd_nap_usr_vws.customer_ntn_fact
          group by 1
    ) acq
        on hdr.acp_id = acq.acp_id
    left join prd_nap_usr_vws.day_cal dt
        on coalesce(dtl.tran_date,dtl.business_day_date) = dt.day_date
    where coalesce(dtl.tran_date,dtl.business_day_date)
        between (
            select
                distinct month_start_day_date
            from run_dates
          ) and (
            select
                distinct month_end_day_date
            from run_dates
          )
        and dtl.error_flag = 'N'
        and dtl.tran_latest_version_ind = 'Y'
        and not business_unit_desc is null
        and dtl.line_net_usd_amt > 0
        and dtl.line_item_quantity > 0
        and hdr.acp_id is not null
        and brand_name is not null
    group by 1,2,3,4,5,6,7,8
) with data primary index(trip_id)
 partition by range_n(month_num  between 201901 and 203012 each 1)
 on commit preserve rows;


/* Join across orders to get final output and insert */
delete from {environment_schema}.cross_shopping_monthly where month_num in (select month_num from trip_summary);

insert into {environment_schema}.cross_shopping_monthly
    select
        a.month_num,
        a.year_num,
        a.channel,
        case when a.ntn = 1 then 'NEW' else 'EXISTING' end as ntn,
        a.division as source_division,
        b.division as cross_division,
        a.dept as source_dept,
        b.dept as cross_dept,
        a.category as source_category,
        b.category as cross_category,
        upper(a.brand_name) as source_brand,
        upper(b.brand_name) as cross_brand,
        count(distinct a.trip_id) as source_trips,
        sum(a.sales) as source_spend,
        sum(a.quantity) as source_quantity,
        count(distinct b.trip_id) as cross_trips,
        sum(b.sales) as cross_spend,
        sum(b.quantity) as cross_quantity
    from trip_summary a
    left join trip_summary b
    on a.trip_id = b.trip_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12
;

COLLECT STATISTICS COLUMN(month_num), COLUMN(year_num), COLUMN(channel), COLUMN(ntn), COLUMN(source_dept), COLUMN(cross_dept), COLUMN(source_brand), COLUMN(cross_brand)
 on {environment_schema}.cross_shopping_monthly;
