SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=empdisc_pct_item_trans_11521_ACE_ENG;
     Task_Name=empdisc_pct_item_trans;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_TRUST_EMP.empdisc_pct_item_trans{t2_test}
Team/Owner: Rujira Achawanantakun (rujira.achawanantakun@nordstrom.com)
Date Created/Modified: 4/4/2024

Note:
-- Purpose of the table: to capture item level transaction associated to using
employee discount at item level
-- Update Cadence: Daily
*/

-- create employee based table
create multiset volatile table emp as (
    select distinct e1.worker_number
           ,e1.discount_status
           ,e1.discount_percent
           ,e1.worker_status
           ,e1.pay_rate_type
           ,e1.worker_sub_type
           ,e1.manager_worker_number
           ,e1.hire_status
           ,e1.hire_date
           ,e1.termination_date
           ,e1.location_number --store number
           ,store.business_unit_desc -- in the case that get hired > 1 times into different business units
    from PRD_NAP_HR_USR_VWS.HR_WORKER_V1_DIM e1
       -- get the latest (current) employee information based on max(hire_date)
       right join (select worker_number, max(hire_date) latest_hire_date
                   from PRD_NAP_HR_USR_VWS.HR_WORKER_V1_DIM
                   group by worker_number
                   ) e2
           on e2.worker_number = e1.worker_number
           and e2.latest_hire_date = e1.hire_date
       -- get store info
       left join PRD_NAP_USR_VWS.JWN_STORE_DIM_VW store
            on CAST(store.store_num AS char(5))=CAST(e1.location_number AS char(5))
            and e1.location_number IS NOT NULL --must have, because some employee records have NULL value
    where e1.worker_number IS NOT NULL
) with data primary index(worker_number) on commit preserve rows;
;

-- create sku based table
create multiset volatile table sku_base as (
    SELECT
        sku.rms_sku_num
        , sku.web_style_num
        , sty.style_group_num
        , sty.style_group_desc
        , CAST(sku.div_num AS VARCHAR(20)) || ', ' || sku.div_desc AS division
        , CAST(sku.grp_num AS VARCHAR(20)) || ', ' || sku.grp_desc AS subdiv
        , CAST(sku.dept_num AS VARCHAR(20)) || ', ' || sku.dept_desc AS department
        , CAST(sku.class_num AS VARCHAR(20)) || ', ' || sku.class_desc AS "class"
        , CAST(sku.sbclass_num AS VARCHAR(20)) || ', ' || sku.sbclass_desc AS subclass
        , UPPER(sku.brand_name) AS brand_name
        , UPPER(supp.vendor_name) as supplier
        , UPPER(sty.type_level_1_desc) AS product_type_1
        , UPPER(sty.style_desc) AS style_desc
        , sku.color_desc
        , sku.nord_display_color
        , sku.size_1_num
        , sku.size_1_desc
        , sku.size_2_num
        , sku.size_2_desc
        , sku.supp_color
        , sku.supp_size
        , sku.drop_ship_eligible_ind -- 'Y', 'N'
    FROM
        prd_nap_usr_vws.product_sku_dim_vw sku
        INNER JOIN prd_nap_usr_vws.product_style_dim sty
            ON sku.epm_style_num = sty.epm_style_num
            AND sku.channel_country = sty.channel_country
        LEFT JOIN prd_nap_usr_vws.vendor_dim supp
            ON sku.prmy_supp_num =supp.vendor_num
        --Exclude Gift with purchase (GWP)
        WHERE NOT sku.dept_num IN ('584', '585', '523') AND NOT (sku.div_num = '340' AND sku.class_num = '90')
            AND sku.channel_country = 'US'
) with data primary index(rms_sku_num) on commit preserve rows;

collect stats primary index(rms_sku_num), column(rms_sku_num) on sku_base;

-- create store and region based table
CREATE MULTISET VOLATILE TABLE dma_base AS (
select
    distinct dma.store_num-- AS "Store Number"
    ,dma.store_dma_code-- as "DMA Code"
    ,mkt.dma_desc
    ,TRIM(dma.store_num||', '||dma.store_short_name) as location
    ,dma.store_type_desc
    ,dma.gross_square_footage
    ,dma.store_open_date
    ,dma.store_close_date
    ,dma.region_desc
    ,dma.region_medium_desc
    ,dma.region_short_desc
    ,dma.business_unit_desc
    ,dma.group_desc
    ,dma.subgroup_desc
    ,dma.subgroup_medium_desc
    ,dma.subgroup_short_desc
    ,dma.store_address_line_1
    ,dma.store_address_city
    ,dma.store_address_state
    ,dma.store_address_state_name
    ,dma.store_postal_code
    ,dma.store_address_county
    ,dma.store_country_code
    ,dma.store_country_name
    ,dma.store_location_latitude
    ,dma.store_location_longitude
    ,dma.distribution_center_num
    ,dma.distribution_center_name
    ,dma.channel_desc
    ,dma.comp_status_desc
    FROM PRD_NAP_USR_VWS.STORE_DIM DMA
    LEFT JOIN PRD_NAP_USR_VWS.ORG_DMA MKT
        ON DMA.store_dma_code = MKT.dma_code
) with data primary index(store_num) on commit preserve rows;

-- insert data into the table
INSERT INTO T2DL_DAS_TRUST_EMP.empdisc_pct_item_trans{t2_test}
select d.*
FROM (
  select dtl.employee_discount_num emp_number
	,dtl.global_tran_id
	,dtl.business_day_date
	,CAST(ROUND(dtl.employee_discount_usd_amt * -100.0 / (dtl.line_net_usd_amt - dtl.employee_discount_usd_amt)) AS INTEGER) use_discount_pct
	,e1.worker_number e_worker_number
    ,e1.discount_status e_discount_status
    ,e1.discount_percent e_discount_percent
    ,e1.worker_status e_worker_status
    ,e1.pay_rate_type e_pay_rate_type
    ,e1.worker_sub_type e_worker_sub_type
    ,e1.manager_worker_number e_manager_worker_number
    ,e1.hire_status e_hire_status
    ,e1.hire_date e_hire_date
    ,e1.termination_date e_termination_date
    ,e1.location_number e_location_number --store number
    ,e1.business_unit_desc e_business_unit_desc -- in the case that get hir
    ,cal.week_num
    ,cal.month_num
    ,cal.year_num
    ,cal.week_of_fyr
    ,cal.day_short_desc
    ,cal.month_short_desc
	,dtl.line_item_seq_num
	,dtl.unique_source_id
	,dtl.acp_id
	,dtl.marketing_profile_type_ind
	,dtl.deterministic_profile_id
	,dtl.ringing_store_num
	,dtl.fulfilling_store_num
	,dtl.intent_store_num
	,dtl.claims_destination_store_num
	,dtl.register_num
	,dtl.tran_num
	,dtl.tran_version_num
	,dtl.sa_tran_status_code
	,dtl.original_global_tran_id
	,dtl.reversal_flag
	,dtl.data_source_code
	,dtl.pre_sale_type_code
	,dtl.order_num
	,dtl.order_date
	,dtl.followup_slsprsn_num
	,dtl.pbfollowup_slsprsn_num
	,dtl.tran_time
	,dtl.sku_num
	,dtl.upc_num
    ,substring(dtl.upc_num, 1, 10) upc10_num
    ,dtl.line_item_merch_nonmerch_ind -- MERCH, NMERCH
	,dtl.merch_dept_num
     --fee code description
	,dtl.nonmerch_fee_code
    ,(CASE WHEN nonmerch_fee_code is not NULL AND dtl.business_day_date between fc.effective_date AND fc.termination_date THEN 1 ELSE 0 END) fee_code_is_valid
    ,fc.employee_discount_allowed_flag fee_code_empdisc_allowed_flag
    ,fc.price_amt fee_code_price_amt
    ,fc.fee_code_desc
    ,fc.note_text fee_code_text
    ,fc.effective_date fee_code_effective_date
    ,fc.termination_date fee_code_termination_date
    ,fc.display_at_pos_flag fee_code_display_at_pos_flag
    --line item
	,dtl.line_item_promo_id
	,dtl.line_net_usd_amt
	,dtl.line_item_quantity
	,dtl.line_item_fulfillment_type
    ,dtl.line_item_order_type -- RPOS, POS
	,dtl.commission_slsprsn_num
	,dtl.employee_discount_flag
	,dtl.employee_discount_usd_amt
	,dtl.line_item_promo_usd_amt
	,dtl.merch_unique_item_id
	,dtl.merch_price_adjust_reason
	,dtl.line_item_capture_system
	,dtl.original_business_date
	,dtl.original_ringing_store_num
	,dtl.original_register_num
	,dtl.original_tran_num
	,dtl.original_line_item_usd_amt
	,dtl.line_item_regular_price
	,dtl.line_item_regular_price_currency_code
	,dtl.error_flag
	,dtl.tran_latest_version_ind
	,dtl.item_source
	,dtl.price_adj_code
	,dtl.tran_line_id
     --bopus
    ,(CASE WHEN dtl.tran_type_code = 'SALE' AND (line_item_order_type LIKE 'CustInit%' AND
                                             line_item_fulfillment_type = 'StorePickUp' AND
                                             data_source_code = 'COM') THEN 1 ELSE 0 END) is_bopus
    --sku
    , sku.web_style_num
    , sku.style_group_num
    , sku.style_group_desc
    , sku.division
    , sku.subdiv
    , sku.department
    , sku."class"
    , sku.subclass
    , sku.brand_name
    , sku.supplier
    , sku.product_type_1
    , sku.style_desc
    , sku.color_desc
    , sku.nord_display_color
    , sku.size_1_num
    , sku.size_1_desc
    , sku.size_2_num
    , sku.size_2_desc
    , sku.supp_color
    , sku.supp_size
    , sku.drop_ship_eligible_ind
     --channel
    ,case when a1.store_type_code='NL' then 'NL'
          when a1.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FL'
          when a1.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
          when a1.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
          when a1.business_unit_desc in ('OFFPRICE ONLINE') then 'RCOM'
          else null end as ring_channel
    ,case when a2.store_type_code='NL' then 'NL'
          when a2.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FL'
          when a2.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
          when a2.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
          when a2.business_unit_desc in ('OFFPRICE ONLINE') then 'RCOM'
          else null end as fullfill_channel
    ,case when a3.store_type_code='NL' then 'NL'
          when a3.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FL'
          when a3.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
          when a3.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
          when a3.business_unit_desc in ('OFFPRICE ONLINE') then 'RCOM'
          else null end as intent_channel
    --ring store dma
    ,d1.dma_desc ring_dma
    ,d1.region_desc ring_region
    ,d1.store_address_city ring_city
    ,d1.store_address_county ring_county
    ,d1.store_address_state_name ring_state
    ,d1.store_postal_code ring_zipcode
    --product selling event
    ,p_event.selling_event_name prod_event_name
    ,p_event.channel_brand prod_event_channel_brand
    ,p_event.selling_channel prod_event_selling_channel
    --scale event
    ,sc_event.event_name sc_event_name
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
  FROM prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW AS dtl
    left join emp e1
        on e1.worker_number = dtl.employee_discount_num
    left join sku_base AS sku
        on sku.rms_sku_num = dtl.sku_num
    left join prd_nap_usr_vws.day_cal cal
        on dtl.business_day_date=cal.day_date
    left join prd_nap_usr_vws.store_dim a1
        on a1.store_num = dtl.ringing_store_num
    left join prd_nap_usr_vws.store_dim a2
        on a2.store_num = dtl.fulfilling_store_num
    left join prd_nap_usr_vws.store_dim a3
        on a3.store_num = dtl.intent_store_num
    left join dma_base d1
        on d1.store_num = dtl.ringing_store_num
    left join (
       SELECT with_rn.*
        FROM (
            SELECT fee_code_num, employee_discount_allowed_flag,price_amt,fee_code_desc,note_text,effective_date,termination_date,display_at_pos_flag
                   ,ROW_NUMBER() OVER (PARTITION BY fee_code_num ORDER BY termination_date DESC) AS rn
            FROM PRD_NAP_USR_VWS.product_fee_code_dim
            WHERE effective_date between {start_date} AND {end_date} --limit to the events, which happen within the time period of interest
                OR termination_date between {start_date} AND {end_date} --limit to the events, which happen within the time period of interest
        ) AS with_rn
        WHERE rn = 1
    ) fc
        on fc.fee_code_num = dtl.nonmerch_fee_code
        and business_day_date BETWEEN fc.effective_date and fc.termination_date
    left join (
        SELECT selling_event_num, sku_num, channel_brand, selling_channel, selling_event_name, item_start_tmstp, item_end_tmstp
        FROM (
            SELECT selling_event_num, sku_num, channel_brand, selling_channel, selling_event_name, item_start_tmstp, item_end_tmstp,
                   ROW_NUMBER() OVER (PARTITION BY sku_num ORDER BY item_end_tmstp DESC) AS rn
            FROM PRD_NAP_USR_VWS.PRODUCT_SELLING_EVENT_SKU_DIM
            WHERE CAST(item_start_tmstp as date) between {start_date} AND {end_date} --limit to the events, which happen within the time period of interest
                OR cast(item_end_tmstp as date) between {start_date} AND {end_date} --limit to the events, which happen within the time period of interest
        ) AS p_event_with_rn
        WHERE rn = 1
      ) p_event
      on dtl.sku_num = p_event.sku_num
      and dtl.tran_time BETWEEN p_event.item_start_tmstp and p_event.item_end_tmstp
    left join PRD_NAP_USR_VWS.SCALED_EVENTS_DATES_DIM sc_event
        on dtl.business_day_date = sc_event.day_date
  WHERE dtl.business_day_date between {start_date} AND {end_date}
    and dtl.employee_discount_flag = 1
    and dtl.tran_type_code = 'SALE'
    and dtl.line_net_usd_amt > 0 --we see a few transaction that have line_net_usd_amt < 0 in restaurant transaction
    and NOT EXISTS (SELECT 1
                    FROM T2DL_DAS_TRUST_EMP.empdisc_pct_item_trans{t2_test} t
                    WHERE t.business_day_date = dtl.business_day_date
                      and CAST(t.emp_number AS BIGINT) = CAST(dtl.employee_discount_num AS BIGINT)
                      and CAST(t.global_tran_id AS BIGINT) = CAST(dtl.global_tran_id AS BIGINT)
                      and CAST(t.line_item_seq_num  AS BIGINT)= CAST(dtl.line_item_seq_num AS BIGINT)
                      )
  )d
;

COLLECT STATISTICS  COLUMN (emp_number),
                    COLUMN (e_worker_number),
                    COLUMN (business_day_date, emp_number),
                    COLUMN (data_source_code),
                    COLUMN (ringing_store_num),
                    COLUMN (fulfilling_store_num),
                    COLUMN (intent_store_num)
on T2DL_DAS_TRUST_EMP.empdisc_pct_item_trans{t2_test};

/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
