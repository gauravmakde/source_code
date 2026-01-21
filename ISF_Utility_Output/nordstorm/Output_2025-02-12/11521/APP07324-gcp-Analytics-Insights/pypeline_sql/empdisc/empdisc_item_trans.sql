SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=empdisc_item_trans_11521_ACE_ENG;
     Task_Name=empdisc_item_trans;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: empdisc_item_trans
Team/Owner: Rujira Achawanantakun (rujira.achawanantakun@nordstrom.com)
Date Created/Modified: 1/3/2023

Note:
-- Purpose of the table: to capture item level transaction associated to using
employee discount at item level transaction
-- Update Cadence: Daily
*/

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
    ,dma.store_type_desc --AS "Store Type Desc"
    ,dma.gross_square_footage-- AS "Gross Square Footage"
    ,dma.store_open_date-- AS "Store Open Date"
    ,dma.store_close_date-- AS "Store Close Date"
    ,dma.region_desc-- AS "Region Desc"
    ,dma.region_medium_desc-- AS "Region Medium Desc"
    ,dma.region_short_desc-- AS "Region Short Desc"
    ,dma.business_unit_desc --AS "Business Unit Desc"
    ,dma.group_desc-- AS "Group Desc"
    ,dma.subgroup_desc-- AS "Subgroup Desc"
    ,dma.subgroup_medium_desc-- AS "Subgrou Med Desc"
    ,dma.subgroup_short_desc-- AS "Subgroup Short Desc"
    ,dma.store_address_line_1-- AS "Store Address Line 1"
    ,dma.store_address_city-- AS "Store Address City"
    ,dma.store_address_state-- AS "Store Address State"
    ,dma.store_address_state_name-- AS "Store Address State Name"
    ,dma.store_postal_code-- AS "Store Postal Code"
    ,dma.store_address_county-- AS "Store Address County"
    ,dma.store_country_code-- AS "Store Country Code"
    ,dma.store_country_name-- AS "Store Country Name"
    ,dma.store_location_latitude-- AS "Latitude"
    ,dma.store_location_longitude-- AS "Longitude"
    ,dma.distribution_center_num-- AS "Distribution Center Num"
    ,dma.distribution_center_name-- AS "Distribution Center Name"
    ,dma.channel_desc-- AS "Channel Desc"
    ,dma.comp_status_desc-- AS "Comp Status Desc"
    FROM PRD_NAP_USR_VWS.STORE_DIM DMA
    LEFT JOIN PRD_NAP_USR_VWS.ORG_DMA MKT
        ON DMA.store_dma_code = MKT.dma_code
) with data primary index(store_num) on commit preserve rows;

-- insert data into the table
INSERT INTO {empdisc_t2_schema}.empdisc_item_trans
SELECT d.*
FROM (
  SELECT dtl.employee_discount_num emp_number
	,dtl.global_tran_id
	,dtl.business_day_date
    ,cal.week_num
    ,cal.month_num
    ,cal.year_num
	,dtl.tran_type_code
	,dtl.line_item_seq_num
	,dtl.line_item_activity_type_desc
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
    --tender (item level)
    ,ttf.tran_type_code tender_tran_type_code
    ,ttf.tender_item_seq_num
    ,ttf.tran_version_num tender_tran_version_num
    ,ttf.tran_time tender_tran_time
    ,ttf.tender_type_code
    ,ttf.card_subtype_code
    ,ttf.card_type_code
    ,ttf.tender_item_auth_type
    ,ttf.tender_item_protection_method
    ,ttf.tender_item_protection_source
    ,ttf.tender_item_usd_amt
    ,ttf.tender_item_entry_method_code
    ,ttf.tender_item_activity_code
    ,ttf.third_party_gift_card_ind
    ,ttf.error_flag tender_error_flag
    ,ttf.tender_item_account_number_v2
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
    ,case when a4.store_type_code='NL' then 'NL'
          when a4.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FL'
          when a4.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
          when a4.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
          when a4.business_unit_desc in ('OFFPRICE ONLINE') then 'RCOM'
          else null end as org_ring_channel
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
    ,d1.store_address_state_name ring_state
    ,d1.store_address_county ring_county
    ,d1.store_postal_code ring_zipcode
     --original store dma
    ,d2.dma_desc org_ring_dma
    ,d2.region_desc org_ring_region
    ,d2.store_address_city org_ring_city
    ,d2.store_address_state_name org_ring_state
    ,d2.store_address_county org_ring_county
    ,d2.store_postal_code org_ring_zipcode
    ,(case when dtl.tran_type_code = 'RETN' AND dtl.original_business_date is not NULL then (dtl.business_day_date - dtl.original_business_date) else NULL end) retn_delta
    ,(case when dtl.tran_type_code = 'EXCH' AND dtl.original_business_date is not NULL then (dtl.business_day_date - dtl.original_business_date) else NULL end) exch_delta
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
  FROM prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW AS dtl
    left join prd_nap_usr_vws.retail_tran_tender_fact ttf
        on ttf.business_day_date = dtl.business_day_date
        and ttf.global_tran_id = dtl.global_tran_id
        and ttf.tender_item_seq_num = dtl.line_item_seq_num
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
    left join prd_nap_usr_vws.store_dim a4
        on a4.store_num = dtl.original_ringing_store_num
    left join dma_base d1
        on d1.store_num = dtl.ringing_store_num
    left join dma_base d2
        on d2.store_num = dtl.original_ringing_store_num
    left join prd_nap_usr_vws.product_fee_code_dim fc
        on fc.fee_code_num = dtl.nonmerch_fee_code
  WHERE dtl.business_day_date BETWEEN {start_date} AND {end_date}
    and dtl.employee_discount_flag = 1
    and NOT EXISTS (SELECT 1
        FROM {empdisc_t2_schema}.empdisc_item_trans t
        WHERE t.emp_number = CAST(dtl.employee_discount_num AS BIGINT)
            and t.global_tran_id = CAST(dtl.global_tran_id AS BIGINT)
	        and t.line_item_seq_num = CAST(dtl.line_item_seq_num AS BIGINT)
        )
)d
;

COLLECT STATISTICS  COLUMN (emp_number),
                    COLUMN (global_tran_id),
                    COLUMN (order_num),
                    COLUMN (acp_id),
                    COLUMN (deterministic_profile_id),
                    COLUMN (ringing_store_num),
                    COLUMN (fulfilling_store_num),
                    COLUMN (intent_store_num)
on {empdisc_t2_schema}.empdisc_item_trans;

/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
