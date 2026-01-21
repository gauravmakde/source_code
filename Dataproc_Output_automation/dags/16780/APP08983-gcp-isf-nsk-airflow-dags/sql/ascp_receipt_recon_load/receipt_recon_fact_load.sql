--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : receipt_recon_fact_load.sql
-- Description             : deletes from RECEIPT_RECON_FACT & RECEIPT_RECON_PO_FACT
--                           Then runs an insert query into both the tables
-- Reference Documentation : https://confluence.nordstrom.com/display/SCNAP/Optimization+-+Solution+approach
--/***********************************************************************************
-- 2024-02-22  Josh Roter  FA-11686: ascp_receipt_recon_load_15850_TECH_SC_NAP_insights to NSK
--/***********************************************************************************
---- DELETE & INSERT into RECEIPT_RECON_PO_FACT
--************************************************************************************/
LOCK TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_PO_FACT FOR ACCESS
DELETE `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_PO_FACT;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_PO_FACT
  SELECT
    hdr_vw.fiscal_year_num
    ,hdr_vw.fiscal_month_num
    ,hdr_vw.month_start_day_date
    ,hdr_vw.month_end_day_date
    ,hdr_vw.purchase_order_number
    ,item_vw.rms_sku_num
    ,ship_vw.ship_location_id
    ,hdr_vw.epo_purchase_order_id
    ,item_vw.sku_num
    ,hdr_vw.order_from_vendor_id
    ,hdr_vw.department_id
    ,hdr_vw.channel_brands
    ,hdr_vw.cross_reference_external_id
    ,hdr_vw.edi_ind
    ,hdr_vw.exclude_from_backorder_avail_ind
    ,hdr_vw.import_country
    ,hdr_vw.include_on_order_ind
    ,hdr_vw.internal_po_ind
    ,hdr_vw.npg_ind
    ,hdr_vw.order_type
    ,hdr_vw.po_type
    ,hdr_vw.purchase_type
    ,hdr_vw.selling_channel
    ,hdr_vw.written_date
    ,hdr_vw.start_ship_date
    ,hdr_vw.end_ship_date
    ,hdr_vw.original_end_ship_date
    ,hdr_vw.first_approval_event_tmstp_pacific
    ,hdr_vw.latest_approval_event_tmstp_pacific
    ,hdr_vw.first_close_event_tmstp_pacific
    ,hdr_vw.BOM_otb_eow_date
    ,hdr_vw.BOM_status
    ,hdr_vw.EOM_otb_eow_date
    ,hdr_vw.EOM_status
    ,item_vw.initial_unit_cost
    ,item_vw.initial_unit_cost_currency
    ,item_vw.BOM_approved_unit_cost
    ,item_vw.BOM_unit_cost
    ,item_vw.BOM_total_duty_per_unit
    ,item_vw.EOM_approved_unit_cost
    ,item_vw.EOM_unit_cost
    ,item_vw.EOM_total_duty_per_unit
    ,ship_vw.original_quantity_ordered
    ,ship_vw.BOM_quantity_ordered
    ,ship_vw.BOM_quantity_canceled
    ,ship_vw.BOM_approved_quantity_ordered
    ,ship_vw.BOM_total_expenses_per_unit
    ,ship_vw.EOM_quantity_ordered
    ,ship_vw.EOM_quantity_canceled
    ,ship_vw.EOM_approved_quantity_ordered
    ,ship_vw.EOM_total_expenses_per_unit
    ,ship_vw.total_expenses_per_unit_currency
	,hdr_vw.first_approval_event_tmstp_pacific_tz
	,hdr_vw.latest_approval_event_tmstp_pacific_tz
	,hdr_vw.first_close_event_tmstp_pacific_tz
  FROM (
    SELECT
      fd.fiscal_year_num
        ,fd.fiscal_month_num
        ,fd.month_start_day_date
        ,fd.month_end_day_date
        ,last_value(purchase_order_number ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as purchase_order_number
        ,epo_purchase_order_id
        ,last_value(order_from_vendor_id ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as order_from_vendor_id
        ,last_value(department_id ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as department_id
        ,last_value(channel_brands ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as channel_brands
        ,last_value(cross_reference_external_id ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as cross_reference_external_id
        ,last_value(edi_ind ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as edi_ind
        ,last_value(exclude_from_backorder_avail_ind ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as exclude_from_backorder_avail_ind
        ,last_value(import_country ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as import_country
        ,last_value(include_on_order_ind ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as include_on_order_ind
        ,last_value(internal_po_ind ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as internal_po_ind
        ,last_value(npg_ind ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as npg_ind
        ,last_value(order_type ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as order_type
        ,last_value(po_type ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as po_type
        ,last_value(purchase_type ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as purchase_type
        ,last_value(selling_channel ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as selling_channel
        ,last_value(written_date ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as written_date
        ,last_value(start_ship_date ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as start_ship_date
        ,last_value(end_ship_date ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as end_ship_date
        ,last_value(latest_ship_date ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as original_end_ship_date
        ,first_value(case when event_name = 'PurchaseOrderApproved' then event_time at 'America Pacific' end ignore nulls) over (partition by epo_purchase_order_id order by event_time rows between unbounded preceding and unbounded following) as first_approval_event_tmstp_pacific
        ,last_value(case when event_name = 'PurchaseOrderApproved' then event_time at 'America Pacific' end ignore nulls) over (partition by epo_purchase_order_id order by event_time rows between unbounded preceding and unbounded following) as latest_approval_event_tmstp_pacific
		,`{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as first_approval_event_tmstp_pacific_tz
		,`{{params.gcp_project_id}}`.jwn_udf.default_tz_pst()  as latest_approval_event_tmstp_pacific_tz
		,`{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as first_close_event_tmstp_pacific_tz
        ,first_value(case when event_name = 'PurchaseOrderClosed' then event_time at 'America Pacific' end ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as first_close_event_tmstp_pacific
        ,last_value(case when event_date_pacific <= fd.bom_cutoff_date then otb_eow_date end ignore nulls) over (partition by fd.fiscal_year_num, fd.fiscal_month_num, epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as BOM_otb_eow_date
        ,last_value(case when event_date_pacific <= fd.bom_cutoff_date then status end ignore nulls) over (partition by fd.fiscal_year_num, fd.fiscal_month_num, epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as BOM_status
        ,last_value(case when event_date_pacific <= fd.eom_cutoff_date then otb_eow_date end ignore nulls) over (partition by fd.fiscal_year_num, fd.fiscal_month_num, epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as EOM_otb_eow_date
        ,last_value(case when event_date_pacific <= fd.eom_cutoff_date then status end ignore nulls) over (partition by fd.fiscal_year_num, fd.fiscal_month_num, epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) as EOM_status
      FROM (
        SELECT distinct
          fiscal_year_num,
            fiscal_month_num,
            month_start_day_date,
            month_end_day_date,
            month_start_day_date as bom_cutoff_date,
            month_end_day_date as eom_cutoff_date
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.DAY_CAL_454_DIM
          where day_date between current_date - 90 and current_date
      ) fd
      CROSS JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PURCHASE_ORDER_HEADER_EVENT_FACT h
      qualify row_number() over (partition by fd.fiscal_year_num, fd.fiscal_month_num, epo_purchase_order_id order by revision_id desc, event_time desc) = 1
        and last_value(include_on_order_ind ignore nulls) over (partition by epo_purchase_order_id order by revision_id, event_time rows between unbounded preceding and unbounded following) = 'T'
    ) hdr_vw
    CROSS JOIN (
    SELECT
      fd.fiscal_year_num
          ,fd.fiscal_month_num
          ,i.epo_purchase_order_id
          ,last_value(case when i.external_sku_type = 'RMS' then i.external_sku_num end) over (partition by i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as rms_sku_num
          ,i.sku_num
          ,last_value(i.initial_unit_cost ignore nulls) over (partition by i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as initial_unit_cost
          ,last_value(i.initial_unit_cost_currency_code ignore nulls) over (partition by i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as initial_unit_cost_currency
          ,last_value(case when i.event_name = 'PurchaseOrderApproved' and i.event_date_pacific <= fd.bom_cutoff_date then i.unit_cost end ignore nulls)
              over (partition by fd.fiscal_year_num, fd.fiscal_month_num, i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as BOM_approved_unit_cost
          ,last_value(case when i.event_date_pacific <= fd.bom_cutoff_date then i.unit_cost end ignore nulls)
              over (partition by fd.fiscal_year_num, fd.fiscal_month_num, i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as BOM_unit_cost
          ,last_value(case when i.event_name = 'PurchaseOrderApproved' and i.event_date_pacific <= fd.bom_cutoff_date then i.total_duty_per_unit end ignore nulls)
              over (partition by fd.fiscal_year_num, fd.fiscal_month_num, i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as BOM_total_duty_per_unit
          ,last_value(case when i.event_name = 'PurchaseOrderApproved' and i.event_date_pacific <= fd.eom_cutoff_date then i.unit_cost end ignore nulls)
              over (partition by fd.fiscal_year_num, fd.fiscal_month_num, i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as EOM_approved_unit_cost
          ,last_value(case when i.event_date_pacific <= fd.eom_cutoff_date then i.unit_cost end ignore nulls)
              over (partition by fd.fiscal_year_num, fd.fiscal_month_num, i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as EOM_unit_cost
          ,last_value(case when i.event_name = 'PurchaseOrderApproved' and i.event_date_pacific <= fd.eom_cutoff_date then i.total_duty_per_unit end ignore nulls)
              over (partition by fd.fiscal_year_num, fd.fiscal_month_num, i.epo_purchase_order_id, i.sku_num order by i.revision_id rows between unbounded preceding and unbounded following) as EOM_total_duty_per_unit
      FROM (
        SELECT distinct
          fiscal_year_num,
            fiscal_month_num,
            month_start_day_date,
            month_end_day_date,
            month_start_day_date as bom_cutoff_date,
            month_end_day_date as eom_cutoff_date
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.DAY_CAL_454_DIM
          where day_date between current_date - 90 and current_date
      ) fd
      CROSS JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PURCHASE_ORDER_ITEM_EVENT_FACT i
        where i.case_pack_ind = 'F'
        QUALIFY row_number() over (partition by fd.fiscal_year_num, fd.fiscal_month_num, i.epo_purchase_order_id, i.sku_num order by i.revision_id, i.event_time) = 1
      ) item_vw
      CROSS JOIN (
      SELECT
        fd.fiscal_year_num
        ,fd.fiscal_month_num
        ,s.epo_purchase_order_id
        ,s.ship_location_id
        ,s.sku_num
        ,last_value(s.original_quantity_ordered ignore nulls)
                over (partition by s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as original_quantity_ordered
        ,last_value(case when s.event_date_pacific <= fd.bom_cutoff_date then s.quantity_ordered end ignore nulls)
                over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as BOM_quantity_ordered
        ,last_value(case when s.event_date_pacific <= fd.bom_cutoff_date then s.quantity_canceled end ignore nulls)
                over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as BOM_quantity_canceled
        ,last_value(case when s.event_name = 'PurchaseOrderApproved' and s.event_date_pacific <= fd.bom_cutoff_date then s.quantity_ordered end ignore nulls)
                over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as BOM_approved_quantity_ordered
        ,last_value(case when s.event_date_pacific <= fd.bom_cutoff_date then s.total_expenses_per_unit end ignore nulls)
                over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as BOM_total_expenses_per_unit
        ,last_value(case when s.event_date_pacific <= fd.eom_cutoff_date then s.quantity_ordered end ignore nulls)
                over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as EOM_quantity_ordered
        ,last_value(case when s.event_date_pacific <= fd.eom_cutoff_date then s.quantity_canceled end ignore nulls)
                over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as EOM_quantity_canceled
        ,last_value(case when s.event_name = 'PurchaseOrderApproved' and s.event_date_pacific <= fd.eom_cutoff_date then s.quantity_ordered end ignore nulls)
                over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as EOM_approved_quantity_ordered
        ,last_value(case when s.event_date_pacific <= fd.eom_cutoff_date then s.total_expenses_per_unit end ignore nulls)
                over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as EOM_total_expenses_per_unit
        ,last_value(s.total_expenses_per_unit_currency_code ignore nulls)
                over (partition by s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time rows between unbounded preceding and unbounded following) as total_expenses_per_unit_currency
            FROM (
          SELECT distinct
              fiscal_year_num,
              fiscal_month_num,
              month_start_day_date,
              month_end_day_date,
              month_start_day_date as bom_cutoff_date,
              month_end_day_date as eom_cutoff_date
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.DAY_CAL_454_DIM
            where day_date between current_date - 90 and current_date
        ) fd
        CROSS JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PURCHASE_ORDER_SHIPLOCATION_EVENT_FACT s
        qualify row_number() over (partition by fd.fiscal_year_num, fd.fiscal_month_num, s.epo_purchase_order_id, s.sku_num, s.ship_location_id order by s.revision_id, s.event_time) = 1
        ) ship_vw
        WHERE
        item_vw.fiscal_year_num = hdr_vw.fiscal_year_num
         AND item_vw.fiscal_month_num = hdr_vw.fiscal_month_num
         AND item_vw.epo_purchase_order_id = hdr_vw.epo_purchase_order_id
         AND ship_vw.fiscal_year_num = item_vw.fiscal_year_num
         AND ship_vw.fiscal_month_num = item_vw.fiscal_month_num
         AND ship_vw.epo_purchase_order_id = item_vw.epo_purchase_order_id
         AND ship_vw.sku_num = item_vw.sku_num;

COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_PO_FACT INDEX (epo_purchase_order_id,fiscal_year_num,fiscal_month_num,sku_num);
--/***********************************************************************************
---- DELETE & INSERT into RECEIPT_RECON_FACT
--************************************************************************************/
LOCK TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_FACT FOR ACCESS
DELETE `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_FACT;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_FACT
with vasn as (
  select
    purchase_order_num,
    max(edi_date) as latest_edi_date
  from `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.VENDOR_ASN_FACT
  group by purchase_order_num
),
price as (
  select
    rms_sku_num
    ,selling_channel
    ,channel_brand
    ,channel_country
    ,ownership_retail_price_amt
    ,regular_price_amt
    ,selling_retail_price_amt
  from `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_PRICE_TIMELINE_DIM price
  where current_timestamp between eff_begin_tmstp and eff_end_tmstp
)
select
   po.fiscal_year_num
    ,po.fiscal_month_num
    ,po.month_start_day_date
    ,po.month_end_day_date
    ,po.purchase_order_number
    ,po.rms_sku_num
    ,po.ship_location_id
    ,po.epo_purchase_order_id
    ,po.sku_num
    ,po.order_from_vendor_id
    ,po.department_id
    ,po.channel_brands
    ,po.cross_reference_external_id
    ,po.edi_ind
    ,po.exclude_from_backorder_avail_ind
    ,po.import_country
    ,po.include_on_order_ind
    ,po.internal_po_ind
    ,po.npg_ind
    ,po.order_type
    ,po.po_type
    ,po.purchase_type
    ,po.selling_channel
    ,po.written_date
    ,po.start_ship_date
    ,po.end_ship_date
    ,po.original_end_ship_date
    ,po.first_approval_event_tmstp_pacific
	,po.first_approval_event_tmstp_pacific_tz
    ,po.latest_approval_event_tmstp_pacific
	,po.latest_approval_event_tmstp_pacific_tz
    ,po.first_close_event_tmstp_pacific
	,po.first_close_event_tmstp_pacific_tz
    ,po.BOM_otb_eow_date
    ,po.BOM_status
    ,po.EOM_otb_eow_date
    ,po.EOM_status
    ,po.initial_unit_cost
    ,po.initial_unit_cost_currency
    ,po.BOM_approved_unit_cost
    ,po.BOM_unit_cost
    ,po.BOM_total_duty_per_unit
    ,po.EOM_approved_unit_cost
    ,po.EOM_unit_cost
    ,po.EOM_total_duty_per_unit
    ,po.original_quantity_ordered
    ,po.BOM_quantity_ordered
    ,po.BOM_quantity_canceled
    ,po.BOM_approved_quantity_ordered
    ,po.BOM_total_expenses_per_unit
    ,po.EOM_quantity_ordered
    ,po.EOM_quantity_canceled
    ,po.EOM_approved_quantity_ordered
    ,po.EOM_total_expenses_per_unit
    ,po.total_expenses_per_unit_currency
    ,case when vasn.latest_edi_date is not null then 'Y' else 'N' end as po_vasn_signal
    ,vasn.latest_edi_date
    ,vasn_dtl.BOM_vasn_qty
    ,vasn_dtl.EOM_vasn_qty
    ,receipt.BOM_receipt_qty
    ,receipt.EOM_receipt_qty
    ,price.ownership_retail_price_amt as ownership_price_amt
    ,price.regular_price_amt
    ,price.selling_retail_price_amt as current_price_amt
from `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_PO_FACT po
join `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.STORE_DIM  org
  on po.ship_location_id = org.STORE_NUM
left join vasn
  on po.purchase_order_number = vasn.purchase_order_num
left join `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_VWS.RECEIPT_RECON_VASN_VW vasn_dtl
  on po.fiscal_year_num = vasn_dtl.fiscal_year_num
  and po.fiscal_month_num = vasn_dtl.fiscal_month_num
  and po.purchase_order_number = vasn_dtl.purchase_order_num
  and trim(leading '0' from po.rms_sku_num) = trim(leading '0' from vasn_dtl.rms_sku_num)
  and vasn_dtl.receiving_location_id = (
    case when po.ship_location_id in (869, 896, 891, 859, 889) then 868
    when org.STORE_TYPE_CODE in ('RR','RS','WH') and (po.EOM_quantity_ordered <> 0 or po.BOM_quantity_ordered <> 0)  then org.DISTRIBUTION_CENTER_NUM
    else po.ship_location_id end)
left join `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_VWS.RECEIPT_RECON_RECEIPT_VW receipt
  on po.fiscal_year_num = receipt.fiscal_year_num
  and po.fiscal_month_num = receipt.fiscal_month_num
  and po.purchase_order_number = receipt.purchase_order_number
  and trim(leading '0' from po.rms_sku_num) = receipt.rms_sku_num
  and cast(receipt.ship_location_id as integer) = (
    case when po.ship_location_id = 563 then 599
    when po.ship_location_id = 879 then 873
    when po.ship_location_id = 562 then 569
    when po.ship_location_id = 891 then 891  --RR
    when po.ship_location_id = 859 then 859  --RS
    when po.ship_location_id = 889 then 889  --WH
    when org.STORE_TYPE_CODE in ('RR','RS','WH') and (po.EOM_quantity_ordered <> 0 or po.BOM_quantity_ordered <> 0)  then org.DISTRIBUTION_CENTER_NUM
    else cast(po.ship_location_id as integer) end)
left join price
  on po.rms_sku_num = price.rms_sku_num
  and price.selling_channel = (
    case when po.selling_channel = 'ONLINE' then 'ONLINE'
    else 'STORE' end)
  and price.channel_brand = (
    case when po.channel_brands = '["NORDSTROM_RACK"]' then 'NORDSTROM_RACK'
    else 'NORDSTROM' end)
  and price.channel_country = po.import_country;

COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RECEIPT_RECON_FACT INDEX (ship_location_id,purchase_order_number,fiscal_year_num,fiscal_month_num);