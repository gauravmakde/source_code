/*
Purpose:        Creates table for NSO dash 
                    new_store_opening
				Grabs metrics needed for the NSO dash from various tables and joins them together along with location and RP plan data
Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING  (prod) or T3DL_ACE_MCH
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
Date Created/Modified: 10/23/2024 
*/


--Get new stores (filtering out any non customer facing stores with the store_name filters
--filtering to only new stores that haven't opened yet with the current_Date filter).  We are including stores until the weekend after the opening day 
--after that they will fall off the report.

--DROP TABLE new_stores
CREATE MULTISET VOLATILE TABLE new_stores AS (
    SELECT 
        store_num,
        cal.month_idnt,
        cal.week_idnt,
        store_open_date,
        (week_end_day_date - INTERVAL '7' DAY) as week_before_opening_end_day_date
    FROM PRD_NAP_USR_VWS.STORE_DIM_VW s
    JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM cal
        ON cal.day_date = s.store_open_date
    WHERE channel_num = 210
        AND store_open_date >= (
            SELECT DISTINCT week_start_day_date 
            FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM 
            WHERE week_end_day_date >= CURRENT_DATE - 1
                AND week_start_day_date <= CURRENT_DATE - 1
        )
        AND store_name NOT LIKE '%CLOSED%'
        AND store_name NOT LIKE 'VS%'
        AND store_name NOT LIKE 'FINANCIAL STORE%'
        AND store_name NOT LIKE '%TEST %'
        AND store_name NOT LIKE '%CLSD%'
        AND store_name NOT LIKE '%EMPLOYEE%'
        AND store_name NOT LIKE '%BULK%'
        AND store_name NOT LIKE '%UNASSIGNED%'
) WITH DATA 
PRIMARY INDEX (store_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(STORE_NUM) ON new_stores;



--DROP TABLE dc_po;
CREATE VOLATILE MULTISET TABLE dc_po AS (
    SELECT DISTINCT
        cpi.destination_location_num AS store_num,
        cpi.carton_num,
        cpi.rms_sku_num,
        cpi.sku_qty,
        cf.original_carton_num,
        cf.purchase_order_num AS purchase_order_number,
        cf.vendor_ship_notice_date,
        cf.inbound_warehouse_received_tmstp,
        cf.final_warehouse_shipped_tmstp,
        cf.store_received_tmstp,
        cf.unit_qty
    FROM PRD_NAP_USR_VWS.WM_OUTBOUND_CARTON_PACKED_ITEMS_FACT AS cpi
    JOIN new_stores st
        ON st.store_num = cpi.destination_location_num
    LEFT JOIN PRD_NAP_USR_VWS.CARTON_LIFECYCLE_FACT_VW cf
        ON cpi.carton_num = cf.to_carton_num
        AND cpi.rms_sku_num = cf.rms_sku_num
) WITH DATA 
PRIMARY INDEX (carton_num, rms_sku_num, store_num) 
ON COMMIT PRESERVE ROWS;


-- DROP TABLE complete_sku;
CREATE VOLATILE MULTISET TABLE complete_sku AS (
    SELECT DISTINCT
        po.distribute_location_id AS store_num,
        po.rms_sku_num,
        po.purchase_order_number,
        po.otb_eow_date,
        po.po_type,
        po.status,
        po.order_type,
        cf.original_carton_num,
        cf.to_carton_num,
        cf.purchase_order_num AS po_num_cf,
        cf.vendor_ship_notice_date,
        cf.inbound_warehouse_received_tmstp,
        cf.final_warehouse_shipped_tmstp,
        cf.store_received_tmstp,
        cf.unit_qty
    FROM PRD_NAP_USR_VWS.PURCHASE_ORDER_DISTRIBUTELOCATION_FACT po
    JOIN new_stores st
        ON po.distribute_location_id = st.store_num
    LEFT JOIN PRD_NAP_USR_VWS.CARTON_LIFECYCLE_FACT_VW cf
        ON cf.purchase_order_num = po.purchase_order_number
        AND cf.rms_sku_num = po.rms_sku_num
) WITH DATA 
PRIMARY INDEX (rms_sku_num, store_num, purchase_order_number) 
ON COMMIT PRESERVE ROWS;

-- Drop table if it exists
-- DROP TABLE cf_lib;
CREATE VOLATILE MULTISET TABLE cf_lib AS (
    SELECT DISTINCT
        dc_po.store_num,
        -- m.store_num,
        dc_po.rms_sku_num,
        dc_po.carton_num,
        dc_po.purchase_order_number AS po_dc,
        cf.original_carton_num,
        -- cf.to_carton_num,
        cf.purchase_order_number,
        cf.vendor_ship_notice_date,
        cf.unit_qty,
        cf.otb_eow_date,
        cf.po_type,
        cf.status,
        cf.order_type,
        MAX(cf.inbound_warehouse_received_tmstp) AS inbound_warehouse_received_tmstp,
        MAX(cf.final_warehouse_shipped_tmstp) AS final_warehouse_shipped_tmstp,
        MAX(cf.store_received_tmstp) AS store_received_tmstp
    FROM dc_po
    LEFT JOIN complete_sku cf
        ON cf.rms_sku_num = dc_po.rms_sku_num
        AND cf.store_num = dc_po.store_num
        AND cf.purchase_order_number = dc_po.purchase_order_number
        AND cf.original_carton_num = dc_po.original_carton_num
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
) WITH DATA 
PRIMARY INDEX (rms_sku_num, carton_num) 
ON COMMIT PRESERVE ROWS;

-- Drop table if it exists
-- DROP TABLE idco;
CREATE VOLATILE MULTISET TABLE idco AS (
    SELECT DISTINCT
        EVENT_TYPE,
        PACK_TYPE,
        cf.CARTON_LPN AS CARTON_NUM,
        DESTINATION_LOCATION_ID AS STORE_NUM
    FROM PRD_NAP_USR_VWS.INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_FACT cf
    JOIN T2DL_SCA_VWS.DC_OUTBOUND_CARTON_LIFECYCLE_VW clv
        ON clv.carton_lpn = cf.carton_lpn 
        AND clv.store_destination_id = cf.destination_location_id 
    JOIN new_stores ns
        ON ns.Store_NUM = cf.destination_location_id 
    WHERE EVENT_TYPE = 'WAREHOUSE_CARTON_PALLETIZED'
        AND cf.location_id = clv.store_dc_id
) WITH DATA 
PRIMARY INDEX (carton_num, store_num) 
ON COMMIT PRESERVE ROWS;

-- Drop table if it exists
-- DROP TABLE st_rcpt;
CREATE VOLATILE MULTISET TABLE st_rcpt AS (
    SELECT DISTINCT
        cf_lib.purchase_order_number AS purchase_order_num,
        cf_lib.store_num,
        cf_lib.rms_sku_num,
        cf_lib.carton_num,
        cf_lib.otb_eow_date,
        cf_lib.po_type,
        cf_lib.status,
        -- cf_lib.asn_indicator,
        CASE cf_lib.order_type 
            WHEN 'NON_BASIC' THEN 'N/B'
            WHEN 'AUTOMATIC_REORDER' THEN 'ARB'
            WHEN 'BUYER_REORDER' THEN 'BRB'
        END AS order_type_po,
        CASE 
            WHEN cf_lib.inbound_warehouse_received_tmstp IS NOT NULL 
                AND cf_lib.final_warehouse_shipped_tmstp IS NOT NULL 
                AND cf_lib.store_received_tmstp IS NULL
            OR cf_lib.inbound_warehouse_received_tmstp IS NULL 
                AND cf_lib.final_warehouse_shipped_tmstp IS NOT NULL 
                AND cf_lib.store_received_tmstp IS NULL 
            THEN cf_lib.unit_qty 
        END AS shipment_qty_intransit,
        CASE 
            WHEN cf_lib.store_received_tmstp IS NOT NULL 
            THEN cf_lib.unit_qty 
        END AS shipment_qty_st_rcvd,
        CASE 
            WHEN cf_lib.inbound_warehouse_received_tmstp IS NOT NULL 
                AND cf_lib.final_warehouse_shipped_tmstp IS NULL 
                AND cf_lib.store_received_tmstp IS NULL 
            THEN cf_lib.unit_qty 
        END AS shipment_qty_in_dc,
        CASE 
            WHEN idco.carton_num IS NOT NULL 
                AND cf_lib.inbound_warehouse_received_tmstp IS NOT NULL 
                AND cf_lib.final_warehouse_shipped_tmstp IS NULL 
                AND cf_lib.store_received_tmstp IS NULL
            THEN cf_lib.unit_qty 
        END AS palletized_units   FROM cf_lib
    LEFT JOIN idco 
        ON idco.carton_num = cf_lib.carton_num 
        AND idco.store_num = cf_lib.store_num
) WITH DATA 
PRIMARY INDEX (purchase_order_num, store_num, rms_sku_num, carton_num) 
ON COMMIT PRESERVE ROWS;


-- Getting OO and allocated quantities - only grabbing OO if it's OTB EOW Date is less than 4 weeks prior to the store opening date 
-- This is the standard cutoff time for OO that was decided on by the team
-- DROP TABLE oo;
CREATE VOLATILE MULTISET TABLE oo AS (
    SELECT DISTINCT
        ab.purchase_order_number AS purchase_order_num,
        CAST(ab.store_num AS INT) AS store_num,
        ab.rms_sku_num,
        ab.otb_eow_date,
        new_stores.month_idnt,
        ab.po_type,
        ab.status,
        CASE ab.order_type 
            WHEN 'NON_BASIC' THEN 'N/B'
            WHEN 'AUTOMATIC_REORDER' THEN 'ARB'
            WHEN 'BUYER_REORDER' THEN 'BRB'
        END AS order_type_po,
        CAST(0 AS DECIMAL(20,4)) AS quantity_allocated,
        CAST(0 AS DECIMAL(20,4)) AS approved_quantity_allocated_oo,
        SUM(ab.quantity_open) AS open_oo_u_oo,
        CASE 
            WHEN SUM(ab.quantity_open) < 0 THEN 0 
            ELSE SUM(ab.quantity_open) 
        END AS on_order_u_oo
    FROM PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW AS ab
    INNER JOIN new_stores 
        ON new_stores.store_num = ab.store_num
        AND otb_eow_date <= (new_stores.store_open_date - INTERVAL '28' DAY)
    WHERE ab.status IN ('APPROVED', 'CLOSED', 'WORKSHEET')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

    UNION ALL

    SELECT DISTINCT 
        ab.purchase_order_number AS purchase_order_num,
        CAST(ab.distribute_location_id AS INT) AS store_num,
        ab.rms_sku_num,
        ab.otb_eow_date,
        new_stores.month_idnt,
        ab.po_type,
        ab.status,
        CASE ab.order_type 
            WHEN 'NON_BASIC' THEN 'N/B'
            WHEN 'AUTOMATIC_REORDER' THEN 'ARB'
            WHEN 'BUYER_REORDER' THEN 'BRB'
        END AS order_type_po,
        SUM(ab.quantity_allocated) AS quantity_allocated,
        SUM(ab.approved_quantity_allocated) AS approved_quantity_allocated_oo,
        CAST(0 AS DECIMAL(20,4)) AS open_oo_u_oo,
        CAST(0 AS DECIMAL(20,4)) AS on_order_u_oo
    FROM PRD_NAP_USR_VWS.PURCHASE_ORDER_DISTRIBUTELOCATION_FACT ab
    INNER JOIN new_stores 
        ON new_stores.store_num = ab.distribute_location_id
    WHERE ab.distribution_status IN ('APPROVED', 'CLOSED', 'WORKSHEET')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
) WITH DATA 
PRIMARY INDEX (store_num, rms_sku_num, otb_eow_date) 
ON COMMIT PRESERVE ROWS;

-- Drop table if it exists
-- DROP TABLE shpd;
CREATE VOLATILE MULTISET TABLE shpd AS (
    SELECT DISTINCT
        b.operation_number AS transfer_id,
        b.item_product_id AS rms_sku_num,
        b.carton_number AS carton_num,
        CAST(b.to_location_facility AS INT) AS store_num,
        CAST(crte.create_time AS DATE) AS create_date,
        b.shipment_date_utc AS ship_date,
        b.receipt_date_utc AS receipt_date,
        crte.transfer_context_type,
        CASE crte.transfer_context_type 
            WHEN 'NORDSTROM_QUALITY_CENTER' THEN 'NQC'
            WHEN 'NRHL_EXHAUST' THEN 'Rack.com_Exhaust'
            WHEN 'RACK_PACK_AND_HOLD' THEN 'PH'
            WHEN 'RESERVE_STOCK_TRANSFER' THEN 'RESERVESTOCK'
            WHEN 'STOCK_BALANCE' THEN 'STOCKBALANCE'
            WHEN 'RACKING' THEN 'RACKING'
            ELSE 'OTHER_TR_U'
        END AS transfer_type,
        b.shipment_quantity,
        COALESCE(crte.quantity, 0) AS create_transfer_u,
        CASE 
            WHEN b.shipment_date_utc IS NOT NULL AND b.receipt_date_utc IS NULL THEN COALESCE(b.shipment_quantity, 0) 
            ELSE 0 
        END AS intransit_transfer_u,
        CASE 
            WHEN b.shipment_date_utc IS NOT NULL AND b.receipt_date_utc IS NOT NULL THEN COALESCE(b.shipment_quantity, 0) 
            ELSE 0 
        END AS st_rcvd_transfer_u,
        (create_transfer_u - (intransit_transfer_u + st_rcvd_transfer_u)) AS remaining_transfer_u
    FROM PRD_NAP_USR_VWS.RMS_COST_INTERNAL_SHIPMENT_FACT b
    INNER JOIN new_stores ns
        ON ns.store_num = b.to_location_facility
    FULL OUTER JOIN PRD_NAP_USR_VWS.RMS_COST_TRANSFERS_FACT crte
        ON crte.rms_sku_num = b.item_product_id
        AND crte.transfer_num = b.operation_number
        AND crte.to_location_facility = b.to_location_facility
        AND crte.create_time IS NOT NULL
    WHERE b.shipment_date_utc IS NOT NULL
        AND crte.create_time IS NOT NULL
) WITH DATA 
PRIMARY INDEX (rms_sku_num, store_num, transfer_id) 
ON COMMIT PRESERVE ROWS;


-- Grab EOH data (only grabbing current week data for EOH)
-- DROP TABLE eoh;
CREATE VOLATILE MULTISET TABLE eoh AS (
    SELECT DISTINCT
        CAST(eoh_lkup.store_num AS INT) AS store_num,
        eoh_lkup.rms_sku_num AS rms_sku_num_eoh,
        NVL(eoh_lkup.inventory_eoh_total_units_ty, 0) AS total_eoh_u,
        NVL(eoh_lkup.inventory_eoh_regular_units_ty, 0) AS eoh_reg_u,
        NVL(eoh_lkup.inventory_eoh_clearance_units_ty, 0) AS eoh_clr_u
    FROM PRD_NAP_USR_VWS.MERCH_INVENTORY_SKU_STORE_WEEK_AGG_FACT AS eoh_lkup
    INNER JOIN new_stores ns
        ON ns.store_num = eoh_lkup.store_num
    WHERE eoh_lkup.week_num = (
        SELECT MAX(week_num)
        FROM PRD_NAP_USR_VWS.MERCH_INVENTORY_SKU_STORE_WEEK_AGG_FACT
    )
) WITH DATA 
PRIMARY INDEX (store_num, rms_sku_num_eoh) 
ON COMMIT PRESERVE ROWS;

-- Unioning all the tables together to prep for blending in heirarchy info and plan data
-- DROP TABLE full_data;
CREATE VOLATILE MULTISET TABLE full_data AS (
    SELECT
        st_rcpt.purchase_order_num,
        CAST(NULL AS VARCHAR(100)) AS transfer_id,
        st_rcpt.store_num,
        st_rcpt.rms_sku_num,
        st_rcpt.carton_num,
        st_rcpt.otb_eow_date,
        CAST(NULL AS DATE) AS create_date,
        CAST(NULL AS DATE) AS ship_date,
        CAST(NULL AS DATE) AS receipt_date,
        st_rcpt.po_type,
        st_rcpt.status,
        st_rcpt.order_type_po,
        CAST(NULL AS VARCHAR(40)) AS transfer_context_type,
        CAST(NULL AS VARCHAR(40)) AS transfer_type,
        st_rcpt.shipment_qty_intransit,
        st_rcpt.shipment_qty_st_rcvd,
        st_rcpt.shipment_qty_in_dc,
        st_rcpt.palletized_units,
        CAST(0 AS DECIMAL(20,4)) AS quantity_allocated,
        CAST(0 AS DECIMAL(20,4)) AS approved_quantity_allocated_oo,
        CAST(0 AS DECIMAL(20,4)) AS open_oo_u_oo,
        CAST(0 AS DECIMAL(20,4)) AS on_order_u_oo,
        CAST(0 AS DECIMAL(20,4)) AS shipment_quantity,
        CAST(0 AS DECIMAL(20,4)) AS create_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS intransit_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS st_rcvd_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS remaining_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS total_eoh_u,
        CAST(0 AS DECIMAL(20,4)) AS eoh_reg_u,
        CAST(0 AS DECIMAL(20,4)) AS eoh_clr_u
    FROM st_rcpt

    UNION ALL

    SELECT
        oo.purchase_order_num,
        CAST(NULL AS VARCHAR(100)) AS transfer_id,
        oo.store_num,
        oo.rms_sku_num,
        CAST(NULL AS VARCHAR(80)) AS carton_num,
        oo.otb_eow_date,
        CAST(NULL AS DATE) AS create_date,
        CAST(NULL AS DATE) AS ship_date,
        CAST(NULL AS DATE) AS receipt_date,
        oo.po_type,
        oo.status,
        oo.order_type_po,
        CAST(NULL AS VARCHAR(40)) AS transfer_context_type,
        CAST(NULL AS VARCHAR(40)) AS transfer_type,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_intransit,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_st_rcvd,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_in_dc,
        CAST(0 AS DECIMAL(20,4)) AS palletized_units,
        oo.quantity_allocated,
        oo.approved_quantity_allocated_oo,
        oo.open_oo_u_oo,
        oo.on_order_u_oo,
        CAST(0 AS DECIMAL(20,4)) AS shipment_quantity,
        CAST(0 AS DECIMAL(20,4)) AS create_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS intransit_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS st_rcvd_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS remaining_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS total_eoh_u,
        CAST(0 AS DECIMAL(20,4)) AS eoh_reg_u,
        CAST(0 AS DECIMAL(20,4)) AS eoh_clr_u
    FROM oo

    UNION ALL

    SELECT
        CAST(NULL AS VARCHAR(100)) AS purchase_order_num,
        shpd.transfer_id,
        shpd.store_num,
        shpd.rms_sku_num,
        shpd.carton_num,
        CAST(NULL AS DATE) AS otb_eow_date,
        shpd.create_date,
        shpd.ship_date,
        shpd.receipt_date,
        CAST(NULL AS VARCHAR(40)) AS po_type,
        CAST(NULL AS VARCHAR(40)) AS status,
        CAST(NULL AS VARCHAR(10)) AS order_type_po,
        shpd.transfer_context_type,
        shpd.transfer_type,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_intransit,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_st_rcvd,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_in_dc,
        CAST(0 AS DECIMAL(20,4)) AS palletized_units,
        CAST(0 AS DECIMAL(20,4)) AS quantity_allocated,
        CAST(0 AS DECIMAL(20,4)) AS approved_quantity_allocated_oo,
        CAST(0 AS DECIMAL(20,4)) AS open_oo_u_oo,
        CAST(0 AS DECIMAL(20,4)) AS on_order_u_oo,
        shpd.shipment_quantity,
        shpd.create_transfer_u,
        shpd.intransit_transfer_u,
        shpd.st_rcvd_transfer_u,
        shpd.remaining_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS total_eoh_u,
        CAST(0 AS DECIMAL(20,4)) AS eoh_reg_u,
        CAST(0 AS DECIMAL(20,4)) AS eoh_clr_u
    FROM shpd

    UNION ALL

    SELECT
        CAST(NULL AS VARCHAR(100)) AS purchase_order_num,
        CAST(NULL AS VARCHAR(100)) AS transfer_id,
        eoh.store_num,
        eoh.rms_sku_num_eoh,
        CAST(NULL AS VARCHAR(80)) AS carton_num,
        CAST(NULL AS DATE) AS otb_eow_date,
        CAST(NULL AS DATE) AS create_date,
        CAST(NULL AS DATE) AS ship_date,
        CAST(NULL AS DATE) AS receipt_date,
        CAST(NULL AS VARCHAR(40)) AS po_type,
        CAST(NULL AS VARCHAR(40)) AS status,
        CAST(NULL AS VARCHAR(10)) AS order_type_po,
        CAST(NULL AS VARCHAR(40)) AS transfer_context_type,
        CAST(NULL AS VARCHAR(40)) AS transfer_type,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_intransit,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_st_rcvd,
        CAST(0 AS DECIMAL(20,4)) AS shipment_qty_in_dc,
        CAST(0 AS DECIMAL(20,4)) AS palletized_units,
        CAST(0 AS DECIMAL(20,4)) AS quantity_allocated,
        CAST(0 AS DECIMAL(20,4)) AS approved_quantity_allocated_oo,
        CAST(0 AS DECIMAL(20,4)) AS open_oo_u_oo,
        CAST(0 AS DECIMAL(20,4)) AS on_order_u_oo,
        CAST(0 AS DECIMAL(20,4)) AS shipment_quantity,
        CAST(0 AS DECIMAL(20,4)) AS create_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS intransit_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS st_rcvd_transfer_u,
        CAST(0 AS DECIMAL(20,4)) AS remaining_transfer_u,
        eoh.total_eoh_u,
        eoh.eoh_reg_u,
        eoh.eoh_clr_u
    FROM eoh
) WITH DATA 
PRIMARY INDEX (store_num, rms_sku_num) 
ON COMMIT PRESERVE ROWS;



-- RSB Lookup
-- DROP TABLE rsb;
CREATE MULTISET VOLATILE TABLE rsb AS (
    SELECT
        dept_num,
        supplier_idnt
    FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
    WHERE rack_strategic_brand_ind = 'Y'
    GROUP BY 1, 2
) WITH DATA 
PRIMARY INDEX (dept_num, supplier_idnt) 
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
PRIMARY INDEX (dept_num, supplier_idnt) 
ON rsb;

--DROP TABLE hierarchy;
CREATE VOLATILE MULTISET TABLE hierarchy AS (
    SELECT DISTINCT 
        div_idnt,
        div_desc,
        grp_idnt AS subdiv_idnt,
        grp_desc AS subdiv_desc,
        dept_idnt,
        dept_desc,
        class_idnt,
        class_desc,
        sbclass_idnt,
        sbclass_desc,
        quantrix_category
    FROM T2DL_DAS_ASSORTMENT_DIM.assortment_hierarchy ah
    WHERE ah.channel_country = 'US'
) WITH DATA 
PRIMARY INDEX (dept_idnt, class_idnt, quantrix_category) 
ON COMMIT PRESERVE ROWS;

-- Location plan data 
-- DROP TABLE loc_plans;
CREATE VOLATILE MULTISET TABLE loc_plans AS (
    SELECT DISTINCT 
        CAST(NULL AS VARCHAR(20)) AS purchase_order_num,
        rk_loc.loc_idnt AS store_num,
        ns.store_open_date,
        ns.week_before_opening_end_day_date AS nso_wbo_end_date,
        CAST(NULL AS VARCHAR(16)) AS rms_sku_num,
        CAST(NULL AS VARCHAR(30)) AS carton_num,
        CAST(NULL AS DATE) AS otb_eow_date,
        CAST(NULL AS VARCHAR(10)) AS po_type,
        CAST(NULL AS VARCHAR(10)) AS status,
        div_idnt,
        div_desc,
        subdiv_idnt,
        subdiv_desc,
        ah.dept_idnt,
        dept_desc,
        ah.class_idnt,
        class_desc,
        sbclass_idnt AS subclass_idnt,
        sbclass_desc,
        quantrix_category AS category,
        CAST(NULL AS VARCHAR(60)) AS brand_name,
        CAST(NULL AS VARCHAR(10)) AS supplier_idnt,
        CAST(NULL AS VARCHAR(30)) AS vpn_label,
        CAST(NULL AS VARCHAR(100)) AS transfer_id,
        CAST(NULL AS VARCHAR(40)) AS transfer_type,
        CAST(NULL AS VARCHAR(40)) AS transfer_context_type,
        CAST(NULL AS DATE) AS create_date,
        CAST(NULL AS DATE) AS ship_date,
        CAST(NULL AS DATE) AS receipt_date,
        CAST(NULL AS VARCHAR(3)) AS order_type_po,
        CAST(NULL AS BYTEINT) AS strategic_brand_flag,
        CAST(NULL AS DECIMAL(38, 4)) AS on_order_u_actual,
        CAST(NULL AS INTEGER) AS shipment_qty_intransit,
        CAST(NULL AS INTEGER) AS shipment_qty_st_rcvd,
        CAST(NULL AS INTEGER) AS shipment_qty_in_dc,
        CAST(NULL AS INTEGER) AS rp_in_dc_u,
        CAST(NULL AS INTEGER) AS rp_st_rcvd,
        CAST(NULL AS INTEGER) AS rp_po_intransit,
        CAST(NULL AS INTEGER) AS palletized_units,
        CAST(NULL AS DECIMAL(38, 4)) AS quantity_allocated,
        CAST(NULL AS DECIMAL(38, 4)) AS approved_quantity_allocated_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS open_oo_u_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS on_order_u_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS rp_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS nonrp_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS total_eoh_u,
        CAST(NULL AS DECIMAL(38, 4)) AS eoh_reg_u,
        CAST(NULL AS DECIMAL(38, 4)) AS eoh_clr_u,
        CAST(NULL AS DECIMAL(38, 4)) AS create_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS intransit_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS st_rcvd_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS remaining_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS rs_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS rp_rs_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS rp_st_rcvd_transfer_u,
        SUM(rk_loc.bop_plan) AS bop_plan_u,
        CAST(NULL AS DECIMAL(18, 2)) AS rp_plan_u
    FROM T2DL_DAS_LOCATION_PLANNING.LOC_PLAN_PRD_VW rk_loc
    INNER JOIN new_stores ns 
        ON ns.store_num = rk_loc.loc_idnt 
        AND ns.month_idnt = rk_loc.mth_idnt
    JOIN hierarchy ah 
        ON rk_loc.dept_idnt = ah.dept_idnt 
        AND rk_loc.class_idnt = ah.class_idnt 
        AND rk_loc.subclass_idnt = ah.sbclass_idnt 
        AND rk_loc.category = ah.quantrix_category
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
             19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
             37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54
) WITH DATA 
PRIMARY INDEX (store_num, dept_idnt, class_idnt, subclass_idnt) 
ON COMMIT PRESERVE ROWS;


-- RP plans
-- DROP TABLE rp_plans;
CREATE MULTISET VOLATILE TABLE rp_plans AS (
    SELECT 
        CAST(NULL AS VARCHAR(20)) AS purchase_order_num,
        CAST(rp.store_num AS INTEGER) AS store_num,
        ns.store_open_date,
        ns.week_before_opening_end_day_date AS nso_wbo_end_date,
        rms_sku_num,
        CAST(NULL AS VARCHAR(30)) AS carton_num,
        CAST(NULL AS DATE) AS otb_eow_date,
        CAST(NULL AS VARCHAR(10)) AS po_type,
        CAST(NULL AS VARCHAR(10)) AS status,
        ah.div_idnt,
        ah.div_desc,
        ah.grp_idnt AS subdiv_idnt,
        ah.grp_desc AS subdiv_desc,
        ah.dept_idnt,
        ah.dept_desc,
        ah.class_idnt,
        ah.class_desc,
        ah.sbclass_idnt AS subclass_idnt,
        ah.sbclass_desc,
        ah.quantrix_category AS category,
        ah.brand_name,
        ah.supplier_idnt,
        CAST(NULL AS VARCHAR(30)) AS vpn_label,
        CAST(NULL AS VARCHAR(100)) AS transfer_id,
        CAST(NULL AS VARCHAR(40)) AS transfer_type,
        CAST(NULL AS VARCHAR(40)) AS transfer_context_type,
        CAST(NULL AS DATE) AS create_date,
        CAST(NULL AS DATE) AS ship_date,
        CAST(NULL AS DATE) AS receipt_date,
        CAST(NULL AS VARCHAR(3)) AS order_type_po,
        CAST(NULL AS BYTEINT) AS strategic_brand_flag,
        CAST(NULL AS DECIMAL(38, 4)) AS on_order_u_actual,
        CAST(NULL AS INTEGER) AS shipment_qty_intransit,
        CAST(NULL AS INTEGER) AS shipment_qty_st_rcvd,
        CAST(NULL AS INTEGER) AS shipment_qty_in_dc,
        CAST(NULL AS INTEGER) AS rp_in_dc_u,
        CAST(NULL AS INTEGER) AS rp_st_rcvd,
        CAST(NULL AS INTEGER) AS rp_po_intransit,
        CAST(NULL AS INTEGER) AS palletized_units,
        CAST(NULL AS DECIMAL(38, 4)) AS quantity_allocated,
        CAST(NULL AS DECIMAL(38, 4)) AS approved_quantity_allocated_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS open_oo_u_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS on_order_u_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS rp_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS nonrp_oo,
        CAST(NULL AS DECIMAL(38, 4)) AS total_eoh_u,
        CAST(NULL AS DECIMAL(38, 4)) AS eoh_reg_u,
        CAST(NULL AS DECIMAL(38, 4)) AS eoh_clr_u,
        CAST(NULL AS DECIMAL(38, 4)) AS create_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS intransit_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS st_rcvd_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS remaining_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS rs_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS rp_rs_transfer_u,
        CAST(NULL AS DECIMAL(38, 4)) AS rp_st_rcvd_transfer_u,
        CAST(NULL AS DECIMAL(18, 2)) AS bop_plan_u,
        SUM(units) AS rp_plan_u
    FROM T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.rp_plans rp
    JOIN new_stores ns
        ON rp.store_num = ns.store_num
        AND rp.week_idnt = ns.week_idnt
    JOIN T2DL_DAS_ASSORTMENT_DIM.assortment_hierarchy ah
        ON ah.sku_idnt = rp.rms_sku_num
        AND ah.channel_country = 'US'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
) WITH DATA 
PRIMARY INDEX (store_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(store_num) ON rp_plans;

-- Drop table if it exists
-- DROP TABLE final_metrics;
CREATE MULTISET VOLATILE TABLE final_metrics AS (
    SELECT 
        purchase_order_num,
        fd.store_num,
        ns.store_open_date,
        ns.week_before_opening_end_day_date AS nso_wbo_end_date,
        rms_sku_num,
        carton_num,
        otb_eow_date,
        po_type,
        status,
        div_idnt,
        div_desc,
        grp_idnt AS subdiv_idnt,
        grp_desc AS subdiv_desc,
        dept_idnt,
        dept_desc,
        class_idnt,
        class_desc,
        sbclass_idnt AS subclass_idnt,
        sbclass_desc,
        quantrix_category AS category,
        brand_name,
        ah.supplier_idnt,
        supp_part_num AS vpn_label,
        transfer_id,
        transfer_type,
        transfer_context_type,
        create_date,
        ship_date,
        receipt_date,
        order_type_po,
        CASE WHEN rsb.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS strategic_brand_flag,
        SUM((on_order_u_oo) - ((shipment_qty_intransit) + (shipment_qty_st_rcvd) + (shipment_qty_in_dc))) AS on_order_u_actual,
        COALESCE(SUM(shipment_qty_intransit), 0) AS shipment_qty_intransit,
        COALESCE(SUM(shipment_qty_st_rcvd), 0) AS shipment_qty_st_rcvd,
        COALESCE(SUM(shipment_qty_in_dc), 0) AS shipment_qty_in_dc,
        SUM(CASE WHEN order_type_po = 'ARB' THEN shipment_qty_in_dc ELSE 0 END) AS rp_in_dc_u,
        SUM(CASE WHEN order_type_po = 'ARB' THEN shipment_qty_st_rcvd ELSE 0 END) AS rp_st_rcvd,
        SUM(CASE WHEN order_type_po = 'ARB' AND status = 'APPROVED' THEN shipment_qty_intransit ELSE 0 END) AS rp_po_intransit,
        COALESCE(SUM(palletized_units), 0) AS palletized_units,
        COALESCE(SUM(quantity_allocated), 0) AS quantity_allocated,
        COALESCE(SUM(approved_quantity_allocated_oo), 0) AS approved_quantity_allocated_oo,
        COALESCE(SUM(open_oo_u_oo), 0) AS open_oo_u_oo,
        COALESCE(SUM(on_order_u_oo), 0) AS on_order_u_oo,
        SUM(CASE WHEN order_type_po = 'ARB' AND status = 'APPROVED' THEN on_order_u_oo ELSE 0 END) AS rp_oo,
        SUM(CASE WHEN order_type_po <> 'ARB' AND status = 'APPROVED' THEN on_order_u_oo ELSE 0 END) AS nonrp_oo,
        COALESCE(SUM(total_eoh_u), 0) AS total_eoh_u,
        COALESCE(SUM(eoh_reg_u), 0) AS eoh_reg_u,
        COALESCE(SUM(eoh_clr_u), 0) AS eoh_clr_u,
        COALESCE(SUM(create_transfer_u), 0) AS create_transfer_u,
        COALESCE(SUM(intransit_transfer_u), 0) AS intransit_transfer_u,
        COALESCE(SUM(st_rcvd_transfer_u), 0) AS st_rcvd_transfer_u,
        COALESCE(SUM(remaining_transfer_u), 0) AS remaining_transfer_u,
        SUM(CASE WHEN transfer_type = 'RESERVE_STOCK' THEN create_transfer_u ELSE 0 END) AS rs_transfer_u,
        SUM(CASE WHEN transfer_type = 'RESERVE_STOCK' AND order_type_po = 'ARB' AND status = 'APPROVED' THEN create_transfer_u ELSE 0 END) AS rp_rs_transfer_u,
        SUM(CASE WHEN order_type_po = 'ARB' AND status = 'APPROVED' THEN st_rcvd_transfer_u ELSE 0 END) AS rp_st_rcvd_transfer_u,
        CAST(NULL AS DECIMAL(18, 2)) AS bop_plan_u,
        CAST(NULL AS DECIMAL(18, 2)) AS rp_plan_u
    FROM full_data fd
    JOIN T2DL_DAS_ASSORTMENT_DIM.assortment_hierarchy ah
        ON fd.rms_sku_num = ah.sku_idnt
        AND ah.channel_country = 'US'
    LEFT JOIN rsb 
        ON ah.dept_idnt = rsb.dept_num
        AND ah.supplier_idnt = rsb.supplier_idnt
    JOIN new_stores ns
        ON fd.store_num = ns.store_num
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
) WITH DATA 
PRIMARY INDEX (store_num, rms_sku_num)  
ON COMMIT PRESERVE ROWS;


--DROP TABLE final;
CREATE MULTISET VOLATILE TABLE final AS (
    SELECT
        purchase_order_num,
        store_num,
        store_open_date,
        nso_wbo_end_date,
        rms_sku_num,
        carton_num,
        otb_eow_date,
        po_type,
        status,
        div_idnt,
        div_desc,
        subdiv_idnt,
        subdiv_desc,
        dept_idnt,
        dept_desc,
        class_idnt,
        class_desc,
        subclass_idnt,
        sbclass_desc,
        category,
        brand_name,
        supplier_idnt,
        vpn_label,
        transfer_id,
        transfer_type,
        transfer_context_type,
        create_date,
        ship_date,
        receipt_date,
        order_type_po,
        strategic_brand_flag,
        COALESCE(SUM(on_order_u_actual), 0) AS on_order_u_actual,
        COALESCE(SUM(shipment_qty_intransit), 0) AS shipment_qty_intransit,
        COALESCE(SUM(shipment_qty_st_rcvd), 0) AS shipment_qty_st_rcvd,
        COALESCE(SUM(shipment_qty_in_dc), 0) AS shipment_qty_in_dc,
        COALESCE(SUM(rp_in_dc_u), 0) AS rp_in_dc_u,
        COALESCE(SUM(rp_st_rcvd), 0) AS rp_st_rcvd,
        COALESCE(SUM(rp_po_intransit), 0) AS rp_po_intransit,
        COALESCE(SUM(palletized_units), 0) AS palletized_units,
        COALESCE(SUM(quantity_allocated), 0) AS quantity_allocated,
        COALESCE(SUM(approved_quantity_allocated_oo), 0) AS approved_quantity_allocated_oo,
        COALESCE(SUM(open_oo_u_oo), 0) AS open_oo_u_oo,
        COALESCE(SUM(on_order_u_oo), 0) AS on_order_u_oo,
        COALESCE(SUM(rp_oo), 0) AS rp_oo,
        COALESCE(SUM(nonrp_oo), 0) AS nonrp_oo,
        COALESCE(SUM(total_eoh_u), 0) AS total_eoh_u,
        COALESCE(SUM(eoh_reg_u), 0) AS eoh_reg_u,
        COALESCE(SUM(eoh_clr_u), 0) AS eoh_clr_u,
        COALESCE(SUM(create_transfer_u), 0) AS create_transfer_u,
        COALESCE(SUM(intransit_transfer_u), 0) AS intransit_transfer_u,
        COALESCE(SUM(st_rcvd_transfer_u), 0) AS st_rcvd_transfer_u,
        COALESCE(SUM(remaining_transfer_u), 0) AS remaining_transfer_u,
        COALESCE(SUM(rs_transfer_u), 0) AS rs_transfer_u,
        COALESCE(SUM(rp_rs_transfer_u), 0) AS rp_rs_transfer_u,
        COALESCE(SUM(rp_st_rcvd_transfer_u), 0) AS rp_st_rcvd_transfer_u,
        COALESCE(SUM(bop_plan_u), 0) AS bop_plan_u,
        COALESCE(SUM(rp_plan_u), 0) AS rp_plan_u
    FROM (
        SELECT * FROM final_metrics 
        UNION ALL
        SELECT * FROM loc_plans 
        UNION ALL
        SELECT * FROM rp_plans
    ) a
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
) WITH DATA 
PRIMARY INDEX (store_num, rms_sku_num) 
ON COMMIT PRESERVE ROWS;

DELETE FROM {environment_schema}.new_store_opening{env_suffix} ALL;
INSERT INTO {environment_schema}.new_store_opening{env_suffix}
SELECT final.*, current_timestamp as last_update_time FROM final;