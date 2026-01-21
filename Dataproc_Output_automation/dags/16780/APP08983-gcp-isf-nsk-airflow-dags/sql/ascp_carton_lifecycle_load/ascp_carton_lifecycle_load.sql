

CREATE TEMPORARY TABLE IF NOT EXISTS wm_inbound_carton_to_split_carton (
     sku_id STRING ,
     purchase_order_number STRING NOT NULL,
     original_carton_id STRING ,
     received_tmstp TIMESTAMP,
     received_tmstp_tz STRING,
     carton_lpn STRING ,
     sku_qty INT64,
     location_id STRING )
;


-- This step will identify the if there are any split cartONs for the inbound cartON.
INSERT INTO  wm_inbound_carton_to_split_carton

-- Identify cartONs with split . Exclude the cartONs that are split AND split cartON is also in Inbound table AS original cartON
SELECT
	LTRIM(a.rms_sku_num,'0') AS SKU_ID
	, TRIM(a.purchase_order_number,'0') purchase_order_number
	, a.carton_id AS original_carton_id
	, CAST(a.received_tmstp AS TIMESTAMP)
  , a.received_tmstp_tz
	, COALESCE(split.to_carton_lpn, a.carton_id) AS carton_lpn
	, COALESCE(split.qty, a.shipment_qty) AS sku_qty
	, a.location_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_fact_vw a 
LEFT JOIN
   (
    SELECT DISTINCT LTRIM(sku_id,'0') AS sku_id, original_carton_id, TRIM(to_carton_purchase_order_number,'0') AS to_carton_purchase_order_number, to_carton_lpn
    ,SUM(qty) AS qty
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_sku_adjustment_events_fact
    WHERE LOWER(adjustment_event_type) = LOWER('WAREHOUSE_PURCHASE_ORDER_CARTON_SPLIT') AND qty > 0
	AND  adjustment_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
	GROUP BY sku_id,original_carton_id,to_carton_purchase_order_number,to_carton_lpn
   ) split
	ON LOWER(LTRIM(a.rms_sku_num,'0')) =  LOWER(split.sku_id)
		AND LOWER(a.carton_id) = LOWER(split.original_carton_id)
		AND LOWER(a.purchase_order_number) = LOWER(split.to_carton_purchase_order_number)
WHERE split.original_carton_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY a.carton_id,LTRIM(a.RMS_SKU_NUM,'0'),COALESCE(split.to_carton_lpn, a.carton_id),LTRIM(a.PURCHASE_ORDER_NUMBER,'0')  order by a.received_tmstp desc )=1

UNION ALL
-- identify cartONs with no split . Exclude the cartONs that are split AND split carton is also in WM table AS original carton
SELECT
	LTRIM(a.rms_sku_num,'0') AS sku_id
	, LTRIM(a.purchase_order_number,'0') AS purchase_order_number
	, a.carton_id AS original_carton_id
	, CAST(a.received_tmstp AS TIMESTAMP)
  , a.received_tmstp_tz
	, a.carton_id AS carton_lpn
	, a.shipment_qty AS sku_qty
	, a.location_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_fact_vw a LEFT JOIN
   (
    SELECT DISTINCT LTRIM(sku_id,'0') AS sku_id, original_carton_id, LTRIM(to_carton_purchase_order_number,'0') AS to_carton_purchase_order_number, to_carton_lpn
    ,SUM(qty) AS qty
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_sku_adjustment_events_fact
    WHERE LOWER(adjustment_event_type) = LOWER('WAREHOUSE_PURCHASE_ORDER_CARTON_SPLIT') AND qty > 0
	AND  adjustment_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
	GROUP BY sku_id,original_carton_id,to_carton_purchase_order_number,to_carton_lpn
   ) split
	ON LOWER(LTRIM(a.rms_sku_num,'0')) =  LOWER(split.sku_id)
		AND LOWER(a.carton_id) = LOWER(split.original_carton_id)
		AND LOWER(a.purchase_order_number) = LOWER(split.to_carton_purchase_order_number)
LEFT JOIN (
    SELECT DISTINCT LTRIM(sku_id,'0') AS sku_id, original_carton_id, LTRIM(to_carton_purchase_order_number,'0') AS to_carton_purchase_order_number, to_carton_lpn,SUM(qty) AS qty
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_sku_adjustment_events_fact
    WHERE LOWER(adjustment_event_type) = LOWER('WAREHOUSE_PURCHASE_ORDER_CARTON_SPLIT') AND qty > 0
	AND  adjustment_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
	GROUP BY sku_id,original_carton_id,to_carton_purchase_order_number,to_carton_lpn
   ) split1
ON LOWER(LTRIM(a.rms_sku_num,'0')) =  LOWER(split1.sku_id)
	AND LOWER(a.carton_id) = LOWER(split1.to_carton_lpn)
	AND LOWER(a.purchase_order_number) = LOWER(split1.to_carton_purchase_order_number)
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim s
    ON  CAST(a.location_id AS INT64) = s.store_num
WHERE split.original_carton_id is null
	  AND split1.to_carton_lpn IS NULL  --Exclude the cartONs that are split AND split cartON is also in WM table AS original cartON
	  AND  received_tmstp >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
	  AND LOWER(s.store_country_code) <> LOWER('CA') -- Exclude Canada locations
QUALIFY ROW_NUMBER() OVER(PARTITION BY carton_id,LTRIM(a.rms_sku_num,'0'),LTRIM(a.purchase_order_number,'0')  ORDER BY received_tmstp DESC )=1

UNION ALL
--identify cartON that is part of the split and the orginial carton still have qty > 0
SELECT
	LTRIM(a.rms_sku_num,'0') AS sku_id
	, LTRIM(a.purchase_order_number,'0') AS purchase_order_number
	, a.carton_id AS original_carton_id
	, CAST(a.received_tmstp AS TIMESTAMP)
  , a.received_tmstp_tz
	, a.carton_id AS carton_lpn
	, a.shipment_qty AS sku_qty
	, a.location_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_fact_vw a 
LEFT JOIN
     (
    SELECT DISTINCT LTRIM(sku_id,'0') AS sku_id, original_carton_id, LTRIM(to_carton_purchase_order_number,'0') AS to_carton_purchase_order_number, to_carton_lpn,SUM(qty) AS qty
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_sku_adjustment_events_fact
    WHERE LOWER(adjustment_event_type) = LOWER('WAREHOUSE_PURCHASE_ORDER_CARTON_SPLIT') AND qty > 0
	AND  adjustment_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
	GROUP BY sku_id,original_carton_id,to_carton_purchase_order_number,to_carton_lpn
   ) split
	ON LOWER(LTRIM(a.rms_sku_num,'0')) =  LOWER(split.sku_id)
	   AND LOWER(a.carton_id) = LOWER(split.original_carton_id)
       AND LOWER(a.purchase_order_number) = LOWER(split.to_carton_purchase_order_number)
WHERE split.original_carton_id IS NOT NULL
	  AND  received_tmstp >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
	  AND a.shipment_qty > 0
QUALIFY ROW_NUMBER() OVER(PARTITION BY carton_id,LTRIM(a.rms_sku_num,'0'),LTRIM(a.purchase_order_number,'0')  ORDER BY received_tmstp DESC )=1;



CREATE TEMPORARY TABLE IF NOT EXISTS wm_inbound_carton_to_split_bulk_carton (
     sku_id STRING,
     purchase_order_number STRING NOT NULL,
     original_carton_id STRING,
     received_tmstp TIMESTAMP,
     received_tmstp_tz STRING,
     carton_lpn STRING,
     sku_qty INT64,
     location_id STRING)
;

-- This step will identify the cartON that went through the bulk sort process AND split into multiple cartONs. ( a cartON can go though bulk sort after receiving in WM and also a inbound cartON can go though split and then through bulksort )
INSERT INTO  wm_inbound_carton_to_split_bulk_carton
SELECT
    sku_id
	, purchase_order_number
	, wspt.original_carton_id
	, wspt.received_tmstp
  , wspt.received_tmstp_tz
	, COALESCE(wcpi.carton_num,wspt.carton_lpn)AS carton_lpn
	, SUM(COALESCE(wcpi.sku_qty, wspt.sku_qty)) AS sku_qty
	, wspt.location_id
FROM wm_inbound_carton_to_split_carton wspt
	 LEFT  JOIN
	  (SELECT * FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_outbound_carton_packed_items_fact wcpi
		QUALIFY ROW_NUMBER() OVER (PARTITION BY original_carton_num,rms_sku_num,carton_num
		            ORDER BY pack_tmstp DESC  ) =1
		) wcpi
	  ON LOWER(wspt.carton_lpn)=LOWER(wcpi.original_carton_num)
		AND LOWER(wspt.sku_id)=LOWER(LTRIM(wcpi.rms_sku_num,'0'))
    AND wcpi.original_carton_num IS NOT NULL
GROUP by sku_id,purchase_order_number,wspt.original_carton_id,wspt.received_tmstp,received_tmstp_tz,carton_lpn,location_id
;



CREATE TEMPORARY TABLE IF NOT EXISTS wm_inbound_carton_to_split_bulk_putaway_carton (
     sku_id STRING,
     purchase_order_number STRING NOT NULL,
     original_carton_id STRING,
     received_tmstp TIMESTAMP,
     received_tmstp_tz STRING,
     carton_lpn STRING,
     sku_qty INT64,
     location_id STRING,
     warehouse_putaway_location_num STRING,
     warehouse_putaway_sku_qty INT64,
     warehouse_putaway_tmstp TIMESTAMP,
     warehouse_putaway_tmstp_tz STRING)
;

INSERT INTO wm_inbound_carton_to_split_bulk_putaway_carton
SELECT
winc.sku_id,
winc.purchase_order_number,
winc.original_carton_id,
winc.received_tmstp,
winc.received_tmstp_tz,
winc.carton_lpn,
winc.sku_qty,
winc.location_id,
putaw.event_location_id as warehouse_putaway_location_num,
CASE WHEN ABS(COALESCE(putaw.total_receiving_qty,0)) <= sku_qty THEN putaw.total_receiving_qty ELSE receiving_qty END AS warehouse_putaway_sku_qty,
CAST(putaw.event_time AS TIMESTAMP) AS  warehouse_putaway_tmstp,
putaw.event_time_tz AS  warehouse_putaway_tmstp_tz
FROM  wm_inbound_carton_to_split_bulk_carton  winc
LEFT JOIN  (
SELECT putag.carton_lpn carton_lpn,putag.sku_id sku_id,putag.event_location_id event_location_id,putmin.receiving_qty receiving_qty,putag.total_receiving_qty total_receiving_qty,event_time,event_time_tz
--Get the  total qty of each carton/sku
FROM (select carton_lpn,sku_id,event_location_id,SUM(receiving_qty) total_receiving_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.fc_product_in_storage_change_fact
WHERE LOWER(event_type) = LOWER('WAREHOUSE_SKU_INVENTORY_PUTAWAY') AND LOWER(carton_lpn) <>LOWER('')
GROUP by carton_lpn,sku_id,event_location_id
)putag
INNER JOIN
--Get the min time stamp associated with each carton/sku and correspoing qty at that min timestamp
(select carton_lpn,sku_id,event_location_id,receiving_qty,event_time,event_time_tz
from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.fc_product_in_storage_change_fact
where LOWER(event_type) = LOWER('WAREHOUSE_SKU_INVENTORY_PUTAWAY') and LOWER(carton_lpn) <>LOWER('')
qualify row_number() over(partition by carton_lpn,sku_id,event_location_id order by event_time ) =1
) putmin
on  LOWER(putag.carton_lpn)=LOWER(putmin.carton_lpn)
and LOWER(putag.sku_id)=LOWER(putmin.sku_id)
and LOWER(putag.event_location_id)=LOWER(putmin.event_location_id)
) putaw
on  LOWER(putaw.carton_lpn)=LOWER(winc.carton_lpn)
and LOWER(putaw.sku_id)=LOWER(winc.sku_id)
and LOWER(putaw.event_location_id)=LOWER(winc.location_id)
;

-- prepare data from VASN table with direct to store identification
CREATE TEMPORARY TABLE IF NOT EXISTS carton_vasn_to_wm_to_split_bulk_prep
AS (
 SELECT
    LTRIM(a.purchase_order_num,'0') AS purchase_order_num,
    a.carton_id,
    LTRIM(a.rms_sku_num,'0') AS rms_sku_num,
    a.edi_date,
    a.vasn_sku_qty,
    a.receiving_location_id,
    a.ship_to_location_id,
	COALESCE(d.carton_num,a.carton_id) AS to_carton_id,
	CASE WHEN LOWER(v.store_type_code) IN (LOWER('FL'),LOWER('RK'))
	          AND LOWER(CAST(a.ship_to_location_id AS STRING)) <>LOWER('828')
	          AND (LOWER(CAST(a.ship_to_location_id AS STRING)) <>LOWER('922') OR LOWER(CAST(a.receiving_location_id AS STRING))<>LOWER('922') OR a.ship_to_location_id = a.receiving_location_id)
		 			THEN 1 ELSE 0 END AS dts_ind
	FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_asn_fact a
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_outbound_carton_packed_items_fact d
		    ON LOWER(a.carton_id)=LOWER(d.original_carton_num)
		    AND LOWER(LTRIM(a.rms_sku_num,'0'))=LOWER(LTRIM(d.rms_sku_num,'0'))
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim v
	 				ON a.receiving_location_id = v.store_num
		WHERE a.EDI_DATE >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
		      AND LOWER(v.store_country_code) <> LOWER('CA') -- exclude Canada locations
		QUALIFY rank() OVER (PARTITION BY a.carton_id, a.rms_sku_num, d.CARTON_NUM ORDER BY a.edi_date DESC, a.vasn_sku_qty)=1
	)
  ;

-- remove non-DC component from "mixed"  VASNs
-- "mixed" cartons have non-dc VASN locations as well as DC.
-- COUNT(DTS_IND)>SUM(DTS_IND) AND SUM(DTS_IND)>0
DELETE FROM carton_vasn_to_wm_to_split_bulk_prep d
WHERE EXISTS
        (SELECT 1 FROM
                    (
                        SELECT carton_id, rms_sku_num, edi_date,vasn_sku_qty, to_carton_id ,purchase_order_num
                        FROM carton_vasn_to_wm_to_split_bulk_prep
                        GROUP BY  carton_id, rms_sku_num, edi_date,vasn_sku_qty, to_carton_id ,purchase_order_num
                        HAVING COUNT(dts_ind)>SUM(dts_ind) AND SUM(dts_ind)>0
                    ) d1
        WHERE
            LOWER(d1.carton_id) = LOWER(d.carton_id)
            AND LOWER(d1.rms_sku_num)  = LOWER(d.rms_sku_num)
            AND d1.edi_date = d.edi_date
            AND d1.vasn_sku_qty = d.vasn_sku_qty
            AND LOWER(d1.to_carton_id) = LOWER(d.to_carton_id)
            AND LOWER(d1.purchase_order_num) = LOWER(d.purchase_order_num)
            AND d.dts_ind=1
        )		;


CREATE TEMPORARY TABLE IF NOT EXISTS carton_vasn_to_wm_to_split_bulk (
     rms_sku_num STRING,
     wm_rms_sku_num STRING,
     purchase_order_number STRING,
     carton_id STRING,
     original_carton_id STRING,
     received_tmstp TIMESTAMP,
     received_tmstp_tz STRING,
     vasn_sku_qty INTEGER,
	   vasn_location_id  STRING,
	   vasn_location_ind  STRING,
     shipment_qty INTEGER,
     edi_date DATE,                                                                       -- FORMAT 'YYYY-MM-DD'
     location_id STRING,
     warehouse_putaway_location_num STRING,
     warehouse_putaway_sku_qty INTEGER,
     warehouse_putaway_tmstp TIMESTAMP,
     warehouse_putaway_tmstp_tz STRING)                      --purchase_order_number omitted as the limit is 4
;


--This step will join VASN data with WM inbound data . These joins are full outer which means if there is standalone data available at any step with out connection to each other , then it will be present as is.
--add VASN_SKU_QTY to order by VASN row number to avoid duplication after full join , so VASN will be randomness
INSERT INTO  carton_vasn_to_wm_to_split_bulk
SELECT
	COALESCE(vasn.rms_sku_num, wmin.sku_id) AS rms_sku_num,
	COALESCE(wmin.sku_id,vasn.rms_sku_num) AS wm_rms_sku_num,
	COALESCE(wmin.purchase_order_number,vasn.purchase_order_num ) AS purchase_order_num,
	COALESCE(wmin.carton_lpn, vasn.to_carton_id) AS carton_id ,
	COALESCE(vasn.carton_id, wmin.original_carton_id) AS original_carton_num,
  wmin.received_tmstp AS received_tmstp,
  wmin.received_tmstp_tz AS received_tmstp_tz,
	vasn.vasn_sku_qty AS vasn_sku_qty,
	RTRIM (LTRIM (vasn.receiving_location_id,'['),']' ) AS receiving_location_id ,
	vasn.receiving_location_ind,
	wmin.sku_qty AS shipment_qty,
	PARSE_DATE('%F',CAST(vasn.edi_date AS STRING))  AS edi_date,
	wmin.location_id AS location_id,
	wmin.warehouse_putaway_location_num,
  -(wmin.warehouse_putaway_sku_qty),
  wmin.warehouse_putaway_tmstp,
  wmin.warehouse_putaway_tmstp_tz
FROM ( SELECT
  carton_id,
  rms_sku_num,
  edi_date,
  vasn_sku_qty,
  to_carton_id,
  purchase_order_num,
  loclistagg AS receiving_location_id,
  CASE 
    WHEN agg_store_cnt > 0 AND agg_store_cnt = total_cnt THEN 'ALL DIRECT TO STORE'
    WHEN agg_store_cnt > 0 AND agg_store_cnt < total_cnt THEN 'PARTIALLY DIRECT TO STORE'
    ELSE 'DC' 
  END AS receiving_location_ind
FROM
  (
  SELECT
    carton_id,
    rms_sku_num,
    edi_date,
    vasn_sku_qty,
    to_carton_id,
    MIN(purchase_order_num) AS purchase_order_num,
    COUNT(dts_ind) AS total_cnt,
    SUM(dts_ind) AS agg_store_cnt,
    STRING_AGG(DISTINCT CAST(receiving_location_id AS STRING), ',') AS loclistagg
    FROM
      (SELECT
      carton_id,
      rms_sku_num,
      edi_date,
      vasn_sku_qty,
      to_carton_id,
      purchase_order_num,
      receiving_location_id,
      dts_ind,
      ROW_NUMBER() OVER (
        PARTITION BY carton_id, rms_sku_num, edi_date, vasn_sku_qty, to_carton_id
        ORDER BY receiving_location_id
      ) AS row_num
      FROM
      carton_vasn_to_wm_to_split_bulk_prep)
    GROUP BY
      carton_id, rms_sku_num, edi_date, vasn_sku_qty, to_carton_id 
) 
      ) vasn
FULL JOIN wm_inbound_carton_to_split_bulk_putaway_carton wmin
	ON LOWER(vasn.rms_sku_num) = LOWER(wmin.sku_id)
	   AND LOWER(vasn.carton_id) = LOWER(wmin.original_carton_id);

-- remove from CARTON_VASN_TO_WM_TO_SPLIT_BULK direct to store cartons after full join
DELETE FROM carton_vasn_to_wm_to_split_bulk
WHERE LOWER(vasn_location_ind) =LOWER('ALL DIRECT TO STORE')
AND location_id IS NULL;

-- COLLECT STATS INDEX(CARTON_ID,RMS_SKU_NUM) ON  #CARTON_VASN_TO_WM_TO_SPLIT_BULK;


CREATE TEMPORARY TABLE IF NOT EXISTS carton_vasn_to_wm_to_split_bulk_pack_outbound
(
     purchase_order_num STRING ,
     rms_sku_num STRING,
     last_rms_sku_num  STRING,
     original_carton_num STRING,
     to_carton_num STRING,
     last_carton_num  STRING,
     vendor_ship_notice_date DATE,                                              --FORMAT 'YYYY-MM-DD'
     vendor_shipped_qty INTEGER,
	   vendor_location_id  STRING,
     inbound_warehouse_num INTEGER ,
     inbound_warehouse_received_qty INTEGER,
     inbound_warehouse_received_tmstp TIMESTAMP,
     inbound_warehouse_received_tmstp_tz STRING,
     latest_warehouse_num  INTEGER,
     latest_warehouse_shipped_qty INTEGER,
     latest_warehouse_shipped_tmstp TIMESTAMP,
     latest_warehouse_shipped_tmstp_tz STRING,
     warehouse_putaway_location_num STRING,
     warehouse_putaway_sku_qty INTEGER,
     warehouse_putaway_tmstp TIMESTAMP,
     warehouse_putaway_tmstp_tz STRING,
   	 old_purchase_order_num STRING,
   	 new_purchase_order_num STRING
     )             --rms_sku_num omitted as the limit is 4
;

--This step join the inbound data with outbound for both US and CA

INSERT INTO carton_vasn_to_wm_to_split_bulk_pack_outbound
SELECT
  COALESCE(wmcmb.purchase_order_number, last_dc_ship.purchase_order_num) AS purchase_order_num,  -- po FROM nls_dc need to
  COALESCE(wmcmb.wm_rms_sku_num, last_dc_ship.sku_id) AS rms_sku_num,
  last_dc_ship.sku_id AS last_rms_sku_num,
  COALESCE(wmcmb.original_carton_id, last_dc_ship.carton_lpn) AS original_carton_num,
  COALESCE(wmcmb.carton_id, last_dc_ship.carton_lpn) AS to_carton_num,
  last_dc_ship.carton_lpn AS last_carton_num,
  PARSE_DATE('%F',CAST(wmcmb.edi_date AS STRING)) AS vendor_ship_notice_date,
  wmcmb.vasn_sku_qty AS vendor_shipped_qty,
  wmcmb.vasn_location_id AS vendor_location_id,
  CAST(wmcmb.location_id AS INT64) AS inbound_warehouse_num,
  wmcmb.shipment_qty AS inbound_warehouse_received_qty,
  wmcmb.received_tmstp  AS inbound_warehouse_received_tmstp,
  wmcmb.received_tmstp_tz  AS inbound_warehouse_received_tmstp_tz,
  CAST(last_dc_ship.ship_from_location_id AS INT64) AS latest_warehouse_num,
  -(last_dc_ship.shipment_qty) AS latest_warehouse_shipped_qty,
  CAST(last_dc_ship.shipped_time AS TIMESTAMP) AS latest_warehouse_shipped_tmstp,
  last_dc_ship.shipped_time_tz AS latest_warehouse_shipped_tmstp_tz,
  wmcmb.warehouse_putaway_location_num AS warehouse_putaway_location_num,
  wmcmb.warehouse_putaway_sku_qty AS warehouse_putaway_sku_qty,
  wmcmb.warehouse_putaway_tmstp AS warehouse_putaway_tmstp,
  wmcmb.warehouse_putaway_tmstp_tz AS warehouse_putaway_tmstp_tz,
  wmcmb.purchase_order_number AS old_purchase_order_num,
  last_dc_ship.purchase_order_num AS new_purchase_order_num
FROM  carton_vasn_to_wm_to_split_bulk wmcmb
FULL JOIN (SELECT carton_lpn
                  ,CASE WHEN  (STRPOS(purchase_order_num , ' ') > 0)
                    THEN  SUBSTR(purchase_order_num, 1, (STRPOS(purchase_order_num , ' ') - 1))
                    ELSE purchase_order_num
                    END AS purchase_order_num
                   ,sku_id,shipped_time,shipped_time_tz, ship_from_location_id,SUM(shipment_qty) AS shipment_qty 
                   FROM
            (
             SELECT * FROM (
                     SELECT distinct carton_lpn,transfer_id,
						 LTRIM(purchase_order_num,'0') AS purchase_order_num,
						 LTRIM(sku_id,'0') AS sku_id,
						 shipped_time,
             shipped_time_tz,
						 shipment_qty,
						 ship_from_location_id
                     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.dc_internal_transfer_shipment_receipt_fact
                     WHERE LOWER(transfer_type) = LOWER('ALLOCATION')
						 AND LOWER(warehouse_shipment_stage)=LOWER('LAST_HOP')
						 AND SHIPPED_TIME >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
                     UNION DISTINCT
                     SELECT DISTINCT carton_lpn,transfer_id,LTRIM(purchase_order_num,'0') AS purchase_order_num,LTRIM(sku_id,'0') AS sku_id, shipped_time,shipped_time_tz, shipment_qty, ship_from_location_id
                     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.dc_internal_transfer_shipment_receipt_fact
                     WHERE LOWER(transfer_type) = LOWER('ALLOCATION')
						 AND LOWER(warehouse_shipment_stage)=LOWER('DIRECT_SHIPMENT')
						 AND  shipped_time >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL -36 MONTH)
                           )a -- Identify the last for Hop of shipment . With hop the last hop will stage='LAST_HOP', with out hop stage='DIRECT_SHIPMENT'
             QUALIFY ROW_NUMBER() OVER (PARTITION by LTRIM(sku_id,'0') , carton_lpn, transfer_id ORDER BY shipped_time DESC) = 1
            )DIT
          GROUP BY carton_lpn,purchase_order_num,sku_id,shipped_time,shipped_time_tz, ship_from_location_id
         )last_dc_ship
	ON  LOWER(wmcmb.carton_id) = LOWER(last_dc_ship.carton_lpn)
		AND LOWER(wmcmb.rms_sku_num)  = LOWER(last_dc_ship.sku_id)  -------------------- Outbound shipments for the US. Take the latest shipment
;
-- COLLECT STATS INDEX( ORIGINAL_CARTON_NUM,RMS_SKU_NUM) ON  #CARTON_VASN_TO_WM_TO_SPLIT_BULK_PACK_OUTBOUND;
-- COLLECT STATS  COLUMN (PURCHASE_ORDER_NUM) ON  #CARTON_VASN_TO_WM_TO_SPLIT_BULK_PACK_OUTBOUND;

-- remove from table rows where CARTON_VASN_TO_WM_TO_SPLIT_BULK po doesn't match with DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_FACT
DELETE FROM  carton_vasn_to_wm_to_split_bulk_pack_outbound a
WHERE (new_purchase_order_num IS NOT NULL OR LOWER(new_purchase_order_num) <> LOWER(''))
AND LOWER(new_purchase_order_num) <> LOWER(old_purchase_order_num);
-- remove carton reusing duplicates
DELETE FROM  carton_vasn_to_wm_to_split_bulk_pack_outbound AS a
WHERE EXISTS
		(
		SELECT
		    b.rms_sku_num,
		    b.original_carton_num,
		    b.to_carton_num,
		    b.inbound_warehouse_num,
		    b.latest_warehouse_num,
		    b.purchase_order_num
	  FROM carton_vasn_to_wm_to_split_bulk_pack_outbound AS b
    INNER JOIN carton_vasn_to_wm_to_split_bulk_pack_outbound AS a
	  ON 	LOWER(a.rms_sku_num)=LOWER(b.rms_sku_num)
	        AND LOWER(a.original_carton_num)=LOWER(b.original_carton_num)
	        AND LOWER(a.to_carton_num) =LOWER(b.to_carton_num)
	        AND a.inbound_warehouse_num=b.inbound_warehouse_num
	        AND a.latest_warehouse_num =b.latest_warehouse_num
	        AND LOWER(a.purchase_order_num)=LOWER(b.purchase_order_num)
	  GROUP BY rms_sku_num,original_carton_num, to_carton_num,inbound_warehouse_num,latest_warehouse_num,purchase_order_num
	  HAVING COUNT(1)>1)
	  AND a.new_purchase_order_num IS  NULL
    ;

-- volatile table for audit events
CREATE TEMPORARY TABLE IF NOT EXISTS all_carton_audits_events
AS
 (
    SELECT
        SPLIT(inbound_carton_id,'-')[OFFSET(0)] AS carton_lpn,
        SPLIT(inbound_carton_id,'-')[OFFSET(1)] AS purchase_order_number,
        CAST(event_location_num AS INT64) AS dc_id,
        MIN(CAST(event_tmstp AS TIMESTAMP))	AS event_timestamp,
        'PST8PDT' AS event_timestamp_tz,
        warehouse_event_type AS event
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.wm_inbound_carton_events_fact
        WHERE CAST(event_location_num AS INT64) IN (89, 299, 399, 499, 699, 799)
        AND LOWER(warehouse_event_type) IN (LOWER('WAREHOUSE_PURCHASE_ORDER_ALL_CARTON_AUDITS_COMPLETED'))
    GROUP BY carton_lpn,purchase_order_number,dc_id,event
 ) 
  ;

-- volatile table for put to store events
CREATE TEMPORARY TABLE IF NOT EXISTS all_pts_events
AS
 (
    SELECT
    put_to_store.inbound_carton_num	AS inbound_carton_lpn,
    put_to_store.outbound_carton_num AS outbound_carton_num,
    put_to_store.purchase_order_num AS purchase_order_number,
    CAST(COALESCE(put_to_store.earliest_carton_sort_started_location_num,put_to_store.earliest_sku_sort_location_num) AS INT64)	AS dc_id,
		sku_num,
		MIN(CAST(put_to_store.earliest_carton_sort_started_event_tmstp AS TIMESTAMP))	AS carton_sort_started_timestamp,
    'PST8PDT' AS carton_sort_started_timestamp_tz,
		MAX(CAST(put_to_store.earliest_carton_sort_completed_event_tmstp AS TIMESTAMP))   AS carton_sort_completed_timestamp,
    'PST8PDT' AS carton_sort_completed_timestamp_tz
	FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.put_to_store_bulk_sort_detail_fact AS put_to_store
		WHERE put_to_store.inbound_carton_num IS NOT NULL
	GROUP BY inbound_carton_lpn,outbound_carton_num,purchase_order_number,dc_id,sku_num
 ) 
   ;

-- volatile table for MHE events
CREATE TEMPORARY TABLE IF NOT EXISTS all_mhe_events
AS
(
    SELECT
    sort.carton_num AS inbound_carton_lpn,
    CAST(sort.chain_created_location_num AS INT64) AS dc_id,
		sort.purchase_order_num AS purchase_order_number,
		pud.rms_sku_num AS rms_sku_num,
		MIN(CAST(sort.chain_created_event_tmstp AS TIMESTAMP)) 	AS chain_created_timestamp,
    'PST8PDT' AS chain_created_timestamp_tz,
		MIN(CAST(sort.sku_scanned_event_tmstp AS TIMESTAMP))	as min_sku_scanned_timestamp,
    'PST8PDT' AS min_sku_scanned_timestamp_tz,
	FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.warehouse_unit_sorter_detail_fact AS sort
	    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS pud
	        ON LOWER(sort.sku_num)=LOWER(pud.upc_num)
    WHERE carton_num IS NOT NULL
	GROUP BY inbound_carton_lpn,dc_id,purchase_order_number,rms_sku_num
) 
 ;

-- volatile table for MHE detail events
CREATE TEMPORARY TABLE IF NOT EXISTS all_mhe_detail_events
AS
(
    SELECT
        carton_num AS inbound_carton_lpn,
        pud.rms_sku_num	AS rms_sku_num,
        CAST(RIGHT(location_num,3) AS INT64) AS dc_id,
        MAX(CAST(latest_unit_sorted_tmstp AS TIMESTAMP)) AS latest_unit_sorted_timestamp,
        'PST8PDT' AS latest_unit_sorted_timestamp_tz
	FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.warehouse_unit_sorter_item_detail_fact AS sort
		LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS pud
		        ON LOWER(sort.sku_num)=LOWER(pud.upc_num)
    WHERE LOWER(sort.status_code) = LOWER('SUCCESSFUL')
    GROUP BY inbound_carton_lpn,rms_sku_num,dc_id
) 
  ;

-- volatile table for packed events
CREATE TEMPORARY TABLE IF NOT EXISTS all_packed_events
AS
(
    SELECT
        i.location_id,
        i.carton_lpn,
        MAX(CAST(i.event_time AS TIMESTAMP)) AS packed_time,
        'PST8PDT' AS packed_time_tz
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_distribution_center_outbound_carton_events_fact AS i
    WHERE LOWER(event_type) =LOWER('WAREHOUSE_CARTON_PACKED')
    GROUP BY location_id,carton_lpn
) 
  ;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.carton_lifecycle_fact;

INSERT INTO  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.carton_lifecycle_fact
(
	purchase_order_num
	,original_carton_num
	,to_carton_num
	,rms_sku_num
	,inbound_warehouse_num
	,inbound_warehouse_received_qty
	,inbound_warehouse_received_tmstp
  ,inbound_warehouse_received_tmstp_tz
  ,all_audits_tmstp
  ,all_audits_tmstp_tz
	,mhe_chain_created_tmstp
  ,mhe_chain_created_tmstp_tz
	,mhe_min_sku_scanned_tmstp
  ,mhe_min_sku_scanned_tmstp_tz
	,mhe_latest_unit_sorted_tmstp
  ,mhe_latest_unit_sorted_tmstp_tz
	,pts_sort_started_tmstp
  ,pts_sort_started_tmstp_tz
	,pts_sort_completed_tmstp
  ,pts_sort_completed_tmstp_tz
	,packed_tmstp
  ,packed_tmstp_tz
	,final_warehouse_num
	,final_warehouse_shipped_qty
	,final_warehouse_shipped_tmstp
  ,final_warehouse_shipped_tmstp_tz
	,vendor_shipped_qty
	,vendor_location_num
	,vendor_ship_notice_date
	,store_num
	,store_received_qty
	,store_received_tmstp
  ,store_received_tmstp_tz
	,warehouse_putaway_tmstp
  ,warehouse_putaway_tmstp_tz
	,warehouse_putaway_sku_qty
	,warehouse_putaway_location_num
	,vendor_shipped_to_store_received_days
	,vendor_shipped_to_warehouse_received_days
	,warehouse_received_to_warehouse_shipped_days
	,warehouse_shipped_to_store_received_days
	,ship_window_start_to_vendor_shipped_days
	,ship_window_start_to_store_received_days
	,purchase_order_original_approval_tmstp
  ,purchase_order_original_approval_tmstp_tz
	,purchase_order_original_approval_date
	,purchase_order_start_ship_date
	,purchase_order_end_ship_date
	,purchase_order_premark_ind
	,supplier_num
	,shipping_method
	,purchase_order_type
	,order_type
	,edi_ind
	,dropship_ind
	,import_order_ind
	,nordstrom_productgroup_ind
	,purchase_type
	,purchase_order_status
	,currency_code
	,direct_to_store_ind
	,quality_check_ind
	,internal_po_ind
  ,freight_terms
  ,freight_payment_method
  ,merchandise_source
	,dw_batch_id
	,dw_batch_date
	,dw_sys_load_tmstp
	,dw_sys_updt_tmstp
)
SELECT
   ldc.purchase_order_num AS purchase_order_num,
   ldc.original_carton_num AS original_carton_num,
   ldc.to_carton_num,
   ldc.rms_sku_num,
   ldc.inbound_warehouse_num,
   ldc.inbound_warehouse_received_qty,
   ldc.inbound_warehouse_received_tmstp,
   ldc.inbound_warehouse_received_tmstp_tz,
   acae.event_timestamp AS all_audits_tmstp,
   acae.event_timestamp_tz AS all_audits_tmstp_tz,
   amhe.chain_created_timestamp AS mhe_chain_created_tmstp,
   amhe.chain_created_timestamp_tz AS mhe_chain_created_tmstp_tz,
   amhe.min_sku_scanned_timestamp AS mhe_min_sku_scanned_tmstp,
   amhe.min_sku_scanned_timestamp_tz AS mhe_min_sku_scanned_tmstp_tz,
   amhe.latest_unit_sorted_timestamp AS mhe_latest_unit_sorted_tmstp,
   amhe.latest_unit_sorted_timestamp_tz AS mhe_latest_unit_sorted_tmstp_tz,
   ape.carton_sort_started_timestamp AS pts_sort_started_tmstp,
   ape.carton_sort_started_timestamp_tz AS pts_sort_started_tmstp_tz,
   ape.carton_sort_completed_timestamp AS pts_sort_completed_tmstp,
   ape.carton_sort_completed_timestamp_tz AS pts_sort_completed_tmstp_tz,
   apacked.packed_time AS packed_tmstp,
   apacked.packed_time_tz AS packed_tmstp_tz,
   ldc.latest_warehouse_num AS final_warehouse_num,
   ldc.latest_warehouse_shipped_qty AS final_warehouse_shipped_qty,
   ldc.latest_warehouse_shipped_tmstp AS final_warehouse_shipped_tmstp,
   ldc.latest_warehouse_shipped_tmstp_tz AS final_warehouse_shipped_tmstp_tz,
   ldc.vendor_shipped_qty AS vendor_shipped_qty,
   ldc.vendor_location_id AS vendor_location_num,
   ldc.vendor_ship_notice_date,
   CAST(store.to_location_num AS INT64) AS store_num,
   store.receipt_sku_qty AS store_received_qty,
   CAST(store.receipt_tmstp AS TIMESTAMP) AS store_received_tmstp,
   store.receipt_tmstp_tz AS store_received_tmstp_tz,
   ldc.warehouse_putaway_tmstp AS warehouse_putaway_tmstp,
   ldc.warehouse_putaway_tmstp_tz AS warehouse_putaway_tmstp_tz,
   ldc.warehouse_putaway_sku_qty AS warehouse_putaway_sku_qty,
   CAST(ldc.warehouse_putaway_location_num AS INT64) AS warehouse_putaway_location_num,
   DATE_DIFF(CAST( store.receipt_tmstp AS DATE), ldc.vendor_ship_notice_date, DAY) AS vendor_shipped_to_store_received_days,
   DATE_DIFF(CAST(ldc.inbound_warehouse_received_tmstp AS DATE), ldc.vendor_ship_notice_date,DAY) AS vendor_shipped_to_warehouse_received_days,
   DATE_DIFF(CAST(ldc.latest_warehouse_shipped_tmstp AS DATE) ,CAST(ldc.inbound_warehouse_received_tmstp AS DATE),DAY) AS warehouse_received_to_warehouse_shipped_days,
   DATE_DIFF(CAST( store.receipt_tmstp AS DATE), CAST(ldc.latest_warehouse_shipped_tmstp AS DATE),DAY) AS warehouse_shipped_to_store_received_days,
   DATE_DIFF(ldc.vendor_ship_notice_date, po.start_ship_date,DAY) AS ship_window_start_to_vendor_shipped_days,
   DATE_DIFF(CAST( store.receipt_tmstp AS DATE), po.start_ship_date,DAY) AS ship_window_start_to_store_received_days,
   CAST(po.first_approval_event_tmstp_pacific AS TIMESTAMP) AS purchase_order_original_approval_tmstp,
   po.first_approval_event_tmstp_pacific_tz AS purchase_order_original_approval_tmstp_tz,
   CAST(po.first_approval_event_tmstp_pacific AS DATE) AS purchase_order_original_approval_date,
   po.start_ship_date AS purchase_order_start_ship_date,
   po.end_ship_date AS purchase_order_end_ship_date,
   po.premark_ind AS purchase_order_premark_ind,
   po.order_from_vendor_id AS supplier_num,
   po.shipping_method AS shipping_method,
   po.po_type AS purchase_order_type,
   po.order_type AS order_type,
   po.edi_ind,
   'F' AS dropship_ind, --default as false since 04/22/24 as the field is not available in PO_headers_fact
   po.import_order_ind,
   po.npg_ind AS nordstrom_productgroup_ind ,
   po.purchase_type,
   po.status AS purchase_order_status,
   po.currency AS currency_code,
   'F' AS direct_to_store_ind,
   po.quality_check_ind,
   po.internal_po_ind,
   po.freight_terms,
   po.freight_payment_method,
   po.merchandise_source,
   (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('NAP_ASCP_CARTON_LIFECYCLE')) AS dw_batch_id,
   (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('NAP_ASCP_CARTON_LIFECYCLE')) AS dw_batch_date,
   CURRENT_DATETIME('PST8PDT'),
   CURRENT_DATETIME('PST8PDT')
FROM  carton_vasn_to_wm_to_split_bulk_pack_outbound AS ldc
LEFT JOIN all_carton_audits_events AS acae
		ON LOWER(acae.carton_lpn) = LOWER(ldc.original_carton_num)
		AND LOWER(acae.purchase_order_number) = LOWER(ldc.purchase_order_num)
		AND acae.dc_id = ldc.inbound_warehouse_num
LEFT JOIN all_pts_events AS ape ON
		    LOWER(ldc.original_carton_num) = LOWER(ape.inbound_carton_lpn)
		AND LOWER(ldc.to_carton_num) = LOWER(ape.outbound_carton_num)
		AND LOWER(ldc.purchase_order_num) = LOWER(ape.purchase_order_number)
		AND ldc.inbound_warehouse_num = ape.dc_id
		AND LOWER(ldc.rms_sku_num) = LOWER(ape.sku_num)
LEFT JOIN
     (SELECT  mhe.*
            , mhe_details.latest_unit_sorted_timestamp,
              mhe_details.latest_unit_sorted_timestamp_tz
        FROM all_mhe_events AS mhe
        LEFT JOIN all_mhe_detail_events AS mhe_details
            ON LOWER(mhe.inbound_carton_lpn)=LOWER(mhe_details.inbound_carton_lpn)
            AND mhe.dc_id=mhe_details.dc_id
            AND LOWER(mhe.rms_sku_num)=LOWER(mhe_details.rms_sku_num) ) amhe
        ON  LOWER(amhe.inbound_carton_lpn) = LOWER(ldc.original_carton_num)
        AND LOWER(amhe.purchase_order_number) = LOWER(ldc.purchase_order_num)
        AND amhe.dc_id = ldc.inbound_warehouse_num
        AND LOWER(amhe.rms_sku_num) = LOWER(ldc.rms_sku_num)
LEFT JOIN all_packed_events AS apacked
 		ON  LOWER(ldc.to_carton_num)= LOWER(apacked.carton_lpn)
        AND	 apacked.location_id = ldc.LATEST_WAREHOUSE_NUM
LEFT JOIN (SELECT
			   transfer_num,
			   carton_num,
			   LTRIM(sku_num,'0') AS sku_num,
			   receipt_tmstp,
         receipt_tmstp_tz,
			   receipt_sku_qty,
			   to_location_num
           FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_inventory_transfer_shipment_receipt_vw
           LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
                ON CAST(store_inventory_transfer_shipment_receipt_vw.to_location_num AS INT64) = store_dim.store_num
           WHERE LOWER(transfer_type) = LOWER('ALLOCATION')
			     AND receipt_tmstp >= DATETIME_ADD(CURRENT_DATETIME('PST8PDT'), INTERVAL -36 MONTH)
			     AND LOWER(store_dim.store_country_code) <> LOWER('CA')
          ) store
	ON  LOWER(store.carton_num) = LOWER(ldc.to_carton_num)
	AND LOWER(store.sku_num) = LOWER(ldc.rms_sku_num)
LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_header_fact AS po
	ON LOWER(po.purchase_order_number) = LOWER(LTRIM(ldc.purchase_order_num,'0'));

