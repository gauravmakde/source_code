/*
--DROP TABLE {environment_schema}.ANNIVERSARY_NONSELL;
 CREATE MULTISET TABLE {environment_schema}.ANNIVERSARY_NONSELL
			 , FALLBACK
			 , NO BEFORE JOURNAL
			 , NO AFTER JOURNAL
			 , CHECKSUM = DEFAULT
			 , DEFAULT MERGEBLOCKRATIO
(
		rms_sku_id  			VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
		, snapshot_date 		DATE NOT NULL
	, snapshot_date_BOH 	DATE NOT NULL
	, value_updated_time	TIMESTAMP WITH TIME ZONE
		, location_id       VARCHAR(30)
		, nonsellable_qty		INTEGER
		, current_price_type VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC

)
PRIMARY INDEX (rms_sku_id,snapshot_date,location_id);
GRANT SELECT ON {environment_schema}.ANNIVERSARY_NONSELL  TO PUBLIC;
*/


create multiset volatile TABLE STORE_01
AS
(
SELECT store_num
	,price_store_num
	,store_type_code
	,CASE
		when price_store_num in (808,867,835,1) then 'FL'
		when price_store_num = -1 and channel_num = 120 then 'FL'
		when price_Store_num in (844,828,338) then 'RK'
		when price_store_num = -1 and channel_num = 250 then 'RK'
		else NULL
	 end STORE_TYPE_CODE_NEW
	,CASE
		when price_store_num in (808,828,338,1) then 'USA'
		when price_store_num = -1 and channel_country = 'US' then 'USA'
		when price_Store_num in (844,867,835) then 'CAN'
		when price_store_num = -1 and channel_country = 'CA' then 'CAN'
		else NULL
	 end STORE_COUNTRY_CODE
	 ,CASE
		 when price_store_num = -1 and channel_num = 120 then 'ONLINE'
		 when price_store_num = -1 and channel_num = 250 then 'ONLINE'
		 else SELLING_CHANNEL
	 END SELLING_CHANNEL
	 ,channel_num
FROM prd_nap_usr_vws.price_store_dim_vw
group by 1,2,3,4,5,6,7
)
WITH DATA
PRIMARY INDEX (STORE_NUM)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	 PRIMARY INDEX (STORE_NUM)
	,COLUMN (store_num, store_type_code, store_country_code, selling_channel)
	,COLUMN (store_num)
	,COLUMN (selling_channel)
		ON STORE_01;

create multiset volatile TABLE price_01 AS
(
SELECT
	 b.rms_sku_num
	,a.store_type_code
	,a.store_country_code
    ,a.selling_channel
	,b.channel_country
	,b.ownership_price_type
	,b.ownership_price_amt
	,b.regular_price_amt
	,b.current_price_amt
	,b.current_price_currency_code
	,b.current_price_type
	,b.eff_begin_tmstp
	,b.eff_end_tmstp
FROM (
		SELECT
			 price_store_num
			,store_type_code_new AS store_type_code
			,store_country_code
            ,selling_channel
		FROM store_01
		GROUP BY 1,2,3,4
	) a
JOIN (
		SELECT
			 rms_sku_num
			,store_num
			,case when channel_country = 'US' then 'USA' when channel_country = 'CA' then 'CAN' end as channel_country
			,case when ownership_retail_price_type_code = 'CLEARANCE' then 'C' when ownership_retail_price_type_code = 'REGULAR' then 'R' else ownership_retail_price_type_code end as ownership_price_type
			,ownership_retail_price_amt as ownership_price_amt
			,regular_price_amt
			,selling_retail_price_amt as current_price_amt
			,selling_retail_currency_code as current_price_currency_code
			,case when selling_retail_price_type_code = 'CLEARANCE' then 'C' when selling_retail_price_type_code = 'REGULAR' then 'R' when selling_retail_price_type_code = 'PROMOTION' then 'P' else selling_retail_price_type_code end as current_price_type
			,eff_begin_tmstp
			,eff_end_tmstp
		FROM prd_nap_usr_vws.PRODUCT_PRICE_TIMELINE_DIM
		WHERE CAST(eff_begin_tmstp as DATE) < CURRENT_DATE + 1
	) b
  ON a.price_store_num = b.store_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
WITH DATA
PRIMARY INDEX (rms_sku_num, store_type_code, channel_country, selling_channel, eff_begin_tmstp, eff_end_tmstp)
ON COMMIT preserve ROWS;

COLLECT STATS
	PRIMARY INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,selling_channel,eff_begin_tmstp, eff_end_tmstp)
	,COLUMN (RMS_SKU_NUM)
	,COLUMN(EFF_BEGIN_TMSTP, EFF_END_TMSTP)
		ON PRICE_01;


CREATE MULTISET VOLATILE TABLE SKU_SOH_01
AS
(
select RMS_SKU_ID
	,snapshot_date
	,snapshot_date+1 snapshot_date_boh
	,value_updated_time
	,location_id
	,coalesce(unavailable_qty,0) nonsellable_qty
	,location_type
from PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT
where snapshot_date between {start_date_soh} AND {end_date}
and nonsellable_qty <> 0
GROUP BY 1,2,3,4,5,6,7
)
WITH DATA
PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID)
	,COLUMN (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID)
	,COLUMN (RMS_SKU_ID)
	,COLUMN (LOCATION_ID)
		ON SKU_SOH_01;


create multiset volatile table sku_soh_02
as
(
SELECT
	a.rms_sku_id
	,b.store_type_code
	,b.store_type_code_new
	,b.store_country_code
	,b.selling_channel
	,a.snapshot_date
	,a.snapshot_date_boh
	,a.value_updated_time
	,a.location_id
	,a.nonsellable_qty
	,a.location_type
FROM sku_soh_01 a
LEFT JOIN
	store_01 b
		on a.location_id = b.store_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
with DATA
primary index (rms_sku_id,store_type_code,store_country_code,snapshot_date)
on commit preserve rows;

collect stats
	primary index ( rms_sku_id, store_type_code, store_country_code, snapshot_date)
	,column ( rms_sku_id, store_type_code, store_country_code, snapshot_date)
		on sku_soh_02;


DELETE FROM {environment_schema}.ANNIVERSARY_NONSELL  where snapshot_date BETWEEN {start_date_soh} AND {end_date};

INSERT INTO {environment_schema}.ANNIVERSARY_NONSELL
select rms_sku_id
	,snapshot_date
	,snapshot_date_boh
	,value_updated_time
	,location_id
	,nonsellable_qty
	,current_price_type
from (
	select a.rms_sku_id
		,a.snapshot_date
		,a.snapshot_date_boh
		,a.value_updated_time
		,a.location_id
		,a.nonsellable_qty
		,min(b.ownership_price_type) as current_price_type
	from sku_soh_02 a left JOIN
		price_01 b
			on a.rms_sku_id = b.rms_sku_num
				and a.store_type_code_new = b.store_type_code
				and a.store_country_code = b.channel_country
				and a.selling_channel = b.selling_channel
				and cast(a.snapshot_date as timestamp) between b.EFF_BEGIN_TMSTP AND b.EFF_END_TMSTP - interval '0.001' second
	where a.nonsellable_qty <> 0
	group by 1,2,3,4,5,6) a
;

collect stats
	primary index (rms_sku_id,snapshot_date,location_id)
	,column (rms_sku_id)
	,column (snapshot_date)
	,column (location_id)
		on {environment_schema}.ANNIVERSARY_NONSELL;
