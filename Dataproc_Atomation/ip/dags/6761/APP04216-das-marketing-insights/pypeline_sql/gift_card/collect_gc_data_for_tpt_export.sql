{teradata_autocommit_off};

CREATE VOLATILE TABLE VT_GIFT_CARD_RPT_ATTRIBUTES_EXT AS
(
	SELECT Trim(Coalesce(gctf.alt_merchant_id,'')) || Coalesce((' - ' || Trim(gctf.alt_merchant_desc)),'') AS alt_merchant_desc
	, Trim(Coalesce(gctf.merchant_id,'')) || Coalesce((' - ' || Trim(gctf.merchant_desc)),'') AS merchant_desc
	, Trim(Coalesce(gctf.class_id,'')) || Coalesce((' - ' || Trim(gctf.class_name)),'') AS class_name
	, Trim(Coalesce(gctf.promo_code,'')) || Coalesce((' - ' || Trim(gctf.promo_code_desc)),'') AS promo_code_desc
	, gctf.rpt_business_unit_name
	, gctf.rpt_location
	, gctf.rpt_geog
	, gctf.rpt_channel
	, gctf.rpt_banner
	, gctf.rpt_distribution_partner_desc
	, gctf.tran_type
	, Min(gctf.tran_date_est ) AS min_tran_date
	, Max(gctf.tran_date_est ) AS max_tran_date
	, Sum(Abs(gctf.tran_usd_amt)) AS tran_usd_amt
	, Count(*) AS tran_cnt
	FROM {db_env}_NAP_USR_VWS.GIFT_CARD_TRAN_FACT gctf
	LEFT JOIN {db_env}_NAP_USR_VWS.DAY_CAL dc
	ON dc.day_date=gctf.tran_date_est
	WHERE Trim(gctf.rpt_business_unit_name) = ''
	OR Trim(gctf.rpt_location) = ''
	OR Trim(gctf.rpt_geog) = ''
	OR Trim(gctf.rpt_channel) = ''
	OR Trim(gctf.rpt_banner ) = ''
	OR Trim(gctf.rpt_distribution_partner_desc) = ''
	OR gctf.rpt_business_unit_name is NULL
	OR gctf.rpt_location is NULL
	OR gctf.rpt_geog is NULL
	OR gctf.rpt_channel is NULL
	OR gctf.rpt_banner is NULL
	OR gctf.rpt_distribution_partner_desc is NULL
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA
NO PRIMARY INDEX
ON COMMIT PRESERVE ROWS;

ET;

DELETE FROM {db_env}_NAP_STG.GIFT_CARD_RPT_ATTRIBUTES_EXPORT_LDG ALL;

INSERT INTO {db_env}_NAP_STG.GIFT_CARD_RPT_ATTRIBUTES_EXPORT_LDG
SELECT alt_merchant_desc
, merchant_desc
, class_name
, promo_code_desc
, rpt_business_unit_name
, rpt_location
, rpt_geog
, rpt_channel
, rpt_banner
, rpt_distribution_partner_desc
, (TRIM(TRAILING '|' FROM (XMLAGG(TRIM(tran_type)|| '|' ORDER BY tran_type) (VARCHAR(500))))) tran_type_list
, Cast( Cast( Min(min_tran_date) AS FORMAT 'YYYY-MM-DD') as varchar(10)) AS min_tran_date
, Cast( Cast( Max(max_tran_date) AS FORMAT 'YYYY-MM-DD') as varchar(10)) AS max_tran_date
, CAST(Sum(tran_usd_amt) AS VARCHAR(50)) as tran_usd_amt
, CAST(Sum(tran_cnt) AS VARCHAR(30)) as tran_cnt
FROM VT_GIFT_CARD_RPT_ATTRIBUTES_EXT
GROUP BY 1,2,3,4,5,6,7,8,9,10
;

ET;