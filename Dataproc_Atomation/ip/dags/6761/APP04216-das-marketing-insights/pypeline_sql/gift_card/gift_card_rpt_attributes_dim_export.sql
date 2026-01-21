SELECT *
FROM (
    SELECT
      CAST('alt_merchant_desc' AS VARCHAR(255)) as alt_merchant_desc
    , CAST('merchant_desc' AS VARCHAR(255)) as merchant_desc
    , CAST('class_name' AS VARCHAR(200)) as class_name
    , CAST('promo_code_desc' AS VARCHAR(100)) as promo_code_desc
    , CAST('rpt_business_unit_name' AS VARCHAR(200)) as rpt_business_unit_name
    , CAST('rpt_location' AS VARCHAR(50)) as rpt_location
    , CAST('rpt_geog' AS VARCHAR(50)) as rpt_geog
    , CAST('rpt_channel' AS VARCHAR(50)) as rpt_channel
    , CAST('rpt_banner' AS VARCHAR(50)) as rpt_banner
    , CAST('rpt_distribution_partner_desc' AS VARCHAR(200)) as rpt_distribution_partner_desc
    , CAST('tran_type_list' AS VARCHAR(500)) as tran_type
    , CAST('min_tran_date' AS CHAR(10)) as min_tran_date
    , CAST('max_tran_date' AS CHAR(10)) as max_tran_date
    , CAST('tran_usd_amt' AS VARCHAR(50)) as tran_usd_amt
    , CAST('tran_cnt' AS VARCHAR(30)) as tran_cnt
    FROM ( SELECT 1 as one) dummy

    UNION ALL

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
    , tran_type
    , min_tran_date
    , max_tran_date
    , tran_usd_amt
    , tran_cnt
    FROM {db_env}_NAP_STG.GIFT_CARD_RPT_ATTRIBUTES_EXPORT_LDG
) rsltset
ORDER BY CASE WHEN alt_merchant_desc = ''alt_merchant_desc'' THEN 1 ELSE 2 END;
