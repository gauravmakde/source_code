SELECT *
FROM (
    SELECT
      CAST('business_date' AS VARCHAR(1000)) as business_date
    , CAST('business_unit_desc' AS VARCHAR(1000)) as business_unit_desc
    , CAST('store_num' AS VARCHAR(1000)) as store_num
    , CAST('bill_zip_code' AS VARCHAR(1000)) as bill_zip_code
    , CAST('price_type' AS VARCHAR(1000)) as price_type
    , CAST('order_platform_type' AS VARCHAR(1000)) as order_platform_type
    , CAST('loyalty_status' AS VARCHAR(1000)) as loyalty_status
    , CAST('engagement_cohort' AS VARCHAR(1000)) as engagement_cohort
    , CAST('cust_status' AS VARCHAR(1000)) as cust_status
    , CAST('division_name' AS VARCHAR(1000)) as division_name
    , CAST('subdivision_name' AS VARCHAR(1000)) as subdivision_name
    , CAST('jwn_reported_net_sales_amt' AS VARCHAR(1000)) as jwn_reported_net_sales_amt
    FROM ( SELECT 1 as one) dummy

    UNION ALL

    SELECT 
      TO_CHAR(business_date, 'YYYY-MM-DD'),
      business_unit_desc,
      TRIM(store_num (VARCHAR(100))) as store_num,
      bill_zip_code,
      price_type,
      order_platform_type,
      loyalty_status,
      engagement_cohort,
      cust_status,
      division_name,
      subdivision_name,
      TRIM(jwn_reported_net_sales_amt (VARCHAR(100))) as jwn_reported_net_sales_amt
    FROM {proto_schema}.MMM_FINANCE_NET_SALES_KPI_LDG
)rsltset
ORDER BY CASE WHEN business_date = ''business_date'' THEN 1 ELSE 2 END;
