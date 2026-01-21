--- Input: T2DL_DAS_SALES_RETURNS.retail_sales_and_returns
--- Output1: T2DL_DAS_BIE_DEV.test_return_isf_stg2
--- Output2: S3
create multiset volatile table return_model_date_range as
(select {start_date} as start_dt,
        {end_date} as end_dt
)with data
ON COMMIT PRESERVE ROWS;

-- delete
DELETE FROM T2DL_DAS_BIE_DEV.test_return_isf_stg2 ALL; --- remove all historic data for testing purpose
-- insert
INSERT INTO T2DL_DAS_BIE_DEV.test_return_isf_stg2  ---- insert new data
select acp_id
       ,case when business_unit_desc in ('FULL LINE') then 'FLS'
             when business_unit_desc in ('N.COM','TRUNK CLUB') then 'NCOM'
             when business_unit_desc in ('RACK') then 'RACK'
             when business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
             when business_unit_desc in ('FULL LINE CANADA', 'N.CA') then 'CANADA_FP'
             when business_unit_desc in ('RACK CANADA') then 'CANADA_OP'
       end as channel
       ,sum(shipped_sales) as shipped_sales 
       ,sum(shipped_qty) as shipped_qty 
       ,sum(return_usd_amt) as return_amt
       ,sum(return_qty) as return_qty
       ,EXTRACT (YEAR FROM tran_date) as s3_year
       ,EXTRACT (MONTH FROM tran_date) as s3_month
       ,EXTRACT (DAY FROM tran_date) as s3_day
 from T2DL_DAS_SALES_RETURNS.retail_sales_and_returns
where 1=1
 and coalesce(order_date,tran_date) BETWEEN (select start_dt from return_model_date_range) and (select end_dt from return_model_date_range)
 and acp_id is not null
 group by acp_id, channel, s3_year, s3_month, s3_day
;
-------
COLLECT STATISTICS PRIMARY INDEX(acp_id),
        COLUMN(shipped_sales, shipped_qty, return_amt, return_qty) ON T2DL_DAS_BIE_DEV.test_return_isf_stg2;
