-- VOLATILE TABLE(S):
-- return_model_date_range
--
-- OUTPUT TABLE(S):
-- T2DL_DAS_BIE_DEV.test_return_isf_stg (a staging table)

-- drop table return_model_date_range; (This statement can be skipped since volatile table drops once session is close)
create multiset volatile table return_model_date_range as
(select current_date - 10 as start_dt,
        current_date as end_dt
)with data
ON COMMIT PRESERVE ROWS;
-- delete
DELETE FROM T2DL_DAS_BIE_DEV.test_return_isf_stg ALL; --- remove all historic data/
-- insert
INSERT INTO T2DL_DAS_BIE_DEV.test_return_isf_stg  ---- insert new data
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
       ,CURRENT_TIMESTAMP as update_timestamp
 from T2DL_DAS_SALES_RETURNS.retail_sales_and_returns
where 1=1
 and coalesce(order_date,tran_date) BETWEEN (select start_dt from return_model_date_range) and (select end_dt from return_model_date_range)
 --and coalesce(order_date,tran_date) BETWEEN current_date - 10  and current_date
 and acp_id is not null
 group by acp_id, channel, update_timestamp
;


COLLECT STATISTICS PRIMARY INDEX(acp_id),
        COLUMN(shipped_sales, shipped_qty, return_amt, return_qty) ON T2DL_DAS_BIE_DEV.test_return_isf_stg;

