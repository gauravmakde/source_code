/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09142;
     DAG_ID=flash_event_output_11521_ACE_ENG;
     Task_Name=flash_event_output;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_digitalmerch_flash.flash_event_output
Team/Owner: Nicole Miao, Sean Larkin, Cassie Zhang, Rae Ann Boswell
Date Created/Modified: 06/28/2024, modified on 06/28/2024
*/


create multiset volatile table flash_event_temp_table as (
    with evt_raw as (
            select
                s.selling_event_num as event_id,
                sk.brand_name,
                max(case when e.tag_name = 'FEATURED' then 1 else 0 end) as featured_flag,
                max(case when e.tag_name = 'PRIVATE_SALE' then 1 else 0 end) as private_sale_flag,
                count(distinct s.sku_num) sku_cnt,
                max(s.selling_event_name) as event_name,
                cast(min(event_start_tmstp at time zone 'America Pacific') as DATE) as event_start,
                cast(max(event_end_tmstp at time zone 'America Pacific') as DATE) as event_end
            from prd_nap_usr_vws.product_selling_event_valid_sku_vw s
            join prd_nap_usr_vws.product_selling_event_tags_dim e
            on s.selling_event_num = e.selling_event_num
                and s.selling_event_association_id = e.selling_event_association_id
            left join prd_nap_usr_vws.product_sku_dim sk
            on sk.rms_sku_num = s.sku_num
                and substr(sk.channel_country,1,2) = 'US'
            where
                e.tag_name in ('FLASH','FEATURED','PRIVATE_SALE')
            and selling_event_status = 'APPROVED'
            and s.channel_country = 'US'
            and s.channel_brand = 'NORDSTROM_RACK'
            and event_start_tmstp >= date'2023-01-01'
            group by 1,2
    )

    select event_id,
        REGEXP_REPLACE(REGEXP_REPLACE(oreplace(REGEXP_REPLACE(oreplace(event_name, ',', '/'), '\r?\n', ' '), '"', 'inch '), '[^a-zA-Z0-9]+$',''), '^[^a-zA-Z0-9]+','') as event_name,
        featured_flag,
        private_sale_flag,
        unique_brands,
        case when unique_brands <= 5 then 'Branded' else 'Curated' end as branded_flag,
        case when unique_brands <= 5 then brand_name end as brand_name,
        event_start,
        event_end
    from
        (select event_id,
            event_name,
            max(featured_flag)  over(partition by event_id) as featured_flag,
            max(private_sale_flag) over(partition by event_id) as private_sale_flag,
            count(brand_name) over(partition by event_id) unique_brands,
            brand_name,
            sku_cnt,
            row_number() over(partition by event_id order by sku_cnt desc) as rk,
            min(event_start) over(partition by event_id) as event_start,
            max(event_end) over(partition by event_id) as event_end
        from evt_raw
        ) tmp
    where rk = 1
) with data primary index (event_id, event_name, event_start, event_end) on commit preserve rows
;
COLLECT STATISTICS primary index (event_id, event_name, event_start, event_end) on flash_event_temp_table;

------------------------------------------------------------------------------
--- Consolidate the Tables
------------------------------------------------------------------------------
DELETE 
FROM    {digital_merch_t2_schema}.flash_event_output
;

INSERT INTO {digital_merch_t2_schema}.flash_event_output
    select 	
        a.event_id 
        ,TRIM(REGEXP_REPLACE(REGEXP_REPLACE(CAST(a.event_name AS VARCHAR(8000)), '[\t\r\n\v\f|]',' ',1,0, 'i'), '[^[:print:]]','',1,0, 'i')) as event_name   
        ,a.featured_flag 
        ,a.private_sale_flag 
        ,a.unique_brands      
        ,a.branded_flag       
        ,a.brand_name 
        ,a.event_start         
        ,a.event_end   
        ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
    from flash_event_temp_table a
;


COLLECT STATISTICS 
COLUMN (PARTITION),
COLUMN (event_id, event_name, event_start, event_end), -- column names used for primary index
COLUMN (event_id, event_start)  -- column names used for partition
on {digital_merch_t2_schema}.flash_event_output;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


