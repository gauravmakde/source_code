/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=item_demand_forecasting_rp;'
     FOR SESSION VOLATILE;
 

/*
T2/Table Name: IDF_RP
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:  
This table stores all RP data by EPM_CHOICE_NUM X WEEK using RP flag effective dates

*/ 

DELETE FROM T2DL_DAS_INV_POSITION_FORECAST.IDF_RP
WHERE (week_start_date >= '2021-06-13' AND week_start_date <= '2021-06-27');

INSERT INTO T2DL_DAS_INV_POSITION_FORECAST.IDF_RP
with store as (
    select
        store_num,
        CASE
            WHEN channel_num in (110, 120) THEN 'NORDSTROM'
            WHEN channel_num in (210, 250) THEN 'NORDSTROM_RACK'
        END AS channel_brand,
        CASE
            WHEN channel_num in (110, 210) THEN 'STORE'
            WHEN channel_num in (120, 250) THEN 'ONLINE'
        END AS selling_channel
    from
        PRD_NAP_USR_VWS.STORE_DIM
    where
        channel_num in (110, 210, 120, 250)
),
wk as (
    SELECT distinct
        week_idnt as week_num, 
        cast(day_date as date) as day_date,  
        CAST(week_start_day_date as date) as week_start_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dcd 
        where cast(week_start_date as date) between  '2021-06-13' and '2021-06-27'
), 
sku as ( 
    select distinct
        rms_sku_num,
        epm_choice_num
    from 
        PRD_NAP_USR_VWS.PRODUCT_SKU_DIM
    where
          channel_country = 'US'
)
select 
	store.channel_brand,
	store.selling_channel,
	sku.epm_choice_num,
	wk.week_num,
    wk.week_start_date,
	current_timestamp(0) at time zone 'gmt' as last_updated_utc,
	1 as rp_ind,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from prd_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
    join wk
		on wk.day_date >= begin(rp.RP_PERIOD) --what about scenario where it stops being effective in between these dates (would need some weeks and not others)
		and wk.day_date <= end(rp.RP_PERIOD)
     JOIN store
        ON store.STORE_NUM = CAST(RP.location_num AS INTEGER)
     JOIN sku
        ON RP.rms_sku_num  = sku.rms_sku_num
    WHERE sku.epm_choice_num IS NOT NULL
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        8;

COLLECT STATISTICS  COLUMN (week_start_date),
                    COLUMN (epm_choice_num), -- column names used for primary index
                    COLUMN (channel_brand),  -- column names used for partition
                    COLUMN (selling_channel),
                    COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel)  
on T2DL_DAS_INV_POSITION_FORECAST.IDF_RP;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;