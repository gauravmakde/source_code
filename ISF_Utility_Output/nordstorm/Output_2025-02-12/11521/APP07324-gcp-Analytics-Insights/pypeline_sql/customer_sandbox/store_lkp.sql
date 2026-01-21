SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=store_lkp_11521_ACE_ENG;
     Task_Name=store_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.store_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/14/2023

Notes:
-- Store Look-up table for microstrategy customer sandbox
*/
/*Pulls all distinct store, region, channel, banner & country combinations from T2DL_DAS_STRATEGY.cco_line_items */
--drop table store_mapping;
CREATE MULTISET VOLATILE TABLE store_mapping AS(
	SELECT DISTINCT cli.store_num
                  , cli.store_region
                  , store_dma_desc
                  , store_dma_code as store_dma_num
                  , channel
                  , cli.channel_country
                  , cli.banner
	FROM
	T2DL_DAS_STRATEGY.cco_line_items cli
	left join prd_nap_usr_vws.JWN_STORE_DIM_VW jsd on cli.store_num = jsd.store_num
)WITH DATA PRIMARY INDEX(store_num) ON COMMIT PRESERVE ROWS;

/*Pulls all max region num from store dim to avoid duplication due to region having mutiple identifiers */
CREATE MULTISET VOLATILE TABLE region_mapping AS(
	SELECT sm.store_region
           , max(region_num) as store_region_num
	FROM store_mapping sm
	left join prd_nap_usr_vws.store_dim sd on sm.store_num = sd.store_num
	GROUP BY 1
)WITH DATA PRIMARY INDEX(store_region) ON COMMIT PRESERVE ROWS;


/*Assign unique identifier to store, region, channel, banner & country combinations from T2DL_DAS_STRATEGY.cco_line_items */
--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE FROM {str_t2_schema}.store_lkp;

INSERT INTO {str_t2_schema}.store_lkp
        SELECT DISTINCT COALESCE(sm.store_num ,-1) as store_num
                    , sm.store_num||' - '||store_name as store_desc
                    , sm.store_dma_desc
                    , COALESCE(sm.store_dma_num,-1) as store_dma_num
                    , COALESCE(sm.store_region, 'Unknown') as store_region_desc
                    , COALESCE(store_region_num,-1) store_region_num
                    , COALESCE(channel, 'Unknown') as channel_desc
                    , COALESCE(CAST(LEFT(channel,1)AS INTEGER),-1) AS channel_num
                    , COALESCE(banner, 'Unknown') as banner_desc
                    , CASE WHEN banner_desc='NORDSTROM' THEN 1 
                           WHEN banner_desc='RACK' THEN 2 
                           ELSE 3 END AS banner_num
                    , COALESCE(channel_country, 'Unknown') as channel_country_desc
                    , CASE WHEN channel_country_desc='US' THEN 1 
                           WHEN channel_country_desc ='CA' THEN 2 
                           ELSE 3 END AS channel_country_num
                    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM store_mapping sm
    LEFT JOIN prd_nap_usr_vws.store_dim sd on sm.store_num=sd.store_num
   	LEFT JOIN region_mapping rm on sm.store_region=rm.store_region ;

SET QUERY_BAND = NONE FOR SESSION;