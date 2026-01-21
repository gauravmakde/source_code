SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=store_dim_11521_ACE_ENG;
     Task_Name=store_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.store_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Store-level lookup table containing attributes specific to a store (including location, size, & demographics of the surrounding area).
-- What is the update cadence/lookback window: weekly refreshment, run every Monday at 8am UTC
*/

--drop table store_dim_prep;
CREATE VOLATILE TABLE store_dim_prep AS 
(select distinct 
                -- smart sql fields
                --  ds.store_num as smart_store_id
                -- ,business_unit_desc as smart_business_unit

                -- standardized fields
                -- ,
                ds.store_num as store_number
                ,ds.store_name
                ,business_unit_desc as business_unit
                ,case when regexp_instr(business_unit_desc, 'FULL LINE')>0 then 'fls'
                      when regexp_instr(business_unit_desc, 'RACK')>0 then 'rs'
                      when regexp_instr(business_unit_desc, 'N[.]COM')>0 then 'ncom'  
                      when regexp_instr(business_unit_desc, 'N[.]CA')>0 then 'nca'  
                      when regexp_instr(business_unit_desc, 'OFFPRICE ONLINE')>0 then 'rcom'        
                      when regexp_instr(business_unit_desc, 'TRUNK CLUB')>0 then 'tc'        
                      else 'other'  
                  end as channel_id
                ,case when channel_id IN ('fls','ncom') then 'nordstrom' 
                      when channel_id IN ('rs','rcom') then 'rack'
                      else 'other'
                  end as channel_banner
                ,case when channel_id IN ('rcom','ncom','tc') then 'digital' 
                      when channel_id IN ('rs','fls') then 'stores'
                      else 'other'
                  end as channel_journey

                -- base store info
                ,coalesce(case when NOT(channel_journey = 'stores') then 'undefined' 
                               when store_segment = '' then 'undefined' 
                               else store_segment
                           end,'undefined') as store_segment
                ,store_open_date
                ,store_close_date

                -- location
                ,case when NOT(channel_journey = 'stores') then 'undefined' else coalesce(area_type,'undefined' ) end as store_location_areatype
                ,case when NOT(channel_journey = 'stores') then 'undefined' else coalesce(center_type,'undefined') end as store_location_centertype
                ,case when NOT(channel_journey = 'stores') then 'undefined' else coalesce(mall_type,'undefined') end as store_location_malltype
                ,case when NOT(channel_journey = 'stores') then 'undefined' else coalesce(trade_area_type,'undefined') end as store_location_tradeareatype
                ,location_quality_score as store_location_qualityscore

                -- geo
                ,store_address_city as store_city
                ,msa as store_msa
                ,dma as store_dma
                ,store_address_state as store_state
                ,region_desc store_region
                ,case when regexp_instr(lower(business_unit_desc),'.ca|canada') > 0 then 'canada' else 'us' end as country
                ,store_location_latitude as store_latitude
                ,store_location_longitude as store_longitude

                -- layout
                ,gross_square_footage as store_layout_sqft
                ,floors as store_layout_floors
                ,max_occupancy_load as store_layout_maxoccupancy
                ,exterior_entries as store_layout_entries

                -- proximity/commute
                ,ta_avg_drivetime as store_commute_drivetime
                ,ta_avg_drivedistance as store_commute_drivedistance
                ,nearest_fls_drivedistance as store_proximity_fls
                ,nearest_rack_drivedistance as store_proximity_rack

                -- population attributes
                ,population as store_population_people
                ,coalesce(households,0) as store_population_households
                ,population_density as store_population_density
                ,student_population as store_population_students
                ,case when coalesce(households,0) = 0 then 0 else coalesce(population,0)/coalesce(households,0) end as store_population_avghhsize
                ,case when coalesce(population,0) = 0 then 0 else coalesce(population_5yr_projection,0)/coalesce(population,0) end as store_population_growth


                -- income attributes
                ,median_income as store_income_median
                ,case when coalesce(households,0) = 0 then 0 else coalesce(hhlds_75k_plus,0)/coalesce(households,0) end as store_income_75k
                ,case when coalesce(households,0) = 0 then 0 else coalesce(hhlds_100k_plus,0)/coalesce(households,0) end as store_income_100k
                ,case when coalesce(households,0) = 0 then 0 else coalesce(hhlds_100k_plus_5yr_projection,0)/coalesce(households,0) end as store_income_growth

                -- apparel
                ,apparel_spend_index as store_apparelspend_index

                -- store segment
                ,case when NOT(channel_journey = 'stores') then 'undefined'
                      when store_population_density <1000 then '0 - 1K / mile ~ low density'
                      when store_population_density <2000 then '1 - 2K / mile ~ medium density'
                      when store_population_density <5000 then '2 - 5K / mile ~ high density'	
                      else  'very high density (5K+ / mile)'
                  end as store_population_density_segment
                ,case when NOT(channel_journey = 'stores') then 'undefined'
                      when store_commute_drivetime <15 then ' 0 - 15 mins to nearest store'
                      when store_commute_drivetime <30 then '15 - 30 mins to nearest store'	
                      else '30 mins + to nearest store'
                  end as store_commute_drivetime_segment
                ,case when NOT(channel_journey = 'stores') then 'undefined'
                      when store_commute_drivedistance < 2 then ' 0 -  2 Miles to nearest store'
                      when store_commute_drivedistance <10 then ' 2 - 10 Miles to nearest store'
                      else '10 Miles + to nearest store'
                  end as store_commute_drivedistance_segment
                ,case when NOT(channel_journey = 'stores') then 'undefined'
                      when store_proximity_fls < 2 then ' 0 -  2 Miles to nearest FLS'
                      when store_proximity_fls <10 then ' 2 - 10 Miles to nearest FLS'
                      else '10 Miles + to nearest FLS'
                  end as store_proximity_fls_segment
                ,case when NOT(channel_journey = 'stores') then 'undefined'
                      when store_proximity_rack < 2 then ' 0 -  2 Miles to nearest Rack'
                      when store_proximity_rack <10 then ' 2 - 10 Miles to nearest Rack'
                      else '10 Miles + to nearest Rack'
                  end as store_proximity_rack_segment
                ,case when NOT(channel_journey = 'stores') then 'undefined'
                      when store_layout_maxoccupancy <1000 then '0 - 1K people ~ small occupancy store'
                      when store_layout_maxoccupancy <3000 then '1 - 3K people ~ medium occupancy store'	
                      else '3K people + ~ large occupancy store'
                  end as store_layout_maxoccupancy_segment

                -- smartsql: store segment 
                ,hash_md5(concat(coalesce(channel_id,'')
                         ,concat('|',coalesce(store_region,'') )
                         ,concat('|',coalesce(store_segment,'') )
                         ,concat('|',coalesce(store_location_areatype,'') )
                         ,concat('|',coalesce(store_location_tradeareatype,'') )
                         ,concat('|',coalesce(store_population_density_segment,'') )
                         ,concat('|',coalesce(store_commute_drivedistance_segment,'') )
                         ,concat('|',coalesce(store_proximity_fls_segment,'')  )
                         ,concat('|',coalesce(store_proximity_rack_segment,'') )
                         ,concat('|',coalesce(store_layout_maxoccupancy_segment,'') )
                        ) ) as store_segment_id
                ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
  from prd_nap_usr_vws.store_dim as ds
  left join t2dl_das_real_estate.re_store_attributes as sat
    on ds.store_num = sat.store_number 
  left join t2dl_das_real_estate.re_store_trade_area_attributes as ts
    on ds.store_num = ts.store_number
)with data primary index (store_number) ON COMMIT PRESERVE ROWS;

DELETE FROM {usl_t2_schema}.store_dim;
INSERT INTO {usl_t2_schema}.store_dim
SELECT * FROM store_dim_prep;
COLLECT STATISTICS COLUMN (store_number) on {usl_t2_schema}.store_dim;

SET QUERY_BAND = NONE FOR SESSION;