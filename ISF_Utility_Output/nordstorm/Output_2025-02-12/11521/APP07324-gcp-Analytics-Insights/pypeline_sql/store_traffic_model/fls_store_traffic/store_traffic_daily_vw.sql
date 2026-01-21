SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=store_traffic_daily_vw_11521_ACE_ENG;
     Task_Name=store_traffic_daily_vw;'
     FOR SESSION VOLATILE;

/*

T2/View Name: t2dl_das_fls_traffic_model.store_traffic_daily_vw
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Modified: 03/07/2023

Notes: 
-- This view combines traffic for all the different store types - Nordstrom Stores, Nordstrom Rack Stores,
-- Nordstrom Locals & Last Chance Stores, in a single view. Along with that it also includes information commonly
-- used with traffic metrics by Finance & other groups like store information, fiscal calendar information and
-- LY & LLY traffic information based on re-aligned 454 calendar

*/


REPLACE
VIEW {fls_traffic_model_t2_schema}.store_traffic_daily_vw AS
select st.store_number
     , trim(concat(st.store_number, ' - ', sd.store_name)) as                location
     , case when current_date() - 1 >= sd.store_close_date then 1 else 0 end closed_store_flag
     , st.covid_store_closure_flag

     --Store Location Details
     , sd.region_desc                                      as                region
     , st.dma
     , sd.store_type_code
     , sd.business_unit_desc
     , sd.comp_status_desc
     --this flag tags stores with complete data starting fiscal 2019
     , CASE
           WHEN msrmnt_start_date <= (select min(day_date) from prd_nap_usr_vws.DAY_CAL where year_num = 2019)
               AND store_open_date <= (select min(day_date) from prd_nap_usr_vws.DAY_CAL where year_num = 2019)
               THEN 1
           ELSE 0 END                                      AS                traffic_reporting_flag

     --Fiscal Calendar Details
     , st.day_date
     , dt.week_454_num                                     as                fiscal_week
     , dt.month_454_num                                    as                fiscal_month
     , dt.year_num                                         as                fiscal_year

     --Traffic Metrics
     , st.purchase_trips
     , st.traffic
     , st.traffic_source

     --LY & LLY traffic information
     , ly_traffic
     , ly_purchase_trips
     , lly_traffic
     , lly_purchase_trips


from (
         --FLS Store Traffic Metrics
         select ty.store_number
              , ty.store_name
              , ty.region
              , ty.dma
              , ty.covid_store_closure_flag

              , ty.day_date

              --Traffic Metrics
              , ty.purchase_trips
              , ty.traffic
              , ty.traffic_source

              --LY & LLY traffic information
              , ly.traffic         AS ly_traffic
              , ly.purchase_trips  AS ly_purchase_trips
              , lly.traffic        AS lly_traffic
              , lly.purchase_trips AS lly_purchase_trips

         from (select a.store_number
                    , a.store_name
                    , a.region
                    , a.dma
                    , a.covid_store_closure_flag
                    , a.day_date
                    , a.purchase_trips
                    , a.traffic
                    , a.traffic_source
                    , dt.day_num
               from {fls_traffic_model_t2_schema}.fls_traffic_daily a
                        left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) ty
                  left join (select a.store_number
                                  , a.traffic
                                  , a.purchase_trips
                                  , dt.day_num
                             from {fls_traffic_model_t2_schema}.fls_traffic_daily a
                                      left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) ly
                            on ty.store_number = ly.store_number and ty.day_num - 1000 = ly.day_num
                  left join (select a.store_number
                                  , a.traffic
                                  , a.purchase_trips
                                  , dt.day_num
                             from {fls_traffic_model_t2_schema}.fls_traffic_daily a
                                      left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) lly
                            on ty.store_number = lly.store_number and ty.day_num - 2000 = lly.day_num

         union
         --NL & LC Store Traffic Metrics
         select ty.store_number
              , ty.store_name
              , ty.region
              , ty.dma
              , ty.covid_store_closure_flag

              , ty.day_date

              --Traffic Metrics
              , ty.purchase_trips
              , ty.traffic
              , ty.traffic_source

              --LY & LLY traffic information
              , ly.traffic         AS ly_traffic
              , ly.purchase_trips  AS ly_purchase_trips
              , lly.traffic        AS lly_traffic
              , lly.purchase_trips AS lly_purchase_trips

         from (select a.store_number
                    , a.store_name
                    , a.region
                    , a.dma
                    , a.covid_store_closure_flag
                    , a.day_date
                    , a.purchase_trips
                    , a.traffic
                    , a.traffic_source
                    , dt.day_num
               from {fls_traffic_model_t2_schema}.nl_lc_traffic_daily a
                        left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) ty
                  left join (select a.store_number
                                  , a.traffic
                                  , a.purchase_trips
                                  , dt.day_num
                             from {fls_traffic_model_t2_schema}.nl_lc_traffic_daily a
                                      left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) ly
                            on ty.store_number = ly.store_number and ty.day_num - 1000 = ly.day_num
                  left join (select a.store_number
                                  , a.traffic
                                  , a.purchase_trips
                                  , dt.day_num
                             from {fls_traffic_model_t2_schema}.nl_lc_traffic_daily a
                                      left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) lly
                            on ty.store_number = lly.store_number and ty.day_num - 2000 = lly.day_num

         union
         -- Rack Store Traffic Metrics
         select ty.store_number
              , ty.store_name
              , ty.region
              , ty.dma
              , ty.covid_store_closure_flag

              , ty.day_date

              --Traffic Metrics
              , ty.purchase_trips
              , ty.traffic
              , ty.traffic_source

              --LY & LLY traffic information
              , ly.traffic         AS ly_traffic
              , ly.purchase_trips  AS ly_purchase_trips
              , lly.traffic        AS lly_traffic
              , lly.purchase_trips AS lly_purchase_trips

         from (select a.store_number
                    , a.store_name
                    , a.region
                    , a.dma
                    , a.covid_store_closure_flag
                    , a.day_date
                    , a.purchase_trips
                    , a.traffic
                    , a.traffic_source
                    , dt.day_num
               from {fls_traffic_model_t2_schema}.rack_traffic_daily a
                        left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) ty
                  left join (select a.store_number
                                  , a.traffic
                                  , a.purchase_trips
                                  , dt.day_num
                             from {fls_traffic_model_t2_schema}.rack_traffic_daily a
                                      left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) ly
                            on ty.store_number = ly.store_number and ty.day_num - 1000 = ly.day_num
                  left join (select a.store_number
                                  , a.traffic
                                  , a.purchase_trips
                                  , dt.day_num
                             from {fls_traffic_model_t2_schema}.rack_traffic_daily a
                                      left join PRD_NAP_USR_VWS.DAY_CAL dt on a.day_date = dt.day_date) lly
                            on ty.store_number = lly.store_number and ty.day_num - 2000 = lly.day_num) st
         left join prd_nap_usr_vws.DAY_CAL dt on st.day_date = dt.day_date
         left join prd_nap_usr_vws.STORE_DIM sd on st.store_number = sd.store_num
         left join (select store_number, min(day_date) msrmnt_start_date
                    from {fls_traffic_model_t2_schema}.fls_traffic_daily
                    group by 1
                    union
                    select store_number, min(day_date) msrmnt_start_date
                    from {fls_traffic_model_t2_schema}.rack_traffic_daily
                    group by 1
                    union
                    select store_number, min(day_date) msrmnt_start_date
                    from {fls_traffic_model_t2_schema}.nl_lc_traffic_daily
                    group by 1) msd on st.store_number = msd.store_number;

SET QUERY_BAND = NONE FOR SESSION;