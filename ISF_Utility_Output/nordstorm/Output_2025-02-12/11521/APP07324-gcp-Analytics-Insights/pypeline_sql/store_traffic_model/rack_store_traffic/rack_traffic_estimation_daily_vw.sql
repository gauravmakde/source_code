SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_traffic_estimation_daily_vw_11521_ACE_ENG;
     Task_Name=rack_traffic_estimation_daily_vw;'
     FOR SESSION VOLATILE;

/*

T2/View Name: t2dl_das_fls_traffic_model.rack_traffic_estimation_daily_vw
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/10/2023

Notes: 
-- Purpose: Brings all the data together in a single view + applies additional adjustments. Best view for any data diagnostics

*/


REPLACE VIEW {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily_vw AS

SELECT tr.store_number
        , store_name
        , tr.day_date
        , fiscal_week
        , fiscal_month
        , fiscal_year
        , day_454_num
        , store_open_date
        , store_close_date
        , tr.region
        , dma
        , reopen_date
        , covid_store_closure_flag
        , COALESCE(cmt.corrected_wifi, tr.wifi_users) as wifi_users
        , holiday_flag
        , unplanned_closure
        , period_adj_type
        , purchase_trips
        , net_sales
        , gross_sales
        , camera_flag
        , camera_traffic
        , store_intercept_pred
	      , store_slope_pred
	      , saturday_intercept
	      , saturday_slope
        , base_traffic_estimate
        , coalesce(rgt.traffic,(COALESCE(cmt.corrected_traffic,tr.pre_calibrated_traffic))) as pre_calibrated_traffic
        , COALESCE (case when tr.day_date between COALESCE(rm.clbr_start_date, date'2099-12-31') and COALESCE(rm.clbr_end_date, date'2099-12-31') and ABS(1.000-pre_cal_1wk_ratio) > ABS(1.000-last_3wk_ratio)
        			then COALESCE(rgt.traffic,(COALESCE(cmt.corrected_traffic,tr.pre_calibrated_traffic)))/pre_cal_1wk_ratio*last_3wk_ratio end
        			,COALESCE(rgt.traffic,(COALESCE(cmt.corrected_traffic,tr.pre_calibrated_traffic))) )  as estimated_traffic
        , CASE WHEN camera_flag=1 THEN camera_traffic else estimated_traffic end as traffic

    FROM {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily tr

      LEFT JOIN {fls_traffic_model_t2_schema}.corrected_modeled_traffic cmt on tr.store_number=cmt.store_number and cmt.day_date=tr.day_date
      LEFT JOIN {fls_traffic_model_t2_schema}.rack_region_mapping rm on tr.region=rm.region and tr.day_date between COALESCE(rm.clbr_start_date, date'2099-12-31') and COALESCE(rm.clbr_end_date, date'2099-12-31')
      LEFT JOIN {fls_traffic_model_t2_schema}.rack_traffic_model_calibration_coeff cra on rm.region_group = cra.region_group and tr.day_date between cra.week_start_date and cra.week_end_date
      LEFT JOIN {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic rgt on tr.store_number=rgt.store_number AND tr.day_date =rgt.day_dt and rgt.day_dt<=date'2019-03-30'
  ;
 
SET QUERY_BAND = NONE FOR SESSION;