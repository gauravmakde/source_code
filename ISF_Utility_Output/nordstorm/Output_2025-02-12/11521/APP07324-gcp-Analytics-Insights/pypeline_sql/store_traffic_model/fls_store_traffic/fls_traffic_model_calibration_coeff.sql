
-- t2dl_das_fls_traffic_model.fls_traffic_model_calibration_coeff
-- This job supports fls model calibration based on store's region

CREATE MULTISET VOLATILE TABLE region_last_1wk_ratio_updt  as
 (   select region_group
                      , fiscal_year
                      , fiscal_month
                      , fiscal_week
                      ,  sum(pre_calibrated_traffic)/sum(camera_traffic) as ratio
                      , min(day_date) as start_date
                      , max(Day_date) as end_Date
                 from T2DL_DAS_FLS_Traffic_Model.fls_traffic_estimation_daily_vw tr
                 left join (select distinct region, region_group from T2DL_DAS_FLS_Traffic_Model.fls_region_mapping) rm on tr.region=rm.region
                 where day_date between CURRENT_DATE-6-td_day_of_week(CURRENT_DATE) and CURRENT_DATE-td_day_of_week(CURRENT_DATE)
                 and camera_flag=1
                 and store_number not in (384, 421,210,212,57,4,10,223) -- stores that are outliers compared to rest of the camera store population
                 group by 1,2,3,4
                 having count(distinct day_date)=7 )with data UNIQUE PRIMARY INDEX (region_group, start_date) ON COMMIT PRESERVE ROWS;

---updating ratio for the last complete fiscal week once the job is complete
update x
FROM {fls_traffic_model_t2_schema}.fls_traffic_model_calibration_coeff x, region_last_1wk_ratio_updt y
set pre_cal_1wk_ratio = y.ratio
where y.start_date=x.week_start_date and y.end_date=x.week_end_date and y.region_group=x.region_group;


--calculates the 3 week start date and end date based on the inputted date range
create multiset volatile table ratio_start_end_date as(
	select day_date-20 as start_date
	      , day_date as end_date
	from prd_nap_usr_vws.DAY_CAL
	where day_date = (select max(day_date) from prd_nap_usr_vws.DAY_CAL where day_454_num=7 and day_date<={end_date})
    --between {end_date} and {end_date}
	and day_454_num = 7
)WITH DATA
UNIQUE PRIMARY INDEX(start_date)
ON COMMIT PRESERVE ROWS;


--calculates 3 week average calibration ratio based on actual & estimated traffic by region
create multiset volatile table region_3wk_ratio as(
	select region_group
           , end_date+1  as week_start_date
           , end_date+7 as  week_end_Date
           , avg(ratio) as last_3wk_ratio
     from  (select region_group
                      , fiscal_year
                      , fiscal_month
                      , fiscal_week
                      , sum(estimated_traffic)/sum(camera_traffic) as ratio
                      , max(day_date) as week_date
                 from t2dl_das_fls_traffic_model.fls_traffic_estimation_daily_vw tr
                 left join (select distinct region, region_group, min(clbr_start_date) as clbr_start_date from T2DL_DAS_FLS_Traffic_Model.fls_region_mapping group by 1,2) rm on tr.region=rm.region
                 where day_date between (select min(start_date) from ratio_start_end_date) and (select max(end_date) from ratio_start_end_date)
                 and camera_flag=1
                 and clbr_start_date is not null
                 and store_number not in (384, 421, 57,4,10,223)
                 group by 1,2,3,4
                 having count(distinct day_date)=7
                 )z
      left join ratio_start_end_date y on z.week_date between y.start_date and y.end_date
      group by 1,2,3
)WITH DATA
UNIQUE PRIMARY INDEX(region_group, week_start_Date)
ON COMMIT PRESERVE ROWS;



--DROP TABLE region_1wk_ratio;
 CREATE MULTISET VOLATILE TABLE region_1wk_ratio  as
 (   select region_group
                      , fiscal_year
                      , fiscal_month
                      , fiscal_week
                      , sum(pre_calibrated_traffic)/sum(camera_traffic) as ratio
                      , min(day_date) as start_date
                      , max(Day_date) as end_Date
                 from t2dl_das_fls_traffic_model.fls_traffic_estimation_daily_vw tr
                 left join (SELECT distinct region, region_group from t2dl_das_fls_traffic_model.fls_region_mapping) rm on tr.region=rm.region
                 where day_date between (select min(start_date) from ratio_start_end_date) and (select max(end_date) from ratio_start_end_date)
                 and camera_flag=1
                 and store_number not in (384, 421,210,212, 57,4,10,223) -- stores that are outliers compared to rest of the camera store population
                 group by 1,2,3,4
                 having count(distinct day_date)=7 )with data UNIQUE PRIMARY INDEX (region_group, start_date) ON COMMIT PRESERVE ROWS;


---Joining the  3 week average and inserting into prod table
delete from {fls_traffic_model_t2_schema}.fls_traffic_model_calibration_coeff where week_start_date=(select distinct week_start_date from region_3wk_ratio) and week_end_date = (select distinct week_end_Date from region_3wk_ratio);
insert into {fls_traffic_model_t2_schema}.fls_traffic_model_calibration_coeff
	select distinct w3.region_group
	       , w3.week_start_date
	       , w3.week_end_Date
	       , w3.last_3wk_ratio
	       , w1.ratio as pre_cal_1wk_ratio
             , CURRENT_TIMESTAMP as dw_sys_load_tmstp
	from region_3wk_ratio w3
	left join region_1wk_ratio w1 on w3.region_group=w1.region_group and w3.week_start_date=w1.start_date and w3.week_end_date=w1.end_date
	left join (select distinct region, region_group, min(clbr_start_date) as clbr_start_date from T2DL_DAS_FLS_Traffic_Model.fls_region_mapping group by 1,2) rm on w3.region_group=rm.region_group
	where w3.week_start_date>=clbr_start_date;

collect statistics column(week_start_date) on {fls_traffic_model_t2_schema}.fls_traffic_model_calibration_coeff;

--Running update statement to set the ratio the same as the previous complete week for the ongoing week (where we have incomplete data)
update x
FROM {fls_traffic_model_t2_schema}.fls_traffic_model_calibration_coeff  x, region_1wk_ratio y
set pre_cal_1wk_ratio = y.ratio
where pre_cal_1wk_ratio is NULL and CURRENT_DATE between week_start_date and week_end_date
and y.start_date=x.week_start_date-7 and y.end_date=x.week_end_date-7 and y.region_group=x.region_group;
