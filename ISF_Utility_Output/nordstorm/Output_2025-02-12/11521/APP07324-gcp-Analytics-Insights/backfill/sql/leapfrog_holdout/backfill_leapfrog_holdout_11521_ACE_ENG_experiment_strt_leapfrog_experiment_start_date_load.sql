
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=leapfrog_holdout_11521_ACE_ENG;
     Task_Name=leapfrog_experiment_start_date_load;'
     FOR SESSION VOLATILE;
    
  

CREATE OR REPLACE TEMPORARY VIEW csv_test
(
`date` date,
channel string,
experience string,
experimentname string
) USING CSV 
OPTIONS(path "s3://digital-insights/gv2f/experimentstartdate/experimentstart.csv",
  sep  ",",
  header "true",
  dateFormat "M/d/yy"
  );

  create
or replace temp view cal as
SELECT
        DISTINCT TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) day_date,
       TO_DATE(CAST(
                        UNIX_TIMESTAMP(week_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )) as week_start_day_date,
        week_end_day_date,
        TO_DATE(CAST(
                        UNIX_TIMESTAMP(month_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )) as month_start_day_date
        ,
        month_end_day_date,
        TO_DATE(CAST(
                        UNIX_TIMESTAMP(quarter_start_day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )) as quarter_start_day_date
        ,
        quarter_end_day_date,
        fiscal_day_num,
        fiscal_week_num,
        fiscal_month_num,
        fiscal_quarter_num,
        fiscal_year_num 
FROM
        object_model.day_cal_454_dim b
WHERE
        TO_DATE(
                CAST(
                        UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP
                )
        ) between date'2022-06-01' and current_date();


create or replace temp view daily as 
select 
activity_date_partition,
channel,
experience,
CONCAT_WS(',', collect_set(experimentname)) experimentname
from 
(select 
 TO_DATE(CAST(
                        UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP
                )) as activity_date_partition,
channel,
experience,                
experimentname
from csv_test
)
group by 1,2,3;


create
or replace temp view wtd as
select 
week_start_day_date,
channel,
experience,
CONCAT_WS(',', collect_set(experimentname)) experimentname
from 
(
select
week_start_day_date,
channel,
experience,
experimentname
from daily  a
left join cal
on a.activity_date_partition = cal.day_date
)
group by 1,2,3;


create
or replace temp view mtd as
select 
month_start_day_date,
channel,
experience,
CONCAT_WS(',', collect_set(experimentname))  experimentname
from 
(
select
month_start_day_date,
channel,
experience,
experimentname
from daily  a
left join cal
on a.activity_date_partition = cal.day_date
)
group by 1,2,3;

create
or replace temp view qtd as
select 
quarter_start_day_date,
channel,
experience,
CONCAT_WS(',', collect_set(experimentname))  experimentname
from 
(
select
quarter_start_day_date,
channel,
experience,
experimentname
from daily  a
left join cal
on a.activity_date_partition = cal.day_date
)
group by 1,2,3;


create or replace temp view final as 
select 
a.activity_date_partition,
a.channel,
a.experience,
cal.week_start_day_date,
cal.month_start_day_date,
cal.quarter_start_day_date,
a.experimentname as daily_experimentname,
wtd.experimentname as wtd_experimentname,
mtd.experimentname as mtd_experimentname,
qtd.experimentname as qtd_experimentname
from daily a 
left join cal 
on a.activity_date_partition = cal.day_date
left join wtd
on cal.week_start_day_date = wtd.week_start_day_date
and a.channel = wtd.channel
and a.experience = wtd.experience
left join mtd
on cal.month_start_day_date = mtd.month_start_day_date
and a.channel = mtd.channel
and a.experience = mtd.experience
left join qtd
on cal.quarter_start_day_date = qtd.quarter_start_day_date
and a.channel = qtd.channel
and a.experience = qtd.experience;

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert
        overwrite table leapfrog_experiment_start_date_output
select
        *
from
        final;