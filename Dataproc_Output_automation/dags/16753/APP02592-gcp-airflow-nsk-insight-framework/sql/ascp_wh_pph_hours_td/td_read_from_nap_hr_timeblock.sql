--*****************************************************************************************************************************************
-- Script Details
--*****************************************************************************************************************************************
-- File              : td_read_from_nap_hr_timeblock.sql
-- Author            : Bharat
-- Description       : Read, store and update pph hours timeblock table and aggregated table for PPH Reporting needs
-- Source Tables     : NAP HR timeblock tables
-- ETL Run Frequency : Four Times daily
-- Version :         : 0.1
--*****************************************************************************************************************************************
--*****************************************************************************************************************************************




CREATE TEMPORARY TABLE IF NOT EXISTS pph_timeblock_time_track (
id INTEGER,
last_completed_run_tmstp TIMESTAMP,
last_completed_run_tmstp_tz STRING,
max_nap_hr_available_tmstp TIMESTAMP,
max_nap_hr_available_tmstp_tz STRING
);


INSERT INTO pph_timeblock_time_track (id, last_completed_run_tmstp,last_completed_run_tmstp_tz,max_nap_hr_available_tmstp, max_nap_hr_available_tmstp_tz)
SELECT 1 AS id,
   (SELECT table_last_consumed_tmstp_utc
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pph_hours_last_refreshed_xref_fact
   WHERE LOWER(table_name) = LOWER('HR_TIMEBLOCK_PPH_FACT')) AS last_completed_run_tmstp,
   (select table_last_consumed_tmstp_tz  
   from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pph_hours_last_refreshed_xref_fact) as last_completed_run_tmstp_tz,
  CAST((SELECT MAX(dw_sys_load_tmstp)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_timeblock_pph_fact) AS TIMESTAMP) AS max_nap_hr_available_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST((SELECT MAX(dw_sys_load_tmstp)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_timeblock_pph_fact) AS string)) as max_nap_hr_available_tmstp_tz;


CREATE TEMPORARY TABLE IF NOT EXISTS isf_nap_timeblock_pph_fact AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_timeblock_pph_fact AS tbpph
WHERE dw_sys_load_tmstp BETWEEN CAST((SELECT last_completed_run_tmstp
    FROM pph_timeblock_time_track
    WHERE id = 1) AS DATETIME) AND (CAST((SELECT max_nap_hr_available_tmstp
     FROM pph_timeblock_time_track
     WHERE id = 1) AS DATETIME))
QUALIFY (ROW_NUMBER() OVER (PARTITION BY reference_id ORDER BY modified_moment DESC, dw_sys_load_tmstp DESC)) = 1;


CREATE TEMPORARY TABLE IF NOT EXISTS isf_nap_calculated_timeblock_pph_fact AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_calculated_timeblock_pph_fact AS ctbpph
WHERE dw_sys_load_tmstp >= DATETIME_SUB((SELECT MIN(dw_sys_load_tmstp)
    FROM isf_nap_timeblock_pph_fact), INTERVAL 60 DAY);
    
    
CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_tmp_timeblock_calculated_tbl AS
SELECT b.reference_id,
  CASE
  WHEN LOWER(SUBSTR(b.modified_moment_utc_offset, 1, 1)) = LOWER('-')
  THEN DATETIME_SUB(DATETIME_SUB(b.modified_moment, INTERVAL CAST(TRUNC(CAST(SUBSTR(b.modified_moment_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER)
    HOUR), INTERVAL CAST(TRUNC(CAST(SUBSTR(b.modified_moment_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER) MINUTE)
  WHEN LOWER(SUBSTR(b.modified_moment_utc_offset, 1, 1)) = LOWER('+')
  THEN DATETIME_ADD(DATETIME_ADD(b.modified_moment, INTERVAL CAST(TRUNC(CAST(SUBSTR(b.modified_moment_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER)
    HOUR), INTERVAL CAST(TRUNC(CAST(SUBSTR(b.modified_moment_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER) MINUTE)
  ELSE b.modified_moment
  END AS modified_moment,
 COALESCE(b.payroll_store_code, c.payroll_store) AS home_store,
 COALESCE(b.payroll_department_code, c.payroll_department) AS home_department,
 b.override_codes_store AS override_store,
 b.override_codes_department AS override_department,
  CASE
  WHEN LOWER(SUBSTR(b.in_time_utc_offset, 1, 1)) = LOWER('-')
  THEN DATETIME_SUB(DATETIME_SUB(b.in_time, INTERVAL CAST(TRUNC(CAST(SUBSTR(b.in_time_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER) HOUR), INTERVAL CAST(TRUNC(CAST(SUBSTR(b.in_time_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER)
   MINUTE)
  WHEN LOWER(SUBSTR(b.in_time_utc_offset, 1, 1)) = LOWER('+')
  THEN DATETIME_ADD(DATETIME_ADD(b.in_time, INTERVAL CAST(TRUNC(CAST(SUBSTR(b.in_time_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER) HOUR), INTERVAL CAST(TRUNC(CAST(SUBSTR(b.in_time_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER)
   MINUTE)
  ELSE b.in_time
  END AS in_time,
  CASE
  WHEN LOWER(SUBSTR(b.out_time_utc_offset, 1, 1)) = LOWER('-')
  THEN DATETIME_SUB(DATETIME_SUB(b.out_time, INTERVAL CAST(TRUNC(CAST(SUBSTR(b.out_time_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER) HOUR),
   INTERVAL CAST(TRUNC(CAST(SUBSTR(b.out_time_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER) MINUTE)
  WHEN LOWER(SUBSTR(b.out_time_utc_offset, 1, 1)) = LOWER('+')
  THEN DATETIME_ADD(DATETIME_ADD(b.out_time, INTERVAL CAST(TRUNC(CAST(SUBSTR(b.out_time_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER) HOUR),
   INTERVAL CAST(TRUNC(CAST(SUBSTR(b.out_time_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER) MINUTE)
  ELSE b.out_time
  END AS out_time,
 b.punch_location_code,
 a.calculation_tags,
  CASE
  WHEN LOWER(SUBSTR(a.calculated_in_time_utc_offset, 1, 1)) = LOWER('-')
  THEN DATETIME_SUB(DATETIME_SUB(a.calculated_in_time, INTERVAL CAST(TRUNC(CAST(SUBSTR(a.calculated_in_time_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER)
    HOUR), INTERVAL CAST(TRUNC(CAST(SUBSTR(a.calculated_in_time_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER) MINUTE)
  WHEN LOWER(SUBSTR(a.calculated_in_time_utc_offset, 1, 1)) = LOWER('+')
  THEN DATETIME_ADD(DATETIME_ADD(a.calculated_in_time, INTERVAL CAST(TRUNC(CAST(SUBSTR(a.calculated_in_time_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER)
    HOUR), INTERVAL CAST(TRUNC(CAST(SUBSTR(a.calculated_in_time_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER) MINUTE)
  ELSE a.calculated_in_time
  END AS calculated_in_time,
  CASE
  WHEN LOWER(SUBSTR(a.calculated_out_time_utc_offset, 1, 1)) = LOWER('-')
  THEN DATETIME_SUB(DATETIME_SUB(a.calculated_out_time, INTERVAL CAST(TRUNC(CAST(SUBSTR(a.calculated_out_time_utc_offset, 2, 2)  AS FLOAT64)) AS INTEGER)
    HOUR), INTERVAL CAST(TRUNC(CAST(SUBSTR(a.calculated_out_time_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER) MINUTE)
  WHEN LOWER(SUBSTR(a.calculated_out_time_utc_offset, 1, 1)) = LOWER('+')
  THEN DATETIME_ADD(DATETIME_ADD(a.calculated_out_time, INTERVAL CAST(TRUNC(CAST(SUBSTR(a.calculated_out_time_utc_offset, 2, 2) AS FLOAT64)) AS INTEGER)
    HOUR), INTERVAL CAST(TRUNC(CAST(SUBSTR(a.calculated_out_time_utc_offset, 4, 2) AS FLOAT64)) AS INTEGER) MINUTE)
  ELSE a.calculated_out_time
  END AS calculated_out_time,
 a.calculated_total_hours,
 b.is_deleted,
 a.dw_sys_load_tmstp AS nap_hr_sys_load_tmstp
FROM `isf_nap_calculated_timeblock_pph_fact` AS a
 INNER JOIN `isf_nap_timeblock_pph_fact` AS b 
 ON LOWER(a.reference_id) = LOWER(b.reference_id) 
 AND LOWER(a.worker_number) = LOWER(b.worker_number)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_org_details_dim_eff_date_pph_vw AS c 
 ON LOWER(b.worker_number) = LOWER(c.worker_number
   )
WHERE COALESCE(a.calculated_in_time, b.in_time) BETWEEN c.eff_begin_date AND c.eff_end_date;


CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_tmp_timeblock_del_ref_ids AS
SELECT DISTINCT reference_id
FROM isf_dag_tmp_timeblock_calculated_tbl
WHERE is_deleted = 1;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.pph_hours_calculated_timeblock_fact AS a
WHERE reference_id IN (SELECT reference_id
  FROM isf_dag_tmp_timeblock_del_ref_ids);
  
  DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.pph_hours_calculated_timeblock_fact AS tgt
WHERE EXISTS (SELECT *
 FROM isf_dag_tmp_timeblock_calculated_tbl AS src
 WHERE LOWER(reference_id) = LOWER(tgt.reference_id)
  AND modified_moment >= tgt.modified_moment_tmstp);
  
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.pph_hours_calculated_timeblock_fact
  SELECT reference_id,
 modified_moment AS modified_moment_tmstp,
 home_store AS home_store_num,
 home_department AS home_department_num,
 override_store AS override_store_num,
 override_department AS override_department_num,
 in_time AS in_time_tmstp,
 out_time AS out_time_tmstp,
 punch_location_code AS punch_location_num,
 calculation_tags AS calculation_tags_desc,
 calculated_in_time AS calculated_in_time_tmstp,
 calculated_out_time AS calculated_out_time_tmstp,
 calculated_total_hours AS calculated_total_hours_qty,
 RPAD(CAST(is_deleted AS STRING), 1, ' ') AS is_deleted_ind,
 nap_hr_sys_load_tmstp AS nap_hr_dw_sys_load_tmstp,
   (SELECT dw_batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pph_hours_last_refreshed_xref_fact
   WHERE LOWER(table_name) = LOWER('HR_TIMEBLOCK_PPH_FACT')) + 1 AS dw_batch_id,
 CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_load_tmstp,
 `{{params.gcp_project_id}}.jwn_udf.DEFAULT_TZ_PST`() AS dw_sys_load_tmstp_tz,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_updt_tmstp,
 `{{params.gcp_project_id}}.jwn_udf.DEFAULT_TZ_PST`() AS dw_sys_updt_tmstp_tz
FROM isf_dag_tmp_timeblock_calculated_tbl
WHERE is_deleted = 0
 AND (LOWER(home_store) IN (LOWER('0089'), LOWER('0156'), LOWER('0187'), LOWER('0209'), LOWER('0299'), LOWER('0399'),
     LOWER('0499'), LOWER('0580'), LOWER('0581'), LOWER('0583'), LOWER('0584'), LOWER('0699'), LOWER('0799')) OR LOWER(override_store
     ) IN (LOWER('0089'), LOWER('0156'), LOWER('0187'), LOWER('0209'), LOWER('0299'), LOWER('0399'), LOWER('0499'),
     LOWER('0580'), LOWER('0581'), LOWER('0583'), LOWER('0584'), LOWER('0699'), LOWER('0799')));
     
     
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.pph_hours_last_refreshed_xref_fact
set dw_batch_id = (select max(dw_batch_id) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pph_hours_calculated_timeblock_fact ),
 dw_batch_date = current_date('PST8PDT'),
 dw_sys_updt_tmstp = timestamp(current_datetime('PST8PDT')),
 table_last_consumed_tmstp = (SELECT max_nap_hr_available_tmstp from pph_timeblock_time_track),
 table_last_consumed_tmstp_tz = (SELECT max_nap_hr_available_tmstp_tz from pph_timeblock_time_track)
where LOWER(table_name) = LOWER('HR_TIMEBLOCK_PPH_FACT');


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.pph_hours_agg_fact fact
USING (
SELECT
building_id,
payroll_store_num,
cost_center_num,
department_desc,
department_num,
worked_day_date,
worked_day_hour_num,
total_hours_qty,
overtime_qty,
differential_second_qty,
differential_third_qty,
differential_weekend_am_qty,
differential_weekend_pm_qty,
 dw_batch_id,
dw_batch_date
from (
SELECT
 building_id as building_id,
 coalesce_store as payroll_store_num,
 cost_center as cost_center_num,
 department_rollup_desc as department_desc,
 coalesce_department as department_num,
 worked_date as worked_day_date,
 worked_hour as worked_day_hour_num,
 CAST(sum(hours_worked) as NUMERIC) as total_hours_qty,
 CAST(sum(OT) as NUMERIC) as overtime_qty,
 CAST(sum(Diff2nd) as NUMERIC) as differential_second_qty,
 CAST(sum(Diff3rd)  as NUMERIC) as differential_third_qty,
 CAST(sum(DiffWeekendAM) as NUMERIC) as differential_weekend_am_qty,
 CAST(sum(DiffWeekendPM) as NUMERIC) as differential_weekend_pm_qty,
 max(dw_batch_id) as dw_batch_id,
 max(dw_batch_date) as dw_batch_date
 from (
 SELECT
  reference_id,
  cast(coalesce_store as STRING) as coalesce_store,
  coalesce_department,
  cost_center,
   building_id,
   department_desc,
   department_rollup_desc ,
  calculated_in_time_tmstp, calculated_out_time_tmstp,
  cast(range_start(pd) as DATE) as worked_date,
  Extract(HOUR From range_start(pd)) as worked_hour,
  cast(CAST(Coalesce((INTERVAL timestamp_diff(range_start(calculate_range) ,range_end(calculate_range),MINUTE) minute),INTERVAL '0' MINUTE) AS STRING) as FLOAT64)/60 as hours_worked,
  calculation_tags_desc,
  case
  	when LOWER(calculation_tags_desc) like LOWER('%OT%')
  	then cast(CAST(Coalesce((INTERVAL timestamp_diff(range_start(calculate_range) ,range_end(calculate_range),MINUTE) minute),INTERVAL '0' MINUTE) AS STRING) as FLOAT64)/60
  	else 0
  end as OT,
  case
  	when LOWER(calculation_tags_desc) like LOWER('%ShiftDifferential4pm-10pm%')
  	then cast(CAST(Coalesce((INTERVAL timestamp_diff(range_start(calculate_range) ,range_end(calculate_range),MINUTE) minute),INTERVAL '0' MINUTE) AS STRING) as FLOAT64)/60
  	else 0
  end as Diff2nd,
  case
  	when LOWER(calculation_tags_desc) like LOWER('%ShiftDifferential10pm-4am%')
  	then cast(CAST(Coalesce((INTERVAL timestamp_diff(range_start(calculate_range) ,range_end(calculate_range),MINUTE) minute),INTERVAL '0' MINUTE) AS STRING) as FLOAT64)/60
  	else 0
  end as Diff3rd,
  case
  	when LOWER(calculation_tags_desc) like LOWER('%ShiftDifferential4am-6pm%')
  	then cast(CAST(Coalesce((INTERVAL timestamp_diff(range_start(calculate_range) ,range_end(calculate_range),MINUTE) minute),INTERVAL '0' MINUTE) AS STRING) as FLOAT64)/60
  	else 0
  end as DiffWeekendAM,
  case
  	when LOWER(calculation_tags_desc) like LOWER('%ShiftDifferential6pm-4am%')
  	then cast(CAST(Coalesce((INTERVAL timestamp_diff(range_start(calculate_range) ,range_end(calculate_range),MINUTE) minute),INTERVAL '0' MINUTE) AS STRING) as FLOAT64)/60
  	else 0
  end as DiffWeekendPM,
  case
  	when LOWER(calculation_tags_desc) like LOWER('%ShiftDifferential%')
  	then cast(CAST(Coalesce((INTERVAL timestamp_diff(range_start(calculate_range) ,range_end(calculate_range),MINUTE) minute),INTERVAL '0' MINUTE) AS STRING) as FLOAT64)/60
  	else 0
  end as Diff,
  dw_batch_id,
  dw_batch_date
from( select *, SAFE.RANGE_INTERSECT(pd , RANGE(calculated_in_time_tmstp,calculated_out_time_tmstp)) as calculate_range

FROM
(
   SELECT a.reference_id,
   cast(TRUNC(CAST(COALESCE (a.override_store_num , a.home_store_num) AS FLOAT64)) as INTEGER) as coalesce_store,
  cast(TRUNC(CAST(COALESCE (a.override_department_num , a.home_department_num) AS FLOAT64)) as INTEGER) as coalesce_department,
  b.cost_center_num as cost_center,
  a.calculated_in_time_tmstp,
  a.calculated_out_time_tmstp,
  --  pd,
   a.calculation_tags_desc,
   b.building_id,
   b.department_desc,
   b.department_rollup_desc,
   a.dw_batch_id,
   a.dw_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PPH_HOURS_CALCULATED_TIMEBLOCK_FACT a
   inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_DIM.PPH_WBR_HOURS_MAPPING_DIM b
   on cast(TRUNC(CAST(COALESCE (a.override_store_num , a.home_store_num) AS FLOAT64)) as INTEGER)= cast(TRUNC(CAST(b.location_num AS FLOAT64)) as integer) 
   and cast(TRUNC(CAST(COALESCE (a.override_department_num , a.home_department_num) AS FLOAT64)) as INTEGER)  = b.department_num
   where 1=1
   and cast(a.calculated_in_time_tmstp as date) between b.start_date and b.end_date
   and LOWER(b.is_active_ind) = LOWER('Y')
   and a.calculated_in_time_tmstp < a.calculated_out_time_tmstp)
   INNER JOIN UNNEST(generate_range_array(range(calculated_in_time_tmstp,calculated_out_time_tmstp),interval 1 hour)) AS PD
   ON TRUE
) ) outsideTable
group by 1,2,3,4,5,6,7
) finalTab 
where  worked_day_date > ( (SELECT cast(last_completed_run_tmstp as date) from pph_timeblock_time_track where id = 1)  - INTERVAL '45' DAY ) 
) daily_hrs_table
on  LOWER(daily_hrs_table.building_id) = LOWER(fact.building_id)
and LOWER(daily_hrs_table.payroll_store_num) = LOWER(fact.payroll_store_num)
and daily_hrs_table.cost_center_num = fact.cost_center_num
and LOWER(daily_hrs_table.department_desc) = LOWER(fact.department_desc)
and daily_hrs_table.department_num = fact.department_num
and daily_hrs_table.worked_day_date = fact.worked_day_date
and daily_hrs_table.worked_day_hour_num = fact.worked_day_hour_num
WHEN MATCHED THEN
UPDATE SET
total_hours_qty = daily_hrs_table.total_hours_qty,
overtime_qty = daily_hrs_table.overtime_qty,
differential_second_qty = daily_hrs_table.differential_second_qty,
differential_third_qty = daily_hrs_table.differential_third_qty,
differential_weekend_am_qty = daily_hrs_table.differential_weekend_am_qty,
differential_weekend_pm_qty = daily_hrs_table.differential_weekend_pm_qty,
dw_batch_id = daily_hrs_table.dw_batch_id,
dw_batch_date = daily_hrs_table.dw_batch_date,
dw_sys_updt_tmstp = timestamp(current_datetime('PST8PDT')),
dw_sys_updt_tmstp_tz =`{{params.gcp_project_id}}`.jwn_udf.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN
INSERT (
building_id,
payroll_store_num,
cost_center_num,
department_desc,
department_num,
worked_day_date,
worked_day_hour_num,
total_hours_qty,
overtime_qty,
differential_second_qty,
differential_third_qty,
differential_weekend_am_qty,
differential_weekend_pm_qty,
dw_batch_id,
dw_batch_date,
dw_sys_load_tmstp,
dw_sys_load_tmstp_tz,
dw_sys_updt_tmstp,
dw_sys_updt_tmstp_tz
)
VALUES
(
daily_hrs_table.building_id,
daily_hrs_table.payroll_store_num,
daily_hrs_table.cost_center_num,
daily_hrs_table.department_desc,
daily_hrs_table.department_num,
daily_hrs_table.worked_day_date,
daily_hrs_table.worked_day_hour_num,
daily_hrs_table.total_hours_qty,
daily_hrs_table.overtime_qty,
daily_hrs_table.differential_second_qty,
daily_hrs_table.differential_third_qty,
daily_hrs_table.differential_weekend_am_qty,
daily_hrs_table.differential_weekend_pm_qty,
daily_hrs_table.dw_batch_id,
current_date('PST8PDT'),
timestamp(current_datetime('PST8PDT')),
`{{params.gcp_project_id}}.jwn_udf.DEFAULT_TZ_PST`(),
timestamp(current_datetime('PST8PDT')),
`{{params.gcp_project_id}}.jwn_udf.DEFAULT_TZ_PST`()
);

drop table isf_nap_timeblock_pph_fact;
drop table isf_nap_calculated_timeblock_pph_fact;
drop table isf_dag_tmp_timeblock_calculated_tbl;
drop table isf_dag_tmp_timeblock_del_ref_ids;
drop table pph_timeblock_time_track;



