-- SET QUERY_BAND = 'App_ID=APP08866;
--      DAG_ID= outcomes_ops_scorecard_11521_ACE_ENG;
--      Task_Name= outcomes_ops_scorecard;'
--      FOR SESSION VOLATILE;

------------------------------------------------------------
-- OUTCOMES AND FUNNEL OPS SCORECARD --
-- Table Name: 
      -- Dev: PROTO_DSA_AI_BASE_VWS.WEEKLY_OPS_STANDUP_AGG
      --Prod: PRD_NAP_DSA_AI_BASE_VWS.WEEKLY_OPS_STANDUP_AGG
-- Date Created: 7/16/2024
-- Last Updated: 7/16/2024
-- Author: Aman Kumar

-- Note:
-- Purpose: To capture Outcomes and Funnel of Store and Digital KPIs
-- Update Cadence:  Monday,Tuesday,Wednesday Morning at 10:00am PST 
------------------------------------------------------------
-- code begins here --
CREATE TEMPORARY TABLE IF NOT EXISTS weekly_ops_standup (
ops_name STRING(50),
banner STRING(50),
channel STRING(50),
metric_name STRING(50),
fiscal_num INTEGER,
fiscal_desc STRING(50),
label STRING(50),
rolling_fiscal_ind INTEGER,
plan_op FLOAT64,
plan_cp FLOAT64,
ty FLOAT64,
ly FLOAT64,
day_date DATE
) ;


CREATE TEMPORARY TABLE IF NOT EXISTS curr_wk
AS
SELECT week_idnt,
 week_start_day_date,
 week_end_day_date,
 month_idnt,
 month_start_day_date,
 month_end_day_date,
 quarter_idnt,
 fiscal_quarter_num,
 quarter_start_day_date,
 quarter_start_week_idnt,
 quarter_end_week_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
WHERE day_date
  = DATE_SUB({{params.start_date}},INTERVAL 7 DAY); 

CREATE TEMPORARY TABLE IF NOT EXISTS cal_lkup
AS
SELECT DISTINCT a.week_num,
 a.week_454_num,
 a.month_short_desc,
 b.week_start_day_date,
 b.week_end_day_date,
 a.month_num,
 b.month_start_day_date,
 b.month_end_day_date,
 a.quarter_num,
 a.quarter_454_num,
 a.year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS b ON a.week_num = b.week_idnt AND a.month_num = b.month_idnt
WHERE b.day_date BETWEEN DATE_SUB({{params.start_date}},INTERVAL 120 DAY) AND DATE_ADD({{params.start_date}},INTERVAL 120 DAY); 


CREATE TEMPORARY TABLE IF NOT EXISTS e2e_store_dim_vw
AS
SELECT st1.business_unit_desc,
 st1.store_country_code,
 st1.store_type_code,
 st1.store_num,
 st1.store_name,
 st1.store_short_name,
 st1.store_open_date,
  CASE
  WHEN st1.store_num = 57
  THEN DATE '2023-12-31'
  WHEN st1.store_num = 56
  THEN DATE '2023-10-14'
  WHEN st1.store_num = 537
  THEN DATE '2024-02-05'
  WHEN st1.store_num IN (427, 474, 804)
  THEN DATE '2023-07-01'
  WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
  THEN DATE '2023-06-13'
  WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
  THEN DATE '2023-05-14'
  ELSE st1.store_close_date
  END AS revised_close_date,
 st1.comp_status_desc AS comp_status_original,
  CASE
  WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52, 813,
      1443) OR st1.store_open_date <= DATE '2019-02-03' AND CASE
     WHEN st1.store_num = 57
     THEN DATE '2023-12-31'
     WHEN st1.store_num = 56
     THEN DATE '2023-10-14'
     WHEN st1.store_num = 537
     THEN DATE '2024-02-05'
     WHEN st1.store_num IN (427, 474, 804)
     THEN DATE '2023-07-01'
     WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
     THEN DATE '2023-06-13'
     WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
     THEN DATE '2023-05-14'
     ELSE st1.store_close_date
     END IS NULL
  THEN 'COMP'
  WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
    WHEN st1.store_num = 57
    THEN DATE '2023-12-31'
    WHEN st1.store_num = 56
    THEN DATE '2023-10-14'
    WHEN st1.store_num = 537
    THEN DATE '2024-02-05'
    WHEN st1.store_num IN (427, 474, 804)
    THEN DATE '2023-07-01'
    WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
    THEN DATE '2023-06-13'
    WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
    THEN DATE '2023-05-14'
    ELSE st1.store_close_date
    END IS NULL
  THEN 'COMP'
  ELSE 'NON-COMP'
  END AS comp_status_vs_19,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52,
         813, 1443) OR st1.store_open_date <= DATE '2019-02-03' AND CASE
        WHEN st1.store_num = 57
        THEN DATE '2023-12-31'
        WHEN st1.store_num = 56
        THEN DATE '2023-10-14'
        WHEN st1.store_num = 537
        THEN DATE '2024-02-05'
        WHEN st1.store_num IN (427, 474, 804)
        THEN DATE '2023-07-01'
        WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
        THEN DATE '2023-06-13'
        WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
        THEN DATE '2023-05-14'
        ELSE st1.store_close_date
        END IS NULL
     THEN 'COMP'
     WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
       WHEN st1.store_num = 57
       THEN DATE '2023-12-31'
       WHEN st1.store_num = 56
       THEN DATE '2023-10-14'
       WHEN st1.store_num = 537
       THEN DATE '2024-02-05'
       WHEN st1.store_num IN (427, 474, 804)
       THEN DATE '2023-07-01'
       WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
       THEN DATE '2023-06-13'
       WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
       THEN DATE '2023-05-14'
       ELSE st1.store_close_date
       END IS NULL
     THEN 'COMP'
     ELSE 'NON-COMP'
     END) = LOWER('COMP')
  THEN 'COMP'
  WHEN CASE
   WHEN st1.store_num = 57
   THEN DATE '2023-12-31'
   WHEN st1.store_num = 56
   THEN DATE '2023-10-14'
   WHEN st1.store_num = 537
   THEN DATE '2024-02-05'
   WHEN st1.store_num IN (427, 474, 804)
   THEN DATE '2023-07-01'
   WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
   THEN DATE '2023-06-13'
   WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
   THEN DATE '2023-05-14'
   ELSE st1.store_close_date
   END IS NOT NULL
  THEN 'CLOSED'
  WHEN st1.store_open_date >= DATE '2019-02-03'
  THEN 'NEW'
  ELSE NULL
  END AS comp_status_dtl_vs_19,
  CASE
  WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52, 813,
      1443) OR st1.store_open_date <= DATE '2021-01-31' AND CASE
     WHEN st1.store_num = 57
     THEN DATE '2023-12-31'
     WHEN st1.store_num = 56
     THEN DATE '2023-10-14'
     WHEN st1.store_num = 537
     THEN DATE '2024-02-05'
     WHEN st1.store_num IN (427, 474, 804)
     THEN DATE '2023-07-01'
     WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
     THEN DATE '2023-06-13'
     WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
     THEN DATE '2023-05-14'
     ELSE st1.store_close_date
     END IS NULL
  THEN 'COMP'
  WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
    WHEN st1.store_num = 57
    THEN DATE '2023-12-31'
    WHEN st1.store_num = 56
    THEN DATE '2023-10-14'
    WHEN st1.store_num = 537
    THEN DATE '2024-02-05'
    WHEN st1.store_num IN (427, 474, 804)
    THEN DATE '2023-07-01'
    WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
    THEN DATE '2023-06-13'
    WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
    THEN DATE '2023-05-14'
    ELSE st1.store_close_date
    END IS NULL
  THEN 'COMP'
  ELSE 'NON-COMP'
  END AS comp_status_vs_21,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52,
         813, 1443) OR st1.store_open_date <= DATE '2021-01-31' AND CASE
        WHEN st1.store_num = 57
        THEN DATE '2023-12-31'
        WHEN st1.store_num = 56
        THEN DATE '2023-10-14'
        WHEN st1.store_num = 537
        THEN DATE '2024-02-05'
        WHEN st1.store_num IN (427, 474, 804)
        THEN DATE '2023-07-01'
        WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
        THEN DATE '2023-06-13'
        WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
        THEN DATE '2023-05-14'
        ELSE st1.store_close_date
        END IS NULL
     THEN 'COMP'
     WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
       WHEN st1.store_num = 57
       THEN DATE '2023-12-31'
       WHEN st1.store_num = 56
       THEN DATE '2023-10-14'
       WHEN st1.store_num = 537
       THEN DATE '2024-02-05'
       WHEN st1.store_num IN (427, 474, 804)
       THEN DATE '2023-07-01'
       WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
       THEN DATE '2023-06-13'
       WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
       THEN DATE '2023-05-14'
       ELSE st1.store_close_date
       END IS NULL
     THEN 'COMP'
     ELSE 'NON-COMP'
     END) = LOWER('COMP')
  THEN 'COMP'
  WHEN CASE
   WHEN st1.store_num = 57
   THEN DATE '2023-12-31'
   WHEN st1.store_num = 56
   THEN DATE '2023-10-14'
   WHEN st1.store_num = 537
   THEN DATE '2024-02-05'
   WHEN st1.store_num IN (427, 474, 804)
   THEN DATE '2023-07-01'
   WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
   THEN DATE '2023-06-13'
   WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
   THEN DATE '2023-05-14'
   ELSE st1.store_close_date
   END IS NOT NULL
  THEN 'CLOSED'
  WHEN st1.store_open_date >= DATE '2021-01-31'
  THEN 'NEW'
  ELSE NULL
  END AS comp_status_dtl_vs_21,
  CASE
  WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52, 813,
      1443) OR st1.store_open_date <= DATE '2022-01-30' AND CASE
     WHEN st1.store_num = 57
     THEN DATE '2023-12-31'
     WHEN st1.store_num = 56
     THEN DATE '2023-10-14'
     WHEN st1.store_num = 537
     THEN DATE '2024-02-05'
     WHEN st1.store_num IN (427, 474, 804)
     THEN DATE '2023-07-01'
     WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
     THEN DATE '2023-06-13'
     WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
     THEN DATE '2023-05-14'
     ELSE st1.store_close_date
     END IS NULL
  THEN 'COMP'
  WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
    WHEN st1.store_num = 57
    THEN DATE '2023-12-31'
    WHEN st1.store_num = 56
    THEN DATE '2023-10-14'
    WHEN st1.store_num = 537
    THEN DATE '2024-02-05'
    WHEN st1.store_num IN (427, 474, 804)
    THEN DATE '2023-07-01'
    WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
    THEN DATE '2023-06-13'
    WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
    THEN DATE '2023-05-14'
    ELSE st1.store_close_date
    END IS NULL
  THEN 'COMP'
  ELSE 'NON-COMP'
  END AS comp_status_vs_22,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52,
         813, 1443) OR st1.store_open_date <= DATE '2022-01-30' AND CASE
        WHEN st1.store_num = 57
        THEN DATE '2023-12-31'
        WHEN st1.store_num = 56
        THEN DATE '2023-10-14'
        WHEN st1.store_num = 537
        THEN DATE '2024-02-05'
        WHEN st1.store_num IN (427, 474, 804)
        THEN DATE '2023-07-01'
        WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
        THEN DATE '2023-06-13'
        WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
        THEN DATE '2023-05-14'
        ELSE st1.store_close_date
        END IS NULL
     THEN 'COMP'
     WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
       WHEN st1.store_num = 57
       THEN DATE '2023-12-31'
       WHEN st1.store_num = 56
       THEN DATE '2023-10-14'
       WHEN st1.store_num = 537
       THEN DATE '2024-02-05'
       WHEN st1.store_num IN (427, 474, 804)
       THEN DATE '2023-07-01'
       WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
       THEN DATE '2023-06-13'
       WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
       THEN DATE '2023-05-14'
       ELSE st1.store_close_date
       END IS NULL
     THEN 'COMP'
     ELSE 'NON-COMP'
     END) = LOWER('COMP')
  THEN 'COMP'
  WHEN CASE
   WHEN st1.store_num = 57
   THEN DATE '2023-12-31'
   WHEN st1.store_num = 56
   THEN DATE '2023-10-14'
   WHEN st1.store_num = 537
   THEN DATE '2024-02-05'
   WHEN st1.store_num IN (427, 474, 804)
   THEN DATE '2023-07-01'
   WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
   THEN DATE '2023-06-13'
   WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
   THEN DATE '2023-05-14'
   ELSE st1.store_close_date
   END IS NOT NULL
  THEN 'CLOSED'
  WHEN st1.store_open_date >= DATE '2022-01-30'
  THEN 'NEW'
  ELSE NULL
  END AS comp_status_dtl_vs_22,
  CASE
  WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52, 813,
      1443) OR st1.store_open_date <= DATE '2023-01-29' AND CASE
     WHEN st1.store_num = 57
     THEN DATE '2023-12-31'
     WHEN st1.store_num = 56
     THEN DATE '2023-10-14'
     WHEN st1.store_num = 537
     THEN DATE '2024-02-05'
     WHEN st1.store_num IN (427, 474, 804)
     THEN DATE '2023-07-01'
     WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
     THEN DATE '2023-06-13'
     WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
     THEN DATE '2023-05-14'
     ELSE st1.store_close_date
     END IS NULL
  THEN 'COMP'
  WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
     WHEN st1.store_num = 57
     THEN DATE '2023-12-31'
     WHEN st1.store_num = 56
     THEN DATE '2023-10-14'
     WHEN st1.store_num = 537
     THEN DATE '2024-02-05'
     WHEN st1.store_num IN (427, 474, 804)
     THEN DATE '2023-07-01'
     WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
     THEN DATE '2023-06-13'
     WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
     THEN DATE '2023-05-14'
     ELSE st1.store_close_date
     END IS NULL OR st1.store_num IN (9997, 9998)
  THEN 'COMP'
  ELSE 'NON-COMP'
  END AS comp_status_vs_23,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52,
         813, 1443) OR st1.store_open_date <= DATE '2023-01-29' AND CASE
        WHEN st1.store_num = 57
        THEN DATE '2023-12-31'
        WHEN st1.store_num = 56
        THEN DATE '2023-10-14'
        WHEN st1.store_num = 537
        THEN DATE '2024-02-05'
        WHEN st1.store_num IN (427, 474, 804)
        THEN DATE '2023-07-01'
        WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
        THEN DATE '2023-06-13'
        WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
        THEN DATE '2023-05-14'
        ELSE st1.store_close_date
        END IS NULL
     THEN 'COMP'
     WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
        WHEN st1.store_num = 57
        THEN DATE '2023-12-31'
        WHEN st1.store_num = 56
        THEN DATE '2023-10-14'
        WHEN st1.store_num = 537
        THEN DATE '2024-02-05'
        WHEN st1.store_num IN (427, 474, 804)
        THEN DATE '2023-07-01'
        WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
        THEN DATE '2023-06-13'
        WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
        THEN DATE '2023-05-14'
        ELSE st1.store_close_date
        END IS NULL OR st1.store_num IN (9997, 9998)
     THEN 'COMP'
     ELSE 'NON-COMP'
     END) = LOWER('COMP')
  THEN 'COMP'
  WHEN CASE
   WHEN st1.store_num = 57
   THEN DATE '2023-12-31'
   WHEN st1.store_num = 56
   THEN DATE '2023-10-14'
   WHEN st1.store_num = 537
   THEN DATE '2024-02-05'
   WHEN st1.store_num IN (427, 474, 804)
   THEN DATE '2023-07-01'
   WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
   THEN DATE '2023-06-13'
   WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
   THEN DATE '2023-05-14'
   ELSE st1.store_close_date
   END IS NOT NULL
  THEN 'CLOSED'
  WHEN st1.store_open_date >= DATE '2023-01-29'
  THEN 'NEW'
  ELSE NULL
  END AS comp_status_dtl_vs_23,
  CASE
  WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52, 813,
      1443) OR st1.store_open_date <= DATE '2023-01-29' AND CASE
     WHEN st1.store_num = 57
     THEN DATE '2023-12-31'
     WHEN st1.store_num = 56
     THEN DATE '2023-10-14'
     WHEN st1.store_num = 537
     THEN DATE '2024-02-05'
     WHEN st1.store_num IN (427, 474, 804)
     THEN DATE '2023-07-01'
     WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
     THEN DATE '2023-06-13'
     WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
     THEN DATE '2023-05-14'
     ELSE st1.store_close_date
     END IS NULL
  THEN 'COMP'
  WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
     WHEN st1.store_num = 57
     THEN DATE '2023-12-31'
     WHEN st1.store_num = 56
     THEN DATE '2023-10-14'
     WHEN st1.store_num = 537
     THEN DATE '2024-02-05'
     WHEN st1.store_num IN (427, 474, 804)
     THEN DATE '2023-07-01'
     WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
     THEN DATE '2023-06-13'
     WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
     THEN DATE '2023-05-14'
     ELSE st1.store_close_date
     END IS NULL OR st1.store_num IN (9997, 9998)
  THEN 'COMP'
  ELSE 'NON-COMP'
  END AS comp_status,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(st1.store_type_code) IN (LOWER('CO'), LOWER('FC'), LOWER('OC'), LOWER('OF')) OR st1.store_num IN (52,
         813, 1443) OR st1.store_open_date <= DATE '2023-01-29' AND CASE
        WHEN st1.store_num = 57
        THEN DATE '2023-12-31'
        WHEN st1.store_num = 56
        THEN DATE '2023-10-14'
        WHEN st1.store_num = 537
        THEN DATE '2024-02-05'
        WHEN st1.store_num IN (427, 474, 804)
        THEN DATE '2023-07-01'
        WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
        THEN DATE '2023-06-13'
        WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
        THEN DATE '2023-05-14'
        ELSE st1.store_close_date
        END IS NULL
     THEN 'COMP'
     WHEN LOWER(st1.store_type_code) = LOWER('VS') AND CASE
        WHEN st1.store_num = 57
        THEN DATE '2023-12-31'
        WHEN st1.store_num = 56
        THEN DATE '2023-10-14'
        WHEN st1.store_num = 537
        THEN DATE '2024-02-05'
        WHEN st1.store_num IN (427, 474, 804)
        THEN DATE '2023-07-01'
        WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
        THEN DATE '2023-06-13'
        WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
        THEN DATE '2023-05-14'
        ELSE st1.store_close_date
        END IS NULL OR st1.store_num IN (9997, 9998)
     THEN 'COMP'
     ELSE 'NON-COMP'
     END) = LOWER('COMP')
  THEN 'COMP'
  WHEN CASE
   WHEN st1.store_num = 57
   THEN DATE '2023-12-31'
   WHEN st1.store_num = 56
   THEN DATE '2023-10-14'
   WHEN st1.store_num = 537
   THEN DATE '2024-02-05'
   WHEN st1.store_num IN (427, 474, 804)
   THEN DATE '2023-07-01'
   WHEN st1.store_num IN (830, 831, 832, 833, 834, 835)
   THEN DATE '2023-06-13'
   WHEN st1.store_num IN (840, 841, 842, 843, 844, 845, 846)
   THEN DATE '2023-05-14'
   ELSE st1.store_close_date
   END IS NOT NULL
  THEN 'CLOSED'
  WHEN st1.store_open_date >= DATE '2023-01-29'
  THEN 'NEW'
  ELSE NULL
  END AS comp_status_dtl,
  CASE
  WHEN LOWER(st1.business_unit_desc) IN (LOWER('CORPORATE'), LOWER('CORPORATE CANADA'), LOWER('OFFPRICE ONLINE')) OR
    LOWER(st1.subgroup_medium_desc) = LOWER('NORD FLS HQ')
  THEN NULL
  WHEN LOWER(st1.store_type_code) = LOWER('CC')
  THEN 'LAST CHANCE'
  WHEN LOWER(st1.subgroup_desc) LIKE LOWER('% CANADA')
  THEN 'CANADA'
  WHEN LOWER(st1.subgroup_medium_desc) = LOWER('SCAL FLS')
  THEN 'SCAL'
  ELSE SUBSTR(st1.subgroup_desc, 0, STRPOS(LOWER(st1.subgroup_desc), LOWER(' ')))
  END AS store_region,
  CASE
  WHEN LOWER(st1.region_desc) = LOWER('MANHATTAN')
  THEN 'NYC'
  ELSE CASE
   WHEN LOWER(st1.business_unit_desc) IN (LOWER('CORPORATE'), LOWER('CORPORATE CANADA'), LOWER('OFFPRICE ONLINE')) OR
     LOWER(st1.subgroup_medium_desc) = LOWER('NORD FLS HQ')
   THEN NULL
   WHEN LOWER(st1.store_type_code) = LOWER('CC')
   THEN 'LAST CHANCE'
   WHEN LOWER(st1.subgroup_desc) LIKE LOWER('% CANADA')
   THEN 'CANADA'
   WHEN LOWER(st1.subgroup_medium_desc) = LOWER('SCAL FLS')
   THEN 'SCAL'
   ELSE SUBSTR(st1.subgroup_desc, 0, STRPOS(LOWER(st1.subgroup_desc), LOWER(' ')))
   END
  END AS store_subregion,
  CASE
  WHEN st1.store_num IN (206, 214, 502)
  THEN 501
  WHEN st1.store_num IN (58, 485)
  THEN 819
  WHEN st1.store_num IN (98, 1413)
  THEN 803
  ELSE COALESCE(udma.dma_code, cdma.ca_dma_code)
  END AS dma_code,
  CASE
  WHEN st1.store_num IN (206, 214, 502)
  THEN 'New York NY'
  WHEN cdma.ca_dma_code = 505
  THEN 'Ottawa ON'
  WHEN cdma.ca_dma_code = 535
  THEN 'Toronto ON'
  WHEN cdma.ca_dma_code = 825
  THEN 'Calgary AB'
  WHEN cdma.ca_dma_code = 835
  THEN 'Edmonton AB'
  WHEN cdma.ca_dma_code = 933
  THEN 'Vancouver BC'
  WHEN st1.store_num IN (58, 485)
  THEN 'Seattle-Tacoma WA'
  WHEN st1.store_num IN (98, 1413)
  THEN 'Los Angeles CA'
  ELSE COALESCE(udma.dma_desc, cdma.ca_dma_desc)
  END AS dma_desc,
  CASE
  WHEN LOWER(st1.store_type_code) = LOWER('VS')
  THEN CAST(TRUNC(CAST(CASE
    WHEN REGEXP_SUBSTR(st1.store_short_name, '(?i)\\d+', 1, 1) = ''
    THEN '0'
    ELSE REGEXP_SUBSTR(st1.store_short_name, '(?i)\\d+', 1, 1)
    END AS FLOAT64)) AS INTEGER)
  ELSE st1.store_num
  END AS physical_store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS st1
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS st2 ON st2.store_num = CASE
   WHEN LOWER(st1.store_type_code) = LOWER('VS') AND LOWER(st1.store_short_name) <> LOWER('VS UNASSIGNED')
   THEN CAST(TRUNC(CAST(CASE
     WHEN REGEXP_SUBSTR(st1.store_short_name, '(?i)\\d+', 1, 1) = ''
     THEN '0'
     ELSE REGEXP_SUBSTR(st1.store_short_name, '(?i)\\d+', 1, 1)
     END AS FLOAT64)) AS INTEGER)
   ELSE NULL
   END
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_dma AS udma ON udma.dma_code = CAST(CASE
    WHEN LOWER(st1.store_country_code) = LOWER('US')
    THEN COALESCE(st1.store_dma_code, st2.store_dma_code)
    ELSE NULL
    END AS FLOAT64)
 LEFT JOIN (SELECT ca_dma_code,
   ca_dma_desc
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_ca_zip_dma
  WHERE ca_dma_code IN (SELECT CAST(TRUNC(CAST(store_dma_code AS FLOAT64)) AS INTEGER) AS store_dma_code
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim)
  GROUP BY ca_dma_code,
   ca_dma_desc) AS cdma ON cdma.ca_dma_code = CAST(CASE
    WHEN LOWER(st1.store_country_code) = LOWER('CA')
    THEN COALESCE(st1.store_dma_code, st2.store_dma_code)
    ELSE NULL
    END AS FLOAT64)
WHERE st1.store_num IN (SELECT DISTINCT intent_store_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_detail_fact
   WHERE business_day_date >= DATE '2019-02-03');


CREATE TEMPORARY TABLE IF NOT EXISTS ft_ops_1
AS
SELECT week_num,
  CASE
  WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'), LOWER('MARKETPLACE'))
  THEN 'NORDSTROM'
  WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('RACK'))
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
  CASE
  WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'))
  THEN 'FLS'
  WHEN LOWER(business_unit_desc) IN (LOWER('N.COM'), LOWER('MARKETPLACE'))
  THEN 'N.COM'
  WHEN LOWER(business_unit_desc) IN (LOWER('RACK'))
  THEN 'RACK STORE'
  WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'R.COM'
  ELSE NULL
  END AS channel,
 SUM(CASE
   WHEN year_num = 2024 AND LOWER(comp_status_vs_23) = LOWER('COMP') OR year_num = 2023 AND LOWER(comp_status_vs_22) =
      LOWER('COMP')
   THEN store_traffic
   ELSE NULL
   END) AS store_traffic_act,
 SUM(CASE
   WHEN year_num = 2024 AND LOWER(comp_status_vs_23) = LOWER('COMP') OR year_num = 2023 AND LOWER(comp_status_vs_22) =
      LOWER('COMP')
   THEN store_purchase_trips
   ELSE NULL
   END) AS purchase_trips_act,
 SUM(CASE
   WHEN year_num = 2024 AND LOWER(comp_status_vs_23) = LOWER('COMP') OR year_num = 2023 AND LOWER(comp_status_vs_22) =
      LOWER('COMP')
   THEN reported_demand_usd_amt_bopus_in_digital
   ELSE NULL
   END) AS demand_amt_act,
 SUM(op_gmv_usd_amt) AS net_op_sales_amt_act,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('N.COM'), LOWER('OFFPRICE ONLINE')) AND (LOWER(business_unit_desc) <> LOWER('OFFPRICE ONLINE'
        ) OR NOT day_date BETWEEN '2020-11-21' AND '2021-03-05')
   THEN orders_count
   ELSE NULL
   END) AS digital_orders_act,
 SUM(visitors) AS digital_traffic_act,
 SUM(sessions) AS sessions
FROM (SELECT dc.year_num,
   dc.month_454_num,
   dc.week_of_fyr,
   dc.week_num,
   fsdmtf.tran_date AS day_date,
   DATE_ADD(dc.day_date, INTERVAL 364 * (2024 - dc.year_num) DAY) AS ty_date,
    CASE
    WHEN LOWER(fsdmtf.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('RACK'), LOWER('RACK CANADA'
       ))
    THEN 'STORE'
    WHEN LOWER(fsdmtf.business_unit_desc) IN (LOWER('N.COM'), LOWER('N.CA'), LOWER('OFFPRICE ONLINE'), LOWER('MARKETPLACE'
       ))
    THEN 'DIGITAL'
    ELSE NULL
    END AS bu_type,
   fsdmtf.business_unit_desc,
   st.comp_status_vs_21,
   st.comp_status_vs_22,
   st.comp_status_vs_23,
   SUM(CASE
     WHEN fsdmtf.visitors IS NULL
     THEN fsdmtf.store_traffic
     ELSE fsdmtf.visitors
     END) AS traffic,
   SUM(fsdmtf.store_traffic) AS store_traffic,
   SUM(fsdmtf.visitors) AS visitors,
   SUM(CASE
     WHEN LOWER(fsdmtf.platform_code) = LOWER('Direct to Customer (DTC)')
     THEN 0
     ELSE fsdmtf.orders_count
     END) AS orders_count,
   SUM(fsdmtf.store_purchase_trips) AS store_purchase_trips,
   SUM(CASE
     WHEN fsdmtf.orders_count IS NULL
     THEN fsdmtf.store_purchase_trips
     WHEN LOWER(fsdmtf.platform_code) = LOWER('Direct to Customer (DTC)')
     THEN 0
     ELSE fsdmtf.orders_count
     END) AS orders_or_trips,
   SUM(fsdmtf.reported_demand_usd_amt_excl_bopus + fsdmtf.bopus_attr_digital_reported_demand_usd_amt) AS
   reported_demand_usd_amt_bopus_in_digital,
   SUM(fsdmtf.sessions) AS sessions,
   SUM(fsdmtf.op_gmv_usd_amt) AS op_gmv_usd_amt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dsa_ai_base_vws.finance_sales_demand_margin_traffic_fact AS fsdmtf
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS dc ON fsdmtf.tran_date = dc.day_date
   INNER JOIN e2e_store_dim_vw AS st ON fsdmtf.store_num = st.store_num
  WHERE fsdmtf.tran_date BETWEEN '2024-02-04' AND date_sub({{params.start_date}},INTERVAL 1 DAY)--- ---Need to pass value START_DATE
  GROUP BY dc.year_num,
   dc.month_454_num,
   dc.week_of_fyr,
   dc.week_num,
   day_date,
   ty_date,
   bu_type,
   fsdmtf.business_unit_desc,
   st.comp_status_vs_21,
   st.comp_status_vs_22,
   st.comp_status_vs_23) AS a
GROUP BY week_num,
 banner,
 channel;


CREATE TEMPORARY TABLE IF NOT EXISTS ft_ops_2
AS
SELECT week_num,
  CASE
  WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'))
  THEN 'NORDSTROM'
  WHEN LOWER(business_unit_desc) IN (LOWER('RACK'))
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
  CASE
  WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'))
  THEN 'FLS'
  WHEN LOWER(business_unit_desc) IN (LOWER('RACK'))
  THEN 'RACK STORE'
  ELSE NULL
  END AS channel,
 SUM(traffic) AS store_traffic_plan,
 SUM(purchase_trips) AS purchase_trips_plan,
 SUM(demand_amt) AS store_demand_amt_plan
FROM t2dl_das_osu.e2e_store_funnel_plan
WHERE LOWER(comp_status) = LOWER('COMP')
 AND LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK'))
 AND week_num IN (SELECT DISTINCT week_num
   FROM cal_lkup)
 AND LOWER(plan_version) = LOWER('FY24 Jun Plan')
GROUP BY week_num,
 banner,
 channel;


CREATE TEMPORARY TABLE IF NOT EXISTS ft_ops_3
AS
SELECT b.week_num,
  CASE
  WHEN LOWER(a.business_unit_desc) IN (LOWER('FULL LINE'))
  THEN 'NORDSTROM'
  WHEN LOWER(a.business_unit_desc) IN (LOWER('RACK'))
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
  CASE
  WHEN LOWER(a.business_unit_desc) IN (LOWER('FULL LINE'))
  THEN 'FLS'
  WHEN LOWER(a.business_unit_desc) IN (LOWER('RACK'))
  THEN 'RACK STORE'
  ELSE NULL
  END AS channel,
 SUM(CAST(a.op_gmv AS FLOAT64)) AS op_gmv
FROM t2dl_das_osu.e2e_store_sales_plan AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS b ON a.day_date = b.day_date
WHERE LOWER(a.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK'))
 AND b.week_num IN (SELECT DISTINCT week_num
   FROM cal_lkup)
 AND LOWER(a.plan_version) = LOWER('FY24 Jun Plan')
GROUP BY b.week_num,
 banner,
 channel;


CREATE TEMPORARY TABLE IF NOT EXISTS ft_ops_4
AS
SELECT b.week_num,
  CASE
  WHEN LOWER(a.business_unit_desc) IN (LOWER('N.COM'))
  THEN 'NORDSTROM'
  WHEN LOWER(a.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
  CASE
  WHEN LOWER(a.business_unit_desc) IN (LOWER('N.COM'))
  THEN 'N.COM'
  WHEN LOWER(a.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'R.COM'
  ELSE NULL
  END AS channel,
 SUM(a.total_visitors) AS digital_traffic_plan,
 SUM(a.order_count) AS digital_orders_plan,
 SUM(a.demand_amt) AS digital_demand_amt_plan,
 SUM(a.net_sales_amt) AS net_sales_amt_plan
FROM t2dl_das_osu.e2e_digital_funnel_plan AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS b ON a.day_date = b.day_date
WHERE LOWER(a.business_unit_desc) IN (LOWER('N.COM'), LOWER('OFFPRICE ONLINE'))
 AND b.week_num IN (SELECT DISTINCT week_num
   FROM cal_lkup)
 AND LOWER(a.plan_version) = LOWER('FY24 Jun Plan')
GROUP BY b.week_num,
 banner,
 channel;

CREATE TEMPORARY TABLE IF NOT EXISTS ft_ops_5
AS
SELECT COALESCE(a.week_num, b.week_num, c.week_num, d.week_num) AS week_num,
 e.week_454_num,
 e.month_short_desc,
 e.week_start_day_date,
 e.week_end_day_date,
 COALESCE(a.banner, b.banner, c.banner, d.banner) AS banner,
 COALESCE(a.channel, b.channel, c.channel, d.channel) AS channel,
 CAST(COALESCE(a.store_traffic_act, 0) AS FLOAT64) AS store_traffic_act,
 CAST(COALESCE(a.sessions, 0) AS FLOAT64) AS sessions,
 CAST(COALESCE(b.store_traffic_plan, 0) AS FLOAT64) AS store_traffic_plan,
 CAST(COALESCE(a.digital_traffic_act, 0) AS FLOAT64) AS digital_traffic_act,
 CAST(COALESCE(d.digital_traffic_plan, 0) AS FLOAT64) AS digital_traffic_plan,
 CAST(COALESCE(a.purchase_trips_act, 0) AS FLOAT64) AS purchase_trips_act,
 CAST(COALESCE(b.purchase_trips_plan, 0) AS FLOAT64) AS purchase_trips_plan,
 CAST(COALESCE(a.net_op_sales_amt_act, 0) AS FLOAT64) AS net_op_sales_amt_act,
 COALESCE(c.op_gmv, 0) AS op_gmv,
 COALESCE(d.net_sales_amt_plan, 0) AS net_sales_amt_plan,
 CAST(COALESCE(a.digital_orders_act, 0) AS FLOAT64) AS digital_orders_act,
 CAST(COALESCE(d.digital_orders_plan, 0) AS FLOAT64) AS digital_orders_plan,
 CAST(COALESCE(a.demand_amt_act, 0) AS FLOAT64) AS demand_amt_act,
 COALESCE(b.store_demand_amt_plan, 0) AS store_demand_amt_plan,
 COALESCE(d.digital_demand_amt_plan, 0) AS digital_demand_amt_plan
FROM ft_ops_1 AS a
 FULL JOIN ft_ops_2 AS b ON a.week_num = b.week_num AND LOWER(a.banner) = LOWER(b.banner) AND LOWER(a.channel) = LOWER(b
    .channel)
 FULL JOIN ft_ops_3 AS c ON a.week_num = c.week_num AND LOWER(a.banner) = LOWER(c.banner) AND LOWER(a.channel) = LOWER(c
    .channel)
 FULL JOIN ft_ops_4 AS d ON a.week_num = d.week_num AND LOWER(a.banner) = LOWER(d.banner) AND LOWER(a.channel) = LOWER(d
    .channel)
 INNER JOIN cal_lkup AS e ON COALESCE(a.week_num, b.week_num, c.week_num, d.week_num) = e.week_num;



INSERT INTO weekly_ops_standup
(SELECT 'OUTCOMES' AS ops_name,
  banner,
  channel,
  'OP GMV' AS metric_name,
  week_num AS fiscal_num,
  CONCAT(month_short_desc, ' ', 'WK ', SUBSTR(CAST(week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN week_start_day_date <= DATE_SUB({{params.start_date}},INTERVAL 7 DAY)
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
  SUM(op_gmv) AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
  SUM(net_op_sales_amt_act) AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5
 WHERE week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk)
  AND LOWER(channel) IN (LOWER('FLS'), LOWER('RACK STORE'))
 GROUP BY ops_name,
  banner,
  channel,
  metric_name,
  fiscal_num,
  fiscal_desc,
  label,
  rolling_fiscal_ind,
  plan_cp,
  ly,
  day_date);



INSERT INTO weekly_ops_standup
(SELECT 'OUTCOMES' AS ops_name,
  a.banner,
  a.channel,
  'OP GMV QTD' AS metric_name,
  B.quarter_num AS fiscal_num,
  CONCAT('Q', SUBSTR(CAST(B.quarter_454_num AS STRING), 1, 12)) AS fiscal_desc,
  'QTD' AS label,
   CASE
   WHEN a.week_start_day_date <= DATE_SUB({{params.start_date}},INTERVAL 7 DAY) 
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
  SUM(a.op_gmv) AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
  SUM(a.net_op_sales_amt_act) AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5 AS a
  INNER JOIN (SELECT DISTINCT week_num,
    quarter_num,
    quarter_454_num
   FROM cal_lkup
   WHERE week_num BETWEEN (SELECT quarter_start_week_idnt
      FROM curr_wk) AND (SELECT week_idnt
      FROM curr_wk)) AS B ON a.week_num = B.week_num
 WHERE LOWER(a.channel) IN (LOWER('FLS'), LOWER('RACK STORE'))
 GROUP BY ops_name,
  a.banner,
  a.channel,
  metric_name,
  fiscal_num,
  fiscal_desc,
  label,
  rolling_fiscal_ind,
  plan_cp,
  day_date,
  ly);

INSERT INTO weekly_ops_standup
(SELECT 'OUTCOMES' AS ops_name,
  banner,
  channel,
  'OP GMV' AS metric_name,
  week_num AS fiscal_num,
  CONCAT(month_short_desc, ' ', 'WK ', SUBSTR(CAST(week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN week_start_day_date
    <= DATE_SUB({{params.start_date}},INTERVAL 7 DAY)   
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
  SUM(net_sales_amt_plan) AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
  SUM(net_op_sales_amt_act) AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5
 WHERE week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk)
  AND LOWER(channel) IN (LOWER('N.COM'), LOWER('R.COM'))
 GROUP BY ops_name,
  banner,
  channel,
  metric_name,
  fiscal_num,
  fiscal_desc,
  label,
  rolling_fiscal_ind,
  plan_cp,
  ly,
  day_date);


INSERT INTO weekly_ops_standup
(SELECT 'OUTCOMES' AS ops_name,
  a.banner,
  a.channel,
  'OP GMV QTD' AS metric_name,
  B.quarter_num AS fiscal_num,
  CONCAT('Q', SUBSTR(CAST(B.quarter_454_num AS STRING), 1, 12)) AS fiscal_desc,
  'QTD' AS label,
   CASE
   WHEN a.week_start_day_date
    <= DATE_SUB({{params.start_date}},INTERVAL 7 DAY)
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
  SUM(a.net_sales_amt_plan) AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
  SUM(a.net_op_sales_amt_act) AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5 AS a
  INNER JOIN (SELECT DISTINCT week_num,
    quarter_num,
    quarter_454_num
   FROM cal_lkup
   WHERE week_num BETWEEN (SELECT quarter_start_week_idnt
      FROM curr_wk) AND (SELECT week_idnt
      FROM curr_wk)) AS B ON a.week_num = B.week_num
 WHERE LOWER(a.channel) IN (LOWER('N.COM'), LOWER('R.COM'))
 GROUP BY ops_name,
  a.banner,
  a.channel,
  metric_name,
  fiscal_num,
  fiscal_desc,
  label,
  rolling_fiscal_ind,
  plan_cp,
  day_date,
  ly);



INSERT INTO weekly_ops_standup
(SELECT 'FUNNEL' AS ops_name,
  banner,
  channel,
  'Traffic' AS metric_name,
  week_num AS fiscal_num,
  CONCAT(month_short_desc, ' ', 'WK ', SUBSTR(CAST(week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN week_start_day_date <= DATE_SUB({{params.start_date}},INTERVAL 7 DAY) 
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
  SUM(store_traffic_plan) AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
  SUM(store_traffic_act) AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5
 WHERE week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk)
  AND LOWER(channel) IN (LOWER('FLS'), LOWER('RACK STORE'))
 GROUP BY ops_name,
  banner,
  channel,
  metric_name,
  fiscal_num,
  fiscal_desc,
  label,
  rolling_fiscal_ind,
  plan_cp,
  ly,
  day_date);


INSERT INTO weekly_ops_standup
(SELECT 'FUNNEL' AS ops_name,
  banner,
  channel,
  'ATV' AS metric_name,
  week_num AS fiscal_num,
  CONCAT(month_short_desc, ' ', 'WK ', SUBSTR(CAST(week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN week_start_day_date
    <=  DATE_SUB({{params.start_date}},INTERVAL 7 DAY) 
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
   CASE
   WHEN SUM(purchase_trips_plan) = 0
   THEN 0
   ELSE SUM(store_demand_amt_plan) / SUM(purchase_trips_plan)
   END AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
   CASE
   WHEN SUM(purchase_trips_act) = 0
   THEN 0
   ELSE SUM(demand_amt_act) / SUM(purchase_trips_act)
   END AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5
 WHERE week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk)
  AND LOWER(channel) IN (LOWER('FLS'), LOWER('RACK STORE'))
 GROUP BY week_num,
  week_454_num,
  month_short_desc,
  week_start_day_date,
  banner,
  channel);


INSERT INTO weekly_ops_standup
(SELECT 'FUNNEL' AS ops_name,
  banner,
  channel,
  'Conversion' AS metric_name,
  week_num AS fiscal_num,
  CONCAT(month_short_desc, ' ', 'WK ', SUBSTR(CAST(week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN week_start_day_date
    <=  DATE_SUB({{params.start_date}},INTERVAL 7 DAY)
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
   CASE
   WHEN SUM(store_traffic_plan) = 0
   THEN 0
   ELSE SUM(purchase_trips_plan) / SUM(store_traffic_plan)
   END AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
   CASE
   WHEN SUM(store_traffic_act) = 0
   THEN 0
   ELSE SUM(purchase_trips_act) / SUM(store_traffic_act)
   END AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5
 WHERE week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk)
  AND LOWER(channel) IN (LOWER('FLS'), LOWER('RACK STORE'))
 GROUP BY week_num,
  week_454_num,
  month_short_desc,
  week_start_day_date,
  banner,
  channel);


INSERT INTO weekly_ops_standup
(SELECT 'FUNNEL' AS ops_name,
  banner,
  channel,
  'Sessions' AS metric_name,
  week_num AS fiscal_num,
  CONCAT(month_short_desc, ' ', 'WK ', SUBSTR(CAST(week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN week_start_day_date
    <=  DATE_SUB({{params.start_date}},INTERVAL 7 DAY) 
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
  SUM(digital_traffic_plan) AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
  SUM(sessions) AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5
 WHERE week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk)
  AND LOWER(channel) IN (LOWER('N.COM'), LOWER('R.COM'))
 GROUP BY ops_name,
  banner,
  channel,
  metric_name,
  fiscal_num,
  fiscal_desc,
  label,
  rolling_fiscal_ind,
  plan_cp,
  ly,
  day_date);


INSERT INTO weekly_ops_standup
(SELECT 'FUNNEL' AS ops_name,
  banner,
  channel,
  'AOV' AS metric_name,
  week_num AS fiscal_num,
  CONCAT(month_short_desc, ' ', 'WK ', SUBSTR(CAST(week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN week_start_day_date <=  DATE_SUB({{params.start_date}},INTERVAL 7 DAY) 
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
   CASE
   WHEN SUM(digital_orders_plan) = 0
   THEN 0
   ELSE SUM(digital_demand_amt_plan) / SUM(digital_orders_plan)
   END AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
   CASE
   WHEN SUM(digital_orders_act) = 0
   THEN 0
   ELSE SUM(demand_amt_act) / SUM(digital_orders_act)
   END AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5
 WHERE week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk)
  AND LOWER(channel) IN (LOWER('N.COM'), LOWER('R.COM'))
 GROUP BY week_num,
  week_454_num,
  month_short_desc,
  week_start_day_date,
  banner,
  channel);


INSERT INTO weekly_ops_standup
(SELECT 'FUNNEL' AS ops_name,
  banner,
  channel,
  'Conversion' AS metric_name,
  week_num AS fiscal_num,
  CONCAT(month_short_desc, ' ', 'WK ', SUBSTR(CAST(week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN week_start_day_date <=  DATE_SUB({{params.start_date}},INTERVAL 7 DAY) 
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
   CASE
   WHEN SUM(digital_traffic_plan) = 0
   THEN 0
   ELSE SUM(digital_orders_plan) / SUM(digital_traffic_plan)
   END AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
   CASE
   WHEN SUM(sessions) = 0
   THEN 0
   ELSE SUM(digital_orders_act) / SUM(sessions)
   END AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM ft_ops_5
 WHERE week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk)
  AND LOWER(channel) IN (LOWER('N.COM'), LOWER('R.COM'))
 GROUP BY week_num,
  week_454_num,
  month_short_desc,
  week_start_day_date,
  banner,
  channel);



CREATE TEMPORARY TABLE IF NOT EXISTS retail_tran_detail_fact_vw_temp
AS
SELECT dtl.global_tran_id,
 dtl.business_day_date,
 dtl.tran_type_code,
 dtl.line_item_seq_num,
 dtl.line_item_activity_type_code,
 dtl.line_item_activity_type_desc,
 dtl.unique_source_id,
 hdr.acp_id,
 hdr.marketing_profile_type_ind,
 hdr.deterministic_acp_id,
 dtl.ringing_store_num,
 hdr.fulfilling_store_num,
 dtl.intent_store_num,
 dtl.claims_destination_store_num,
 dtl.register_num,
 dtl.tran_num,
 dtl.tran_version_num,
 hdr.transaction_identifier,
 hdr.transaction_identifier_source,
 hdr.sa_tran_status_code,
 hdr.original_global_tran_id,
 hdr.reversal_flag,
 hdr.data_source_code,
 hdr.pre_sale_type_code,
 hdr.order_num,
 hdr.order_date,
 hdr.followup_slsprsn_num,
 hdr.pbfollowup_slsprsn_num,
 dtl.tran_date,
 dtl.tran_time,
 dtl.tran_time_tz,
 dtl.sku_num,
 dtl.upc_num,
 dtl.line_item_merch_nonmerch_ind,
 dtl.merch_dept_num,
 dtl.nonmerch_fee_code,
 dtl.line_item_promo_id,
 dtl.line_net_usd_amt,
 dtl.line_net_amt,
 dtl.line_item_net_amt_currency_code,
 dtl.line_item_quantity,
 dtl.line_item_fulfillment_type,
 dtl.line_item_order_type,
 dtl.commission_slsprsn_num,
 dtl.employee_discount_flag,
 dtl.employee_discount_num,
 dtl.employee_discount_usd_amt,
 dtl.employee_discount_amt,
 dtl.employee_discount_currency_code,
 dtl.line_item_promo_usd_amt,
 dtl.line_item_promo_amt,
 dtl.line_item_promo_amt_currency_code,
 dtl.merch_unique_item_id,
 dtl.merch_price_adjust_reason,
 dtl.line_item_capture_system,
 dtl.original_business_date,
 dtl.original_ringing_store_num,
 dtl.original_register_num,
 dtl.original_tran_num,
 dtl.original_transaction_identifier,
 dtl.original_line_item_usd_amt,
 dtl.original_line_item_amt,
 dtl.original_line_item_amt_currency_code,
 dtl.line_item_regular_price,
 dtl.line_item_regular_price_currency_code,
 dtl.line_item_split_tax_seq_num,
 dtl.line_item_split_tax_pct,
 dtl.line_item_split_tax_type,
 dtl.line_item_split_tax_amt,
 dtl.dw_batch_date,
 dtl.dw_sys_load_tmstp,
 dtl.dw_sys_updt_tmstp,
 dtl.error_flag,
 dtl.tran_latest_version_ind,
 dtl.item_source,
 dtl.line_item_tax_amt,
 dtl.line_item_tax_currency_code,
 dtl.line_item_tax_usd_amt,
 dtl.line_item_tax_exempt_flag,
 dtl.line_item_tax_pct,
 dtl.price_adj_code,
 dtl.tran_line_id,
 hdr.deterministic_profile_id,
 dtl.financial_retail_tran_record_id,
 dtl.financial_retail_tran_line_id,
 dtl.original_financial_retail_tran_record_id,
 dtl.ownership_model,
 dtl.partner_relationship_num,
 dtl.partner_relationship_type_code,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
    LOWER('TRUNK CLUB'))
  THEN 'NORDSTROM'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
  THEN 'RACK'
  ELSE NULL
  END AS banner,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'N_STORE'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'N_COM'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'R_STORE'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'R_COM'
  ELSE NULL
  END AS channel,
  CASE
  WHEN dtl.line_net_usd_amt < 0
  THEN dtl.line_item_quantity * - 1
  WHEN dtl.line_net_usd_amt = 0
  THEN 0
  ELSE dtl.line_item_quantity
  END AS net_items,
  CASE
  WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) = LOWER('6666')
  THEN 1
  ELSE 0
  END AS gc_flag,
  CASE
  WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('USD')
  THEN 808
  WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('CAD')
  THEN 867
  ELSE dtl.intent_store_num
  END AS store_num,
  CASE
  WHEN dtl.line_net_usd_amt > 0
  THEN COALESCE(hdr.order_date, dtl.tran_date)
  ELSE dtl.tran_date
  END AS customer_shopped_date,
  CASE
  WHEN dtl.line_net_usd_amt > 0
  THEN hdr.acp_id || FORMAT('%11d', st.store_num) || CAST(CASE
     WHEN dtl.line_net_usd_amt > 0
     THEN COALESCE(hdr.order_date, dtl.tran_date)
     ELSE dtl.tran_date
     END AS STRING)
  ELSE NULL
  END AS trip_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_hdr_fact AS hdr 
 ON dtl.global_tran_id = hdr.global_tran_id 
 AND dtl.business_day_date = hdr.business_day_date
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS st
  ON CASE
   WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') 
   AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp' ) 
   AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('CAD')
   THEN 867
   WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') 
   AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp') 
   AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('USD')
   THEN 808
   ELSE dtl.intent_store_num
   END = st.store_num
WHERE dtl.business_day_date
  BETWEEN DATE_SUB({{params.start_date}},INTERVAL 90 DAY) AND {{params.start_date}};


CREATE TEMPORARY TABLE IF NOT EXISTS transactions
AS
SELECT b.week_idnt,
 b.week_start_day_date,
 b.week_end_day_date,
  CASE
  WHEN ac.us_dma_code = - 1
  THEN 'UNKNOWN'
  WHEN ac.us_dma_code IS NOT NULL
  THEN 'US'
  WHEN ac.ca_dma_code IS NOT NULL
  THEN 'CA'
  ELSE 'UNKNOWN'
  END AS country,
  CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'), LOWER('FULL LINE CANADA'), LOWER('N.CA'),
    LOWER('TRUNK CLUB'))
  THEN 'NORDSTROM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('OFFPRICE ONLINE'), LOWER('RACK CANADA'))
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
  CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'))
  THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.COM'))
  THEN 'N.COM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'))
  THEN 'RACK STORE'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'R.COM'
  ELSE NULL
  END AS channel,
 COUNT(DISTINCT dtl.acp_id) AS cust_count,
 SUM(dtl.line_net_usd_amt) AS net_spend,
 SUM(CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS gross_spend,
 COUNT(DISTINCT CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN dtl.acp_id || FORMAT('%11d', str.store_num) || CAST(COALESCE(dtl.order_date, dtl.tran_date) AS STRING)
   ELSE NULL
   END) AS trips
FROM retail_tran_detail_fact_vw_temp AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS str ON CASE
   WHEN LOWER(dtl.line_item_order_type) LIKE LOWER('CustInit%') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp'
       ) AND LOWER(dtl.data_source_code) = LOWER('COM')
   THEN dtl.ringing_store_num
   ELSE dtl.intent_store_num
   END = str.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS b 
 ON CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN COALESCE(dtl.order_date, dtl.tran_date)
   ELSE dtl.tran_date
   END = b.day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.analytical_customer AS ac ON LOWER(dtl.acp_id) = LOWER(ac.acp_id)
WHERE CASE WHEN LINE_NET_USD_AMT > 0 THEN COALESCE(DTL.ORDER_DATE,DTL.TRAN_DATE) ELSE DTL.TRAN_DATE END BETWEEN DATE_SUB({{params.start_date}},INTERVAL 28 DAY) AND (SELECT week_end_day_date
           FROM curr_wk)
 AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND dtl.acp_id IS NOT NULL
GROUP BY b.week_idnt,
 b.week_start_day_date,
 b.week_end_day_date,
 country,
 banner,
 channel;


INSERT INTO weekly_ops_standup
(SELECT 'OUTCOMES' AS ops_name,
  A.banner,
  A.channel,
  'PURCHASE TRIPS' AS metric_name,
  A.week_num AS fiscal_num,
  CONCAT(b0.month_short_desc, ' ', 'WK ', SUBSTR(CAST(b0.week_454_num AS STRING), 1, 12)) AS fiscal_desc,
  CAST(NULL AS STRING) AS label,
   CASE
   WHEN b0.week_start_day_date <=  DATE_SUB({{params.start_date}},INTERVAL 7 DAY)  
   THEN 1
   ELSE 0
   END AS rolling_fiscal_ind,
  A.trips_plan AS plan_op,
  CAST(NULL AS FLOAT64) AS plan_cp,
  CAST(A.trips AS FLOAT64) AS ty,
  CAST(NULL AS FLOAT64) AS ly,
  CAST(NULL AS DATE) AS day_date
 FROM (SELECT DISTINCT COALESCE(A.banner, b.banner) AS banner,
    COALESCE(A.channel, b.channel) AS channel,
    COALESCE(A.week_idnt, b.week_num) AS week_num,
    b.trips_plan,
    A.trips
   FROM (SELECT week_idnt,
      week_start_day_date,
      week_end_day_date,
      banner,
      channel,
      SUM(trips) AS trips
     FROM transactions
     WHERE LOWER(country) IN (LOWER('US'), LOWER('UNKNOWN'))
     GROUP BY week_idnt,
      week_start_day_date,
      week_end_day_date,
      banner,
      channel) AS A
    FULL JOIN t2dl_das_osu.weekly_ops_customer_channel_plan AS b ON A.week_idnt = b.week_num AND LOWER(A.banner) = LOWER(b
        .banner) AND LOWER(A.channel) = LOWER(b.channel)) AS A
  INNER JOIN cal_lkup AS b0 ON A.week_num = b0.week_num
 WHERE b0.week_start_day_date BETWEEN (SELECT DATE_SUB(week_start_day_date, INTERVAL 14 DAY)
    FROM curr_wk) AND (SELECT DATE_ADD(week_start_day_date, INTERVAL 27 DAY)
    FROM curr_wk));



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dsa_ai_fct.weekly_ops_standup_agg
WHERE LOWER(ops_name) IN (LOWER('OUTCOMES'), LOWER('FUNNEL')) AND updated_week = (SELECT week_idnt
   FROM curr_wk);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dsa_ai_fct.weekly_ops_standup_agg
(SELECT a.ops_name,
  a.banner,
  a.channel,
  a.metric_name,
  a.fiscal_num,
  a.fiscal_desc,
  a.label,
  a.rolling_fiscal_ind,
  a.plan_op,
  a.plan_cp,
  a.ty,
  a.ly,
  curr_wk.week_idnt AS updated_week,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  a.day_date
 FROM weekly_ops_standup AS a
  INNER JOIN curr_wk ON TRUE);
-------------------------------------------------------------------------------------------------------------------------------------------------------------

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
-- SET QUERY_BAND = NONE FOR SESSION;
