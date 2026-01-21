/*
ast_pipeline_prod_main_size_curve_eval_adoption_query.sql
Author: Paria Avij
project: Size curve evaluation
Purpose: Create a table for size curve dashboard monthly adoption view
Date Created: 12/26/23
Datalab: {{params.environment_schema}},

*/

--drop table date_m;



CREATE TEMPORARY TABLE IF NOT EXISTS date_m
AS
SELECT month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 fiscal_halfyear_num,
 fiscal_year_num,
 half_label
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
GROUP BY month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 fiscal_halfyear_num,
 fiscal_year_num,
 half_label;


CREATE TEMPORARY TABLE IF NOT EXISTS adoption_1_m
AS
SELECT a.fiscal_month AS fiscal_month_num,
 ca.month_label,
 ca.fiscal_halfyear_num,
 ca.half_label,
 a.banner,
  CASE
  WHEN a.channel_id = 110
  THEN 'FLS'
  WHEN a.channel_id = 210
  THEN 'RACK'
  WHEN a.channel_id = 250
  THEN 'NRHL'
  WHEN a.channel_id = 120
  THEN 'N.COM'
  ELSE NULL
  END AS chnl_idnt,
 a.dept_id,
 a.size_profile,
 SUM(a.rcpt_dollars) AS rcpt_dollars,
 SUM(a.rcpt_units) AS rcpt_units
FROM `{{params.gcp_project_id}}`.t2dl_das_size.adoption_metrics AS a
 INNER JOIN date_m AS ca ON a.fiscal_month = ca.month_idnt
WHERE CASE
  WHEN a.channel_id = 110
  THEN 'FLS'
  WHEN a.channel_id = 210
  THEN 'RACK'
  WHEN a.channel_id = 250
  THEN 'NRHL'
  WHEN a.channel_id = 120
  THEN 'N.COM'
  ELSE NULL
  END IS NOT NULL
GROUP BY fiscal_month_num,
 ca.month_label,
 ca.fiscal_halfyear_num,
 ca.half_label,
 a.banner,
 chnl_idnt,
 a.dept_id,
 a.size_profile;


CREATE TEMPORARY TABLE IF NOT EXISTS adoption_percetange_m_1_j_rev
AS
SELECT TRIM(FORMAT('%11d', a.fiscal_halfyear_num)) AS fiscal_halfyear_num,
 TRIM(a.half_label) AS half_label,
 TRIM(FORMAT('%11d', a.fiscal_month_num)) AS fiscal_month_num,
 TRIM(a.month_label) AS month_label,
 a.dept_id,
 a.chnl_idnt,
 TRIM(FORMAT('%11d', b.division_num) || ',' || b.division_name) AS division,
 TRIM(FORMAT('%11d', b.subdivision_num) || ',' || b.subdivision_name) AS subdivision,
 TRIM(FORMAT('%11d', a.dept_id) || ',' || b.dept_name) AS department,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS non_sc_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) = LOWER('ARTS')
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS arts_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) = LOWER('EXISTING CURVE')
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS existing_curves_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) = LOWER('NONE')
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS none_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) = LOWER('OTHER')
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS other_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
    THEN COALESCE(a.rcpt_units, 0)
    ELSE NULL
    END), 0) AS sc_fls_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
    THEN COALESCE(a.rcpt_units, 0)
    ELSE NULL
    END), 0) AS sc_rack_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
    THEN COALESCE(a.rcpt_units, 0)
    ELSE NULL
    END), 0) AS sc_ncom_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
    THEN COALESCE(a.rcpt_units, 0)
    ELSE NULL
    END), 0) AS sc_nrhl_rcpt_units,
     COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
       THEN a.rcpt_units
       ELSE NULL
       END), 0) + COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) + COALESCE(SUM(CASE
     WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
     THEN COALESCE(a.rcpt_units, 0)
     ELSE NULL
     END), 0) AS ttl_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_sc_fls_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_sc_rack_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_sc_ncom_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_sc_nrhl_rcpt_units,
  CASE
  WHEN COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) >= 70 OR COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) >= 70 OR COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) >= 70 OR COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) * 100.0 / IF(COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) = 0, NULL, COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
            THEN a.rcpt_units
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0)) >= 70
  THEN 'High Adopting'
  WHEN COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) >= 30 AND COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) < 70 OR COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) >= 30 AND COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) < 70 OR COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) >= 30 AND COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) < 70 OR COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) >= 30 AND COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) < 70
  THEN 'Medium Adopting'
  WHEN COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) < 30 OR COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) < 30 OR COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) < 30 OR COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) * 100.0 / IF(COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) = 0, NULL, COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
            THEN a.rcpt_units
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0)) < 30
  THEN 'Low Adopting'
  ELSE NULL
  END AS adoption,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) = LOWER('ARTS')
      THEN a.rcpt_units
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_arts_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) = LOWER('EXISTING CURVE')
      THEN a.rcpt_units
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_existing_curves_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) = LOWER('NONE')
      THEN a.rcpt_units
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_none_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) = LOWER('OTHER')
      THEN a.rcpt_units
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_other_rcpt_units,
          COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) = LOWER('NONE')
             THEN a.rcpt_units
             ELSE NULL
             END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                  WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                  THEN COALESCE(a.rcpt_units, 0)
                  ELSE NULL
                  END), 0) + COALESCE(SUM(CASE
                  WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                  THEN COALESCE(a.rcpt_units, 0)
                  ELSE NULL
                  END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                 THEN a.rcpt_units
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) = 0, NULL, COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0)) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) = LOWER('OTHER')
             THEN a.rcpt_units
             ELSE NULL
             END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                  WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                  THEN COALESCE(a.rcpt_units, 0)
                  ELSE NULL
                  END), 0) + COALESCE(SUM(CASE
                  WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                  THEN COALESCE(a.rcpt_units, 0)
                  ELSE NULL
                  END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                 THEN a.rcpt_units
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) = 0, NULL, COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0)) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) = LOWER('EXISTING CURVE')
            THEN a.rcpt_units
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) = LOWER('ARTS')
           THEN a.rcpt_units
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) * 100.0 / IF(COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) = 0, NULL, COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
            THEN a.rcpt_units
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0)) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) * 100.0 / IF(COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
            THEN a.rcpt_units
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) = 0, NULL, COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
           THEN a.rcpt_units
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0)) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0) * 100.0 / IF(COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
           THEN a.rcpt_units
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) = 0, NULL, COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0)) AS total_percent
FROM adoption_1_m AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS b ON a.dept_id = b.dept_num
GROUP BY fiscal_halfyear_num,
 half_label,
 fiscal_month_num,
 month_label,
 a.dept_id,
 a.chnl_idnt,
 division,
 subdivision,
 department;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_evaluation_adoption_view{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_evaluation_adoption_view{{params.env_suffix}}
(SELECT fiscal_halfyear_num,
  half_label,
  fiscal_month_num,
  month_label,
  dept_id,
  chnl_idnt,
  division,
  subdivision,
  department,
  non_sc_rcpt_units,
  arts_rcpt_units,
  existing_curves_rcpt_units,
  none_rcpt_units,
  other_rcpt_units,
  sc_fls_rcpt_units,
  sc_rack_rcpt_units,
  sc_ncom_rcpt_units,
  sc_nrhl_rcpt_units,
  ttl_rcpt_units,
  CAST(percent_sc_fls_rcpt_units AS NUMERIC) AS percent_sc_fls_rcpt_units,
  CAST(percent_sc_rack_rcpt_units AS NUMERIC) AS percent_sc_rack_rcpt_units,
  CAST(percent_sc_ncom_rcpt_units AS NUMERIC) AS percent_sc_ncom_rcpt_units,
  CAST(percent_sc_nrhl_rcpt_units AS NUMERIC) AS percent_sc_nrhl_rcpt_units,
  adoption,
  CAST(percent_arts_rcpt_units AS NUMERIC) AS percent_arts_rcpt_units,
  CAST(percent_existing_curves_rcpt_units AS NUMERIC) AS percent_existing_curves_rcpt_units,
  CAST(percent_none_rcpt_units AS NUMERIC) AS percent_none_rcpt_units,
  CAST(percent_other_rcpt_units AS NUMERIC) AS percent_other_rcpt_units,
  ROUND(CAST(total_percent AS NUMERIC), 2) AS total_percent,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz
 FROM adoption_percetange_m_1_j_rev AS a);