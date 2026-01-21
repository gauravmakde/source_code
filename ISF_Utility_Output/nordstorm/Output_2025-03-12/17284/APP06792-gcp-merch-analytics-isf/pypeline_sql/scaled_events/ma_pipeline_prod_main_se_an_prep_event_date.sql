/******************************************************************************
Name: Scaled Events Dates Lookup Table
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the backend lookup table for Anniv/Cyber event dates and TY/LY mapping
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
DAG: merch_se_an_prep_event_date
TABLE NAME: T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DATES (T3: T3DL_ACE_PRA.SCALED_EVENT_DATES)
Author(s): Alli Moore
Date Last Updated: 03-01-2024
******************************************************************************/
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'SCALED_EVENT_DATES', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.SCALED_EVENT_DATES ,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
     day_dt               DATE FORMAT 'YYYY-MM-DD' NOT NULL
     , day_idnt           INTEGER NOT NULL
     , yr_idnt            INTEGER NOT NULL COMPRESS(2024, 2023, 2022)
     , month_idnt         INTEGER NOT NULL
     , month_label        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , ty_ly_lly          VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
     , wk_of_fyr          INTEGER NOT NULL
     , wk_idnt            INTEGER NOT NULL
     , wk_end_dt          DATE FORMAT 'YYYY-MM-DD' NOT NULL
     , event_type         VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , event_country      CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS('US')
     , day_dt_aligned     DATE FORMAT 'YYYY-MM-DD'
     , event_day          INTEGER NOT NULL
     , event_day_mod      INTEGER
     , event_type_mod     VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
     , anniv_ind          INTEGER NOT NULL
     , cyber_ind          INTEGER NOT NULL
     , process_tmstp      TIMESTAMP(6) WITH TIME ZONE
     )
     PRIMARY INDEX (day_dt)
PARTITION BY RANGE_N(day_idnt  BETWEEN 2022001  AND 2025001  EACH 1 );

GRANT SELECT ON {environment_schema}.SCALED_EVENT_DATES TO PUBLIC;

--DELETE FROM {environment_schema}.SCALED_EVENT_DATES ALL;

INSERT INTO {environment_schema}.SCALED_EVENT_DATES
-- 2024 US ANNIV
SELECT DISTINCT
    c.day_date AS day_dt
    , c.day_num AS day_idnt
    , c.year_num AS yr_idnt
    , c454.month_idnt
    , c454.month_label
    , CASE
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE) THEN 'TY'
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN 'LY'
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE)-2 THEN 'LLY'
        ELSE NULL
     END AS ty_ly_lly
    , c.week_of_fyr AS wk_of_fyr
    , c.week_num AS wk_idnt
    , c454.week_end_day_date AS wk_end_dt
    , CAST((CASE
        WHEN day_dt_aligned BETWEEN '2024-07-08' AND '2024-07-09' THEN 'EA Icon'
        WHEN day_dt_aligned = '2024-07-10' THEN 'EA Amb'
        WHEN day_dt_aligned BETWEEN '2024-07-11' AND '2024-07-12' THEN 'EA Inf'
        WHEN day_dt_aligned BETWEEN '2024-07-13' AND '2024-07-14' THEN 'EA GT'
        WHEN day_dt_aligned BETWEEN '2024-07-15' AND '2024-08-04' THEN 'PE'
        WHEN day_dt_aligned > '2024-08-04' THEN 'Post'
        ELSE NULL
    END) AS VARCHAR(20)) AS event_type
    , 'US' AS event_country
    , c.day_date AS day_dt_aligned -- FOR US TY = same AS day_date
    , ROW_NUMBER() OVER (PARTITION BY yr_idnt ORDER BY day_dt ASC) AS event_day
    , event_day AS event_day_mod -- FOR US TY = same AS event_day
    , event_type AS event_type_mod -- FOR US TY = same AS event_type
    , 1 AS anniv_ind
    , 0 AS cyber_ind
    , current_timestamp as process_tmstp
FROM PRD_NAP_USR_VWS.DAY_CAL c
LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM c454
    ON c454.day_date = c.day_date
WHERE c.day_date BETWEEN '2024-07-08' AND '2024-09-03' --Event + 30 DAYS

UNION ALL
-- 2023 US Anniv
SELECT DISTINCT
    c.day_date AS day_dt
    , c.day_num AS day_idnt
    , c.year_num AS yr_idnt
    , c454.month_idnt
    , c454.month_label
    , CASE
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE) THEN 'TY'
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN 'LY'
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE)-2 THEN 'LLY'
        ELSE NULL
     END AS ty_ly_lly
    , c.week_of_fyr AS wk_of_fyr
    , c.week_num AS wk_idnt
    , c454.week_end_day_date AS wk_end_dt
    , CAST((CASE
        WHEN c.day_date BETWEEN '2023-07-10' AND '2023-07-11' THEN 'EA Icon'
        WHEN c.day_date = '2023-07-12' THEN 'EA Amb'
        WHEN c.day_date BETWEEN '2023-07-13' AND '2023-07-14' THEN 'EA Inf'
        WHEN c.day_date BETWEEN '2023-07-15' AND '2023-07-16' THEN 'EA GT'
        WHEN c.day_date BETWEEN '2023-07-17' AND '2023-08-06' THEN 'PE'
        WHEN c.day_date > '2023-08-06' THEN 'Post'
        ELSE NULL
    END) AS VARCHAR(20)) AS event_type
    , 'US' AS event_country
    , CAST((DATE '2024-07-08' + (c.day_date - DATE '2023-07-10')) AS DATE FORMAT 'YYYY-MM-DD') AS day_dt_aligned
    , ROW_NUMBER() OVER (PARTITION BY yr_idnt ORDER BY day_dt ASC) AS event_day
    , event_day AS event_day_mod -- FOR US TY = same AS event_day
    , CAST((CASE
        WHEN day_dt_aligned BETWEEN '2024-07-08' AND '2024-07-09' THEN 'EA Icon'
        WHEN day_dt_aligned = '2024-07-10' THEN 'EA Amb'
        WHEN day_dt_aligned BETWEEN '2024-07-11' AND '2024-07-12' THEN 'EA Inf'
        WHEN day_dt_aligned BETWEEN '2024-07-13' AND '2024-07-14' THEN 'EA GT'
        WHEN day_dt_aligned BETWEEN '2024-07-15' AND '2024-08-04' THEN 'PE'
        WHEN day_dt_aligned > '2024-08-04' THEN 'Post'
        ELSE NULL
    END) AS VARCHAR(20)) AS event_type_mod
    , 1 AS anniv_ind
    , 0 AS cyber_ind
    , current_timestamp as process_tmstp
FROM PRD_NAP_USR_VWS.DAY_CAL c
LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM c454
    ON c454.day_date = c.day_date
WHERE c.day_date BETWEEN '2023-07-10' AND '2023-09-05'

UNION ALL
-- CYBER 2024 + 2023 US Dates
SELECT
    c.day_date AS day_dt
    , c.day_num AS day_idnt
    , c.year_num AS yr_idnt
    , c454.month_idnt
    , c454.month_label
    , CASE
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE) THEN 'TY'
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN 'LY'
        ELSE NULL
     END AS ty_ly_lly
     , c.week_of_fyr AS wk_of_fyr
     , c.week_num AS wk_idnt
     , c454.week_end_day_date AS wk_end_dt
    , 'Cyber' AS event_type
    , 'US' AS event_country
    , CASE
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE) THEN c.day_date
        WHEN c.year_num = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN CAST((DATE '2024-11-10' + (c.day_date - DATE '2023-11-05')) AS DATE FORMAT 'YYYY-MM-DD')
        ELSE NULL
    END AS day_dt_aligned
    , row_number() over (PARTITION BY yr_idnt ORDER BY day_dt ASC) AS event_day
    , event_day AS event_day_mod
    , event_type AS event_type_mod
    , 0 AS anniv_ind
    , 1 AS cyber_ind
    , current_timestamp as process_tmstp
FROM PRD_NAP_USR_VWS.DAY_CAL c
LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM c454
    ON c454.day_date = c.day_date
WHERE c.day_date BETWEEN '2024-11-10' AND '2025-02-01'
    OR c.day_date BETWEEN '2023-11-05' AND '2024-01-27'
;

COLLECT STATS
    PRIMARY INDEX ( DAY_DT )
    ,COLUMN ( DAY_DT )
        ON {environment_schema}.scaled_event_dates;
