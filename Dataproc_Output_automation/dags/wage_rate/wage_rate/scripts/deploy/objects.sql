
/*SET QUERY_BAND='AppName=DATALAB;AppRelease=1;AppFreq=ONCE;AppPhase=DEPLOY;AppSubArea=SCA;'  update FOR SESSION;*/
-- COMMIT TRANSACTION;

DROP TABLE IF EXISTS <bq_project_id>.<DBENV>_SCA_PRF.WAGE_RATE_STG;

-- COMMIT TRANSACTION;

CREATE TABLE IF NOT EXISTS <bq_project_id>.<DBENV>_sca_prf.wage_rate_stg (
store STRING(200),
department STRING(200),
average_rate STRING(200),
rate_date STRING(200)
);

-- END;


DROP TABLE IF EXISTS <bq_project_id>.<DBENV>_SCA_PRF.WAGE_RATE_OPR;


-- COMMIT TRANSACTION;

CREATE TABLE IF NOT EXISTS <bq_project_id>.<DBENV>_sca_prf.wage_rate_opr (
store STRING(4),
department STRING(4),
average_rate NUMERIC(32,4),
rate_date DATE,
btch_id INTEGER,
rcd_load_tmstp DATETIME DEFAULT CURRENT_DATETIME('PST8PDT') NOT NULL,
rcd_updt_tmstp DATETIME DEFAULT CURRENT_DATETIME('PST8PDT') NOT NULL
)
PARTITION BY RANGE_BUCKET(btch_id, GENERATE_ARRAY(1, 1000000, 999))
CLUSTER BY store, department, rate_date;

-- COMMIT TRANSACTION;
--COLLECT STATISTICS COLUMN store ,COLUMN department ,COLUMN rate_date ON  <bq_project_id>.<DBENV>_SCA_PRF.WAGE_RATE_OPR;

-- COMMIT TRANSACTION;


DROP TABLE IF EXISTS <bq_project_id>.<DBENV>_SCA_PRF.WAGE_RATE_OPR;


-- COMMIT TRANSACTION;

CREATE TABLE IF NOT EXISTS <bq_project_id>.<DBENV>_sca_prf.wage_rate_fct (
store STRING(4),
department STRING(4),
average_rate NUMERIC(32,4),
rate_date DATE,
btch_id INTEGER,
rcd_load_tmstp DATETIME DEFAULT CURRENT_DATETIME('PST8PDT') NOT NULL,
rcd_updt_tmstp DATETIME DEFAULT CURRENT_DATETIME('PST8PDT') NOT NULL
) CLUSTER BY store, department, rate_date;

-- COMMIT TRANSACTION;

--COLLECT STATISTICS COLUMN store ,COLUMN department ,COLUMN rate_date ON  <bq_project_id>.<DBENV>_SCA_PRF.WAGE_RATE_FCT;

-- COMMIT TRANSACTION;
