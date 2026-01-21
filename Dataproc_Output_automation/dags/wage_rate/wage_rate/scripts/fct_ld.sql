
MERGE INTO <DBENV>_sca_prf.wage_rate_fct AS e
USING 
(SELECT
 store
    ,department
    ,average_rate
    ,rate_date
	,BTCH_ID
    ,RCD_LOAD_TMSTP
    ,RCD_UPDT_TMSTP
    FROM <DBENV>_sca_prf.wage_rate_opr
    WHERE btch_id = (SELECT btch_id
                FROM <DBENV>_sca_prf.dl_interface_dt_lkup
                WHERE end_tmstp IS NULL 
                AND LOWER(interface_id) = LOWER('<project_id>') 
                AND LOWER(subject_id) = LOWER('<subject_id>'))) AS C
ON LOWER(e.store) = LOWER(C.store) 
AND LOWER(e.department) = LOWER(C.department) 
AND e.rate_date = C.rate_date
WHEN MATCHED THEN
 UPDATE SET
    average_rate = C.average_rate,
    btch_id = C.btch_id,
    rcd_updt_tmstp = C.rcd_updt_tmstp
WHEN NOT MATCHED THEN 
INSERT 
(
	 store
    ,department
    ,average_rate
    ,rate_date
	,BTCH_ID
    ,RCD_LOAD_TMSTP
    ,RCD_UPDT_TMSTP
)
VALUES
(C.store, 
C.department, 
C.average_rate,
 C.rate_date, 
 C.btch_id,
  C.rcd_load_tmstp, 
  C.rcd_updt_tmstp);

--COMMIT TRANSACTION;

