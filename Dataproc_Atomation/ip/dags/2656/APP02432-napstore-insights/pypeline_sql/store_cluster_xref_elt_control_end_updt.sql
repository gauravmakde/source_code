-- DML stmt to close current batch
UPDATE {db_env}_NAP_UTL.ELT_CONTROL
   SET batch_end_tmstp = CURRENT_TIMESTAMP
     , active_load_ind = 'N'
 WHERE subject_area_nm = 'STORE_CLUSTER_XREF';
