-- DML stmt to open current batch
UPDATE {db_env}_NAP_UTL.ELT_CONTROL
   SET batch_id = batch_id+1
     , curr_batch_date = CURRENT_DATE
     , batch_start_tmstp = CURRENT_TIMESTAMP
     , active_load_ind = 'Y'
 WHERE subject_area_nm = 'STORE_CLUSTER_XREF';
