-- Build time series lookup for ICON-to-ACP_ID 



TRUNCATE TABLE {{params.DBJWNENV}}_NAP_JWN_METRICS_STG.jwn_order_acp_wrk;


INSERT INTO {{params.DBJWNENV}}_NAP_JWN_METRICS_STG.jwn_order_acp_wrk (acp_id, cust_id, eff_begin_tmstp, eff_end_tmstp)
(SELECT acp_id,
  cust_id,
   CASE
   WHEN (ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY dw_sys_load_tmstp)) = 1
   THEN CAST('2000-01-01 00:00:00' AS DATETIME)
   ELSE dw_sys_load_tmstp
   END AS eff_begin_tmstp,
  COALESCE(MIN(dw_sys_load_tmstp) OVER (PARTITION BY cust_id ORDER BY dw_sys_load_tmstp ROWS BETWEEN 1 FOLLOWING AND 1
    FOLLOWING), CAST('9999-12-31 00:00:00' AS DATETIME)) AS eff_end_tmstp
 FROM {{params.DBENV}}_NAP_BASE_VWS.acp_analytical_cust_xref
 WHERE LOWER(cust_source) = LOWER('icon')
  AND LOWER(cust_id) <> LOWER('NULL'));

  