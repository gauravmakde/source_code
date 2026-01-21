

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_fee_code_dim_vtw;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_fee_code_dim_vtw (fee_code_num, fee_code_desc, note_text, effective_date, termination_date, type_code, type_code_desc, price_amt, employee_discount_allowed_flag, taxable_flag, tax_category_code, display_at_pos_flag, entry_allowed_at_pos_flag, charge_ringing_store_flag, legacy_general_ledger_account_num, legacy_general_ledger_department_num, legacy_general_ledger_store_num, oracle_general_ledger_account_num, unassigned_item_flag, unassigned_item_desc, src_seq_num, eff_begin_tmstp,
eff_begin_tmstp_tz,
eff_end_tmstp,
eff_end_tmstp_tz
)


WITH SRC AS (
              SELECT
                  fee_code_num,
                  fee_code_desc,
                  note_text,
                  effective_date,
                  termination_date,
                  type_code,
                  type_code_desc,
                  price_amt,
                  employee_discount_allowed_flag,
                  taxable_flag,
                  tax_category_code,
                  display_at_pos_flag,
                  entry_allowed_at_pos_flag,
                  charge_ringing_store_flag,
                  legacy_general_ledger_account_num,
                  legacy_general_ledger_department_num,
                  legacy_general_ledger_store_num,
                  oracle_general_ledger_account_num,
                  unassigned_item_flag,
                  unassigned_item_desc,
                  src_seq_num,
                  eff_begin_tmstp,
                  eff_end_tmstp
                  FROM(
                SELECT  src_seq_num,
                        fee_code_num,
                        effective_date,
                        termination_date,
                        fee_code_desc,
                        note_text,
                        price_amt,
                        employee_discount_allowed_flag,
                        taxable_flag,
                        tax_category_code,
                        display_at_pos_flag,
                        entry_allowed_at_pos_flag,
                        charge_ringing_store_flag,
                        legacy_general_ledger_account_num,
                        legacy_general_ledger_department_num,
                        legacy_general_ledger_store_num,
                        unassigned_item_flag,
                        unassigned_item_desc,
                        type_code,
                        type_code_desc,
                        oracle_general_ledger_account_num, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY  fee_code_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY  fee_code_num ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from
 (
                    SELECT
                        src1.src_seq_num,
                        src1.fee_code_num,
                        src1.effective_date,
                        src1.termination_date,
                        src1.fee_code_desc,
                        src1.note_text,
                        src1.price_amt,
                        src1.employee_discount_allowed_flag,
                        src1.taxable_flag,
                        src1.tax_category_code,
                        src1.display_at_pos_flag,
                        src1.entry_allowed_at_pos_flag,
                        src1.charge_ringing_store_flag,
                        src1.legacy_general_ledger_account_num,
                        src1.legacy_general_ledger_department_num,
                        src1.legacy_general_ledger_store_num,
                        src1.unassigned_item_flag,
                        src1.unassigned_item_desc,
                        src1.type_code,
                        src1.type_code_desc,
                        src1.oracle_general_ledger_account_num,
                        src1.eff_begin_tmstp,
                        COALESCE(min(src1.eff_begin_tmstp) OVER (PARTITION BY src1.fee_code_num ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), CAST(CAST(/* expression of unknown or erroneous type */ src1.termination_date as DATE) as TIMESTAMP)) AS eff_end_tmstp,
						eff_begin_tmstp_tz,
						`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(COALESCE(min(src1.eff_begin_tmstp) OVER (PARTITION BY src1.fee_code_num ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), 
CAST(CAST(/* expression of unknown or erroneous type */ src1.termination_date as DATE) as TIMESTAMP)) as string)) as eff_end_tmstp_tz
                      FROM
                        (
                          SELECT
                              ldg1.seq_num AS src_seq_num,
                              ldg1.code_num AS fee_code_num,
                              ldg1.is_deleted_flag,
                              ldg1.effective_date,
                              ldg1.termination_date,
                              ldg1.fee_code_desc,
                              ldg1.note_text,
                              ldg1.price_amt,
                              ldg1.employee_discount_allowed_flag,
                              ldg1.taxable_flag,
                              ldg1.tax_category_code,
                              ldg1.display_at_pos_flag,
                              ldg1.entry_allowed_at_pos_flag,
                              ldg1.charge_ringing_store_flag,
                              ldg1.legacy_general_ledger_account_num,
                              ldg1.legacy_general_ledger_department_num,
                              ldg1.legacy_general_ledger_store_num,
                              ldg1.unassigned_item_flag,
                              ldg1.unassigned_item_desc,
                              `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg1.last_update_date) AS last_update_date,
                              ldg1.type_code,
                              ldg1.type_code_desc,
                              ldg1.oracle_general_ledger_account_num,
                              CAST(CAST(ldg1.effective_date as DATE) as TIMESTAMP) AS eff_begin_tmstp,
							  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg1.effective_date)) as eff_begin_tmstp_tz
                            FROM
                              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_fee_code_ldg AS ldg1
                              LEFT OUTER JOIN (
                                SELECT
                                    product_fee_code_ldg.code_num,
                                    product_fee_code_ldg.seq_num
                                  FROM
                                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_fee_code_ldg
                                  WHERE LOWER(product_fee_code_ldg.is_deleted_flag) = 'Y'
                              ) AS ldg2 ON ldg1.seq_num = ldg2.seq_num
                               AND ldg1.code_num = ldg2.code_num
                            WHERE ldg2.code_num IS NULL
                            QUALIFY row_number() OVER (PARTITION BY fee_code_num, src_seq_num ORDER BY last_update_date DESC) = 1
                        ) AS src1
                  ) AS src2
                WHERE src2.eff_begin_tmstp < src2.eff_end_tmstp) AS ordered_data ) AS grouped_data
GROUP BY src_seq_num,
                        fee_code_num,
                        effective_date,
                        termination_date,
                        fee_code_desc,
                        note_text,
                        price_amt,
                        employee_discount_allowed_flag,
                        taxable_flag,
                        tax_category_code,
                        display_at_pos_flag,
                        entry_allowed_at_pos_flag,
                        charge_ringing_store_flag,
                        legacy_general_ledger_account_num,
                        legacy_general_ledger_department_num,
                        legacy_general_ledger_store_num,
                        unassigned_item_flag,
                        unassigned_item_desc,
                        type_code,
                        type_code_desc,
                        oracle_general_ledger_account_num, range_group
ORDER BY  fee_code_num, eff_begin_tmstp))


  SELECT
      fee_code_num,
      fee_code_desc,
      note_text,
      CAST(effective_date AS DATE) AS effective_date,
      CAST(termination_date AS DATE) AS termination_date,
      type_code,
      type_code_desc,
      price_amt,
      employee_discount_allowed_flag,
      taxable_flag,
      tax_category_code,
      display_at_pos_flag,
      entry_allowed_at_pos_flag,
      charge_ringing_store_flag,
      legacy_general_ledger_account_num,
      legacy_general_ledger_department_num,
      legacy_general_ledger_store_num,
      oracle_general_ledger_account_num,
      unassigned_item_flag,
      unassigned_item_desc,
      src_seq_num,
      MIN(RANGE_START(eff_period)) AS eff_begin,
      `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MIN(RANGE_START(eff_period)) AS STRING)) AS eff_begin_tmstp_tz,
	  -- nrml.eff_begin_tmstp_tz,
      MAX(RANGE_END(eff_period)) AS eff_end,
      `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MIN(RANGE_END(eff_period)) AS STRING)) AS eff_end_tmstp_tz,
	  -- nrml.eff_end_tmstp_tz
      FROM(
    --inner normalize
            SELECT *,range_group
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY fee_code_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (   --  NORMALIZE
        SELECT
            src.fee_code_num,
            src.fee_code_desc,
            src.note_text,
            src.effective_date,
            src.termination_date,
            src.type_code,
            src.type_code_desc,
            src.price_amt,
            src.employee_discount_allowed_flag,
            src.taxable_flag,
            src.tax_category_code,
            src.display_at_pos_flag,
            src.entry_allowed_at_pos_flag,
            src.charge_ringing_store_flag,
            src.legacy_general_ledger_account_num,
            src.legacy_general_ledger_department_num,
            src.legacy_general_ledger_store_num,
            src.oracle_general_ledger_account_num,
            src.unassigned_item_flag,
            src.unassigned_item_desc,
            src.src_seq_num,
            tgt.eff_begin_tmstp,
            tgt.eff_end_tmstp,
             CASE 
                WHEN LAG(TGT.eff_end_tmstp) OVER (PARTITION BY src.fee_code_num ORDER BY TGT.eff_begin_tmstp) >= 
                DATE_SUB(TGT.eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
            COALESCE(
			RANGE_INTERSECT(range(SRC.EFF_BEGIN_TMSTP, SRC.EFF_END_TMSTP), RANGE(tgt.eff_begin_tmstp, tgt.	eff_end_tmstp)),range(SRC.EFF_BEGIN_TMSTP, SRC.EFF_END_TMSTP)) AS eff_period
			-- src.eff_begin_tmstp_tz,
			-- src.eff_end_tmstp_tz
           FROM SRC
            LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_fee_code_dim AS tgt ON src.fee_code_num = tgt.fee_code_num
             AND RANGE_OVERLAPS(range(SRC.EFF_BEGIN_TMSTP, SRC.EFF_END_TMSTP),RANGE(tgt.eff_begin_tmstp, tgt.eff_end_tmstp))
          WHERE tgt.fee_code_num IS NULL
           OR src.src_seq_num <> tgt.src_seq_num
           OR src.src_seq_num IS NOT NULL
           AND tgt.src_seq_num IS NULL
           OR src.src_seq_num IS NULL
           AND tgt.src_seq_num IS NOT NULL
           OR CAST(src.effective_date AS DATE) <> tgt.effective_date
           OR src.effective_date IS NOT NULL
           AND tgt.effective_date IS NULL
           OR src.effective_date IS NULL
           AND tgt.effective_date IS NOT NULL
           OR CAST(src.termination_date AS DATE) <> tgt.termination_date
           OR src.termination_date IS NOT NULL
           AND tgt.termination_date IS NULL
           OR src.termination_date IS NULL
           AND tgt.termination_date IS NOT NULL
           OR LOWER(src.fee_code_desc) <> LOWER(tgt.fee_code_desc)
           OR src.fee_code_desc IS NOT NULL
           AND tgt.fee_code_desc IS NULL
           OR src.fee_code_desc IS NULL
           AND tgt.fee_code_desc IS NOT NULL
           OR LOWER(src.note_text) <> LOWER(tgt.note_text)
           OR src.note_text IS NOT NULL
           AND tgt.note_text IS NULL
           OR src.note_text IS NULL
           AND tgt.note_text IS NOT NULL
           OR src.price_amt <> tgt.price_amt
           OR src.price_amt IS NOT NULL
           AND tgt.price_amt IS NULL
           OR src.price_amt IS NULL
           AND tgt.price_amt IS NOT NULL
           OR LOWER(src.employee_discount_allowed_flag) <> LOWER(tgt.employee_discount_allowed_flag)
           OR src.employee_discount_allowed_flag IS NOT NULL
           AND tgt.employee_discount_allowed_flag IS NULL
           OR src.employee_discount_allowed_flag IS NULL
           AND tgt.employee_discount_allowed_flag IS NOT NULL
           OR LOWER(src.taxable_flag) <> LOWER(tgt.taxable_flag)
           OR src.taxable_flag IS NOT NULL
           AND tgt.taxable_flag IS NULL
           OR src.taxable_flag IS NULL
           AND tgt.taxable_flag IS NOT NULL
           OR LOWER(src.tax_category_code) <> LOWER(tgt.tax_category_code)
           OR src.tax_category_code IS NOT NULL
           AND tgt.tax_category_code IS NULL
           OR src.tax_category_code IS NULL
           AND tgt.tax_category_code IS NOT NULL
           OR LOWER(src.display_at_pos_flag) <> LOWER(tgt.display_at_pos_flag)
           OR src.display_at_pos_flag IS NOT NULL
           AND tgt.display_at_pos_flag IS NULL
           OR src.display_at_pos_flag IS NULL
           AND tgt.display_at_pos_flag IS NOT NULL
           OR LOWER(src.entry_allowed_at_pos_flag) <> LOWER(tgt.entry_allowed_at_pos_flag)
           OR src.entry_allowed_at_pos_flag IS NOT NULL
           AND tgt.entry_allowed_at_pos_flag IS NULL
           OR src.entry_allowed_at_pos_flag IS NULL
           AND tgt.entry_allowed_at_pos_flag IS NOT NULL
           OR LOWER(src.charge_ringing_store_flag) <> LOWER(tgt.charge_ringing_store_flag)
           OR src.charge_ringing_store_flag IS NOT NULL
           AND tgt.charge_ringing_store_flag IS NULL
           OR src.charge_ringing_store_flag IS NULL
           AND tgt.charge_ringing_store_flag IS NOT NULL
           OR src.legacy_general_ledger_account_num <> tgt.legacy_general_ledger_account_num
           OR src.legacy_general_ledger_account_num IS NOT NULL
           AND tgt.legacy_general_ledger_account_num IS NULL
           OR src.legacy_general_ledger_account_num IS NULL
           AND tgt.legacy_general_ledger_account_num IS NOT NULL
           OR src.legacy_general_ledger_department_num <> tgt.legacy_general_ledger_department_num
           OR src.legacy_general_ledger_department_num IS NOT NULL
           AND tgt.legacy_general_ledger_department_num IS NULL
           OR src.legacy_general_ledger_department_num IS NULL
           AND tgt.legacy_general_ledger_department_num IS NOT NULL
           OR LOWER(src.legacy_general_ledger_store_num) <> LOWER(tgt.legacy_general_ledger_store_num)
           OR src.legacy_general_ledger_store_num IS NOT NULL
           AND tgt.legacy_general_ledger_store_num IS NULL
           OR src.legacy_general_ledger_store_num IS NULL
           AND tgt.legacy_general_ledger_store_num IS NOT NULL
           OR LOWER(src.unassigned_item_flag) <> LOWER(tgt.unassigned_item_flag)
           OR src.unassigned_item_flag IS NOT NULL
           AND tgt.unassigned_item_flag IS NULL
           OR src.unassigned_item_flag IS NULL
           AND tgt.unassigned_item_flag IS NOT NULL
           OR LOWER(src.unassigned_item_desc) <> LOWER(tgt.unassigned_item_desc)
           OR src.unassigned_item_desc IS NOT NULL
           AND tgt.unassigned_item_desc IS NULL
           OR src.unassigned_item_desc IS NULL
           AND tgt.unassigned_item_desc IS NOT NULL
           OR src.type_code <> tgt.type_code
           OR src.type_code IS NOT NULL
           AND tgt.type_code IS NULL
           OR src.type_code IS NULL
           AND tgt.type_code IS NOT NULL
           OR LOWER(src.type_code_desc) <> LOWER(tgt.type_code_desc)
           OR src.type_code_desc IS NOT NULL
           AND tgt.type_code_desc IS NULL
           OR src.type_code_desc IS NULL
           AND tgt.type_code_desc IS NOT NULL
           OR LOWER(src.oracle_general_ledger_account_num) <> LOWER(tgt.oracle_general_ledger_account_num)
           OR src.oracle_general_ledger_account_num IS NOT NULL
           AND tgt.oracle_general_ledger_account_num IS NULL
           OR src.oracle_general_ledger_account_num IS NULL
           AND tgt.oracle_general_ledger_account_num IS NOT NULL
  )
    )  AS ordered_data
) AS grouped_data
GROUP BY     fee_code_num,
      fee_code_desc,
      note_text,
      effective_date,
      termination_date,
      type_code,
      type_code_desc,
      price_amt,
      employee_discount_allowed_flag,
      taxable_flag,
      tax_category_code,
      display_at_pos_flag,
      entry_allowed_at_pos_flag,
      charge_ringing_store_flag,
      legacy_general_ledger_account_num,
      legacy_general_ledger_department_num,
      legacy_general_ledger_store_num,
      oracle_general_ledger_account_num,
      unassigned_item_flag,
      unassigned_item_desc,
      src_seq_num
ORDER BY  fee_code_num ;




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_fee_code_dim
WHERE (fee_code_num, src_seq_num) IN (SELECT (fee_code_num, src_seq_num)
  FROM (SELECT code_num AS fee_code_num,
     seq_num AS src_seq_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_fee_code_ldg
    WHERE LOWER(is_deleted_flag) = LOWER('Y')
    GROUP BY fee_code_num,
     src_seq_num) AS t1);


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_fee_code_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_fee_code_dim_vtw AS src
 WHERE LOWER(fee_code_num) = LOWER(tgt.fee_code_num));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_fee_code_dim (fee_code_num, fee_code_desc, note_text, effective_date, termination_date,
 type_code, type_code_desc, price_amt, employee_discount_allowed_flag, taxable_flag, tax_category_code,
 display_at_pos_flag, entry_allowed_at_pos_flag, charge_ringing_store_flag, legacy_general_ledger_account_num,
 legacy_general_ledger_department_num, legacy_general_ledger_store_num, oracle_general_ledger_account_num,
 unassigned_item_flag, unassigned_item_desc, src_seq_num, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp)
(SELECT DISTINCT fee_code_num,
  fee_code_desc,
  note_text,
  effective_date,
  termination_date,
  type_code,
  type_code_desc,
  price_amt,
  employee_discount_allowed_flag,
  taxable_flag,
  tax_category_code,
  display_at_pos_flag,
  entry_allowed_at_pos_flag,
  charge_ringing_store_flag,
  legacy_general_ledger_account_num,
  legacy_general_ledger_department_num,
  legacy_general_ledger_store_num,
  oracle_general_ledger_account_num,
  unassigned_item_flag,
  unassigned_item_desc,
  src_seq_num,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT fee_code_num,
    fee_code_desc,
    note_text,
    effective_date,
    termination_date,
    type_code,
    type_code_desc,
    price_amt,
    employee_discount_allowed_flag,
    taxable_flag,
    tax_category_code,
    display_at_pos_flag,
    entry_allowed_at_pos_flag,
    charge_ringing_store_flag,
    legacy_general_ledger_account_num,
    legacy_general_ledger_department_num,
    legacy_general_ledger_store_num,
    oracle_general_ledger_account_num,
    unassigned_item_flag,
    unassigned_item_desc,
    src_seq_num,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_fee_code_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_fee_code_dim
   WHERE fee_code_num = t.fee_code_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY fee_code_num)) = 1);