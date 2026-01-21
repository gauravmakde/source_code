MERGE INTO acp_sandbox.loyaltymember_dim_delta AS tgt
USING(
  SELECT * FROM acp_sandbox.loyalty_member_wrk
) AS wrk
on (tgt.loyaltyId = wrk.loyaltyId)
WHEN MATCHED
  THEN DELETE;

INSERT INTO acp_sandbox.loyaltymember_dim_delta
SELECT 
  loyaltyId
  , customerId
  , merged_to_loyalty_id
  , loyalty_id_active_ind
  , member_ind
  , member_status_desc
  , member_status_date
  , member_enroll_date
  , member_enroll_store_num
  , member_enroll_channel_desc
  , member_enroll_region_desc
  , member_enroll_country_code
  , cardmember_ind
  , cardmember_status_date
  , cardmember_enroll_Date
  , member_migration_ind
  , rewards_level
  , rewards_level_start_date
  , rewards_level_end_date
  , sys_time
  , COALESCE(dw_sys_load_tmstp, CURRENT_TIMESTAMP()) AS dw_sys_load_tmstp
  , CURRENT_TIMESTAMP() AS dw_sys_updt_tmstp
  , loyalty_points_balance
FROM acp_sandbox.loyalty_member_wrk;

VACUUM acp_sandbox.loyaltymember_dim_delta;

OPTIMIZE acp_sandbox.loyaltymember_dim_delta ZORDER BY (loyaltyId);
