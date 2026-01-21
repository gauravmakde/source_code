CREATE EXTERNAL TABLE IF NOT EXISTS acp_sandbox.loyaltymember_dim_delta
(
   loyaltyId string,
   customerId string,
   merged_to_loyalty_id string,
   loyalty_id_active_ind string,
   member_ind string,
   member_status_desc string,
   member_status_date date,
   member_enroll_date date,
   member_enroll_store_num int,
   member_enroll_channel_desc string,
   member_enroll_region_desc string,
   member_enroll_country_code string,
   cardmember_ind string,
   cardmember_status_date date,
   cardmember_enroll_Date date,
   member_migration_ind string,
   rewards_level string,
   rewards_level_start_date date,
   rewards_level_end_date date,
   sys_time bigint,
   dw_sys_load_tmstp timestamp,
   dw_sys_updt_tmstp timestamp,
   loyalty_points_balance int
)
USING DELTA
LOCATION 's3://tf-nap-{env}-mkt-objectmodel-landing/event_consumer/delta/loyaltymember_dim_delta/';


CREATE EXTERNAL TABLE IF NOT EXISTS acp_sandbox.loyalty_member_wrk
(
   loyaltyId string,
   customerId string,
   merged_to_loyalty_id string,
   loyalty_id_active_ind string,
   member_ind string,
   member_status_desc string,
   member_status_date date,
   member_enroll_date date,
   member_enroll_store_num int,
   member_enroll_channel_desc string,
   member_enroll_region_desc string,
   member_enroll_country_code string,
   cardmember_ind string,
   cardmember_status_date date,
   cardmember_enroll_Date date,
   member_migration_ind string,
   rewards_level string,
   rewards_level_start_date date,
   rewards_level_end_date date,
   sys_time bigint,
   dw_sys_load_tmstp timestamp,
   loyalty_points_balance int
)
USING DELTA
LOCATION 's3://tf-nap-{env}-mkt-objectmodel-landing/event_consumer/delta/loyaltymember_wrk_tbl/';