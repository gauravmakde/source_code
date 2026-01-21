/*

T2/Table Name: T2DL_DAS_DIGENG.usl_digital_funnel
Team/Owner: Customer Analytics/ Rathin Deshpande
Date Created/Modified: 06/23/2023

Note:
usl_digital_funnel does common transformations and outputs basic metrics and dimensions at the acp_id level
this table rolls them up to a daily grain.  
*/
SET QUERY_BAND = 'App_ID=APP08742;
     DAG_ID=usl_digital_funnel_11521_ACE_ENG;
     Task_Name=ddl_usl_digital_funnel;'
     FOR SESSION VOLATILE;




--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'usl_digital_funnel', OUT_RETURN_MSG);	

create multiset table {usl_t2_schema}.usl_digital_funnel
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
 channel  varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
,experience varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
,channelcountry varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
,acp_id   varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC 
,inception_session_id varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
,new_recognized varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,session_start TIMESTAMP
,landing_page varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,exit_page varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,mrkt_type varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,finance_rollup varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,finance_detail varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,session_count bigint
,acp_count bigint
,unique_cust_count bigint
,bounced_sessions bigint
,total_pages_visited     bigint
,total_unique_pages_visited bigint
--,hp_sessions bigint
,hp_engaged_sessions     bigint
,hp_views bigint
,pdp_sessions bigint
,pdp_engaged_sessions bigint
,pdp_views bigint
,browse_sessions    bigint
,browse_engaged_sessions bigint
,browse_views bigint
,search_sessions    bigint
,search_engaged_sessions bigint
,search_views bigint
,sbn_sessions bigint
,sbn_engaged_sessions bigint
,sbn_views bigint
,wishlist_sessions bigint
,wishlist_engaged_sessions bigint
,wishlist_views bigint
,checkout_sessions bigint
,checkout_engaged_sessions bigint
,checkout_views bigint
,shopb_sessions bigint
,shopb_engaged_sessions bigint
,shopb_views   bigint
,atb_sessions bigint
,atb_views bigint
,atb_events bigint
,ordered_sessions bigint
,sales_qty bigint
,web_orders bigint
,web_demand_usd     decimal(38,6)
,removed_from_sb bigint
,atw bigint
,remove_from_wl bigint
,atb_from_wl bigint
,filters_used bigint
,reviews_eng bigint
,content_eng bigint
,reccommendations bigint
,saved_for_later bigint
,sfl_to_atb bigint
,sfl_removed bigint
,dw_batch_date date not null
,dw_sys_load_tmstp  TIMESTAMP NOT NULL
,activity_date_partition date

)PRIMARY INDEX(activity_date_partition, channelcountry, channel, experience, acp_id, inception_session_id)
partition by range_n(activity_date_partition BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE);
-- Table Comment (STANDARD)
COMMENT ON  {usl_t2_schema}.usl_digital_funnel IS 'Aggregated table on ACP_ID grain built off of speed_user_Attributes and speed_session_fact, on daily level';

SET QUERY_BAND = NONE FOR SESSION;
