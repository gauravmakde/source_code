SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_fls_rn_nsg_wifi_event_dt_11521_ACE_ENG;
     Task_Name=ddl_fls_rn_nsg_wifi_event_dt;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.fls_rn_nsg_wifi_event_dt
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Created/Modified: 03/03/2023

Note:
-- Details on event based adjustment applied to RetailNext wifi adjustment model

*/

create multiset table {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_event_dt
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    wifi_event_date       DATE,
    dw_sys_load_tmstp     TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (wifi_event_date)
;

-- Table Comment (STANDARD)
COMMENT ON {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_event_dt IS 'Details on event based adjustment applied to RetailNext wifi adjustment model';
SET QUERY_BAND = NONE FOR SESSION;