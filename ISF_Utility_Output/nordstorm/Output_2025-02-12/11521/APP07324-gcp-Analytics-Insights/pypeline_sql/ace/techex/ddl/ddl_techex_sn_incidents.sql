SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_sn_incidents_11521_ACE_ENG;
     Task_Name=ddl_techex_sn_incidents;'
     FOR SESSION VOLATILE;


create multiset table {techex_t2_schema}.techex_sn_incidents,
fallback,
no before journal,
no
after
    journal,
    checksum = default,
    default mergeblockratio (
        inc_number varchar(13) CHARACTER SET UNICODE NOT CASESPECIFIC,
        short_description varchar(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
        description varchar(10000) CHARACTER SET UNICODE NOT CASESPECIFIC,
        category varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        dv_assigned_to varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        dv_caller_id varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        priority smallint COMPRESS (1, 2, 3, 4),
        dv_priority varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        dv_state varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        urgency smallint COMPRESS (1, 2, 3, 4),
        sys_created_on timestamp,
        sys_created_by varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        resolved_at timestamp,
        time_worked varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        close_notes varchar(10000) CHARACTER SET UNICODE NOT CASESPECIFIC,
        comments_and_work_notes varchar(10000) CHARACTER SET UNICODE NOT CASESPECIFIC,
        work_notes_list varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        assignment_group_name varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        app_id varchar(8),
        dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    ) PRIMARY INDEX(inc_number);

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.techex_sn_incidents IS 'Contains ServiceNow incidents.';

SET
    QUERY_BAND = NONE FOR SESSION;