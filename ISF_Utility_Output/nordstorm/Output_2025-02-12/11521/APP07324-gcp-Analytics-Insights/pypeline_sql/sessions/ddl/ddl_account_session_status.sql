SET QUERY_BAND = 'App_ID=APP08844;
     DAG_ID=ddl_account_sessions_fact_11521_ACE_ENG;
     Task_Name=ddl_account_session_status;'
     FOR SESSION VOLATILE;

/*
Account Sessions Status table
which contains the status such as account holder, cardmember , loyalty member etc
broken out by date, country, channel, experience on a session basis.
*/
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{sessions_t2_schema}', 'account_session_status', OUT_RETURN_MSG);

create multiset table {sessions_t2_schema}.account_session_status 
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    -- Dimensions
    activity_date_pacific Date
    ,session_id varchar(100)
    ,account_status varchar(20)
    ,channelcountry varchar(5)
    ,channel varchar(20)
    ,experience varchar(20) 
    )
primary index(session_id, activity_date_pacific)
partition by range_n(activity_date_pacific between '2022-01-01' and '2025-01-31' each interval '1' day, unknown)
;

-- Comments on columns
    -- Table
comment on {sessions_t2_schema}.account_session_status                                is 'A session-level table containing channel, platform and account status info.';
    -- Dimensions
comment on {sessions_t2_schema}.account_session_status.activity_date_pacific                 is 'The date when the sessions occurred, converted to Pacific time.';
comment on {sessions_t2_schema}.account_session_status.session_id                            is 'The session ID';
comment on {sessions_t2_schema}.account_session_status.account_status                        is 'The customer account status ("Guest", "Account holders", "Loyalty members", or "Cardholders").';
comment on {sessions_t2_schema}.account_session_status.channelcountry                        is 'The country where the sessions occurred.';
comment on {sessions_t2_schema}.account_session_status.channel                               is 'The channel where the sessions occurred.';
comment on {sessions_t2_schema}.account_session_status.experience                            is 'The experience where the sessions occurred.';

SET QUERY_BAND = NONE FOR SESSION;