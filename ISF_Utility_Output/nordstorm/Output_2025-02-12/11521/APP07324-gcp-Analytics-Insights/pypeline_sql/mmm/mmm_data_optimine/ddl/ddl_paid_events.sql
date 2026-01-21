SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=paid_events_11521_ACE_ENG;
     Task_Name=ddl_paid_events;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.paid_events
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'paid_events', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.paid_events
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Quarter	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Event_Name	VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
Event_Type	VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
Event_Level	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
National_Event_Lead	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Regional_Event_Lead	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Region	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Store	VARCHAR(350) CHARACTER SET UNICODE NOT CASESPECIFIC,
Start_Date	DATE FORMAT 'YY/MM/DD',
End_Date	DATE FORMAT 'YY/MM/DD',
Event_Time	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Event_Division	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Department	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Acq_or_Ret	VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
Music	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
InStore_Volume_Goal	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
InStore_Volume_Actual	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
InStore_Attendance_Goal	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
InStore_Attendance_Actual	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
VirtualNlive_RSVP_Goal	FLOAT,
Virtual_Nlive_Event_RSVP_Actual	FLOAT,
VirtualNLive_Event_Attendance_Goal	FLOAT,
VirtualNLive_Event_Attendance_Actual FLOAT,
FB_Attendance	FLOAT,
Nlive_Replays	FLOAT,
social_media_support	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Social_Media_Posting_Date	FLOAT,
Event_Budget	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Total_Cost_Actual	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Event_National_Lead	VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
Event_Funding_Source	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Comments	VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
Sales_Channel 	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Funnel_Type	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Funding_type	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,	    
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Quarter,Event_Name,Event_Type,Event_Level,National_Event_Lead,Regional_Event_Lead,Region,Store,
Event_Time,Event_Division,Department,Acq_or_Ret,Music,
InStore_Volume_Goal,InStore_Volume_Actual,InStore_Attendance_Goal,InStore_Attendance_Actual,Event_Budget
)
;

COMMENT ON  {mmm_t2_schema}.paid_events IS 'paid_events Data for MMM Data Consolidation';


SET QUERY_BAND = NONE FOR SESSION;
























