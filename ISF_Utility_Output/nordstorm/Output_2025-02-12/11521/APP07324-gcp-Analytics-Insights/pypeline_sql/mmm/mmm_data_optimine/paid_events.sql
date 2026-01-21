SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=paid_events_11521_ACE_ENG;
     Task_Name=paid_events;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MMM.paid_events
Owner: Analytics Engineering
Modified:03/11/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {mmm_t2_schema}.paid_events
;

INSERT INTO {mmm_t2_schema}.paid_events
SELECT 
	Quarter,
Event_Name,
Event_Type,
Event_Level,
National_Event_Lead,
Regional_Event_Lead,
Region,
Store,
Start_Date,
End_Date,
Event_Time,
Event_Division,
Department,
Acq_or_Ret,
Music,
InStore_Volume_Goal,
InStore_Volume_Actual,
InStore_Attendance_Goal,
InStore_Attendance_Actual,
VirtualNlive_RSVP_Goal,
Virtual_Nlive_Event_RSVP_Actual,
VirtualNLive_Event_Attendance_Goal,
VirtualNLive_Event_Attendance_Actual,
FB_Attendance,
Nlive_Replays,
social_media_support,
Social_Media_Posting_Date,
Event_Budget,
Total_Cost_Actual,
Event_National_Lead,
Event_Funding_Source,
Comments,
Sales_Channel,
Funnel_Type,
Funding_type,	
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {mmm_t2_schema}.paid_events_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;





