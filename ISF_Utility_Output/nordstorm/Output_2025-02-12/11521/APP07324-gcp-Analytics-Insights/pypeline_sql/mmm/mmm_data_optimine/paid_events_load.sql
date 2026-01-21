create or replace temporary view paid_events_view
(
	Quarter string,
Event_Name string,
Event_Type string,
Event_Level string,
National_Event_Lead string,
Regional_Event_Lead string,
Region string,
Store string,
Start_Date string,
End_Date string,
Event_Time string,
Event_Division string,
Department string,
Acq_or_Ret string,
Music string,
InStore_Volume_Goal string,
InStore_Volume_Actual string,
InStore_Attendance_Goal string,
InStore_Attendance_Actual string,
VirtualNlive_RSVP_Goal string,
Virtual_Nlive_Event_RSVP_Actual string,
VirtualNLive_Event_Attendance_Goal string,
VirtualNLive_Event_Attendance_Actual string,
FB_Attendance string,
Nlive_Replays string,
social_media_support string,
Social_Media_Posting_Date string,
Event_Budget string,
Total_Cost_Actual string,
Event_National_Lead string,
Event_Funding_Source string,
Comments string,
Sales_Channel string,
Funnel_Type string,
Funding_type string	
   	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/Paid_events.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE paid_events_ldg_output
SELECT
	Quarter,
Event_Name,
Event_Type,
Event_Level,
National_Event_Lead,
Regional_Event_Lead,
Region,
Store,
to_date(Start_Date,"M/d/yyyy") as Start_Date,
to_date(End_Date,"M/d/yyyy") as End_Date,
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
to_date(Social_Media_Posting_Date,"M/d/yyyy") as Social_Media_Posting_Date,
Event_Budget,
Total_Cost_Actual,
Event_National_Lead,
Event_Funding_Source,
Comments,
Sales_Channel,
Funnel_Type,
Funding_type    
FROM paid_events_view
WHERE 1=1
;



