SET QUERY_BAND = '
App_ID=APP07780;
DAG_ID=merch_nap_vndr_portal_extract;
Task_Name=DEL_BKUP_DATA;'
FOR SESSION VOLATILE;

insert overwrite table bkup select 'The File has been deleted';



