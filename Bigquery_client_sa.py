from google.cloud import bigquery
import json
import sys

service_account_file ='service_account/SA_new_cf_nordstrom.json'
clientBQ = bigquery.Client.from_service_account_json(service_account_file)
# client=bigquery.Client()
completeFileContent="""
/*
select workbook_id,workbook_name,view_id,view_name,datasource_id,datasource_name,datasource_type,serveraddress,username,status from  `cf-nordstrom.BI_dataset.master_table`
where workbook_name = 'Anniversary Retro 2023 Customer Sandbox'
and status="Ready To Migrate"
*/
DELETE FROM cf-nordstrom.T2DL_DAS_SESSIONS.dior_session_fact
WHERE activity_date_pacific >= '2023-01-01' AND activity_date_pacific <= '2023-01-01';
--start_date , end_date
"""
# query_results=client.query(sql)
# results=query_results.result()
# print(results)
job = clientBQ.query(completeFileContent)
try:
    if job.result():
        print("It has run")
except :
    print("Unexpected error: {},Description: {},Line No: {}\n\n\n".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno))

    # log_result.append(str(log_datetime) + " " + "Final_Result=Y\n\n")
metricValuePosition= 2
# for row in results:
#     workbook_id=row["workbook_id"]
#     workbook_name = row["workbook_name"]
#     datasource_id = row["datasource_id"]
#     datasource_name = row["datasource_name"]
#     status = row["status"]
#
#     print(f"Workbook is {workbook_name}, it has workbook_id {workbook_id}")

