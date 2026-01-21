import os
import time
import xml.etree.ElementTree as et

import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

# tree = et.parse("C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-01//XML_read_input_file.xml")
file_xml = et.parse(
    "C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-02//XML_input_file.xml")
df = pd.read_csv("reports 2024-02-02\Workflow_ details_overall_result.csv")

# df = df.head(1)
print(df)
root = file_xml.getroot()

cont = 1

final_dataframe_object = []
for input in df.iterrows():
    input_file = []
    search_wf = input[1]['Workflows']
    WFFolder = input[1]['WFFolder']
    root = file_xml.getroot()
    # search_wf="wf_MT_CM_ADJUSTMENT"

    incond_result = []
    outcond_result = []
    job_name_result = []

    comment=""

    for get_job in root.findall('SMART_FOLDER/JOB'):

        cmd_line = get_job.get('CMDLINE')
        job_name = get_job.get('JOBNAME')
        inside_incond = []
        outcond_incond = []
        list_job_name = []

        if str(search_wf).lower() in str(cmd_line).lower():
            comment="WF has found inside it"
            print("Inside this")

            job_name = job_name
            list_job_name.append(job_name)
            job_name_result.append(list_job_name)
            print("search_wf",search_wf )
            print("JOB NAME: ", job_name)

            incond_list = []

            for inner_incond in get_job.findall("INCOND"):
                if (len(inner_incond.attrib)>0):
                    # print(inner_incond.attrib['NAME'])
                    incond_list.append(inner_incond.attrib['NAME'])
            incond_result.append(incond_list)
            print("incond_result", incond_result)

            inside_outcond = []
            for outcond_list in get_job.findall("OUTCOND"):
                if (len(outcond_list.attrib) > 0):
                    # print(outcond_list.attrib['NAME'])
                    inside_outcond.append(outcond_list.attrib['NAME'])
            outcond_result.append(inside_outcond)

            print("outcond_result", outcond_result)

    if comment =="":
        print("Not inside ")
        for get_job in root.findall('SMART_FOLDER/SUB_FOLDER/JOB'):
            cmd_line = get_job.get('CMDLINE')
            job_name = get_job.get('JOBNAME')
            inside_incond = []
            outcond_incond = []
            list_job_name = []
            print(get_job)
            if search_wf.lower() in str(cmd_line).lower():
                comment = "WF has found inside it"
                print("Inside this")

                job_name = job_name
                list_job_name.append(job_name)
                job_name_result.append(list_job_name)
                print("search_wf", search_wf)
                print("JOB NAME: ", job_name)

                incond_list = []

                for inner_incond in get_job.findall("INCOND"):
                    if (len(inner_incond.attrib) > 0):
                        # print(inner_incond.attrib['NAME'])
                        incond_list.append(inner_incond.attrib['NAME'])
                incond_result.append(incond_list)
                print("incond_result", incond_result)

                inside_outcond = []
                for outcond_list in get_job.findall("OUTCOND"):
                    if (len(outcond_list.attrib) > 0):
                        # print(outcond_list.attrib['NAME'])
                        inside_outcond.append(outcond_list.attrib['NAME'])
                outcond_result.append(inside_outcond)

                print("outcond_result", outcond_result)

    if comment =="":
        print("Not inside ")
        for get_job in root.findall('SMART_FOLDER/SUB_FOLDER/SUB_FOLDER/JOB'):
            cmd_line = get_job.get('CMDLINE')
            job_name = get_job.get('JOBNAME')
            inside_incond = []
            outcond_incond = []
            list_job_name = []
            print(get_job)
            if search_wf.lower() in str(cmd_line).lower():
                comment = "WF has found inside it"
                print("Inside this")

                job_name = job_name
                list_job_name.append(job_name)
                job_name_result.append(list_job_name)
                print("search_wf", search_wf)
                print("JOB NAME: ", job_name)

                incond_list = []

                for inner_incond in get_job.findall("INCOND"):
                    if (len(inner_incond.attrib) > 0):
                        # print(inner_incond.attrib['NAME'])
                        incond_list.append(inner_incond.attrib['NAME'])
                incond_result.append(incond_list)
                print("incond_result", incond_result)

                inside_outcond = []
                for outcond_list in get_job.findall("OUTCOND"):
                    if (len(outcond_list.attrib) > 0):
                        # print(outcond_list.attrib['NAME'])
                        inside_outcond.append(outcond_list.attrib['NAME'])
                outcond_result.append(inside_outcond)

                print("outcond_result", outcond_result)




    incond_result = str(incond_result)
    outcond_result = str(outcond_result)
    job_name_result = str(job_name_result)

    print(search_wf)
    input_file.append(WFFolder)
    input_file.append(search_wf)
    input_file.append(job_name_result.replace("[", "").replace("]", ""))
    input_file.append(incond_result.replace("[", "").replace("]", ""))
    input_file.append(outcond_result.replace("[", "").replace("]", ""))

    final_dataframe_object.append(input_file)
    print(input_file)

# exit()
final_dataframe = pd.DataFrame(final_dataframe_object)
final_dataframe.columns = ['WFFolder', 'worflow', 'job_name', 'incond_result', 'outcond_result']
final_dataframe.to_csv(directory + "\Final_workflow_result_" + timestr + ".csv")
