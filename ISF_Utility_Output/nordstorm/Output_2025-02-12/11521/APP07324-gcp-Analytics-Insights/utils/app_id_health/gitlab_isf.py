import base64
import datetime
import json

# import os
import pandas as pd
import re

# import requests

import gitlab_api


class GitLabIsF:
    def __init__(self, PROJECT_ID, BRANCH, ACCESS_TOKEN):
        self.BRANCH = BRANCH
        self.PROJECT_ID = PROJECT_ID

        self.gitlab = gitlab_api.GitLab(ACCESS_TOKEN)

        # check the API connection
        self.gitlab.check_token()

    def find_pypeline_jobs(self):
        endpoint = f"https://git.jwn.app/api/v4/projects/{self.PROJECT_ID}/repository/tree?path=pypeline_jobs"

        pypeline_jobs = self.gitlab.send_endpoint(endpoint)

        self.pypeline_jobs = pypeline_jobs

        return pypeline_jobs

    def find_sql_script_names(self, content):
        """
        find the sql scripts in the json file

        Parameter
        ---------
        content: Dict
            dict from json script in pypeline.jobs directory

        Returns
        -------
        List[str]
            List of the sql file names referenced in the teradata_engine or spark_engine jobs
        """

        # if there aren't any stages then return empty list
        if "stages" not in content:
            return []

        # work through the stages shown in the file
        sql_script_files = []
        for stage_name in content["stages"]:
            for job_name, job_val in content[stage_name].items():
                if "teradata_engine" in job_val.keys():
                    for t_job in job_val["teradata_engine"]["input_details"]:
                        for t_job_script in t_job["scripts"]:
                            sql_script_files.append(t_job_script)
                if "spark_engine" in job_val.keys():
                    for t_job in job_val["spark_engine"]["scripts"]:
                        sql_script_files.append(t_job)
                    for t_job in job_val["spark_engine"]["input_details"]:
                        for t_job_script in t_job["scripts"]:
                            sql_script_files.append(t_job_script)

        return sql_script_files

    def find_queryband_from_sql(self, sql_file_name):
        """ """
        sql_file_name_1 = ("pypeline_sql/" + sql_file_name).replace("/", "%2F")
        endpoint = f"https://git.jwn.app/api/v4/projects/{self.PROJECT_ID}/repository/files/{sql_file_name_1}?ref={self.BRANCH}"
        file_info = self.gitlab.send_endpoint(endpoint)
        content = str(base64.b64decode(file_info["content"]))

        if "SET QUERY_BAND" not in content:
            return None, None, None, sql_file_name

        # find the query band content
        query_band_start = content.find("SET QUERY_BAND = ") + len("SET QUERY_BAND = ")
        query_band_end = content.find("FOR SESSION VOLATILE")

        query_band_content = content[query_band_start:query_band_end]

        # find the app_id
        app_id_start_char = query_band_content.find("APP")
        app_id = query_band_content[app_id_start_char : app_id_start_char + 8]

        # find the dag_id
        dag_id_start_char = query_band_content.find("DAG_ID=") + len("DAG_ID=")
        dag_id_end_char = query_band_content.find(";\\n     Task_Name")
        if dag_id_end_char == -1:
            dag_id_end_char = query_band_content.find(";\\nTask_Name")
        dag_id = query_band_content[dag_id_start_char:dag_id_end_char]

        return app_id, dag_id, query_band_content, sql_file_name

    def find_pd_app_id_in_job(self, content):
        """
        search content for pager duty email and app_id from this

        Parameter
        ---------
        content: str
            content from the job json file from the GitLab API

        Returns
        -------
        str or None:
            the AppID, for example APP12345

        """
        APP_ID_PATTERN = re.compile("APP\d{5}")

        if "prod_email_on_failure" in content:
            pd_email = content["prod_email_on_failure"]
            app_id_start_char = pd_email.find("app")
            if app_id_start_char == -1:
                pd_app_id = None
            else:
                app_string = pd_email[app_id_start_char : app_id_start_char + 8].upper()

                if re.search(APP_ID_PATTERN, app_string):
                    pd_app_id = app_string
                else:
                    pd_app_id = None
        else:
            pd_app_id = None

        return pd_app_id

    def search_pypeline_jobs(self):
        """ """

        # Check if pypeline_jobs has been created
        if hasattr(self, "pypeline_jobs"):
            pypeline_jobs = self.pypeline_jobs
        else:
            pypeline_jobs = self.find_pypeline_jobs()

        # initialize the list of outputs for each job
        job_info_list = []

        # iterate through jobs finding information on each job
        count = 0
        for job in pypeline_jobs:
            job_info_dict = {}
            file_name = job["path"]

            print(f"{count}: {file_name}")
            count += 1

            job_info_dict["job_name"] = file_name[len("pypeline_jobs/") :]
            file_name_1 = file_name.replace("/", "%2F")
            endpoint = f"https://git.jwn.app/api/v4/projects/{self.PROJECT_ID}/repository/files/{file_name_1}?ref={self.BRANCH}"
            file_info = self.gitlab.send_endpoint(endpoint)
            content = json.loads(base64.b64decode(file_info["content"]))

            # search content for pager duty email and app_id from this
            pd_app_id = self.find_pd_app_id_in_job(content)
            job_info_dict["email_app_id"] = pd_app_id

            # find the sql scripts which are referenced
            sql_script_names = self.find_sql_script_names(content)
            job_info_dict["sql_scripts"] = sql_script_names

            # search the sql files for app_id and dag_id
            sql_info = []
            for sql_script in sql_script_names:
                (
                    sql_app_id,
                    dag_id,
                    query_band_content,
                    sql_file_name,
                ) = self.find_queryband_from_sql(sql_script)
                sql_info.append((sql_app_id, dag_id, query_band_content, sql_file_name))
            job_info_dict["sql_info"] = sql_info

            job_info_list += [job_info_dict]

        job_info_df = pd.DataFrame(job_info_list)

        self.job_info_df = job_info_df

        # extract just app_id from the job info
        app_id_list = []
        for index, job_info in job_info_df.iterrows():
            this_job_app_id = {}
            this_job_app_id["job_name"] = job_info["job_name"]
            this_job_app_id["pd_app_id"] = job_info["email_app_id"]
            queryband_app_id = []
            queryband_dag_id = []
            for sql_file in job_info["sql_info"]:
                if sql_file[0]:
                    queryband_app_id.append(sql_file[0])
                if sql_file[1]:
                    queryband_dag_id.append(sql_file[1])

            # find unique queryband app_id
            queryband_app_id_list = list(set(queryband_app_id))
            if None in queryband_app_id_list:
                queryband_app_id_list.remove(None)
            i = 0
            for app_id in queryband_app_id_list:
                this_job_app_id[f"queryband_app_id_{i}"] = app_id
                i += 1

            # find unique queryband dag_id
            queryband_dag_id_list = list(set(queryband_dag_id))
            if None in queryband_dag_id_list:
                queryband_dag_id_list.remove(None)

            i = 0
            for dag_id in queryband_dag_id_list:
                this_job_app_id[f"queryband_dag_id_{i}"] = dag_id
                i += 1

            # add this to dict
            app_id_list += [this_job_app_id]

        app_id_df = pd.DataFrame(app_id_list)

        self.app_id_df = app_id_df

        return app_id_df

    def write_to_excel(self):
        """
        Write the app_id_df to excel for visibility
        """

        self.app_id_df.to_excel("ae_isf_app_id.xlsx")

    def create_bash_script_update_queryband(self):
        """
        Create a bash script to update the queryband in the files where the
        AppID = APP07324 (Insights Framework) to the AppID found in the
        PagerDuty email.  This script will change the value of the AppID in
        the queryband text at the top of the sql files.

        The file that's created is called change_app_id.sh.
        To execute the file, first change it to an executable by executing the command:
            chmod 777 change_app_id.sh
        To execute the file execute the command:
            bash change_app_id.sh
        """

        if not hasattr(self, "job_info_df"):
            job_info_df = self.search_pypeline_jobs()

        # open a text file
        with open("change_app_id.sh", "w") as f:
            f.write("#!/bin/bash \n")
            f.write("\n")

            for index, item in self.job_info_df.iterrows():
                pd_app_id = item["email_app_id"]
                if pd_app_id:
                    sql_file = item["sql_info"]
                    for sf in sql_file:
                        file_name = sf[3]
                        file_app_id = sf[0]
                        if file_app_id == "APP07324":
                            sed_command = (
                                f"sed -i '' 's/APP07324/{pd_app_id}/' {file_name}\n"
                            )
                            f.write(sed_command)
