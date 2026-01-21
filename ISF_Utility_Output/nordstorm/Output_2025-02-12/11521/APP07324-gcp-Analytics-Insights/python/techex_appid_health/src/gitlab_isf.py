import base64
import json
import re
from typing import Dict, List

import pandas as pd

import gitlab_api


class GitLabIsF:
    def __init__(self, isf_gitlab_repos: Dict[str, List[str]], ACCESS_TOKEN: str):
        """

        Parameters
        ----------
        isf_gitlab_repos: Dict[List[str]]
            key = name of GitLab repo
            values = List[project_id, branch, owner code]
            For Example
                key = APP07324-Analytics Insights
                values = ['11521', 'production', 'ACE_ENG']

        ACCESS_TOKEN: str
            personal access token for GitLab

        """
        self.isf_gitlab_repos = isf_gitlab_repos

        self.gitlab = gitlab_api.GitLab(ACCESS_TOKEN)

        # check the API connection
        self.gitlab.check_token()

    def find_pypeline_jobs(self, project_id: str) -> List[Dict[str, str]]:
        """
        find the files in the pypeline_jobs directory

        Parameters
        ----------
        projec_id: str
            project ID for the gitlab repo

        Return
        ------
        List[Dict[str, str]]
            list of the jobs in pypeline_jobs
        """
        endpoint = f"https://git.jwn.app/api/v4/projects/{project_id}/repository/tree?path=pypeline_jobs"

        pypeline_jobs = self.gitlab.send_endpoint(endpoint)

        self.pypeline_jobs = pypeline_jobs

        return pypeline_jobs

    def find_sql_script_names(self, content: Dict[str, str]) -> List[str]:
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
            if stage_name in content:
                for job_name, job_val in content[stage_name].items():
                    if "teradata_engine" in job_val.keys():
                        for t_job in job_val["teradata_engine"]["input_details"]:
                            for t_job_script in t_job["scripts"]:
                                sql_script_files.append(t_job_script)
                    if "spark_engine" in job_val.keys():
                        if "scripts" in job_val["spark_engine"]:
                            for t_job in job_val["spark_engine"]["scripts"]:
                                sql_script_files.append(t_job)
                        for t_job in job_val["spark_engine"]["input_details"]:
                            if "scripts" in t_job:
                                for t_job_script in t_job["scripts"]:
                                    sql_script_files.append(t_job_script)

        return sql_script_files

    def find_queryband_from_sql(
        self, sql_file_name: str, project_id: str, branch: str
    ) -> (str, str, str, str):
        """
        Find the content of the sql file, find the query band and its content.

        Parameters
        ----------
        sql_file_name: str
            The name of the sql file name including the leading directories
            for example: "ace/techex/techex_pd_incidents.sql"
        project_id: str
            the GitLab project ID.  Found under the name on the front page of the repo
        branch: str
            The GitLab branch in the repo

        Returns
        -------
        app_id: str
        dag_id: str
        query_band_content: str
        sql_file_name: str

        """
        sql_file_name_1 = ("pypeline_sql/" + sql_file_name).replace("/", "%2F")
        endpoint = f"https://git.jwn.app/api/v4/projects/{project_id}/repository/files/{sql_file_name_1}?ref={branch}"
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

    def find_pd_app_id_in_job(self, content: str) -> str:
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

    def search_pypeline_jobs(
        self, gitlab_repo: str, project_id: str, branch: str, owner_id: str
    ) -> pd.DataFrame:
        """
        Search pypeline_jobs json files in the AE repo for sql file names and pager duty email.
        Then extract appid from this information.

        Parameters
        ----------
        gitlab_repo: str
            name of the repo
        project_id: str
            the GitLab project ID.  Found under the name on the front page of the repo
        branch: str
            the branch in the repo
        owner_id: str
            Owner ID that is attached to the end of the Dag ID

        Returns
        -------
        app_id_df: pd.DataFrame
            columns:
                job_type
                gitlab_repo
                file_name
                dag_name
                app_id
                dag_id

        """

        # find pypeline_jobs
        pypeline_jobs = self.find_pypeline_jobs(project_id)

        # initialize the list of outputs for each job
        job_info_list = []

        # iterate through jobs finding information on each job
        count = 0
        for job in pypeline_jobs:
            job_info_dict = {}
            file_name = job["path"]

            print(f"{count}: project_id={project_id}, file_name={file_name}")
            count += 1

            job_info_dict["file_name"] = file_name[len("pypeline_jobs/") :]
            file_name_1 = file_name.replace("/", "%2F")
            endpoint = f"https://git.jwn.app/api/v4/projects/{project_id}/repository/files/{file_name_1}?ref={branch}"
            file_info = self.gitlab.send_endpoint(endpoint)
            if "message" in file_info:
                print(file_info["message"])
            try:
                content = json.loads(base64.b64decode(file_info["content"]))
            except KeyError:
                print("No content in this file.  Skipping!")
                continue

            # search content for pager duty email and app_id from this
            pd_app_id = self.find_pd_app_id_in_job(content)
            job_info_dict["email_app_id"] = pd_app_id

            # get the dag_id from content
            if "dag_id" in content:
                job_info_dict["dag_id"] = content["dag_id"]
            else:
                job_info_dict["dag_id"] = None

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
                ) = self.find_queryband_from_sql(sql_script, project_id, branch)
                sql_info.append((sql_app_id, dag_id, query_band_content, sql_file_name))
            job_info_dict["sql_info"] = sql_info

            job_info_list += [job_info_dict]

        job_info_df = pd.DataFrame(job_info_list)

        self.job_info_df = job_info_df

        # extract just app_id from the job info
        app_id_list = []
        for index, job_info in job_info_df.iterrows():
            # start creating dict of this job's info
            this_job_app_id = {}
            this_job_app_id["job_type"] = "AE IsF"
            this_job_app_id["gitlab_repo"] = gitlab_repo
            this_job_app_id["file_name"] = job_info["file_name"]
            # this_job_app_id["pd_app_id"] = job_info["email_app_id"]
            this_job_app_id["dag_name"] = job_info["dag_id"]

            # now look at the querybands in the sql files
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

            # # create new columns for every sql file
            # i = 0
            # for app_id in queryband_app_id_list:
            #     this_job_app_id[f"queryband_app_id_{i}"] = app_id
            #     i += 1

            # # find unique queryband dag_id
            # queryband_dag_id_list = list(set(queryband_dag_id))
            # if None in queryband_dag_id_list:
            #     queryband_dag_id_list.remove(None)

            # i = 0
            # for dag_id in queryband_dag_id_list:
            #     this_job_app_id[f"queryband_dag_id_{i}"] = dag_id
            #     i += 1

            # What is the app_id?
            app_id = job_info["email_app_id"]

            # if no app_id from PD email, look in query bands
            # but don't take APP07324 because this is the default
            if app_id is None:
                # drop thedefault APP07324
                if "APP07324" in queryband_app_id_list:
                    queryband_app_id_list.remove("APP07324")
                if len(queryband_app_id_list) > 0:
                    app_id = queryband_app_id_list[0]

            this_job_app_id["app_id"] = app_id

            # add this to dict
            app_id_list += [this_job_app_id]

        # create a df from the info that has been collected in a list of dicts
        app_id_df = pd.DataFrame(app_id_list)

        # add a column for the dag_name

        app_id_df["dag_id"] = app_id_df["dag_name"].apply(
            lambda x: x + f"_{project_id}_{owner_id}" if x else None
        )

        self.app_id_df = app_id_df

        return app_id_df

    def loop_repos_for_jobs(self) -> pd.DataFrame:
        """
        loop through the repos listed in isf_gitlab_repos

        Return
        ------
        pd.DataFrame
            columns:
                job_type
                gitlab_repo
                file_name
                dag_name
                app_id
                dag_id
        """

        jobs_df = pd.DataFrame()
        for gitlab_repo, values in self.isf_gitlab_repos.items():
            project_id = values[0]
            branch = values[1]
            owner_code = values[2]

            appid_airflow = self.search_pypeline_jobs(
                gitlab_repo, project_id, branch, owner_code
            )

            jobs_df = pd.concat([jobs_df, appid_airflow])

        return jobs_df

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
