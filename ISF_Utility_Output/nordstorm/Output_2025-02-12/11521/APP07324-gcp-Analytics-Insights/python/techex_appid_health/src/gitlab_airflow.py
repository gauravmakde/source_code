import base64
import re
from typing import Dict, List

import pandas as pd

import gitlab_api


class GitLabAirflow:
    """
    Use the GitLab API to look at files to extract Airflow DAG to AppID mapping.

    """

    def __init__(self, ACCESS_TOKEN: str):
        """
        Parameter
        ---------
        ACCESS_TOKEN: str
            To access the GitLab API, you authenticate with your Personal Access Token (PAT).
            You can find (or create) these in GitLab in your profile under Access Tokens
            (https://git.jwn.app/-/profile/personal_access_tokens).  Note that this PAT needs
            to be created with the api and read_api boxes checked to allow access to the API.

        """
        # instantiate an object from the GitLab class found in gitlab_api.py
        self.gitlab = gitlab_api.GitLab(ACCESS_TOKEN)

        # check the API connection
        self.gitlab.check_token()

        # set the project IDs for the repos of interest.  Can be found at the top of
        # the main GitLab repo page.
        self.MLP_PROJECT_ID = "3629"  # airflow-auto-scheduler
        self.ISF_PROJECT_ID = "7319"  # APP08499-isf-airflow-dags
        self.BRANCH = "development"

        # create empty dict to collect mapping
        self.repo_from_project_id_dict = {}

    def find_dir_file(
        self, gitlab_return: List[Dict[str, str]]
    ) -> (List[Dict[str, str]], List[Dict[str, str]]):
        """
        For the repo return from the gitlab API, search through the list and fid files and
        directories.

        Parameter
        ---------
        gitlab_return: List[Dict[str, str]]

        Return
        ------
        List[str], List[str]
            list of directories and files

        """

        dir_list = []
        file_list = []

        # loop through the info returned by GitLab
        for item in gitlab_return:
            if item["type"] == "tree":
                dir_list.append(item["path"])
            elif item["type"] == "blob":
                if item["path"].endswith(".py"):
                    file_list.append(item["path"])

        return dir_list, file_list

    def find_all_files(
        self, gitlab_return: List[Dict[str, str]], project_id: str
    ) -> List[str]:
        """
        To be called recursively until all files are found.  The idea is to start at a top level
        directory looking for files and go down until no more sub-directories are found.

        Parameters
        ----------
        gitlab_return: List[Dict[str, str]]
            The list that the GitLab API returns with the content of the directory
        project_id: str
            The GitLab project ID to search

        Returns
        -------
        List[str]
            A list of file names

        """

        dir_list, file_list = self.find_dir_file(gitlab_return)

        # there are only files, no directories
        if len(dir_list) == 0:
            return file_list

        file_list_total = file_list

        for this_dir in dir_list:
            endpoint = f"https://git.jwn.app/api/v4/projects/{project_id}/repository/tree?path={this_dir}"
            gitlab_return_1 = self.gitlab.send_endpoint(endpoint)

            file_list = self.find_all_files(gitlab_return_1, project_id)

            python_file_list = file_list

            file_list_total += python_file_list

        return file_list_total

    def find_all_isf_files(self) -> pd.DataFrame:
        """
        Search the mlp repo for the python files

        Returns
        -------
        pd.DataFrame
            df with columns of the keys in the file_info dictionary
        """

        endpoint = (
            f"https://git.jwn.app/api/v4/projects/{self.ISF_PROJECT_ID}/repository/tree"
        )
        airflow_return = self.gitlab.send_endpoint(endpoint)

        repo_list = []

        for project_dir in airflow_return:
            project_id_path = project_dir["path"]
            project_id = project_dir["name"]

            # check for digits in directory name (i.e. real project_id)
            PROJECT_ID_PATTERN = re.compile("\d*")
            if re.search(PROJECT_ID_PATTERN, project_id):
                endpoint = f"https://git.jwn.app/api/v4/projects/{self.ISF_PROJECT_ID}/repository/tree?path={project_id}"

                project_team_return = self.gitlab.send_endpoint(endpoint)

                for project_team in project_team_return:
                    project_team_path = project_team["path"]
                    endpoint = f"https://git.jwn.app/api/v4/projects/{self.ISF_PROJECT_ID}/repository/tree?path={project_team_path}"

                    team_projects = self.gitlab.send_endpoint(endpoint)

                    for project in team_projects:
                        dag_files = project["path"] + "/dags"
                        endpoint = f"https://git.jwn.app/api/v4/projects/{self.ISF_PROJECT_ID}/repository/tree?path={dag_files}"

                        project_dags = self.gitlab.send_endpoint(endpoint)

                        # this is the list of info on the .py files
                        for dags in project_dags:
                            dag_name = dags["name"][0:-3]

                            repo_name, app_id = self.repo_from_project_id(project_id)

                            APP_ID_PATTERN = re.compile("APP\d{5}")
                            if re.search(APP_ID_PATTERN, app_id):
                                file_info = {
                                    "job_type": "IsF",
                                    "gitlab_repo": repo_name,
                                    "file_name": dags["path"],
                                    "dag_name": dag_name,
                                    "app_id": app_id,
                                    "dag_id": dag_name,
                                }

                                repo_list.append(file_info)

        return pd.DataFrame(repo_list)

    def find_mlp_job(self) -> pd.DataFrame:
        """
        Find all the python jobs is the repo

        Returns
        -------
        pd.DataFrame
            df with columns equal to the keys in the file_info dict

        """
        endpoint = (
            f"https://git.jwn.app/api/v4/projects/{self.MLP_PROJECT_ID}/repository/tree"
        )
        airflow_return = self.gitlab.send_endpoint(endpoint)

        file_list = self.find_all_files(airflow_return, self.MLP_PROJECT_ID)

        repo_list = []
        for file_name in file_list:
            clean_file_name = (file_name).replace("/", "%2F")
            endpoint = f"https://git.jwn.app/api/v4/projects/{self.MLP_PROJECT_ID}/repository/files/{clean_file_name}?ref={self.BRANCH}"
            file_info = self.gitlab.send_endpoint(endpoint)
            content = str(base64.b64decode(file_info["content"]))

            # find the repo name in content
            KEY_STRING = "project_path"
            start_loc = content.find(KEY_STRING) + 29
            end_loc = content[start_loc:].find("}") + start_loc - 2
            repo_name = content[start_loc:end_loc]

            # find the app_id in the repo name
            app_id = repo_name[:8].upper()

            APP_ID_PATTERN = re.compile("APP\d{5}")
            if repo_name and re.search(APP_ID_PATTERN, app_id):
                # find the dag_id from the file name
                dag_id = file_name[:-3].split("/")[-1]
                directory = file_name[0 : -len(dag_id) - 4]

                # if a repo name was found, add to list
                file_info = {
                    "job_type": "MLP",
                    "gitlab_repo": repo_name,
                    "file_name": file_name,
                    "dag_name": dag_id,
                    "app_id": app_id,
                    "dag_id": dag_id,
                }

                repo_list.append(file_info)

        return pd.DataFrame(repo_list)

    def repo_from_project_id(self, project_id: str) -> (str, str):
        """
        Given a project ID, find the repo name (and app_id) using the GitLab API

        Parameter
        ---------
        project_id: str
            The GitLab project ID

        Returns
        -------
        repo_name: str
            The name of the repo
        app_id: str
            The app_id extracted from the repo name (repos start with the AppID)
        """

        if project_id in self.repo_from_project_id_dict:
            # have info stored, so don't need to look up
            return self.repo_from_project_id_dict[project_id]

        else:
            endpoint = f"https://git.jwn.app/api/v4/projects/{project_id}"
            project_info = self.gitlab.send_endpoint(endpoint)

            repo_name = project_info["name"]

            # find the app_id in the repo name
            app_id = repo_name[:8].upper()

            # store info for next time it's requested
            self.repo_from_project_id_dict[project_id] = (repo_name, app_id)

            return (repo_name, app_id)

    def find_isf_jobs(self) -> pd.DataFrame:
        """
        Find the IsF Airflow jobs (non-AE jobs) from python scripts

        Return
        ------
        pd.DataFrame
            df with columns from the keys of the file_info dict
        """

        endpoint = (
            f"https://git.jwn.app/api/v4/projects/{self.ISF_PROJECT_ID}/repository/tree"
        )
        airflow_return = self.gitlab.send_endpoint(endpoint)

        file_list = self.find_all_files(airflow_return, self.ISF_PROJECT_ID)

        repo_list = []
        for file_name in file_list[0:10]:
            file_name_split = file_name.split("/")
            project_id = file_name_split[0]
            dag_id = file_name_split[-1][0:-3]
            project_team = file_name_split[1]
            project_name = file_name_split[2]

            repo_name, app_idd = self.repo_from_project_id(project_id)

            file_info = {
                "job_type": "IsF",
                "gitlab_repo": repo_name,
                "file_name": file_name,
                "dag_name": dag_id,
                "app_id": app_id,
                "dag_id": dag_id,
            }

            repo_list.append(file_info)

        return pd.DataFrame(repo_list)
