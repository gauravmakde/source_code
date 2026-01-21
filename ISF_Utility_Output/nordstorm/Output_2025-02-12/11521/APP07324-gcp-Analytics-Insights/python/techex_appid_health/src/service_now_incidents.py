import datetime as dt
import os
from typing import Dict, List

import pandas as pd
import pymssql

import util


class SNIncidents:
    """
    Collect the Service Now incidents from support groups supported by given individuals.
    """

    def __init__(
        self,
        SERVICE_ACCOUNT: str,
        PASSWORD: str,
        support_group: Dict[str, List[str]] = None,
    ):
        """

        Parameters
        ----------
        SERVICE_ACCOUNT: str
            individual at Nordstrom's LanID or a Service Account
        PASSWORD: str
            The active password from the SERVICE_ACCOUNT
        support_group: Dict[str, List[str]]
            dictionary with keys
                managers
                directors
                vps
            and values of lists of the names of individuals.
            These names MUST match the names in ServiceNow groups.
            Comes from params.yml file.
        """
        # Secrets - needs to be re-configured later
        self.SERVICE_ACCOUNT = SERVICE_ACCOUNT
        self.PASSWORD = PASSWORD

        # determine if env is 'production' or 'development'
        self.env = os.environ.get("ENV")
        if self.env is None:
            self.env = "production"

        if support_group is None:
            print("must pass a support_group dictionary")
        else:
            self.support_group = support_group

        # initiate the connection to the datamart
        self.create_connection()

        # get the support groups that the incidents will come from
        self.get_support_groups()

        # set the columns that we want for an incident information
        self.incident_col_names = [
            "u_appid",
            "number",
            "short_description",
            "description",
            "category",
            "assignment_group",
            "dv_assigned_to",
            "dv_caller_id",
            # "dv_incident_state",
            "priority",
            "dv_priority",
            "dv_state",
            "urgency",
            # "opened_at",
            "sys_created_on",  # note that times are in UTC
            "sys_created_by",
            "resolved_at",
            "time_worked",
            "close_notes",
            "comments_and_work_notes",
            "work_notes_list",
        ]

    def create_connection(self):
        """
        Create a SQL Server connection to the ServiceNow DataMart database
        """
        # connect to the database!
        conn = pymssql.connect(
            host="sndm.NORDSTROM.NET",
            user=f"NORD\\{self.SERVICE_ACCOUNT}",
            password=self.PASSWORD,
            database="ServiceNow",
        )

        self.cursor = conn.cursor()

    def execute_query(self, query: str, col_names):
        """
        Execute a SQL query and return output as a dataframe.

        Parameter
        ---------
        query: str
            text SQL query
        col_names: List[str]
            List of column names to pull in the SQL query.

        Return
        ------
        pd.DataFrame
        """
        self.cursor.execute(query)
        data = self.cursor.fetchall()

        # check for no data returned
        if len(data) > 0:
            # attach the column names
            data_df = pd.DataFrame(data).set_axis(col_names, axis=1)
        else:
            data_df = pd.DataFrame()

        return data_df

    def get_support_groups(self):
        """
        Get a list of support groups that meet the parameters
        """

        table = "sys_user_group"
        col_names = (
            "name",
            "manager",
            "sys_id",
            "dv_u_director",
            "dv_manager",
            "dv_u_vp",
            "u_vp",
            "x_pd_integration_pagerduty_escalation",
            "x_pd_integration_pagerduty_service",
            "x_pd_integration_pagerduty_team",
            "u_team_id",
        )
        col_names_str = ", ".join(col_names)
        #         filter_col = 'dv_u_vp'
        #         filter_list = "('Jason Keinath')"

        vp_filter_list = "('" + "', '".join(self.support_group["vps"]) + "')"
        dir_filter_list = "('" + "', '".join(self.support_group["directors"]) + "')"
        mgr_filter_list = "('" + "', '".join(self.support_group["managers"]) + "')"

        query = f"""
            SELECT {col_names_str}
            FROM {table}
            WHERE dv_u_vp in {vp_filter_list}
            OR dv_u_director in {dir_filter_list}
            OR dv_manager in {mgr_filter_list};
        """

        self.sys_user_group_df = self.execute_query(query, col_names)

    def get_incidents(
        self,
        start_date: dt.datetime = dt.datetime(2020, 1, 1, 1),
        end_date: dt.datetime = dt.datetime.now(),
    ):
        """
        Get incidents

        Join on sys_user_group to get actualy name of assignment group
        and on u_applications to get actual name of AppID.

        Parameters
        ----------
        start_date: dt.datetime
            When to start looking for incidents.  Defaults to 1-1-2020 if nothing passed.
        end_date: dt.datetime
            When to end looking for incidents.  Defaults to today if nothing passed.
        """

        # get the string value of the dates for the SQL query
        start_date_str = start_date.strftime("%Y-%m-%d %H:%M")
        end_date_str = end_date.strftime("%Y-%m-%d %H:%M")

        table = "incident"

        # create the string for the query with the column names
        col_names_str = f"{table}." + f", {table}.".join(self.incident_col_names)

        # create the filter strings for the query
        filter_col = f"{table}.assignment_group"
        filter_list = "('" + "', '".join(list(self.sys_user_group_df["sys_id"])) + "')"

        # put it all together into a SQL query
        # NOTE - comment out filter on groups -> get ALL the incidents
        query = f"""
            SELECT {col_names_str}, sys_user_group.name, u_applications.u_app_id, ci.u_app_id
            FROM {table}
            LEFT JOIN sys_user_group
            ON {table}.assignment_group = sys_user_group.sys_id
            LEFT JOIN u_applications
            ON {table}.u_appid = u_applications.sys_id
            LEFT JOIN u_applications ci
            ON {table}.cmdb_ci = ci.sys_id
        --    WHERE {filter_col} in {filter_list}
        --    AND {table}.sys_created_on BETWEEN '{start_date_str}' AND '{end_date_str}'
            WHERE {table}.sys_created_on BETWEEN '{start_date_str}' AND '{end_date_str}'
        --    AND dv_incident_state NOT IN ('Closed', 'Canceled', 'Resolved', 'On Hold')
            ;
        """

        # add the column names from the SQL join
        col_names = self.incident_col_names + [
            "assignment_group_name",
            "app_id",
            "configuration_item",
        ]

        # execute the query
        self.incidents_df = self.execute_query(query, col_names)

        # fill app_id with configuration_item if app_id is None
        self.incidents_df["app_id"] = self.incidents_df["app_id"].fillna(
            value=self.incidents_df["configuration_item"]
        )

        # remove the columns with the ids which were used to merge with other tables
        # AND remove configuration_item column since no longer needed
        self.incidents_df = self.incidents_df.drop(
            columns=["u_appid", "assignment_group", "configuration_item"]
        )

    def get_one_incident_status(self, incident_number):
        """
        Have to be able to get the status of a single incident to update in database.

        incident_number: str
            such as INC0003098693
        """

        table = "incident"

        # create the string for the query with the column names
        col_names_str = f"{table}." + f", {table}.".join(self.incident_col_names)

        # put together the SQL query
        query = f"""
            SELECT {col_names_str}, sys_user_group.name, u_applications.u_app_id
            FROM {table}
            LEFT JOIN sys_user_group
            ON {table}.assignment_group = sys_user_group.sys_id
            LEFT JOIN u_applications
            ON {table}.u_appid = u_applications.sys_id
            WHERE {table}.number = '{incident_number}'
        """

        # add the column names from the SQL join and execute the query
        col_names = self.incident_col_names + ["assignment_group_name", "app_id"]
        one_incidents_df = self.execute_query(query, col_names)

        # remove the columns with the ids which were used to merge with other tables
        one_incidents_df = one_incidents_df.drop(
            columns=["u_appid", "assignment_group"]
        )

        return one_incidents_df

    def write_sn_incidents_to_file(self, file_name):
        """
        Write to csv file

        Parameter
        ---------
        file_name: str
            the file name to write to
        """

        self.incidents_df.to_csv(file_name, index=False)

    def read_sn_incidents_from_file_and_update(self, file_name: str) -> pd.DataFrame:
        """
        Read the current csv file with incidents and update the ones that are still active.

        Parameter
        ---------
        file_name: str
            the file name to read from

        Returns
        -------
        pd.DataFrame
            updated incidents
        """

        # which incidents should be updated?
        UPDATE_STATE_LIST = ["In Progress", "On Hold"]

        # the separater for the csv file - Teradata wants a unit separator character.
        unit_separator = """"""

        col_names = self.incident_col_names + ["assignment_group_name", "app_id"]
        col_names.remove("u_appid")
        col_names.remove("assignment_group")

        # read the file that's already there
        read_sn_incidents = pd.read_csv(file_name, names=col_names, sep=unit_separator)

        # filter for the active incidents

        active_sn_incidents = read_sn_incidents[
            read_sn_incidents["dv_state"].isin(UPDATE_STATE_LIST)
        ]

        # get updated information on these incidents
        updated_sn_incidents = pd.DataFrame(columns=read_sn_incidents.columns)
        for index, row in active_sn_incidents.iterrows():
            incident_number = row["number"]

            updated_incident = self.get_one_incident_status(incident_number)

            updated_sn_incidents = pd.concat([updated_sn_incidents, updated_incident])
        updated_sn_incidents = updated_sn_incidents.set_index(active_sn_incidents.index)

        # update the incidents
        self.updated_sn_incidents = updated_sn_incidents
        self.read_sn_incidents = read_sn_incidents
        self.active_sn_incidents = active_sn_incidents

        read_sn_incidents.loc[updated_sn_incidents.index] = updated_sn_incidents

        self.incidents_df = read_sn_incidents

        # write back to file
        self.write_sn_incidents_to_file("updated_" + file_name)

        return self.incidents_df

    def clean_string(self):
        """
        remove unwanted characters from the text
        """

        # if there is a self.incidents_df
        if hasattr(self, "incidents_df"):
            COLUMNS_TO_CLEAN = [
                "short_description",
                "description",
                "close_notes",
                "comments_and_work_notes",
                "work_notes_list",
            ]

            for c in COLUMNS_TO_CLEAN:
                if c in self.incidents_df.columns:
                    self.incidents_df[c] = self.incidents_df[c].apply(util.clean_string)

        # if there is a self.applications
        if hasattr(self, "applications"):
            COLUMNS_TO_CLEAN = ["short_description", "name", "u_long_name", "dv_parent"]

            for c in COLUMNS_TO_CLEAN:
                if c in self.applications.columns:
                    self.applications[c] = self.applications[c].apply(util.clean_string)

            # replace '' in dv_parent column with None
            # self.applications[self.applications["dv_parent"] == ""] = None

    def get_applications(self) -> pd.DataFrame:
        """
        Get ALL application in NERDS.

        Return
        ------
        pd.DataFrame
            df with application info from NERDS
        """

        table = "u_applications"

        col_names = [
            "sys_id",  # id for the application
            "u_app_id",  # AppID in the string format APPxxxxx
            "name",
            # "dv_parent",
            "short_description",
            # "u_assessment",
            # "u_app_health",
            "dv_u_application_category",
            # "u_application_tier",
            "dv_u_application_tier",
            "dv_operational_status",
            "dv_support_group",
            "assignment_group",  # id for the support group
            "u_app_health",
            "sys_created_on",
            "dv_parent",
            "u_long_name",
            "u_assessment",
            "dv_change_control",
            "dv_assignment_group",
        ]

        # set up the strings needed for the SQL query
        col_names_str = f"{table}." + f", {table}.".join(col_names)

        # put together the SQL query
        query = f"""
            SELECT {col_names_str}, u.dv_manager, u.dv_u_director, u.dv_u_vp
            FROM {table}
            LEFT JOIN sys_user_group AS u
            ON {table}.support_group = u.sys_id
        """

        col_names += [
            "group_manager",
            "group_director",
            "group_vp",
        ]

        applications = self.execute_query(query, col_names)
        applications = applications.rename(columns={"u_app_id": "app_id"})

        # remove the ID columns which aren't needed in the output
        applications = applications.drop(columns=["sys_id", "assignment_group"])

        # convert health from int to string
        applications["app_health"] = applications["u_app_health"].apply(self.app_health)
        applications = applications.drop(columns="u_app_health")

        self.applications = applications

        return applications

    def app_health(self, u_app_health):
        """
        Convert the number for u_app_health to the string that is
        shown in NERDS.

        Parameter
        ---------
        u_app_health: int
            integer number for health

        Returns
        -------
        app_health: str
            the string version of healh
        """
        if u_app_health == 0:
            app_health = "Needs Review"
        elif u_app_health == 50:
            app_health = "At Risk"
        elif u_app_health == 100:
            app_health = "Healthy"
        elif u_app_health == 100000:
            app_health = "Retired"

        return app_health
