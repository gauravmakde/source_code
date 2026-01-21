import boto3
import datetime as dt
from typing import Any, Dict, List

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

import util


class PDIncidents:
    """
    Collect the PagerDuty incidents from support groups supported by given individuals.

    """

    def __init__(self, PD_TOKEN: str, pd_team_ids: List[str]):
        """

        Parameters
        ----------
        PD_TOKEN: str
            Token created on a person's profile in PD under User Settings
        pd_team_ids: List[str]
            List of team_ids to consider. Can be found on PD in the url for the team
            For example https://nordstrom.pagerduty.com/teams/P1IN8E6 has team_id=P1IN8E6
        """
        # Secrets - needs to be re-configured later
        self.PD_TOKEN = PD_TOKEN

        self.pd_team_ids = pd_team_ids

        # check the api connection to see that the code can connect
        self.check_api_connection()

    def check_api_connection(self):
        """
        Send a simple endpoint to the GitLab API to check the connection.
        """
        # endpoint to find the account abilities on PagerDuty (a simple request)
        url = f"https://nordstrom.pagerduty.com/api/v1/abilities"
        headers = {"Authorization": f"Token token={self.PD_TOKEN}"}

        response = requests.get(url, headers=headers)

        # if you don't get a 200 response (it worked!) then print a message
        if response.status_code != 200:
            print("*** There's a problem with the PagerDuty API connection. ***")

    def get_pager_duty_services(self) -> List[Dict[str, str]]:
        """
        Get a list of PagerDuty services for a PagerDuty team

        Services must include a string with 'APP' followed by the 5 digit number to be recognized as
        being associated with an AppID.

        Returns
        ----------
        List[Dict]: List of dicts of services containing the following keys
            app_id
            service_name
            service_id
        """

        url = f"https://nordstrom.pagerduty.com/api/v1/services/"
        headers = {"Authorization": f"Token token={self.PD_TOKEN}"}

        all_service_app_id = []
        for team_id in self.pd_team_ids:
            params = {"team_ids[]": team_id, "limit": "100", "offset": "0"}
            pd_services = requests.get(url, headers=headers, params=params).json()

            for service in pd_services["services"]:
                service_details = {}
                name = service["name"]
                if "APP" in name:
                    start = name.find("APP")
                    app_id = name[start : start + 8]

                    # make sure the string after APP is a number
                    if app_id[3:].isnumeric():
                        service_details["app_id"] = app_id
                        service_details["service_name"] = name
                        service_details["service_id"] = service["id"]

                        all_service_app_id.append(service_details)

        self.pd_services_df = pd.DataFrame(all_service_app_id)

        return all_service_app_id

    def extract_data_from_pd_incidents(
        self, pd_incidents_list: List[Dict[str, str]]
    ) -> pd.DataFrame:
        """
        Extract just the information we want from the dict returned by the PagerDuty API with
        information on an incident.

        Parameter
        ---------
        pd_incidents_list[dict]: list of dicts that comes from PD

        Returns
        -------
        List[Dict]: list of dicts with the following keys
            incident_number
            title
            summary
            created_at
            updated_at
            urgency
            status
            incident_id
        """

        pd_incidents_list_extract = []

        for i in pd_incidents_list:
            pd_incident_dict_extract = {}

            # print(i, "\n")

            pd_incident_dict_extract["incident_number"] = int(i["incident_number"])
            pd_incident_dict_extract["title"] = i["title"]
            pd_incident_dict_extract["summary"] = i["summary"]
            pd_incident_dict_extract["created_at"] = i["created_at"]
            pd_incident_dict_extract["updated_at"] = i["updated_at"]
            pd_incident_dict_extract["urgency"] = i["urgency"]
            pd_incident_dict_extract["status"] = i["status"]
            pd_incident_dict_extract["incident_id"] = i["id"]

            pd_incidents_list_extract.append(pd_incident_dict_extract)

        return pd_incidents_list_extract

    def get_pd_service_incidents(
        self,
        service_id: str,
        start_date: dt.datetime,
        end_date: dt.datetime,
    ) -> List[Dict]:
        """
        Given a pd_service_id, return the active incidents on this service

        Note that the API has a max return length of 100.  So to get items beyond
        100 you send a second page. This function returns as many pages as needed
        to get all the results.

        Parameter
        ---------
        service_id: for example 'P1G8ALV'
        start_date: datetime
            date to start searching for incidents - defaults to January 1, 2020
        end_date: datetime
            date to end searching for incidents - defaults to today

        Returns
        -------
        List[Dict] containing keys from self.extract_data_from_pd_incidents()
        """

        start_date_str = start_date.strftime("%Y-%m-%dT%H:%M-7")
        end_date_str = end_date.strftime("%Y-%m-%dT%H:%M-7")

        if start_date_str == end_date_str:
            return []

        url = f"https://nordstrom.pagerduty.com/api/v1/incidents/"
        headers = {"Authorization": f"Token token={self.PD_TOKEN}"}

        offset = 0
        limit = 100
        pd_incidents = []

        # The api only returns 100 items at a time and we want to capture ALL the items.
        # So loop through sets of 100 until we get them all.
        while limit == 100:
            params = {
                "service_ids[]": service_id,
                "since": start_date_str,
                "until": end_date_str,
                "limit": str(limit),
                "offset": str(offset),
            }
            search_results = requests.get(url, headers=headers, params=params)

            # pull just the incidents info we want from the return
            pd_incidents += search_results.json()["incidents"]

            # How many were returned?  Loop back around if it's 100 to see if there's more.
            limit = len(search_results.json())
            offset += 1

        # only return the info we want extracted from the incidents
        return self.extract_data_from_pd_incidents(pd_incidents)

    def get_pd_incidents(
        self,
        start_date=dt.datetime(2020, 1, 1, 1),
        end_date=dt.datetime.now(),
    ) -> pd.DataFrame:
        """
        Get the PD incidents!

        Parameters
        ----------
        start_date: datetime
            date to start searching for incidents - defaults to January 1, 2020
        end_date: datetime
            date to end searching for incidents - defaults to today

        Return
        ------
        pd.DataFrame
            dataframe with rows containing incident information
        """

        # PagerDuty will fail if these date is more than 6 months back (and appears to need some padding of a couple of days too)
        start_date = max(
            start_date,
            dt.datetime.now() - relativedelta(months=6) + relativedelta(days=4),
        )
        end_date = max(
            end_date,
            dt.datetime.now() - relativedelta(months=6) + relativedelta(days=4),
        )

        # get a list of PD services
        pd_services = self.get_pager_duty_services()

        # loop through these services pulling the incidents for each
        pd_incident_df = pd.DataFrame()
        for s in pd_services:
            pd_service_incidents = self.get_pd_service_incidents(
                s["service_id"], start_date, end_date
            )

            if len(pd_service_incidents) > 0:
                this_service_pd_incidents = pd.DataFrame(pd_service_incidents)

                # add columns for service info
                this_service_pd_incidents["app_id"] = s["app_id"]
                this_service_pd_incidents["service_name"] = s["service_name"]
                this_service_pd_incidents["service_id"] = s["service_id"]

                # pd_incident_df = pd_incident_df.append(this_service_pd_incidents)
                pd_incident_df = pd.concat([pd_incident_df, this_service_pd_incidents])

        self.pd_incidents_df = pd_incident_df

        # force incident_number to integer
        self.pd_incidents_df["incident_number"] = self.pd_incidents_df[
            "incident_number"
        ].apply(int)

        return pd_incident_df

    def get_one_incident_status(self, incident_id: str) -> pd.DataFrame:
        """
        Find the information on just one incident

        Parameter
        ---------
        incident_id: str
            such as 'Q1D5M24B75NWZP'

        Return
        ------
        pd.DataFrame
            dataframe with incident information and ONE row - ready to be added to
            a larger df
        """

        # ask API for info on a particular incident
        url = f"https://nordstrom.pagerduty.com/api/v1/incidents/{incident_id}"
        headers = {"Authorization": f"Token token={self.PD_TOKEN}"}

        search_results = requests.get(url, headers=headers)

        pd_incidents = [search_results.json()["incident"]]

        # only return the info we want extracted from the incidents
        return pd.DataFrame(self.extract_data_from_pd_incidents(pd_incidents))

    def write_pd_incidents_to_file(self, file_name: str):
        """
        Write to file with unit separator to soparate the items and record separator
        to separate the rows.

        Parameter
        ---------
        file_name: str
            the file name to write to
        """

        unit_separator = """"""

        self.pd_incidents_df.to_csv(
            file_name, index=False, sep=unit_separator, header=False
        )

    def update_pd_incident_urgency(self, incident_id: float, new_urgency: str) -> Any:
        """
        For a particular incident, update the urgency of the incident.

        Parameters
        ----------
        incident_number: float
            PD incident_number, for example 1208692.0
        new_urgency: string
            PD urgency, for example High, Medium, Low

        Returns
        -------
        json
        """

        url = f"https://nordstrom.pagerduty.com/api/v1/incidents/"
        headers = {
            "Authorization": f"Token token={self.PD_TOKEN}",
            "Content-Type": "application/json",
        }

        data = {
            "incidents": [
                {
                    "id": incident_id,
                    "type": "incident",
                    "urgency": new_urgency,
                }
            ]
        }

        response = requests.put(url, headers=headers, data=json.dumps(data))

        if response.status_code != 200:
            print("update not seccessful")
            print(f"status code: {response.reason}")

        return response.json()

    def older_incidents(self):
        """
        PagerDuty only returns 6 months of incidents.  We want to collect incidents older than 6 months
        and add to the current ones.

        This code first reads the current techex_pd_incidents.csv from S3, combines with the current 6 month
        pull and only keeps the most recent info on the incidents.

        Then, to make sure we have a more permanent record of the older incidents (because they are
        not retreivable after 6 months), this code writes a copy of this file once a month.  This file
        has the date in the name of the file.
        """

        # read the current techex_pd_incidents.csv file, set the column names to existing
        S3_pd_incidents_df = util.read_df_from_s3("pd_incidents.csv")

        # if can't read the olf file from S3
        if S3_pd_incidents_df is None:
            S3_pd_incidents_df = self.pd_incidents_df

        S3_pd_incidents_df.columns = self.pd_incidents_df.columns

        # combine read from S3 from current pull from PD
        S3_plus_current_pd_incidents_df = pd.concat(
            [S3_pd_incidents_df, self.pd_incidents_df]
        )

        # drop the old info on an incident in favor of the most recent info
        self.pd_incidents_df = S3_plus_current_pd_incidents_df.drop_duplicates(
            subset=["incident_number"], keep="last"
        )

        # determine if env is 'production' or 'development'
        bucket = util.determine_bucket()

        # Get the current date and time
        now = dt.datetime.now()

        # what's the file name prefix for the current month?
        DIR_NAME = "techex_appid_health/"
        date_prefix = DIR_NAME + now.strftime("%y-%m")

        # Get the list of objects in the bucket with the specified prefix
        s3_client = boto3.client("s3")
        objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=date_prefix)

        # Check if any objects were found
        if objects.get("Contents"):
            # A file with the specified date prefix exists
            pass
        else:
            # No file with the specified date prefix exists.
            # Generate the file name with the month and day and write the file.
            file_name = f"{now.strftime('%y-%m-%d')}_pd_incidents.csv"
            util.write_df_to_s3(self.pd_incidents_df, file_name)

    def clean_string(self):
        """
        remove unwanted characters from the text
        """

        # which columns to run through cleaner
        COLUMNS_TO_CLEAN = [
            "title",
            "summary",
        ]

        # clean these columns in the df.  Use util.clean_string to do it.
        for c in COLUMNS_TO_CLEAN:
            self.pd_incidents_df[c] = self.pd_incidents_df[c].apply(util.clean_string)

    def create_pd_incident(self, title, details):
        """
        Create a PagerDuty incident to reflect a data error.
        """

        url = f"https://nordstrom.pagerduty.com/api/v1/incidents/"
        headers = {"Authorization": f"Token token={self.PD_TOKEN}"}

        TECHEX_PD_SERVICE = "PM038MS"

        payload = {
            "incident": {
                "type": "incident",
                "title": title,
                "service": {"id": TECHEX_PD_SERVICE, "type": "service_reference"},
                "urgency": "low",
                "body": {
                    "type": "incident_body",
                    "details": details,
                },
            }
        }

        response = requests.post(url, json=payload, headers=headers)

        # print the incident information as well
        print(f"An incident was created:\n, {title}\n {details}")
