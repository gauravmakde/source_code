import base64
import boto3
import datetime as dt
import os
import pandas as pd
import requests
import time
import util


class GetFetch:
    """
    Fetch has an API which provides information from Active Directory about an employee
    at Nordstrom.

    This program uses the Fetch API to gather this information into a format that
    can be used for analysis
    """

    def __init__(self, FETCH_NAUTH_CLIENT_ID, FETCH_NAUTH_CLIENT_SECRET):

        # initialize authentification
        self.FETCH_NAUTH_CLIENT_ID = FETCH_NAUTH_CLIENT_ID
        self.FETCH_NAUTH_CLIENT_SECRET = FETCH_NAUTH_CLIENT_SECRET
        self.get_fetch_auth()

        # Create a space to store profiles
        self.lan_id_info = {}
        self.name_info = {}
        self.manager_hierarchy = {}

        # Is this running locally or in an Airflow DAG?
        # The code needs to know where to find th path
        if os.environ.get("ENV") in ["production", "development"]:
            self.cert_path = "/home/nonroot/python/techex_appid_health/src/"
        else:
            self.cert_path = "./"

    def get_fetch_auth(self):
        """
        Get access token for the Fetch API.
        Note that the token expires after an hour, so will need to be refreshed.
        """
        # update time tracker
        self.auth_start_time = time.time()

        NAUTH_CLIENT_URL = "https://preview.enterprise-auth.vip.nordstrom.com/v1/ausscmhvy3pPz8UVW0h7/token"

        # Get Nauth access token, required to query Fetch API.
        nauth_secret_and_id = base64.b64encode(
            str.encode(f"{self.FETCH_NAUTH_CLIENT_ID}:{self.FETCH_NAUTH_CLIENT_SECRET}")
        ).decode("utf-8")
        nauth_headers = {"Authorization": f"Basic {nauth_secret_and_id}"}
        nauth_data = {"grant_type": "client_credentials"}
        nauth_creds = requests.post(
            NAUTH_CLIENT_URL, headers=nauth_headers, data=nauth_data
        )

        ACCESS_TOKEN = nauth_creds.json()["access_token"]

        self.ACCESS_TOKEN = ACCESS_TOKEN

    def check_time_elapsed(self):
        """
        The auth for the API expires in 3600 seconds.  Check if it's time to
        re-auth the API.  Use 3300 seconds to do it on the early side and not
        run right up to the limit.
        """

        if time.time() > self.auth_start_time + 3300:
            self.get_fetch_auth()

    def get_fetch(self, lan_id):
        """
        Get the info on a person from Fetch.

        Parameter
        ---------
        lan_id: str
            The 4 character LanID of the individual, such as AD12 or CDE3

        Returns
        -------
        Dict(str,str))
            Dict with info on a person with lan_id with keys
            firstName
            lastName
            lanID
            title
            manager
            email
            office
            createdTime
            directReports - List(direct report info)
            directReport_lan_id_list - List(direct report lan_id)
        """

        # make sure the lan_id is upper case
        lan_id = lan_id.upper()

        if lan_id is None:
            return {}

        # see if we have already pulled the name info from the API
        if lan_id in self.lan_id_info.keys():
            # yes, use what we have
            search_results = self.lan_id_info[lan_id]
        else:
            # check if need to re-authorize the API
            self.check_time_elapsed()

            # call the API
            FETCH_SAMPLER_ENDPOINT = "https://fetch-api-nonprod.nordstrom.net/api/v1/"
            SEARCH_TERM = "user/" + lan_id
            fetch_api_auth = {
                "authorization": f"Bearer {self.ACCESS_TOKEN}",
                "nord-client-id": "APP02404",
            }

            search_results = requests.get(
                f"{FETCH_SAMPLER_ENDPOINT}{SEARCH_TERM}",
                verify=self.cert_path + "fetch-api-root-ca.cer",
                headers=fetch_api_auth,
            )

            # store info so don't have to get from API next time (to speed things up)
            self.lan_id_info[lan_id] = search_results

        if search_results.status_code == 404:
            # call failed
            print("404 problem with", lan_id)
            results_output = {
                "firstName": None,
                "lastName": None,
                "lanID": None,
                "title": None,
                "manager": None,
                "email": None,
                "office": None,
                "createdTime": None,
                "active_account": False,
                "directReports": [],
                "directReport_lan_id_list": [],
            }
            return results_output

        search_results_json = search_results.json()

        # if you are the very bottom of the pyramid and don't have a manager....
        # Like Erik Nordstrom
        # or if for some reason there isn't an office directReports in the data
        for key in ["title", "manager", "office", "email"]:
            if key not in search_results_json.keys():
                search_results_json[key] = None
        for key in ["directReports"]:
            if key not in search_results_json.keys():
                search_results_json[key] = []

        # find a list of lan_id of direct reports
        if len(search_results_json["directReports"]) != 0:
            directReport_lan_id_list = self.lan_id_directReports(
                search_results_json["directReports"]
            )
        else:
            directReport_lan_id_list = []

        results_output = {
            "firstName": search_results_json["firstName"],
            "lastName": search_results_json["lastName"],
            "lanID": search_results_json["lanID"],
            "title": search_results_json["title"],
            "manager": search_results_json["manager"],
            "email": search_results_json["email"],
            "office": search_results_json["office"],
            "createdTime": search_results_json["createdTime"],
            "active_account": search_results_json["accountActive"],
            "directReports": search_results_json["directReports"],
            "directReport_lan_id_list": directReport_lan_id_list,
        }

        return results_output

    def confirm_person_has_given_manager(self, manager_lan_id, employee_lan_id):
        """
        Because we are given the manager's name, not the lan_id, we will
        want to confirm which manager lan_id is correct if there are multiple
        names.

        Parameters
        ----------
        manager_lan_id: str
            the lan_id of a manager
        employee_lan_id: str
            the lan_id of an employee

        Returns
        -------
        True if employee is in manager direct reports
        False if employee is NOT in manager direct reports
        """

        manager_direct_reports = self.find_dR(manager_lan_id)

        if employee_lan_id in manager_direct_reports:
            return True
        else:
            return False

    def get_fetch_name(self, name, manager_of_lan_id=None):
        """
        Get the info on a person from Fetch using their name, not a lan_id.

        Parameter
        ---------
        name: str
            the name of the person to find fetch info from
        manager_of_lan_id: str
            the lan_id of someone who is supported by name in case there are multiple
            people with the same name

        Returns
        -------
        Dict
            same output from self.get_fetch
        """
        # see if we have already pulled the name info from the API
        if name in self.name_info:
            # yes, use what we have
            search_results = self.name_info[name]
        else:
            # check if need to re-authorize the API
            self.check_time_elapsed()

            # Nope, have to get it
            FETCH_SAMPLER_ENDPOINT = "https://fetch-api-nonprod.nordstrom.net/api/v1/"

            # call the API
            SEARCH_TERM = f"search?q={name}"
            fetch_api_auth = {
                "authorization": f"Bearer {self.ACCESS_TOKEN}",
                "nord-client-id": "APP02404",
            }

            search_results = requests.get(
                f"{FETCH_SAMPLER_ENDPOINT}{SEARCH_TERM}",
                verify=self.cert_path + "fetch-api-root-ca.cer",
                headers=fetch_api_auth,
            )

        if search_results.json()["total"] == 1:
            lan_id = search_results.json()["results"][0]["item"]["lanID"]

            return self.get_fetch(lan_id)

        else:
            # find the output that are users (there may be other types)
            manager_info_list = []
            for manager_info in search_results.json()["results"]:
                if manager_info["type"] == "user":
                    manager_info_list.append(manager_info)

            # if there's only one user in the list then this is it
            if len(manager_info_list) == 1:
                lan_id = manager_info_list[0]["item"]["lanID"]
                return self.get_fetch(lan_id)

            if manager_of_lan_id:
                # there were more than 1 people with this name.
                for manager_info in manager_info_list:
                    manager_lan_id = manager_info["item"]["lanID"]
                    if self.confirm_person_has_given_manager(
                        manager_lan_id, manager_of_lan_id
                    ):
                        return self.get_fetch(manager_lan_id)
            else:
                print(
                    f"please provide lan_id of a person who works for this person as there is more than one {name}"
                )

    def get_fetch_list(self, lan_id_list):
        """
        Get the info on a bunch of lan_id in a list

        Parameters
        ----------
        lan_id_list: List(str)
            a list of LanIDs to extract info from and put in a dataframe

        Returns
        -------
        pd.DataFrame
            DataFrame with a bunch of info about the people in lan_id_list
        """

        self.get_fetch_auth()  # re-authorize the API

        all_lan_id_dict = {}
        for lan_id in lan_id_list:
            lan_id = lan_id.upper()  # just in case!
            fetch_info = self.get_fetch(lan_id)
            # if fetch_info["active_account"] is True:
            #     all_lan_id_dict[lan_id] = fetch_info

            # capture people, even if not active_account
            all_lan_id_dict[lan_id] = fetch_info

        all_lan_id_df = pd.DataFrame(all_lan_id_dict).transpose()

        # find the names of the direct reports to add to df
        all_lan_id_df["direct_reports"] = all_lan_id_df["directReports"].apply(
            lambda x: [f"{y['firstName']} {y['lastName']}" for y in x]
        )
        all_lan_id_df = all_lan_id_df.drop(columns=["directReports"])

        # find the manager hierarchy and add to df
        all_lan_id_df[["manager_hierarchy", "manager_hierarchy_lan_id"]] = (
            all_lan_id_df.apply(
                lambda x: self.find_manager_hierarchy(x["lanID"]),
                axis=1,
                result_type="expand",
            )
        )

        return all_lan_id_df

    def lan_id_directReports(self, directReports_list):
        """
        Extract just the Lan ID from the directReports output from fetch

        Input
        -----
        directReports_list List[Dict[Any]] A list of Dicts that contain info about a person's direct reports
                           Will be empty if the person does not have any direct reports

        Output
        ------
        List[str] A list of the Lan ID of the direct reports
        """
        if len(directReports_list) == 0:
            return []

        dR_lan_id_list = []
        for dR in directReports_list:
            # if (dR["title"] != "Contingent Worker") and (dR["accountActive"]):
            # if dR["accountActive"]:
            #     dR_lan_id_list.append(dR["lanId"])

            # take actine and inactive
            dR_lan_id_list.append(dR["lanId"])

        return dR_lan_id_list

    def find_dR(self, lan_id):
        """
        Return the all direct reports below this person.

        Note that this function is recursive.

        Input
        -----
        lan_id -- The lan ID of the person (str)

        Output
        ------
        List - List of this person and all the direct reports below them.
               If the person does not have any direct reports, the list returned
               will only contain the person.
        """

        directReport_lan_id_list = self.get_fetch(lan_id)["directReport_lan_id_list"]

        # Check if the 'person' has a 4 digit LanID
        # There's at least one direct report LanID that is not a person and not a 4 digit LanID
        if len(lan_id) != 4:
            return []

        if len(directReport_lan_id_list) == 0:
            # this person doesn't have any direct reports
            return [lan_id]

        # extract just the lan_id from the direct reports
        all_dR_list = []
        for lan_id_2 in directReport_lan_id_list:
            lan_id_dR = self.find_dR(lan_id_2)
            if len(lan_id_dR) != 0:
                all_dR_list += lan_id_dR

        return all_dR_list + [lan_id]

    def find_manager_hierarchy(self, lan_id):
        """
        Find the managers of this person all the way up to the top

        Parameter
        ---------
        lan_id: str
            4 digit identifier
        """

        # find the manager
        fetch_info = self.get_fetch(lan_id)
        if "manager" not in fetch_info.keys():
            return [], []

        manager = fetch_info["manager"]

        first_manager = manager

        # store instances of manager hierarchy or get from stored values
        if manager in self.manager_hierarchy.keys():
            return self.manager_hierarchy[first_manager]

        manager_hierarchy_name = []
        manager_hierarchy_lan_id = []
        while manager:
            manager_info = self.get_fetch_name(manager, lan_id)
            manager_lan_id = manager_info["lanID"]
            manager_first_name = manager_info["firstName"]
            manager_last_name = manager_info["lastName"]
            manager = manager_info["manager"]

            manager_hierarchy_name += [f"{manager_first_name} {manager_last_name}"]
            manager_hierarchy_lan_id += [manager_lan_id]
            lan_id = manager_lan_id

        self.manager_hierarchy[first_manager] = (
            manager_hierarchy_name,
            manager_hierarchy_lan_id,
        )

        return manager_hierarchy_name, manager_hierarchy_lan_id

    def find_leader_people(self, leader_lan_id):
        """
        Find everyone under Erik Nordstrom.

        Do it in bits to keep from exceeding 60 minute limit on the API key and just control things

        Returns
        -------
        List[str]
            full list of LanID for Erik Nordstrom (something over 7000 entries)
        pd.DataFrame
            df of info about these people
        """

        # find this leader's direct reports
        this_leader_dR = self.get_fetch(leader_lan_id)["directReport_lan_id_list"]

        leader_dR = []
        leader_df = self.get_fetch_list([leader_lan_id])

        for dr in this_leader_dR:
            print(f"looking for dR of {dr}")

            # find a list of their dR
            this_person_dR = self.find_dR(dr)
            leader_dR += this_person_dR
            print(
                f"found {len(this_person_dR)} more people for a total of {len(leader_dR)}"
            )

            # find all the info on this person's dR
            STEP = 200
            for i in range(0, len(this_person_dR), STEP):

                this_step_dR = this_person_dR[i : i + STEP]

                this_person_df = self.get_fetch_list(this_step_dR)

                leader_df = pd.concat([leader_df, this_person_df])

                print(
                    f"added {len(this_person_df)} more people to leader_df for {dr}",
                )
            print("\n")

        return leader_dR, leader_df

    def clean_string(self, df):
        """
        remove unwanted characters from the text

        Parameter
        ---------
        df: pd.DataFrame
            DataFrame to clean

        Return
        ------
        pd.DataFrame
            cleaned DataFrame
        """

        # which columns to run through cleaner
        COLUMNS_TO_CLEAN = [
            "first_name",
            "last_name",
        ]

        # clean these columns in the df.  Use util.clean_string to do it.
        for c in COLUMNS_TO_CLEAN:
            df[c] = df[c].apply(util.clean_string)

        return df

    def truncate_list(self, list_of_items, truncate_num):
        """
        If a list is way too long ,then truncate and make the last item "truncated at {truncate_num} items"

        Parameters
        ----------
        list_of_items: list
            the list of items to truncate
        truncate_num: int
            after how many items to truncate the list

        Returns
        -------
        List

        """

        list_of_items = list_of_items[0 : truncate_num - 1]
        list_of_items += [f"truncated at {truncate_num} items"]

        return list_of_items

    def format_for_output(self, format_df):
        """
        Cleanup the output of the df and put the columns the way we want
        them for output.

        Parameter
        ---------
        format_df: pd.DataFrame
            the input df for cleanup

        Return
        ------
        pd.DataFrame
            the output df after cleanup

        """

        # change active_account to 0/1 (bit) instead of True/False (bool) for sql
        format_df["active"] = (
            format_df["active_account"].map({True: 1, False: 0}).astype("str")
        )

        # clean up some columns
        format_df = format_df.rename(
            columns={
                "firstName": "first_name",
                "lastName": "last_name",
                "lanID": "lan_id",
                "createdTime": "created_time",
                "directReport_lan_id_list": "direct_reports_lan_id",
            }
        )

        # Truncate lists that get too long

        for item in ["direct_reports", "direct_reports_lan_id"]:
            format_df[item] = format_df[item].apply(lambda x: self.truncate_list(x, 25))

        format_df["direct_reports_name"] = format_df["direct_reports"].apply(
            lambda x: " - ".join(x)
        )
        format_df["direct_reports_lan_id"] = format_df["direct_reports_lan_id"].apply(
            lambda x: " - ".join(x)
        )
        format_df["manager_hierarchy_name"] = format_df["manager_hierarchy"].apply(
            lambda x: " - ".join(x)
        )
        format_df["manager_hierarchy_lan_id"] = format_df[
            "manager_hierarchy_lan_id"
        ].apply(lambda x: " - ".join(x))

        # clean up the order of the columns
        format_df = format_df[
            [
                "first_name",
                "last_name",
                "lan_id",
                "title",
                "manager",
                "email",
                "created_time",
                "active",
                "office",
                "direct_reports_lan_id",
                "direct_reports_name",
                "manager_hierarchy_name",
                "manager_hierarchy_lan_id",
            ]
        ]

        # drop duplicate entries
        format_df = format_df.drop_duplicates(subset=["lan_id"])

        return format_df

    def keep_older_data(self, fetch_df):
        """
        Once someone is a couple of weeks inactive the data drops out of Fetch.
        To retain the mapping of lan_id to a person and the manager/team that they
        were a part of, the we will merge with older data.

        1. read the old fetch data from S3
        2. merge with the new fetch data and drop old duplicates
        3. once a month (quarter?) filter out inactive people and put in separate df with date to save

        Parameter
        ---------
        fetch_df: pd.DataFrame
            The dataframe with the new data pulled from Fetch

        Returns
        -------
        fetch_df: pd.DataFrame
            Updated with the previously deactivated accounts
        """

        # read the current techex_pd_incidents.csv file, set the column names to existing
        S3_fetch_df = util.read_df_from_s3("fetch_info.csv")

        # if can't read the old file from S3
        if S3_fetch_df is None:
            S3_fetch_df = fetch_df

        # give the S3_fetch_df column names since the file doesn't have them
        S3_fetch_df.columns = fetch_df.columns

        # combine read from S3 from current fetch data
        S3_plus_current_fetch_df = pd.concat([S3_fetch_df, fetch_df])

        # drop the old info on an incident in favor of the most recent info
        fetch_df = S3_plus_current_fetch_df.drop_duplicates(
            subset=["lan_id"], keep="last"
        )

        # Keep an ongoing list of inactive users since these will drop out of fetch over
        # time.  If we loose this data it's gone forever.
        inactive_df = fetch_df[fetch_df["active"] == "0"]

        # determine if env is 'production' or 'development'
        bucket = util.determine_bucket()

        # Get the current date and time
        now = dt.datetime.now()

        # what's the file name prefix for the current month?
        DIR_NAME = "techex_appid_health/"
        date_prefix = DIR_NAME + "inactive_fetch_" + now.strftime("%y-%m")

        # Get the list of objects in the bucket with the specified prefix
        s3_client = boto3.client("s3")
        objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=date_prefix)

        # Check if any objects were found
        if objects.get("Contents"):
            # A file with the specified date prefix exists
            # Only need to write this file once a month since it's just backup.
            pass
        else:
            # No file with the specified date prefix exists.
            # Generate the file name with the month and day and write the file.
            file_name = f"inactive_fetch_{now.strftime('%y-%m-%d')}.csv"
            util.write_df_to_s3(inactive_df, file_name)

        return fetch_df
