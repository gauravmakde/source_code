import os
from io import StringIO, BytesIO
from typing import Dict

import boto3
import pandas as pd
import yaml

import pager_duty_incidents


def read_secrets() -> Dict[str, str]:
    """
    Get all the secrets needed to access
      - ServicNow Datamart
      - PagerDuty API
      - GitLab API

    Returns
    -------
    Dict[str]
        dict with keys
            SERVICE_ACCOUNT
            PASSWORD
            PD_TOKEN
            GITLAB_PAT
    """

    SECRETS = {}

    SECRETS["SERVICE_ACCOUNT"] = "SLOpprodace"  # service account
    SECRETS["PASSWORD"] = os.environ.get(
        "TECHEX_SN_TOKEN"
    )  # SN password for service account
    SECRETS["PD_TOKEN"] = os.environ.get("TECHEX_PD_TOKEN")  # PagerDuty Token
    SECRETS["GITLAB_PAT"] = os.environ.get(
        "TECHEX_GITLAB_PAT_TOKEN"
    )  # GitLab PAT Token

    # id and secrets to retreive Fetch token from Okta
    SECRETS["FETCH_NAUTH_CLIENT_ID"] = os.environ.get("FETCH_NAUTH_CLIENT_ID")
    SECRETS["FETCH_NAUTH_CLIENT_SECRET"] = os.environ.get("FETCH_NAUTH_CLIENT_SECRET")

    return SECRETS


def get_yaml_params() -> Dict[str, Dict]:
    """
    Read file with name 'params.yml' in current directory

    Retreive from this yaml file:
    - support_group:
        - managers
        - directors
        - vps
    - pager_duty_team_ids: the teams to consider for updates
    - isf_gitlab_repos: the repo ids to consider for IsF jobs

    Returns
    -------
    Dict
        dictionary with the different items as keys

    """

    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    params_full_path = os.path.join(parent_dir, "params.yml")

    with open(params_full_path, "r") as file:
        params = yaml.safe_load(file)

    if "pager_duty_team_ids" not in params:
        params["pager_duty_team_ids"] = None

    # extract manager, director, vp lists from params
    GROUP_LEVELS = ["managers", "directors", "vps"]
    support_group = params["support_group"]
    for group_level in GROUP_LEVELS:
        if group_level not in support_group:
            support_group[group_level] = None
        if support_group[group_level] == None:
            support_group[group_level] = []

    # extract PD Team ID from params
    pd_team_ids = params["pager_duty_team_ids"]

    # None should really be an empty list
    if pd_team_ids == None:
        pd_team_ids = []

    # extract the GitLab IsF repos from params
    isf_gitlab_repos = params["isf_gitlab_repos"]

    # None should really be an empty list
    if isf_gitlab_repos == None:
        isf_gitlab_repos = {}

    # extract Fetch leaders for finding Nordstrom employee info
    fetch_leaders = params["fetch_leaders"]

    return {
        "support_group": support_group,
        "pd_team_ids": pd_team_ids,
        "isf_gitlab_repos": isf_gitlab_repos,
        "fetch_leaders": fetch_leaders,
    }


def determine_bucket() -> str:
    """
    The S3 bucket is different for production or development.

    Return
    ------
    bucket: str
        S3 bucket name
    """

    # determine if env is 'production' or 'development'
    env = os.environ.get("ENV")
    if env is None:
        env = "production"

    # set the S3 bucket
    if env == "production":
        bucket = "ace-etl"
    elif env == "development":
        bucket = "acedev-etl"

    return bucket


def write_df_to_s3(df: pd.DataFrame, file_name: str):
    """
    Write to S3 bucket.  Will go into the directory techex_appid_health.

    Parameters
    ----------
    df: pd.DataFrame
        df to write to file
    file_name: str
        file name to write to
    """

    # determine if env is 'production' or 'development'
    bucket = determine_bucket()

    DIR_NAME = "techex_appid_health/"

    # prepare the data
    UNIT_SEPARATOR = """"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep=UNIT_SEPARATOR, header=False)

    # write to S3
    s3_client = boto3.client("s3", "us-west-2")
    s3_client.put_object(
        Bucket=bucket, Key=DIR_NAME + file_name, Body=csv_buffer.getvalue()
    )


def read_df_from_s3(file_name: str) -> pd.DataFrame:
    """
    Read from S3 bucket in the directory techex_appid_health.

    Parameter
    ---------
    file_name: str
        file name to read from

    Return
    ------
    df: pd.DataFrame
        the read csv file in a dataframe
        Note that this dataframe has no column names
    """

    # determine if env is 'production' or 'development'
    bucket = determine_bucket()

    DIR_NAME = "techex_appid_health/"

    # Set up the S3 client
    s3_client = boto3.client("s3", "us-west-2")

    try:
        # Download the file from S3 and put in dataframe
        s3_object = s3_client.get_object(Bucket=bucket, Key=DIR_NAME + file_name)
        UNIT_SEPARATOR = """"""
        df = pd.read_csv(
            BytesIO(s3_object["Body"].read()), sep=UNIT_SEPARATOR, header=None
        )

        return df
    except:
        return None


def read_excel_from_s3(file_name: str) -> pd.DataFrame:
    """
    Write an excel file from S3 bucket in the directory techex_appid_health.

    Parameter
    ---------
    file_name: str
        file name to read from

    Return
    ------
    df: pd.DataFrame
        the read csv file in a dataframe
        Note that this dataframe has no column names
    """

    # determine if env is 'production' or 'development'
    bucket = determine_bucket()

    DIR_NAME = "techex_appid_health/"

    # Set up the S3 client
    s3_client = boto3.client("s3", "us-west-2")

    try:
        # Download the excel file from S3 and put in dataframe
        s3_object = s3_client.get_object(Bucket=bucket, Key=DIR_NAME + file_name)
        UNIT_SEPARATOR = """"""
        df = pd.read_excel(BytesIO(s3_object["Body"].read()))

        return df
    except:
        return None


def clean_string(string: str) -> str:
    """
    Replace characters in REPLACE_STRINGS with ' '.
    Done to make Teradata happy.

    Parameter
    ---------
    string: str
        the string that needs to be cleaned

    Return
    ------
    str
        the cleaned string
    """

    if string is None:
        return ""

    MAX_STRING_COUNT = 10000 - 1

    REPLACE_STRINGS = [
        "\n",
        "\r",
        "\t",
        '"',
        "'",
    ]

    for s in REPLACE_STRINGS:
        string = string.replace(s, " ")

    # truncate long cells
    string = string[0:MAX_STRING_COUNT]

    return string


def create_incident(incident_title, incident_details):
    """
    Given an Incident title and details, create a PagerDuty incident if the environment
    is production or print to standard output if the environment is development.

    Parameters
    ----------
    incident_title: str
        The title of the incident
    incident_details: str
        The details about the incident

    """
    # find out if in production or development
    env = os.environ.get("ENV")
    if env is None:
        env = "production"

    if env == "production":
        # create a PagerDuty incident
        SECRETS = read_secrets()
        pd_class = pager_duty_incidents.PDIncidents(
            SECRETS["PD_TOKEN"], pd_team_ids=None
        )
        pd_class.create_pd_incident(title=incident_title, details=incident_details)
    elif env == "development":
        # print to output
        print(f"{incident_title}\n{incident_details}")


def test_for_no_data(df, csv_file: pd.DataFrame):
    """
    Test that the DataFrame being produced has some data content.

    Parameter
    ---------
    pd.DataFrame

    Returns
    -------
    bool
        true = test passes, there is no data shrinkage
        false = test fails, there is data shrinkage
    """

    if len(df) != 0:
        print(f"test for data file not empty passed for {csv_file}")
        return True

    csv_file_just_name = csv_file.split(".")[0]
    incident_title = f"Error in creating techex_{csv_file_just_name}"
    incident_details = (
        f"There was a failure in creating techex_{csv_file_just_name} in the DAG techex_{csv_file_just_name}_11521_ACE_ENG"
        + " as the output contains no data.  Please investigate."
    )

    create_incident(incident_title, incident_details)

    return False


def test_for_data_shrinking(df: pd.DataFrame, csv_file: str) -> bool:
    """
    Test that the DataFrame being produced has more data than the csv file
    from the last run.

    Parameter
    ---------
    df: pd.DataFrame
        the DataFrame with the new data
    csv_file: str
        name of the csv file in S3 to compare with

    Returns
    -------
    bool
        true = test passes, there is no data shrinkage
        false = test fails, there is data shrinkage
    """

    # read the old file from S3
    old_sn_incidents = read_df_from_s3(csv_file)
    if old_sn_incidents is None:
        old_sn_incidents = pd.DataFrame()

    if len(old_sn_incidents) <= len(df):
        # the file is growing in size (or the same) and all is well
        print(f"test for data file growing passed for {csv_file}")
        return True

    csv_file_just_name = csv_file.split(".")[0]

    incident_title = f"Error in creating {csv_file_just_name}"
    incident_details = (
        f"There was a failure in creating techex_{csv_file_just_name} in the DAG techex_{csv_file_just_name}_11521_ACE_ENG"
        + " as the size of the file has decreased.  Please investigate."
    )

    # the incident file did not increase in length or stay the same length
    create_incident(incident_title, incident_details)

    return False
