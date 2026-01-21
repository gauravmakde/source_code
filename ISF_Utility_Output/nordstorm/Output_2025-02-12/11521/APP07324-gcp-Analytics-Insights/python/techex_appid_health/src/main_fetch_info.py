import fetch_api
import pandas as pd
import util


def main():
    """
    Find the info on users from Fetch (which is powered by Active Directory).
    This uses the Fetch API.

    A group of leaders are fourd in the params.yml file and anyone supported
    by these leaders will be found and outputted.
    """

    # get the secrets required
    SECRETS = util.read_secrets()

    FETCH_NAUTH_CLIENT_ID = SECRETS["FETCH_NAUTH_CLIENT_ID"]
    FETCH_NAUTH_CLIENT_SECRET = SECRETS["FETCH_NAUTH_CLIENT_SECRET"]

    # get the fetch leaders from the params file
    fetch_leaders = util.get_yaml_params()["fetch_leaders"]

    # initialize the class
    get_fetch = fetch_api.GetFetch(FETCH_NAUTH_CLIENT_ID, FETCH_NAUTH_CLIENT_SECRET)

    fetch_df = pd.DataFrame()
    fetch_dR = []

    # step through the leaders in the params file and collect info
    for leader in fetch_leaders:
        leader_dR, leader_df = get_fetch.find_leader_people(leader)
        fetch_df = pd.concat([fetch_df, leader_df])
        fetch_dR += leader_dR

    # clean up some columns
    # NOTE:  This will remove any duplicates that came up in the leaders list
    fetch_df = get_fetch.format_for_output(fetch_df)

    # clean the columns no make happy with Teradata
    fetch_df = get_fetch.clean_string(fetch_df)

    # add previous inactive individuals
    fetch_df = get_fetch.keep_older_data(fetch_df)

    # test the quality of the data
    _ = util.test_for_no_data(fetch_df, "fetch_info.csv")

    # write to S3 bucket
    util.write_df_to_s3(fetch_df, "fetch_info.csv")


if __name__ == "__main__":
    main()
