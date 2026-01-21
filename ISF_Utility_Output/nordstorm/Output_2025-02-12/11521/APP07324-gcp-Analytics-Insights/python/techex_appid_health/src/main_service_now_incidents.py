# import argparse
import datetime as dt

import service_now_incidents
import util


def main():
    """
    Get Service Now incidents for a set of AppID.

    SECRETS --> Dictionary with keys
        'SERVICE_ACCOUNT'
        'PASSWORD'
        'PD_TOKEN'
        'GITLAB_PAT'

    """
    # get the secrets required
    SECRETS = util.read_secrets()

    # get the support groups from the params file
    support_group = util.get_yaml_params()["support_group"]

    incidents = service_now_incidents.SNIncidents(
        SECRETS["SERVICE_ACCOUNT"], SECRETS["PASSWORD"], support_group
    )

    # get incidents from 1-1-2023 to current date.
    # NOTE: this date can easily be changed
    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime.now()
    incidents.get_incidents(start_date=start_date, end_date=end_date)

    # clean the columns no make happy with Teradata
    incidents.clean_string()

    # test the quality of the data
    _ = util.test_for_no_data(incidents.incidents_df, "sn_incidents.csv")
    _ = util.test_for_data_shrinking(incidents.incidents_df, "sn_incidents.csv")

    # write to S3 bucket
    util.write_df_to_s3(incidents.incidents_df, "sn_incidents.csv")


if __name__ == "__main__":
    main()
