import datetime as dt

import pager_duty_incidents
import util


def main():
    """
    SECRETS --> Dictionary with keys
        'SERVICE_ACCOUNT'
        'PASSWORD'
        'PD_TOKEN'
        'GITLAB_PAT'

    """

    # get the secrets required
    SECRETS = util.read_secrets()

    # get the support groups from the params file
    pd_team_ids = util.get_yaml_params()["pd_team_ids"]

    incidents = pager_duty_incidents.PDIncidents(SECRETS["PD_TOKEN"], pd_team_ids)

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime.now()

    _ = incidents.get_pd_incidents(start_date=start_date)

    incidents.clean_string()

    # add previous incidents (older than 6 month limit) and write monthly csv files
    incidents.older_incidents()

    # test the quality of the data
    _ = util.test_for_no_data(incidents.pd_incidents_df, "pd_incidents.csv")
    _ = util.test_for_data_shrinking(incidents.pd_incidents_df, "pd_incidents.csv")

    _ = util.test_for_no_data(incidents.pd_services_df, "pd_services.csv")

    # write to S3 bucket
    util.write_df_to_s3(incidents.pd_incidents_df, "pd_incidents.csv")
    util.write_df_to_s3(incidents.pd_services_df, "pd_services_appid.csv")


if __name__ == "__main__":
    main()
