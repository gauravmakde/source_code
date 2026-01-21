import service_now_incidents
import util


def main():
    """
    Find the NERDS info (Name, Support Group, Description, etc) for AppID.

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

    support_group_appid = service_now_incidents.SNIncidents(
        SECRETS["SERVICE_ACCOUNT"], SECRETS["PASSWORD"], support_group
    )

    # actually get the applications
    support_group_appid.get_applications()

    # clean the columns no make happy with Teradata
    support_group_appid.clean_string()

    # test the quality of the data
    _ = util.test_for_no_data(support_group_appid.applications, "appid_nerds_info.csv")

    # write to S3 bucket
    util.write_df_to_s3(support_group_appid.applications, "appid_nerds_info.csv")


if __name__ == "__main__":
    main()
