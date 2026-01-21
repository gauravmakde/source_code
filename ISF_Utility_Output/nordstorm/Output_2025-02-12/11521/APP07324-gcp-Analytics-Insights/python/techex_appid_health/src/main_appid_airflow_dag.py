import pandas as pd

import gitlab_airflow
import gitlab_isf
import util


def main():
    """
    Find the AppID to Airflow DAG mapping.

    SECRETS --> Dictionary with keys
        'SERVICE_ACCOUNT'
        'PASSWORD'
        'PD_TOKEN'
        'GITLAB_PAT'
    """
    # get the secrets required
    SECRETS = util.read_secrets()

    # get the support groups from the params file
    isf_gitlab_repos = util.get_yaml_params()["isf_gitlab_repos"]

    # get AE IsF mapping
    git_lab_ae_isf = gitlab_isf.GitLabIsF(isf_gitlab_repos, SECRETS["GITLAB_PAT"])
    appid_airflow_ae_isf = git_lab_ae_isf.loop_repos_for_jobs()

    # get the rest of IsF mapping
    git_lab_isf = gitlab_airflow.GitLabAirflow(SECRETS["GITLAB_PAT"])
    appid_airflow_isf = git_lab_isf.find_all_isf_files()

    # get MLP mapping
    git_lab_mlp = gitlab_airflow.GitLabAirflow(SECRETS["GITLAB_PAT"])
    appid_airflow_mlp = git_lab_mlp.find_mlp_job()

    appid_airflow_total = pd.concat(
        [appid_airflow_ae_isf, appid_airflow_isf, appid_airflow_mlp]
    )

    # remove the IsF dag_id that are represented in the AE IsF mapping (because AE IsF
    # gets the AppID from the query band, not the repo name)
    appid_airflow_total = appid_airflow_total.drop_duplicates(
        subset=["dag_id"], keep="first"
    )

    # write to S3 bucket
    util.write_df_to_s3(appid_airflow_total, "appid_airflow_dag.csv")


if __name__ == "__main__":
    main()
