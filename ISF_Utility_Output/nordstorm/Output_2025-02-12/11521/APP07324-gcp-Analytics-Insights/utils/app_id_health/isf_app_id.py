"""


"""
import os

import gitlab_isf

# Pull the GitLab access token from environment variable
#
# This is a Personal Access Token generated from https://git.jwn.app/-/profile/personal_access_tokens.
# Note that the token must have api and read_api scope
#
# In linux, to put the access token into the environment variable use the command
#      export GITLAB_PAT=[the PAT you got from GitLab]

ACCESS_TOKEN = os.getenv("GITLAB_PAT")

# The analysis works on a GitLab repo defined by the project ID and a branch in the repo.
# BRANCH = "feature/DSAE-237-update-queryband-appid-from-pd-appid".replace("/", "%2F")
BRANCH = "production"
PROJECT_ID = 11521  # APP07324-Analytics Insights

# instantiate the class
git_lab_isf = gitlab_isf.GitLabIsF(PROJECT_ID, BRANCH, ACCESS_TOKEN)

# search the pypeline_jobs folder for PagerDuty email (which contains app_id) and the
# referenced SQL files for their app_ids
git_lab_isf.search_pypeline_jobs()

# This will write an Excel file named ae_isf_app_id.xlsx with info on the PagerDuty
# app_id that is found in the files and the queryband app_id that is found in the SQL
# files
git_lab_isf.write_to_excel()

# This will create a bash script to update the queryband in the files where the
# AppID = APP07324 (Insights Framework) to the AppID found in the
# PagerDuty email.  This script will change the value of the AppID in
# the queryband text at the top of the sql files.
#
# The file that's created is called change_app_id.sh.
# To execute the file, first change it to an executable by executing the command:
#      chmod 777 change_app_id.sh
# To execute the file execute the command:
#      bash change_app_id.sh
git_lab_isf.create_bash_script_update_queryband()
