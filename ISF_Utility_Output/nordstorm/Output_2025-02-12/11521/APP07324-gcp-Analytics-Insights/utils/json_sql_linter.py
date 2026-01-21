import glob
import re
import sqlparse
import json
import re
import os
from cron_converter import Cron

# TODO: Massive amounts of cleanup and refactoring :)

# List of prod dags that need to run in the 4 to 9 PST interval.
prod_dag_list = {'speed_search_session_performance', 'divisional_summary_weekly',
                 'mothership_finance_sdotf_traffic_only', 'content_session_analytical', 'wifi_store_users',
                 'fraud_order_validation', 'sales_fact', 'sales_and_returns_fact_base', 'affiliate_publisher_mapping',
                 'ssf_receipts_agg', 'acp_driver_dim', 'mta_email_performance', 'fls_store_traffic', 'nmn_gl',
                 'affiliates_cost', 'mkt_utm_agg', 'sku_item_added', 'loyalty_incremental_sales', 'gl_sourced',
                 'kpi_buyerflow', 'fp_funnel_cost_fact', 'mmm_forecast', 'store_traffic_rn_camera_and_trips_daily',
                 'nmn_brand', 'f2dd_product_cvr', 'brand_health', 'location_inventory_tracking_total',
                 'search_session_analytical_v2', 'analytical_session_fact', 'mmm_decomp_export', 'loyalty',
                 'rack_facebook_reach_and_freq', 'item_demand_forecasting_dataprep', 'browse_session_analytical_v2',
                 'mmm_mth_sales_extract', 'digital_merch_table', 'marketplace_product_relationship', 'customer_plan',
                 'nordstrom_on_digital_styling', 'mmm_mth_inv_extract', 'store_traffic_daily',
                 'teradata_query_activity_daily', 'digital_inventory_age_table', 'inventory_position_master_table',
                 'empdisc_pct_item_trans', 'rack_wifi_gap_traffic', 'manual_adjustment_daily', 'rack_funnel_cost_fact',
                 'cust_pop_initiative_exposal', 'fp_pinterest_reach_and_freq', 'fp_facebook_reach_and_freq',
                 'fp_tiktok_reach_and_freq', 'trigger_asf_rerun', 'trigger_mkt_utm_agg_rerun'}

# List of prod critical dag intervals in UTC as all the schedules are in UTC.
exclude_time_list = ['11', '12', '13', '14', '15', '16']

# Fail when any new dag schedule is between 4 to 9 PST as this conflicts with the prod dag runs
def dag_schedule_checker(json_files):
    for file in json_files:
        if "templates/" in file:
            continue
        f = open(file)
        current_json = json.load(f)
        if current_json.get('dag_schedule'):
            cron = Cron(current_json.get('dag_schedule')).schedule(timezone_str='UTC')
            if current_json['dag_id'] in prod_dag_list:
                continue
            else:
                if exclude_time_list[-1] in current_json.get('dag_schedule') and cron.next().minute == 0:
                    print(f"{current_json.get('dag_id')} : {current_json.get('dag_schedule')}")
                    raise Exception(
                        f"Error: Dag schedule conflicting with prod critical dags between 4 am to 9 am PST. "
                        f"If your dags need to run in this interval please reach out to AE team "
                        f"for approval. Slack: #analytics-engineering")
                for i in exclude_time_list[:len(exclude_time_list)-1]:
                    if i in current_json.get('dag_schedule'):
                        print(f"{current_json.get('dag_id')} : {current_json.get('dag_schedule')}")
                        raise Exception(
                            f"Error: Dag schedule conflicting with prod critical dags between 4 am to 9 am PST. "
                            f"If your dags need to run in this interval please reach out to AE team "
                            f"for approval. Slack: #analytics-engineering")

def lint_dag_dirs():
    # Ensure that all files in these directories are actually JSON, SQL, or MD files (for SQL dirs only)
    all_jobs = glob.glob('./pypeline_jobs/**', recursive=True)
    all_sql = glob.glob('./pypeline_sql/**/*', recursive=True)
    for job in all_jobs:
        if job == "./pypeline_jobs/":
            continue
        if job[-5:] != '.json':
            raise Exception(f"Error - non-JSON file placed in `pypeline_jobs` - got {job} ")
    for sql in all_sql:
        if os.path.isdir(sql):
            continue
        if sql[-4:] != '.sql' and sql[-3:] != '.md':
            raise Exception(f"Error - non-SQL or .md file placed in `pypeline_sql` - got {sql} ")


def get_all_json():
    all_json = glob.glob('./**/*.json', recursive=True)
    return all_json


def get_all_sql():
    all_sql = glob.glob('./**/*.sql', recursive=True)
    return all_sql


def get_all_td_sql():
    all_json = get_all_json()
    sql_files = set()

    for file in all_json:
        if "templates/" in file:
            continue
        f = open(file)
        current_json = json.load(f)
        # we know that we can get the list of stages - every JSON needs them
        try:
            stages = current_json['stages']
        except KeyError as e:
            continue

        # iterate over all stages
        for stage in stages:
            # fix bug where a stage does not correspond to a job
            curr_stage = current_json.get(stage, None)
            if not curr_stage:
                continue
            # can be multiple jobs per stage - ensure that we'll pick the TD one
            for job in curr_stage:
                # find if it's a TD job
                is_td_engine = curr_stage[job].get('teradata_engine', None)
                # if it's td engine - grab all the scripts
                if is_td_engine:
                    for input_dict in curr_stage[job]['teradata_engine']['input_details']:
                        scripts = input_dict['scripts']
                        for script in scripts:
                            sql_files.add(f'./pypeline_sql/{script}')
    return sql_files

def get_all_spark_sql():
    all_json = get_all_json()
    sql_files = set()

    for file in all_json:
        if "templates/" in file:
            continue
        f = open(file)
        current_json = json.load(f)
        # we know that we can get the list of stages - every JSON needs them
        try:
            stages = current_json['stages']
        except KeyError as e:
            continue

        # iterate over all stages
        for stage in stages:
            # fix bug where a stage does not correspond to a job
            curr_stage = current_json.get(stage, None)
            if not curr_stage:
                continue
            # can be multiple jobs per stage - ensure that we'll pick the Spark one
            for job in curr_stage:
                # find if it's a Spark job
                is_spark_engine = curr_stage[job].get('spark_engine', None)
                # if it's Spark engine - grab all the scripts
                if is_spark_engine:
                    for script in curr_stage[job]['spark_engine']['scripts']:
                        sql_files.add(f'./pypeline_sql/{script}')
    return sql_files

def phrase_checker(phrase, clean_statement):
    # Handle executing direcly against S3 paths
    if "s3://ace-etl" in clean_statement and ("create temporary view" not in clean_statement \
        and "create or replace temp view" not in clean_statement \
        and "create or replace temporary view" not in clean_statement):
        print(clean_statement)
        raise Exception("Error: You cannot create Hive tables directly against `s3://ace-etl` - please use {hive_schema}")
    # Handle references to T3 tables
    if "t3dl_" in clean_statement and 't3dl_ace_corp.znxy_liveramp_events' not in clean_statement \
            and 't3dl_paymnt_loyalty.credit_close_dts_20230719_v3' not in clean_statement \
            and 't3dl_paymnt_loyalty.creditloyaltysync' not in clean_statement: # add exception for credit loyalty issues from NAP Engineering
        raise Exception(f"Error: You cannot reference a T3 object in an ISF Pipeline - got {clean_statement}")
    if phrase.lower() in clean_statement:
        clean_statement = (clean_statement[clean_statement.find(phrase):])
        clean_statement_split = clean_statement.split(" ")
        # CALL SYS_MGMT REQUIRES SPECIAL HANDLING
        if phrase.lower() == "call":
            # attenuate for when the literal word "call" is used
            if "sys_mgmt" not in clean_statement_split[1]:
                pass
            else:
                for item in clean_statement_split:
                    if "t2dl_" in item and "t2dl_das_bie_dev" not in item:
                        print(clean_statement_split)
                        raise Exception(f"Error: You cannot execute a CALL SYS_MGMT procedure directly against a T2")
        else:
            # The [xth] element i.e. (DELETE FROM X / DROP TABLE X / CREATE MULTISET TABLE X)
            element = len(phrase.split(" "))
            if "if" in clean_statement_split:
                clean_statement_split.remove("if")
            if "exists" in clean_statement_split:
                clean_statement_split.remove("exists")
            if "not" in clean_statement_split:
                clean_statement_split.remove("not")
            if ("t2dl_" in clean_statement_split[element] or "ace_etl" in clean_statement_split[
                element]) and "t2dl_das_bie_dev" not in clean_statement_split[element]:
                # Add exception for TRUST which does NOT test in BIE_DEV
                if "t2dl_das_trust_emp" not in clean_statement_split[element] and "{t2_test}" not in \
                        clean_statement_split[element]:
                    print(clean_statement_split)
                    raise Exception(f"Error: You cannot execute {phrase.upper()} on a T2DL_ / Hive Table Directly")


def checker(sql_file, is_spark, phrases):
    with open(sql_file, 'r') as file_read:
        sql_str = file_read.read().lower()
        parsed = sqlparse.split(sqlparse.format(sql_str, strip_comments=True, strip_whitespace=True))
        starts_with_qb = False
        if is_spark: # Skip spark jobs to require QB
            starts_with_qb = True
        for statement in parsed:
            clean_statement = re.sub('\s+', ' ', statement.lower())
            if starts_with_qb == False:
                if 'set query_band' in clean_statement.lower():
                    starts_with_qb = True
                else:
                    raise Exception(
                        f"Error - your Teradata SQL file MUST start with a Query Band - got {clean_statement}")
            for phrase in phrases:
                phrase_checker(phrase, clean_statement)


def filename_checker(filename):
    if filename.startswith("./utils/") or filename.startswith("./templates/") or filename.startswith("./lint_stg/"):
        return  # skip these files
    # check filename
    regex = re.compile(r"^[\w\.\/]+$")
    if regex.search(filename) == None:
        raise Exception(
            f"You have provided an invalid JSON, SQL, or directory filename name - please only use alphanumeric characters and underscores - got {filename}")
    # check file location
    if not filename.startswith("./pypeline_jobs/") and not filename.startswith("./pypeline_sql/"):
        raise Exception(f"JSON and SQL files must be placed in `pypeline_sql` or `pypeline_jobs` - got {filename}")


def lint_json():
    all_json = get_all_json()

    for file in all_json:
        if file.startswith("./utils/") or file.startswith("./templates/") or file.startswith("./lint_stg/"):
            continue  # skip these files
        if file in ["./pypeline_jobs/nonprod_global.json", "./pypeline_jobs/prod_global.json"]:
            continue  # also skip these
        sql_files = []
        f = open(file)
        current_json = json.load(f)
        # Ensure JSON has stages
        try:
            stages = current_json['stages']
        except KeyError as e:
            raise Exception(f"ERROR: JSON does not have any `stages` defined - {file}")
        # Ensure JSON has `dag_id`, `prod_email_on_failure:
        if not current_json.get('dag_id', None):
            raise Exception(f"ERROR: `dag_id` is missing from JSON {file}")
        if not current_json.get('prod_email_on_failure', None):
            raise Exception(f"ERROR: `prod_email_on_failure` is missing from JSON {file}")
        # Ensure that JSON has `dag_id` that is alphanumeric and underscores only
        regex = re.compile(r"^[\w\.\/]+$")
        if regex.search(current_json.get('dag_id')) == None:
            raise Exception(f"ERROR: `dag_id` must only be alphanumeric with underscores - got {current_json.get('dag_id')}")
        # Ensures SQL file actually exists
        # iterate over all stages
        for stage in stages:
            curr_stage = current_json.get(stage, None)
            if not curr_stage:
                raise Exception(
                    f"ERROR: You have a stage defined in `stages` that does not exist on your JSON - stage {stage} in file {file}")
            # can be multiple jobs per stage - ensure that we'll pick the TD one
            for job in curr_stage:
                # find if it's a TD job
                is_td_engine = curr_stage[job].get('teradata_engine', None)
                is_spark_engine = curr_stage[job].get('spark_engine', None)
                # if it's td engine - grab all the scripts
                if is_td_engine:
                    for input_dict in curr_stage[job]['teradata_engine']['input_details']:
                        scripts = input_dict['scripts']
                        for script in scripts:
                            sql_files.append(f'./pypeline_sql/{script}')
                if is_spark_engine:
                    for script in curr_stage[job]['spark_engine']['scripts']:
                        sql_files.append(f'./pypeline_sql/{script}')
        for sql_file in sql_files:
            if not os.path.isfile(sql_file):
                raise Exception(
                    f"ERROR: JSON {file} references SQL file that does not exist - file {sql_file} not found")


# Check TD files
td_files = get_all_td_sql()
spark_files = get_all_spark_sql()
for file in td_files:
   filename_checker(file)
   checker(file, is_spark=False, phrases=["delete from", "drop table", "alter table", "create table", "create multiset table", "call", "create table if not exists", "insert overwrite table"])
for file in spark_files:
    filename_checker(file)
    checker(file, is_spark=True, phrases=["delete from", "drop table", "alter table", "create table", "create multiset table", "call", "create table if not exists", "insert overwrite table",])
# Check JSON files
json_files = get_all_json()
# check all json files to make sure that none of the dag schedule interfear with prod dags
dag_schedule_checker(json_files)
for file in json_files:
    filename_checker(file)
# Check all SQL files
all_sql = get_all_sql()
for file in all_sql:
    filename_checker(file)
# Check pypeline_job / pypeline_sql directories
lint_dag_dirs()

# Ensure JSON references file that actually exists, and other mandatory features
lint_json()
