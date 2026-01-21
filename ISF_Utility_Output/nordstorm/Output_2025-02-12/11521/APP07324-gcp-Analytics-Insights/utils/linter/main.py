from config_loader import load_config
from dag_validator import dag_schedule_checker
from files_validator import lint_dag_dirs
from data_fetcher import get_all_td_sql, get_all_spark_sql, get_all_sql, get_all_spark_teradata_json, get_all_json
from files_validator import filename_checker
from json_linter import lint_json
from teradata_sql_checker import TeradataSQLChecker
from spark_sql_checker import SparkSQLChecker


def main():
    # Load config from full path
    config = load_config('./utils/linter/config.yaml')
    restricted_phrases = config['restricted_phrases']
    spark_teradata_json_files = get_all_spark_teradata_json()
    json_files = get_all_json()
    dag_schedule_checker(spark_teradata_json_files, config)
    td_files = get_all_td_sql()
    spark_files = get_all_spark_sql()

    teradata_checker = TeradataSQLChecker(restricted_phrases)
    spark_checker = SparkSQLChecker(restricted_phrases)

    for file in td_files:
        filename_checker(file)
        teradata_checker.check_file(file)
    for file in spark_files:
        filename_checker(file)
        spark_checker.check_file(file)

    # Check JSON files
    for file in json_files:
        filename_checker(file)

    all_sql = get_all_sql()
    for file in all_sql:
        filename_checker(file)

    lint_dag_dirs()
    lint_json()


if __name__ == '__main__':
    main()
