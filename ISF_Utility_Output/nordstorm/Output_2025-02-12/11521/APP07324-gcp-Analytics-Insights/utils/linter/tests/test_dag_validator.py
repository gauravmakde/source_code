import pytest
from unittest.mock import patch, mock_open
from utils.linter.dag_validator import dag_schedule_checker


@patch("builtins.open", new_callable=mock_open, read_data=b'{"dag_id": "test_dag", "dag_schedule": "0 0 * * *"}')
def test_dag_schedule_checker_with_non_conflicting_schedule(mock_file):
    config = {
        'prod_exception_dag_list': [],
        'exclude_time_list': ['4', '5', '6', '7', '8']
    }
    dag_schedule_checker(['test.json'], config)
    mock_file.assert_any_call('test.json', 'r')


@patch("builtins.open", new_callable=mock_open, read_data=b'{"dag_id": "test_dag", "dag_schedule": "0 4 * * *"}')
def test_dag_schedule_checker_with_conflicting_schedule_raises_exception(mock_file):
    config = {
        'prod_exception_dag_list': [],
        'exclude_time_list': ['4', '5', '6', '7', '8']
    }
    with pytest.raises(Exception,
                       match=f"Error: Dag schedule conflicting with prod critical dags between 4 am to 9 am PST. "
                        f"If your dags need to run in this interval please reach out to AE team "
                        f"for approval. Slack: #analytics-engineering"):
        dag_schedule_checker(['test.json'], config)


@patch("builtins.open", new_callable=mock_open, read_data=b'{"dag_id": "test_dag", "dag_schedule": "0 4 * * *"}')
def test_dag_schedule_checker_with_conflicting_schedule_in_exception_list(mock_file):
    config = {
        'prod_exception_dag_list': ['test_dag'],
        'exclude_time_list': ['4', '5', '6', '7', '8']
    }
    dag_schedule_checker(['test.json'], config)
    mock_file.assert_any_call('test.json', 'r')


@patch("builtins.open", new_callable=mock_open, read_data=b'{"dag_id": "test_dag"}')
def test_dag_schedule_checker_with_no_schedule(mock_file):
    config = {
        'prod_exception_dag_list': [],
        'exclude_time_list': ['4', '5', '6', '7', '8']
    }
    dag_schedule_checker(['test.json'], config)
    mock_file.assert_called_once_with('test.json', 'r')
