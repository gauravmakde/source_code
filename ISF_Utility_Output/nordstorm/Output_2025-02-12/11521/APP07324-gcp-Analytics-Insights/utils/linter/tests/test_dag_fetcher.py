import pytest
import json
from unittest.mock import patch, mock_open
from utils.linter.data_fetcher import get_all_json, get_all_sql, get_all_td_sql, get_all_spark_sql, \
    get_all_spark_teradata_json


def test_get_all_json_files_found(mocker):
    # Mock the glob.glob function
    mock_glob = mocker.patch('utils.linter.data_fetcher.glob.glob')
    # Setup the mock to return a list of JSON files
    mock_glob.return_value = [
        './pypeline_jobs/file1.json',
        './pypeline_jobs/subdir/file2.json'
    ]

    # Call the function
    result = get_all_json()

    # Assert the result
    assert result == [
        './pypeline_jobs/file1.json',
        './pypeline_jobs/subdir/file2.json'
    ]
    # Ensure the glob function was called correctly
    mock_glob.assert_called_once_with('./pypeline_jobs/**/*.json', recursive=True)


def test_get_all_json_files_found(mocker):
    # Mock the glob.glob function
    mock_glob = mocker.patch('utils.linter.data_fetcher.glob.glob')
    # Setup the mock to return a list of JSON files
    mock_glob.return_value = [
        './pypeline_jobs/file1.json',
        './pypeline_jobs/subdir/file2.json'
    ]

    # Call the function
    result = get_all_json()

    # Assert the result
    assert result == [
        './pypeline_jobs/file1.json',
        './pypeline_jobs/subdir/file2.json'
    ]
    # Ensure the glob function was called correctly
    mock_glob.assert_called_once_with('./pypeline_jobs/**/*.json', recursive=True)


def test_get_all_td_sql_files_found(mocker):
    # Mock the get_all_json function
    mock_get_all_json = mocker.patch('utils.linter.data_fetcher.get_all_json')
    # Setup the mock to return a list of JSON files
    mock_get_all_json.return_value = [
        './pypeline_jobs/job1.json',
        './pypeline_jobs/job2.json'
    ]

    # Mock the open function to simulate reading JSON files
    mocked_file_data = {
        './pypeline_jobs/job1.json': json.dumps({
            'stages': ['stage1'],
            'stage1': {
                'job1': {
                    'teradata_engine': {
                        'input_details': [
                            {'scripts': ['script1.sql', 'script2.sql']}
                        ]
                    }
                }
            }
        }),
        './pypeline_jobs/job2.json': json.dumps({
            'stages': ['stage1'],
            'stage1': {
                'job2': {
                    'teradata_engine': {
                        'input_details': [
                            {'scripts': ['script3.sql']}
                        ]
                    }
                }
            }
        })
    }

    mock_open_func = mock_open()
    mock_open_func.side_effect = lambda filename, mode: mock_open(read_data=mocked_file_data[filename]).return_value

    mocker.patch('builtins.open', mock_open_func)

    # Call the function
    result = get_all_td_sql()

    # Assert the result
    expected_result = {
        './pypeline_sql/script1.sql',
        './pypeline_sql/script2.sql',
        './pypeline_sql/script3.sql'
    }

    assert result == expected_result


def test_get_all_spark_sql_files_found(mocker):
    # Mock the get_all_json function
    mock_get_all_json = mocker.patch('utils.linter.data_fetcher.get_all_json')
    # Setup the mock to return a list of JSON files
    mock_get_all_json.return_value = [
        './pypeline_jobs/job1.json',
        './pypeline_jobs/job2.json'
    ]

    # Mock the open function to simulate reading JSON files
    mocked_file_data = {
        './pypeline_jobs/job1.json': json.dumps({
            'stages': ['stage1'],
            'stage1': {
                'job1': {
                    'spark_engine': {
                        'scripts': ['script1.sql', 'script2.sql']
                    }
                }
            }
        }),
        './pypeline_jobs/job2.json': json.dumps({
            'stages': ['stage1'],
            'stage1': {
                'job2': {
                    'spark_engine': {
                        'scripts': ['script3.sql']
                    }
                }
            }
        })
    }

    # Mock the open function
    mock_open_func = mock_open()
    mock_open_func.side_effect = lambda filename, mode='r': mock_open(read_data=mocked_file_data[filename]).return_value

    mocker.patch('builtins.open', mock_open_func)

    # Call the function
    result = get_all_spark_sql()

    # Assert the result
    expected_result = {
        './pypeline_sql/script1.sql',
        './pypeline_sql/script2.sql',
        './pypeline_sql/script3.sql'
    }

    assert result == expected_result


def test_get_all_spark_teradata_json_files_found(mocker):
    # Mock the get_all_json function
    mock_get_all_json = mocker.patch('utils.linter.data_fetcher.get_all_json')
    # Setup the mock to return a list of JSON files
    mock_get_all_json.return_value = [
        './pypeline_jobs/job1.json',
        './pypeline_jobs/job2.json'
    ]

    # Mock the open function to simulate reading JSON files
    mocked_file_data = {
        './pypeline_jobs/job1.json': json.dumps({
            'stages': ['stage1'],
            'stage1': {
                'job1': {
                    'spark_engine': {
                        'scripts': ['script1.sql', 'script2.sql']
                    }
                }
            }
        }),
        './pypeline_jobs/job2.json': json.dumps({
            'stages': ['stage1'],
            'stage1': {
                'job2': {
                    'teradata_engine': {
                        'input_details': [
                            {'scripts': ['script3.sql']}
                        ]
                    }
                }
            }
        })
    }

    # Mock the open function
    mock_open_func = mock_open()
    mock_open_func.side_effect = lambda filename, mode='r': mock_open(read_data=mocked_file_data[filename]).return_value

    mocker.patch('builtins.open', mock_open_func)

    # Call the function
    result = get_all_spark_teradata_json()

    # Assert the result
    expected_result = {
        './pypeline_jobs/job1.json',
        './pypeline_jobs/job2.json'
    }

    assert result == expected_result