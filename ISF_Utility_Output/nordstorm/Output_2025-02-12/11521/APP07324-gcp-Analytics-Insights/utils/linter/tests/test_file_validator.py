import pytest
import glob
import os
from utils.linter.files_validator import lint_dag_dirs, filename_checker


def test_lint_dag_dirs_valid_files(mocker):
    # Mock the glob function to return specific files
    mock_glob = mocker.patch('glob.glob')
    mock_glob.side_effect = [
        ['./pypeline_jobs/job1.json', './pypeline_jobs/job2.json'],
        ['./pypeline_sql/script1.sql', './pypeline_sql/script2.md']
    ]

    # Mock the os.path.isdir function to always return False for simplicity
    mock_isdir = mocker.patch('os.path.isdir')
    mock_isdir.return_value = False

    # Call the function
    lint_dag_dirs()

    # If no exception is raised, the test passes
    assert True


def test_lint_dag_dirs_invalid_json_file(mocker):
    # Mock the glob function to return an invalid file
    mock_glob = mocker.patch('glob.glob')
    mock_glob.side_effect = [
        ['./pypeline_jobs/job1.json', './pypeline_jobs/job2.txt'],
        []
    ]

    # Mock the os.path.isdir function to always return False for simplicity
    mock_isdir = mocker.patch('os.path.isdir')
    mock_isdir.return_value = False

    # Call the function and check for the exception
    with pytest.raises(Exception) as excinfo:
        lint_dag_dirs()

    assert "Error - non-JSON file placed in `pypeline_jobs`" in str(excinfo.value)


def test_lint_dag_dirs_invalid_sql_file(mocker):
    # Mock the glob function to return an invalid file
    mock_glob = mocker.patch('glob.glob')
    mock_glob.side_effect = [
        [],
        ['./pypeline_sql/script1.sql', './pypeline_sql/script2.txt']
    ]

    # Mock the os.path.isdir function to always return False for simplicity
    mock_isdir = mocker.patch('os.path.isdir')
    mock_isdir.return_value = False

    # Call the function and check for the exception
    with pytest.raises(Exception) as excinfo:
        lint_dag_dirs()

    assert "Error - non-SQL or .md file placed in `pypeline_sql`" in str(excinfo.value)


def test_filename_checker_valid_filename_in_jobs():
    # Valid filename in pypeline_jobs
    filename = "./pypeline_jobs/valid_file.json"
    # Call the function
    filename_checker(filename)
    # If no exception is raised, the test passes
    assert True


def test_filename_checker_valid_filename_in_sql():
    # Valid filename in pypeline_sql
    filename = "./pypeline_sql/valid_file.sql"
    # Call the function
    filename_checker(filename)
    # If no exception is raised, the test passes
    assert True


def test_filename_checker_invalid_filename():
    # Invalid filename with special characters
    filename = "./pypeline_jobs/invalid_file@.json"
    # Call the function and check for the exception
    with pytest.raises(Exception) as excinfo:
        filename_checker(filename)

    assert "invalid JSON, SQL, or directory filename name" in str(excinfo.value)


def test_filename_checker_disallowed_directory():
    # Valid filename but in a disallowed directory
    filename = "./other_directory/valid_file.json"
    # Call the function and check for the exception
    with pytest.raises(Exception) as excinfo:
        filename_checker(filename)

    assert "JSON and SQL files must be placed in `pypeline_sql` or `pypeline_jobs`" in str(excinfo.value)


def test_filename_checker_skipped_directory_utils():
    # Valid filename in a skipped directory (utils)
    filename = "./utils/valid_file.json"
    # Call the function
    filename_checker(filename)
    # If no exception is raised, the test passes
    assert True

