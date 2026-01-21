import glob
import os
import re


def lint_dag_dirs() -> None:
    """
     Validate that all files in specific directories are of the correct file type.

     This function checks all files in the 'pypeline_jobs' and 'pypeline_sql' directories and their subdirectories.
     It ensures that all files in 'pypeline_jobs' are JSON files and all files in 'pypeline_sql' are either SQL or MD files.
     If a file of a different type is found, an exception is raised.

     Raises:
         Exception: If a non-JSON file is found in 'pypeline_jobs' or a non-SQL/MD file is found in 'pypeline_sql'.
     """

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


def filename_checker(filename: str) -> None:
    """
    Validate the filename and its location.

    Args:
        filename (str): The path to the file to check.

    Raises:
        Exception: If the filename contains invalid characters.
        Exception: If the file is not located in 'pypeline_jobs' or 'pypeline_sql'.
    """
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
