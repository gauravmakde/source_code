import argparse
import ast
import asyncio
import os
import time

from mstrio.connection import Connection
from mstrio.project_objects import OlapCube
from mstrio.project_objects.incremental_refresh_report import \
    IncrementalRefreshReport


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        '--projects', help='MicroStrategy Project Names for Dev and Prod', required=True)
    arg_parser.add_argument(
        '--yaml_config', help='Content IDs in YAML format', required=True)
    arg_parser.add_argument(
        '--is_async', help='Run cube refreshes asynchronously', action="store_true")
    arg_parser.add_argument(
        '--incremental', help='Refresh is of type incremental', action="store_true")
    return arg_parser.parse_args()


def get_env() -> str:
    '''Retrieve the environment from within NSK.'''
    env = os.getenv("ENV")
    if env == 'development':
        return 'dev'
    elif env == 'production':
        return 'prod'
    else:
        raise RuntimeError('Code not running in appropriate environment')


def parse_string_arg(mstr_env: str, content: str) -> list[str]:
    '''
    Parse a passed literal dictionary into an actual dictionary and return a list
    of strings from it.
    '''
    params = ast.literal_eval(content)
    return params.get(mstr_env)


def get_time_elapsed(tic: float, toc: float) -> str:
    '''Get a friendly time format of time elapsed.'''
    total_time_sec = toc - tic
    seconds = int(total_time_sec % 60)
    minutes = int((total_time_sec // 60) % 60)
    hours = int((total_time_sec // 3600) % 24)
    return f'{hours:02d}h {minutes:02d}m {seconds:02d}s'


def incremental_refresh_cube(conn: Connection, report_id: str) -> str:
    '''Incrementally refresh a cube using an incremental refresh report.'''
    incremental_refresh_report = IncrementalRefreshReport(conn, report_id)
    # Get ID of cube being targeted for incremental refresh
    target_cube_id: str = incremental_refresh_report.target_cube.object_id  # type: ignore
    target_cube = OlapCube(connection=conn, id=target_cube_id)
    last_modified_date = target_cube.get_caches()[0].last_update_time
    # Initiate incremental refresh
    incremental_refresh_report.execute()
    # Give API enough time to register publish request
    time.sleep(5)
    # Keep checking the status until the target cube is done refreshing
    while 'Processing' in target_cube.show_status():
        time.sleep(5)
        target_cube.refresh_status()
    toc = time.perf_counter()
    new_modified_date = target_cube.get_caches()[0].last_update_time
    print(
        f'Old modified date: {last_modified_date} \nNew modified date: {new_modified_date}')
    status = f'Finished refreshing {target_cube.name} in {get_time_elapsed(tic, toc)}'
    if last_modified_date == new_modified_date:
        raise RuntimeError('Refresh failed, exiting script')
    else:
        status = f'Finished refreshing {target_cube.name} in {get_time_elapsed(tic, toc)}'
        return status


async def refresh_cube_async(conn: Connection, cube_id: str) -> str:
    '''Refresh a cube asynchronously.'''
    cube = OlapCube(connection=conn, id=cube_id)
    # Refresh cube
    refresh_job = cube.publish()
    last_modified_date = cube.get_caches()[0].last_update_time
    # Give API enough time to register publish request
    time.sleep(5)
    # Get the latest status of the cube
    cube.refresh_status()
    tic = time.perf_counter()
    # Keep checking the status until the cube is done refreshing
    while 'Processing' in cube.show_status():
        await asyncio.sleep(5)
        cube.refresh_status()
    toc = time.perf_counter()
    new_modified_date = cube.get_caches()[0].last_update_time
    print(
        f'Old modified date: {last_modified_date} \nNew modified date: {new_modified_date}')
    # Check if cube cache was updated to determine if Job failed
    if last_modified_date == new_modified_date:
        refresh_job.fetch()
        print(
            f'Unsuccessful status is {refresh_job.status} with error message::\n {refresh_job.error_message}')
        raise RuntimeError('Refresh failed, exiting script')
    else:
        status = f'Finished refreshing {cube.name} in {get_time_elapsed(tic, toc)}'
        return status


def refresh_cube(conn: Connection, cube_id: str) -> str:
    '''Refresh a cube.'''
    cube = OlapCube(connection=conn, id=cube_id)
    last_modified_date = cube.get_caches()[0].last_update_time
    # Refresh cube
    refresh_job = cube.publish()
    # Give API enough time to register publish request
    time.sleep(5)
    # Get the latest status of the cube
    cube.refresh_status()
    tic = time.perf_counter()
    # Keep checking the status until the cube is done refreshing
    while 'Processing' in cube.show_status():
        time.sleep(5)
        cube.refresh_status()
    toc = time.perf_counter()
    new_modified_date = cube.get_caches()[0].last_update_time
    print(
        f'Old modified date: {last_modified_date} \nNew modified date: {new_modified_date}')
    # Check if cube cache was updated to determine if Job failed
    if last_modified_date == new_modified_date:
        refresh_job.fetch()
        print(
            f'Unsuccessful status is {refresh_job.status} with error message::\n {refresh_job.error_message}')
        raise RuntimeError('Refresh failed, exiting script')
    else:
        status = f'Finished refreshing {cube.name} in {get_time_elapsed(tic, toc)}'
        return status


async def main():
    args = get_args()
    projects = args.projects
    content_IDs = args.yaml_config
    mstr_env = get_env()
    mstr_username = os.environ.get('MICROSTRATEGY_ACCOUNT')
    mstr_password = os.environ.get('MICROSTRATEGY_SECRET')
    mstr_url = f'https://mstr{mstr_env}lib.nordstrom.com/MicroStrategyLibrarySTD/api'
    mstr_project = parse_string_arg(mstr_env, projects)[0]
    content_list = parse_string_arg(mstr_env, content_IDs)
    conn = Connection(mstr_url, mstr_username, mstr_password,
                      project_name=mstr_project, login_mode=1)
    print(
        f'Successfully connected with user {conn.username} in {conn.project_name}')
    if args.incremental:
        print('Performing incremental refresh')
        for incremental_report_id in content_list:
            status = incremental_refresh_cube(conn, incremental_report_id)
            print(status)
    elif args.is_async:
        tasks = [refresh_cube_async(conn, cube_id) for cube_id in content_list]
        print('Refreshing cubes asynchronously')
        cube_refresh_statuses = await asyncio.gather(*tasks)
        print(*cube_refresh_statuses, sep='\n')
    else:
        print('Refreshing cubes synchronously')
        for cube_id in content_list:
            status = refresh_cube(conn, cube_id)
            print(status)


if __name__ == '__main__':
    tic = time.perf_counter()
    asyncio.run(main())
    toc = time.perf_counter()
    print(f'{__file__} executed in {get_time_elapsed(tic, toc)}.')
