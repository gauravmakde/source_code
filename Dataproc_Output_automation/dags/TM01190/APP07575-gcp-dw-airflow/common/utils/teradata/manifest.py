from airflow.operators.bash import BashOperator
from datetime import timedelta


def gen_manifest(
    task_id,
    py_scripts_path,
    s3_path,
    process_date,
    to_process_date,
    s3_manifest_path,
    manifest_name_prefix,
    nauth_conn,
    s3_conn,
    dag,
    s3_date_format="ymdh",
    output_level="both",
    success_folders_only=False,
):

    extra_flags = ""
    if success_folders_only:
        extra_flags = "--exclude_success_file --success_folders_only"

    bash_command = (
        f"python {py_scripts_path}/generate_manifest.py "
        f"--nauth_conn {nauth_conn} "
        f"--s3_conn {s3_conn} "
        f"--s3_path {s3_path} "
        f"--process_date {process_date} "
        f"--to_process_date {to_process_date} "
        f"--s3_manifest_path {s3_manifest_path.rstrip('/')} "
        f"--manifest_name_prefix {manifest_name_prefix}_manifest "
        f"--s3_date_format {s3_date_format} "
        f"--output_level {output_level} "
        f"{extra_flags}"
    )

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        retries=3,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )


def gen_tpt_multireader_manifest(
    task_id, py_scripts_path, s3_path, nauth_conn, s3_conn, dag
):
    return BashOperator(
        task_id=task_id,
        bash_command=f"python {py_scripts_path}/generate_tpt_multireader_manifest.py "
        f"--s3_path {s3_path} "
        f"--nauth_conn {nauth_conn} "
        f"--s3_conn {s3_conn} ",
        xcom_push=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )
