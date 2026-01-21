def build_tpt_load_bash_command(
    job_name,
    control_db_schema,
    teradata_db_host,
    teradata_user,
    source_file_key,
    metamorph_audit_count=0,
    manifest_flag=True,
    fast_load_flag=True,
    audit_flag=True,
):
    source_type_selector = "m" if manifest_flag else "f"
    fast_load_selector = "-o LOAD" if fast_load_flag else ""
    audit_selector = f"-a {metamorph_audit_count}" if audit_flag else ""

    command = (
        f"/db/teradata/bin/tpt_load.sh "
        f"-e {control_db_schema} "
        f"-j {job_name} "
        f"-h {teradata_db_host} "
        f"-l 'N' "
        f"-u {teradata_user} "
        f'-p "\$tdwallet({teradata_user}_PWD)" '
        f"-{source_type_selector} {source_file_key} "
        f"{fast_load_selector} "
        f"-z "
        f"{audit_selector}"
    )
    return command


def build_bteq_load_bash_command(
    teradata_environment, teradata_db_host, teradata_user, ec2_sql_file_path
):
    command = (
        f"/db/teradata/bin/bteq_load.sh "
        f"-e {teradata_environment} "
        f"-h {teradata_db_host} "
        f"-l 'N' "
        f"-u {teradata_user} "
        f'-p "\$tdwallet({teradata_user}_PWD)" '
        f"-f {ec2_sql_file_path}"
    )
    return command


def build_tpt_export_bash_command(
    control_db_schema,
    teradata_db_host,
    teradata_user,
    teradata_environment,
    delimiter,
    split_file_count,
    ec2_sql_file_path,
    s3_path,
    td_utilities_flag=False,
):
    td_utilities_selector = "-r TD-UTILITIES-EC2" if td_utilities_flag else ""
    command = (
        f"sed -i 's/<DBENV>/{teradata_environment}/g' {ec2_sql_file_path}\n"
        f"/db/teradata/bin/tpt_export.sh "
        f"-e {control_db_schema} "
        f"-h {teradata_db_host} "
        f"-l 'N' "
        f"-u {teradata_user} "
        f'-p "\$tdwallet({teradata_user}_PWD)" '
        f"-s {ec2_sql_file_path} "
        f"-a 'Y' "
        f"-d {delimiter} "
        f"-t {s3_path} "
        f"-c {split_file_count} "
        f"-q N "
        f"-f NULL "
        f"{td_utilities_selector}"
    )
    return command


def build_sync_s3_to_ec2_bash_command(s3_sql_path, ec2_sql_path):
    return f"aws s3 sync {s3_sql_path} {ec2_sql_path} --delete --exclude '*' --include '*.sql'"


def build_clear_s3_path_bash_command(s3_path):
    return f"aws s3 rm {s3_path} --recursive"


def build_interpolate_vars_in_sql_command(ec2_sql_path, interpolation_map):
    """
    :param ec2_sql_path: Folder on ec2, where sql files placed
    :param interpolation_map: E.g. {'<s3_src_path>': 's3://some-bucket/key/'},
    '<s3_src_path>' will be replaced with 's3://some-bucket/key/' in all sql files
    :return: String that contains bash commands to replace all vars provided in the interpolation_map
    """
    complete_command = ""
    ec2_sql_path = ec2_sql_path.rstrip("/")
    for placeholder, value in interpolation_map.items():
        interpolation_command = (
            f"for orig_file in {ec2_sql_path}/*.sql;"
            f"do converted_file=${{orig_file}}_converted.sql;"
            f"echo ${{converted_file}};"
            f"sed -e's|{placeholder}|{value}|g' ${{orig_file}} > ${{converted_file}};"
            f"rm ${{orig_file}};"
            f"mv ${{converted_file}} ${{orig_file}};"
            f"done;"
        )
        complete_command += interpolation_command

    return complete_command
