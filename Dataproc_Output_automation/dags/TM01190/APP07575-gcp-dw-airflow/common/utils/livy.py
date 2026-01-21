from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


def get_metamorph_trigger_task(
    task_id,
    project_id,
    location,
    conn_id,
    metamorph_jar_path,
    app_class_name,
    support_libs_path,
    config_path,
    config_file_name,
    executor_cores,
    executor_memory,
    batch_id,
    subnet_url,
    service_account_email,
    dag,
    year=None,
    month=None,
    day=None,
    hour=None,
):
    jars = [
        f"{metamorph_jar_path}",
        f"{support_libs_path}/config-1.2.1.jar",
        f"{support_libs_path}/core-HEAD-e7a2aa0.jar",
        f"{support_libs_path}/aws-encryption-sdk-java-1.3.6.jar",
        f"{support_libs_path}/tdgssconfig-16.10.05.00.jar",
        f"{support_libs_path}/logging-HEAD-179f855.jar",
        f"{support_libs_path}/spark-sql-kafka-0-10_2.11-2.4.0.jar",
        f"{support_libs_path}/log4j-api-2.17.0.jar",
        f"{support_libs_path}/kafka-schema-registry-5.2.1.jar",
        f"{support_libs_path}/kafka-schema-registry-client-5.2.1.jar",
        f"{support_libs_path}/kafka-avro-serializer-5.2.2.jar",
        f"{support_libs_path}/common-config-5.2.1.jar",
        f"{support_libs_path}/rest-utils-5.2.1.jar",
        f"{support_libs_path}/common-metrics-5.2.1.jar",
        f"{support_libs_path}/common-utils-5.2.1.jar",
        f"{support_libs_path}/kafka-clients-2.3.0.jar",
        f"{support_libs_path}/kafka_2.12-2.3.0.jar",
        f"{support_libs_path}/java-dogstatsd-client-2.5.jar",
        f"{support_libs_path}/terajdbc4-16.10.05.00.jar",
    ]
    return DataprocCreateBatchOperator(
        task_id=task_id,
        project_id=project_id,
        region=location,
        gcp_conn_id=conn_id,
        batch={
            "runtime_config": {
                "version": "1.1",
                "properties": {
                    "spark.executor.memory": "{}".format(executor_memory),
                    "spark.executor.cores": "{}".format(executor_cores),
                    "spark.hadoop.fs.s3a.fast.upload": "true",
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.coalesce.trigger": "true",
                    "spark.sql.caseSensitive": "false",
                    "spark.driver.extraJavaOptions": f"-Dyear={year} -Dmonth={month} -Dday={day} -Dhour={hour}",
                    "spark.sql.avro.datetimeRebaseModeInRead": "LEGACY",
                    "spark.bigquery.viewsEnabled": "true",
                },
            },
            "spark_batch": {
                "main_class": app_class_name,
                "jar_file_uris": jars,
                "file_uris": [
                    f"{config_path}/{config_file_name}.json",
                    f"{config_path}/{config_file_name}.conf",
                ],
                "args": [f"{config_file_name}.conf"],
            },
            "environment_config": {
                "execution_config": {
                    "service_account": service_account_email,
                    "subnetwork_uri": subnet_url,
                }
            },
        },
        batch_id=batch_id,
        dag=dag,
    )
