import os
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from nordstrom.utils.setup_module import setup_module
from datetime import timedelta , datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import sys
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from nordstrom.utils.cloud_creds import cloud_creds
import logging
from airflow.operators.dummy import DummyOperator




dag_path = os.path.split(__file__)[0]
common_module_path = (os.path.join(dag_path, "clarity_common/"))
sys.path.append(common_module_path)
# from clarity_common.config_utils import get_env, get_config, get_default_args, get_start_date_with_tz, get_none_if_empty
from config_utils import get_global_config,get_config, get_default_args,get_start_date_with_tz,get_none_if_empty,get_env

# from clarity_common.sensor_utils import get_timeliness_sql_sensor, get_completeness_sql_sensor, get_multiple_completeness_sql_sensor_group
#from clarity_common.teradata_utils import get_jdbc_python_operator_timeliness_macro
from bq_utils import get_jdbc_python_operator_timeliness_macro

# env = get_env()
env = 'nonprod'


dag_config = get_config(dag_path, 'incremental_dag.cfg')
default_args = get_default_args(env)
del default_args['sla']  # we'll set sla and/or execution_timeout manually for each task
sql_path = (os.path.join(dag_path, "scripts/"))
config = get_global_config()
project_id = config.get(env,'bq_project_id')
gcp_conn = config.get(env,'gcp_conn_id')
gcp_region = config.get(env,'location')
hook = BigQueryHook(gcp_conn_id=gcp_conn,location=gcp_region, use_legacy_sql=False)
parameters = {'DBENV': config.get(env, 'bq_environment'), 'DBJWNENV': config.get(env, 'bq_jwn_environment'),'bq_project_id' : project_id}

bigquery_environment = config.get(env,'bigquery_environment')
nauth_conn = config.get(env,'nauth_conn_id')
# service_account_email = config.get(env,'bq_dataplex_service_account_email')
service_account_email='airflow-gcp-onix-testing@jwn-nap-user1-nonprod-zmnb.iam.gserviceaccount.com'


def setup_creds_bq(nauth_conn_id, cloud_conn_id, bq_service_account_email):
    @cloud_creds(
        nauth_conn_id=nauth_conn_id,
        cloud_conn_id=cloud_conn_id,
        service_account_email=bq_service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()


with DAG(
        dag_id='demand_and_op_gmv',
        default_args=default_args,
        start_date=get_start_date_with_tz(),
        schedule_interval=get_none_if_empty(dag_config, env, 'schedule_interval'),
        tags=["order", "record_source_o", "sales", "record_source_s", "tran-fact"],
        catchup=False
) as dag:

    creds_setup_bq = PythonOperator(
        task_id='setup_creds_bq',
        python_callable=setup_creds_bq,
        op_kwargs={
            'nauth_conn_id': nauth_conn,
            'cloud_conn_id': gcp_conn,
            'bq_service_account_email': service_account_email
        }
    )

    # creating dummy task for lineage

    curation_fact_sensor = DummyOperator(task_id=f'curation_fact_sensor', dag=dag)
    curation_order_line_fact_sensor = DummyOperator(task_id=f'curation_order_line_fact_sensor', dag=dag)
    order_line_detail_fact_sensor = DummyOperator(task_id=f'order_line_detail_fact_sensor', dag=dag)
    oldf_completeness_group = DummyOperator(task_id=f'oldf_completeness_group', dag=dag)
    cf_compeleteness = DummyOperator(task_id=f'cf_compeleteness', dag=dag)
    colf_compeleteness = DummyOperator(task_id=f'colf_compeleteness', dag=dag)
    retail_tran_hdr_fact_sensor = DummyOperator(task_id=f'retail_tran_hdr_fact_sensor', dag=dag)
    retail_tran_tender_fact_sensor = DummyOperator(task_id=f'retail_tran_tender_fact_sensor', dag=dag)
    retail_tran_detail_fact_sensor = DummyOperator(task_id=f'retail_tran_detail_fact_sensor', dag=dag)
    hdr_completeness_sensor = DummyOperator(task_id=f'hdr_completeness_sensor', dag=dag)
    dtl_completeness_sensor = DummyOperator(task_id=f'dtl_completeness_sensor', dag=dag)
    tender_completeness_sensor = DummyOperator(task_id=f'tender_completeness_sensor', dag=dag)


    # Sensors
    # curation_fact_sensor = get_timeliness_sql_sensor(
    #     table_name='CURATION_FACT',
    #     db_name='<DBENV>_NAP_FCT',
    #     batch_date_offset='+ 1',
    #     sla=timedelta(minutes=60)
    # )
    # curation_order_line_fact_sensor = get_timeliness_sql_sensor(
    #     table_name='CURATION_ORDER_LINE_FACT',
    #     db_name='<DBENV>_NAP_FCT',
    #     batch_date_offset='+ 1',
    #     sla=timedelta(minutes=60)
    # )
    # order_line_detail_fact_sensor = get_timeliness_sql_sensor(
    #     table_name='ORDER_LINE_DETAIL_FACT',
    #     db_name='<DBENV>_NAP_FCT',
    #     batch_date_offset='+ 1',
    #     sla=timedelta(minutes=60)
    # )
    # oldf_completeness_group = get_multiple_completeness_sql_sensor_group(
    #     table_name='OLDF',
    #     dq_id_variance_mapping={
    #         'ollc-orderline-canceled-presto': 10,
    #         'ollc-order-submitted-presto': 10,
    #         'ollc-picked-up-by-customer-presto': 10,
    #         'ollc-orderline-fulfilled-presto': 10
    #     },
    #     sla=timedelta(minutes=60)
    # )
    # cf_compeleteness = get_completeness_sql_sensor(
    #     dq_id='customer-curation',
    #     allowed_variance=10,
    #     sla=timedelta(minutes=60)
    # )
    # colf_compeleteness = get_completeness_sql_sensor(
    #     dq_id='customer-curation-order-line',
    #     allowed_variance=10,
    #     sla=timedelta(minutes=60)
    # )

    # retail_tran_hdr_fact_sensor = get_timeliness_sql_sensor(
    #     table_name='RETAIL_TRAN_HDR_FACT',
    #     db_name='<DBENV>_NAP_FCT',
    #     sla=timedelta(minutes=60)
    # )
    # retail_tran_tender_fact_sensor = get_timeliness_sql_sensor(
    #     table_name='RETAIL_TRAN_TENDER_FACT',
    #     db_name='<DBENV>_NAP_FCT',
    #     sla=timedelta(minutes=60)
    # )
    # retail_tran_detail_fact_sensor = get_timeliness_sql_sensor(
    #     table_name='RETAIL_TRAN_DETAIL_FACT',
    #     db_name='<DBENV>_NAP_FCT',
    #     sla=timedelta(minutes=60)
    # )
    # hdr_completeness_sensor = get_completeness_sql_sensor(
    #     dq_id='retail-transaction-header-compare',
    #     allowed_variance=10,
    #     sla=timedelta(minutes=60)
    # )
    # dtl_completeness_sensor = get_completeness_sql_sensor(
    #     dq_id='retail-transaction-item-compare',
    #     allowed_variance=10,
    #     sla=timedelta(minutes=60)
    # )
    # tender_completeness_sensor = get_completeness_sql_sensor(
    #     dq_id='retail-transaction-tender-compare',
    #     allowed_variance=10,
    #     sla=timedelta(minutes=60)
    # )

    # JDBC Operators
    script_order_batch_update = open(
        os.path.join(sql_path, 'order_batch_update.sql'),
        "r").read()

    order_batch_update = BigQueryInsertJobOperator(
        task_id="order_batch_update",
        configuration={
            "query": {
                "query": script_order_batch_update,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_order_keys_wrk = open(
        os.path.join(sql_path, 'order_keys_wrk_load_to_stg.sql'),
        "r").read()

    order_keys_wrk = BigQueryInsertJobOperator(
        task_id="order_keys_wrk",
        configuration={
            "query": {
                "query": script_order_keys_wrk,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_order_acp_wrk_load_to_stg = open(
        os.path.join(sql_path, 'order_acp_wrk_load_to_stg.sql'),
        "r").read()

    order_acp_wrk = BigQueryInsertJobOperator(
        task_id="order_acp_wrk",
        configuration={
            "query": {
                "query": script_order_acp_wrk_load_to_stg,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_order_por_wrk_load_to_stg = open(
        os.path.join(sql_path, 'order_por_wrk_load_to_stg.sql'),
        "r").read()

    order_por_wrk = BigQueryInsertJobOperator(
        task_id="order_por_wrk",
        configuration={
            "query": {
                "query": script_order_por_wrk_load_to_stg,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_order_dq = open(
        os.path.join(sql_path, 'order_dq.sql'),
        "r").read()

    order_dq_check = BigQueryInsertJobOperator(
        task_id="order_dq_check",
        configuration={
            "query": {
                "query": script_order_dq,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_sales_batch_update = open(
        os.path.join(sql_path, 'sales_batch_update.sql'),
        "r").read()

    sales_batch_update = BigQueryInsertJobOperator(
        task_id="sales_batch_update",
        configuration={
            "query": {
                "query": script_sales_batch_update,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_sales_keys_load_to_stg = open(
        os.path.join(sql_path, 'sales_keys_load_to_stg.sql'),
        "r").read()

    sales_keys_load = BigQueryInsertJobOperator(
        task_id="sales_keys_wrk",
        configuration={
            "query": {
                "query": script_sales_keys_load_to_stg,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_sales_return_xref_ld = open(
        os.path.join(sql_path, 'sales_return_xref_ld.sql'),
        "r").read()

    sales_return_xref_load = BigQueryInsertJobOperator(
        task_id="sales_return_xref_wrk",
        configuration={
            "query": {
                "query": script_sales_return_xref_ld,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_sales_load_to_stg = open(
        os.path.join(sql_path, 'sales_load_to_stg.sql'),
        "r").read()

    sales_transaction_load = BigQueryInsertJobOperator(
        task_id="sales_wrk",
        configuration={
            "query": {
                "query": script_sales_load_to_stg,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_sales_loyalty_load_to_stg = open(
        os.path.join(sql_path, 'sales_loyalty_load_to_stg.sql'),
        "r").read()

    sales_loyalty_load = BigQueryInsertJobOperator(
        task_id="sales_loyalty_wrk",
        configuration={
            "query": {
                "query": script_sales_loyalty_load_to_stg,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_sales_gcb_load_to_stg = open(
        os.path.join(sql_path, 'sales_gcb_load_to_stg.sql'),
        "r").read()

    sales_gcb_load = BigQueryInsertJobOperator(
        task_id="sales_gcb_wrk",
        configuration={
            "query": {
                "query": script_sales_gcb_load_to_stg,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_sales_load_to_fact = open(
        os.path.join(sql_path, 'sales_load_to_fact.sql'),
        "r").read()

    sales_fact_load = BigQueryInsertJobOperator(
        task_id="sales_fact",
        configuration={
            "query": {
                "query": script_sales_load_to_fact,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_sales_dq = open(
        os.path.join(sql_path, 'sales_dq.sql'),
        "r").read()

    sales_dq_check = BigQueryInsertJobOperator(
        task_id="sales_dq_check",
        configuration={
            "query": {
                "query": script_sales_dq,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_jctf_batch_update = open(
        os.path.join(sql_path, 'jctf_batch_update.sql'),
        "r").read()

    jctf_batch_update = BigQueryInsertJobOperator(
        task_id="jctf_batch_update",
        configuration={
            "query": {
                "query": script_jctf_batch_update,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_jctf_wrk_sales_orders_ld = open(
        os.path.join(sql_path, 'jctf_wrk_sales_orders_ld.sql'),
        "r").read()

    jctf_wrk = BigQueryInsertJobOperator(
        task_id="jctf_wrk",
        configuration={
            "query": {
                "query": script_jctf_wrk_sales_orders_ld,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_jctf_fact_sales_orders_ld = open(
        os.path.join(sql_path, 'jctf_fact_sales_orders_ld.sql'),
        "r").read()

    jctf_fct = BigQueryInsertJobOperator(
        task_id="jctf_fct",
        configuration={
            "query": {
                "query": script_jctf_fact_sales_orders_ld,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_jctf_tender_type_fct_ld = open(
        os.path.join(sql_path, 'jctf_tender_type_fct_ld.sql'),
        "r").read()

    jctf_tender_fct = BigQueryInsertJobOperator(
        task_id="jctf_tender_fct",
        configuration={
            "query": {
                "query": script_jctf_tender_type_fct_ld,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    script_jctf_dq = open(
        os.path.join(sql_path, 'jctf_dq.sql'),
        "r").read()

    jctf_dq_check = BigQueryInsertJobOperator(
        task_id="jctf_dq_check",
        configuration={
            "query": {
                "query": script_jctf_dq,
                "useLegacySql": False
            }
        },
        project_id=project_id,
        gcp_conn_id=gcp_conn,
        params=parameters,
        location=gcp_region,
        dag=dag
    )

    # Start and End Timeliness Tasks
    with TaskGroup(group_id='load_start') as load_start:
        tables_start = get_jdbc_python_operator_timeliness_macro(
            task_id='tables_start',
            table_names=['JWN_CLARITY_TRANSACTION_FACT'],
            db_name='<DBENV>_NAP_JWN_METRICS_FCT',
            dag_id=dag.dag_id,
            step_id='1',  # step sequence number of this task
            execution_timeout=timedelta(minutes=30),
            bqhook = hook
        )

        views_start = get_jdbc_python_operator_timeliness_macro(
            task_id='views_start',
            table_names=['JWN_DEMAND_METRIC_VW',
                         'JWN_DEMAND_METRIC_HIST_VW',
                         'JWN_OPERATIONAL_GMV_METRIC_VW',
                         'JWN_OPERATIONAL_GMV_METRIC_HIST_VW',
                         'JWN_GROSS_MARGIN_METRIC_VW',
                         'JWN_GROSS_MARGIN_METRIC_HIST_VW',
                         'JWN_CONTRIBUTION_MARGIN_METRIC_VW',
                         'JWN_CONTRIBUTION_MARGIN_METRIC_HIST_VW'],
            db_name='<DBENV>_NAP_JWN_METRICS_BASE_VWS',
            dag_id=dag.dag_id,
            step_id='1',  # step sequence number of this task
            execution_timeout=timedelta(minutes=30),
            bqhook=hook
        )

    with TaskGroup(group_id='load_end') as load_end:
        tables_end = get_jdbc_python_operator_timeliness_macro(
            task_id='tables_end',
            table_names=['JWN_CLARITY_TRANSACTION_FACT', ],
            db_name='<DBENV>_NAP_JWN_METRICS_FCT',
            dag_id=dag.dag_id,
            step_id='9',  # step sequence number of this task
            execution_timeout=timedelta(minutes=30),
            bqhook=hook
        )

        views_end = get_jdbc_python_operator_timeliness_macro(
            task_id='views_end',
            table_names=['JWN_DEMAND_METRIC_VW',
                         'JWN_DEMAND_METRIC_HIST_VW',
                         'JWN_OPERATIONAL_GMV_METRIC_VW',
                         'JWN_OPERATIONAL_GMV_METRIC_HIST_VW'],
            db_name='<DBENV>_NAP_JWN_METRICS_BASE_VWS',
            dag_id=dag.dag_id,
            step_id='9',  # step sequence number of this task
            execution_timeout=timedelta(minutes=30),
            bqhook = hook
        )

    creds_setup_bq >> curation_fact_sensor >> cf_compeleteness >> [load_start, order_batch_update]
    creds_setup_bq >> curation_order_line_fact_sensor >> colf_compeleteness >> [load_start, order_batch_update]
    creds_setup_bq >> order_line_detail_fact_sensor >> oldf_completeness_group >> [load_start,order_batch_update]
    order_batch_update >> order_keys_wrk >> sales_return_xref_load >> order_acp_wrk >> order_por_wrk >> order_dq_check

    creds_setup_bq >> retail_tran_hdr_fact_sensor >> hdr_completeness_sensor >> [load_start, sales_batch_update]
    creds_setup_bq >> retail_tran_tender_fact_sensor >> tender_completeness_sensor >> [load_start, sales_batch_update]
    creds_setup_bq >> retail_tran_detail_fact_sensor >> dtl_completeness_sensor >> [load_start, sales_batch_update]
    sales_batch_update >> sales_keys_load >> sales_return_xref_load >> sales_transaction_load >> sales_fact_load >> sales_loyalty_load >> sales_gcb_load >> sales_dq_check

    [order_por_wrk, sales_gcb_load] >> jctf_batch_update >> jctf_wrk >> jctf_fct >> jctf_tender_fct >> [load_end,jctf_dq_check]
    [order_por_wrk, sales_gcb_load] >> jctf_fct