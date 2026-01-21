import os,sys
import logging
import pandas.io.sql as psql

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# from nordstrom.utils.setup_module import setup_module
from nordstrom.utils.cloud_creds import cloud_creds

dag_path = os.path.dirname(os.path.abspath(__file__))
# common_module_path = os.path.join(dag_path, "../../common/__init__.py")
# setup_module(
#     module_name="common",
#     file_path=common_module_path,
# )
sys.path.append(os.path.normpath(os.path.join(dag_path,'../')))

from common.bq_utils import  get_jdbc_python_operator, set_s3_filename
from common.config_utils import  get_env,get_config,get_global_config, get_default_args, get_start_date_with_tz, get_none_if_empty, get_config_value, get_config_replacements

env = get_env()


config = get_global_config()
bq_project_id = config.get(env, 'bq_project_id')
aws_conn_id = config.get(env, 'aws_conn_id')
dest_gcs = config.get(env, 'dest_gcs')
s3bucket = config.get(env, 's3bucket')
gcp_conn_id = config.get(env, 'gcp_conn_id')
csv_file_path=config.get(env,'csv_file_path')

path, project_id = os.path.split(dag_path)
dag_config = get_config(dag_path, project_id + '.cfg')
default_args = get_default_args()
sql_folder = f'{dag_path}/scripts/'
app_id = get_config_value('app_id')
replacements = {'<App_ID>': app_id, '<DAG_ID>': project_id, '<project_id>': project_id,'<bq_project_id>': bq_project_id}
replacements = get_config_replacements(replacements, [], dag_config)
dl_env = replacements['<dl_env>']
dbenv = replacements['<dl_env>']
replacements['<DBENV>'] = replacements['<dl_env>']
subject_id = replacements['<subject_id>']
service_account_email='airflow-gcp-onix-testing@jwn-nap-user1-nonprod-zmnb.iam.gserviceaccount.com'
config = get_global_config()
conn_name = config.get(env, 'teradata_jdbc_connection')
location_bq = config.get(env, 'location_bq')
hook=BigQueryHook(gcp_conn_id=conn_name,location=location_bq, use_legacy_sql=False)
email='prashant.ahire@nordstrom.com'

if dl_env.upper() == 'T3DL':
    default_args['email_on_failure'] = False

def print_filename(s3_filename,suffix=None):
    print(f'''***************************************************
          Data file name in S3 is : {s3_filename}
          Suffix is :{suffix}
          ******************************************************''')

def xcom_pull_template(task_id):
    return f"task_instance.xcom_pull(task_ids='{task_id}')"

def setup_creds(service_account_email: str):
    @cloud_creds(
        nauth_conn_id='nauth-connection-nonprod',
        cloud_conn_id='gcp-onix-connection-nonprod',
        service_account_email=service_account_email,
    )
    def setup_credential():
        logging.info("GCP connection is set up")

    setup_credential()

def setup_creds_aws(aws_iam_role_arn: str):
    @cloud_creds(
        nauth_conn_id="nauth-connection-nonprod",
        cloud_conn_id="aws_default",
        aws_iam_role_arn="arn:aws:iam::290445963451:role/gcp_data_transfer",
    )
    def setup_credential():
        logging.info("AWS connection is set up")

    setup_credential()

with DAG(
    dag_id=f"gcp_{dl_env}_{project_id}_1",
    default_args=default_args,
    start_date=get_start_date_with_tz(),
    schedule_interval=get_none_if_empty(dag_config, env, 'schedule_interval'),
    catchup=False
) as dag:

    creds_setup = PythonOperator(
        task_id="setup_creds",
        python_callable=setup_creds,
        op_args=[service_account_email],
    )

    creds_setup_aws = PythonOperator(
        task_id="setup_creds_aws",
        python_callable=setup_creds_aws,
        op_args=["arn:aws:iam::290445963451:role/gcp_data_transfer"],
    )

    s3_filename = f"{{{{ {xcom_pull_template('set_s3_filename')} }}}}"
    #suffix = 'year'+f"{{{{ {xcom_pull_template('set_s3_filename')}.strip().split('/year')[1].split('/part')[0] }}}}"
    suffix = ''
    start_load = DummyOperator(task_id=f'{project_id}_load_start', dag=dag)
    batch_start = get_jdbc_python_operator(hook, f'{project_id}_batch_start', f'{sql_folder}batch_start.sql', replacements,'US')
    set_params = get_jdbc_python_operator(hook, f'{project_id}_set_params', f'{sql_folder}set_params.sql', replacements,'US')
    set_s3_filename1 = set_s3_filename(hook, 'set_s3_filename', replacements)

    s3_to_stg = PythonOperator(
        task_id=f'{project_id}_s3_to_stg',
        python_callable=print_filename,
        op_kwargs={
            's3_filename': s3_filename,
            'suffix':suffix
        }
    )

    s3_to_gcs = S3ToGCSOperator(
        task_id="s3_to_gcs",
        aws_conn_id=aws_conn_id,
        bucket=s3bucket,
        prefix = 'wage_rate',
        gcp_conn_id=gcp_conn_id,
        dest_gcs=dest_gcs,
        replace=False,
        gzip=True,
        dag=dag,
    )
    #BQ load Target table Details
    customer_add_schema='t2dl_sca_prf'
    customer_add_obj_name='wage_rate_stg'

    gcs_to_stg = BigQueryInsertJobOperator(
    task_id='gcs_to_stg',
    configuration={
        'load': {
            'sourceUris':csv_file_path,
            'destinationTable': {
                'projectId': bq_project_id,
                'datasetId': customer_add_schema,
                'tableId': customer_add_obj_name
            },
            'sourceFormat': 'CSV',
            'writeDisposition': 'WRITE_TRUNCATE',
        }
    },
    gcp_conn_id=gcp_conn_id,
    dag=dag,
    )
    
    stg_to_opr = get_jdbc_python_operator(hook, f'{project_id}_stg_to_opr', f'{sql_folder}opr_ld.sql', replacements,'US')
    opr_to_fct = get_jdbc_python_operator(hook, f'{project_id}_opr_to_fct', f'{sql_folder}fct_ld.sql', replacements,'US')
    batch_end = get_jdbc_python_operator(hook, f'{project_id}_batch_end', f'{sql_folder}batch_end.sql', replacements,'US')
    end_load = DummyOperator(task_id=f'{project_id}_load_end', dag=dag)

start_load >> creds_setup >> batch_start >> set_params >> set_s3_filename1 >> creds_setup_aws >> s3_to_stg >> s3_to_gcs >> gcs_to_stg >> stg_to_opr >> opr_to_fct >> batch_end >> end_load
