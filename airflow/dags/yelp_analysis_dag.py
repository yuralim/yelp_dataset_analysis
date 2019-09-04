import airflowlib.emr_util as emr
from yelp_analysis_subdag import subdag

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

DAG_NAME = 'yelp_analysis_dag'
REDSHIFT_CONN_ID = 'redshift'
S3_CONN_ID = 's3'

start_date = airflow.utils.dates.days_ago(51)
default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'catchup' : True,
    'provide_context': True,
    'wait_for_downstream' : True
}

dag = DAG(
    dag_id = DAG_NAME,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)

def clean_temp_s3(**kwargs):
    """Clean S3 bucket where temp files will be saved"""
    s3_conn = S3Hook(S3_CONN_ID)

    temp_keys = s3_conn.list_keys(Variable.get('TEMP_FILE_BUCKET'), prefix='temp')
    print("Delete temp S3 files: " + str(temp_keys))

    if(temp_keys):
        S3Hook(S3_CONN_ID).delete_objects(
            bucket=Variable.get('TEMP_FILE_BUCKET'),
            keys=temp_keys)

### Airflow tasks
start_operator = DummyOperator(
    task_id='begin_execution', 
    dag=dag
)

task_transform_business_to_redshift = SubDagOperator(
    task_id='task_transform_business_to_redshift',
    subdag=subdag(
        parent_dag_name=DAG_NAME,
        child_dag_name='task_transform_business_to_redshift',
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        script_name='business',
        table_names=['business', 'business_category']),
    dag=dag
)

task_transform_checkin_to_redshift = SubDagOperator(
    task_id='task_transform_checkin_to_redshift',
    subdag=subdag(
        parent_dag_name=DAG_NAME,
        child_dag_name='task_transform_checkin_to_redshift',
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        script_name='checkin',
        table_names=['checkin']),
    dag=dag
)

task_transform_user_to_redshift = SubDagOperator(
    task_id='task_transform_user_to_redshift',
    subdag=subdag(
        parent_dag_name=DAG_NAME,
        child_dag_name='task_transform_user_to_redshift',
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        script_name='user',
        table_names=['yelp_user', 'yelp_user_elite', 'yelp_user_friend']),
    dag=dag
)

task_transform_review_to_redshift = SubDagOperator(
    task_id='task_transform_review_to_redshift',
    subdag=subdag(
        parent_dag_name=DAG_NAME,
        child_dag_name='task_transform_review_to_redshift',
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        script_name='review',
        table_names=['review', 'review_text']),
    dag=dag
)

task_transform_tip_to_redshift = SubDagOperator(
    task_id='task_transform_tip_to_redshift',
    subdag=subdag(
        parent_dag_name=DAG_NAME,
        child_dag_name='task_transform_tip_to_redshift',
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        script_name='tip',
        table_names=['tip', 'tip_text']),
    dag=dag
)

tasks_tranform_to_redshift = [
    task_transform_business_to_redshift,
    task_transform_checkin_to_redshift,
    task_transform_user_to_redshift,
    task_transform_review_to_redshift,
    task_transform_tip_to_redshift
]

task_clean_temp_s3 = PythonOperator(
    task_id='task_clean_temp_s3',
    dag=dag,
    python_callable=clean_temp_s3
)

end_operator = DummyOperator(
    task_id='stop_execution',
    dag=dag,
    trigger_rule='none_failed'
)

### Airflow task dependencies
start_operator >> task_clean_temp_s3
task_clean_temp_s3 >> tasks_tranform_to_redshift
tasks_tranform_to_redshift >> end_operator