import airflowlib.emr_util as emr

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

def subdag(
    parent_dag_name,
    child_dag_name,
    redshift_conn_id,
    s3_conn_id,
    script_name,
    table_names):
    
    start_date = airflow.utils.dates.days_ago(51)
    default_args = {
        'owner': 'airflow',
        'start_date': start_date,
        'provide_context': True
    }

    dag = DAG(
        f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
    )

    def get_current_process_file(**kwargs):
        """
        Check the previous run from script_log table and 
        Figure out a file name to process in this run
        """
        print(f"Check the previous script run for script: {script_name}")
        redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

        query = """
        SELECT MAX(processed_file)
        FROM script_log
        WHERE script_name = '{script_name}'
        """.format(script_name=script_name)
        records = redshift.get_records(query)

        last_processed_file = records[0][0]
        if last_processed_file is None:
            last_processed_file = -1

        current_process_file = last_processed_file + 1
        return current_process_file
    
    def build_file_name(**kwargs):
        """
        Build the full file path in S3 using variables from Airflow DB
        """
        ti = kwargs['ti']

        current_process_file = ti.xcom_pull(task_ids=f"{child_dag_name}.task_get_current_process_file")
        file_name = "{bucket}/{script_name}/x{current_process_file}".format(
            bucket=Variable.get('INPUT_DATA_S3_BUCKET'),
            script_name=script_name,
            current_process_file=str(current_process_file).zfill(2)
        )

        return file_name

    def check_s3_file(**kwargs):
        """
        Check the file to process in this run is a valid file in S3.
        If the file doesn't exists, the downstream tasks are will be skipped.
        """
        ti = kwargs['ti']
        file_name = ti.xcom_pull(task_ids=f"{child_dag_name}.task_build_file_name")

        print(f"Check the S3 file {file_name} exists")
        isValidFile = S3Hook('s3').check_for_key(f"s3://{file_name}")
        
        print(f"File {file_name} exists: {isValidFile}")
        return isValidFile
    
    def transform_s3_to_parquet(**kwargs):
        """
        Submit a pyspark script to read/transform S3 files and 
        write parquets in the temp S3 bucket.
        """
        ti = kwargs['ti']
        file_name = ti.xcom_pull(task_ids=f"{child_dag_name}.task_build_file_name")
        cluster_dns = Variable.get('SPARK_MASTER_DNS')
        
        headers = emr.create_spark_session(cluster_dns)
        session_url = emr.wait_for_idle_session(cluster_dns, headers)

        try:
            statement_response = emr.submit_statement(
                session_url, 
                f"/usr/local/airflow/dags/transform/{script_name}.py",
                file_name,
                f"{Variable.get('TEMP_FILE_BUCKET')}/temp")
            emr.track_statement_progress(cluster_dns, statement_response.headers)
        finally:
            emr.kill_spark_session(session_url)
    
    def copy_parquet_to_redshift(table_name, **kwargs):
        """
        Copy data to Redshift from temp parquet files in S3
        """
        s3_credential = S3Hook(s3_conn_id).get_credentials()

        print(f"Load {table_name}.parquet to Redshift")
        copy_sql = """
        COPY {table_name}
        FROM 's3://{PARQUET_FOLDER_NAME}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        """.format(
            table_name=table_name,
            PARQUET_FOLDER_NAME=f"{Variable.get('TEMP_FILE_BUCKET')}/temp/{table_name}.parquet",
            IAM_ROLE=Variable.get('IAM_ROLE')
        )
        PostgresHook(redshift_conn_id).run(copy_sql)

    def insert_script_log(**kwargs):
        """
        Write logs about which file is processed in this run 
        to avoid processing the same file again.
        """
        ti = kwargs['ti']

        processed_file = ti.xcom_pull(task_ids=f"{child_dag_name}.task_get_current_process_file")

        print(f"Insert script_log: script_name={script_name}, processed_file={str(processed_file)}")
        PostgresHook(redshift_conn_id).run(f"INSERT INTO script_log VALUES('{script_name}', {str(processed_file)})")
    
    task_get_current_process_file = PythonOperator(
        task_id=f"{child_dag_name}.task_get_current_process_file",
        dag=dag,
        python_callable=get_current_process_file
    )

    task_build_file_name = PythonOperator(
        task_id=f"{child_dag_name}.task_build_file_name",
        dag=dag,
        python_callable=build_file_name
    )

    task_check_s3_file = ShortCircuitOperator(
        task_id=f"{child_dag_name}.task_check_s3_file",
        dag=dag,
        python_callable=check_s3_file
    )

    task_transform_s3_to_parquet = PythonOperator(
        task_id=f"{child_dag_name}.task_transform_s3_to_parquet",
        dag=dag,
        python_callable=transform_s3_to_parquet
    )

    tasks_copy_parquet_to_redshift = []
    for table_name in table_names:
        task_copy_parquet_to_redshift = PythonOperator(
            task_id=f"{child_dag_name}.task_copy_{table_name}_parquet_to_redshift",
            dag=dag,
            python_callable=copy_parquet_to_redshift,
            op_kwargs={
                'table_name': table_name
            }
        )
        tasks_copy_parquet_to_redshift.append(task_copy_parquet_to_redshift)

    task_insert_script_log = PythonOperator(
        task_id=f"{child_dag_name}.task_insert_script_log",
        dag=dag,
        python_callable=insert_script_log
    )

    task_get_current_process_file >> task_build_file_name
    task_build_file_name >> task_check_s3_file
    task_check_s3_file >> task_transform_s3_to_parquet
    task_transform_s3_to_parquet >> tasks_copy_parquet_to_redshift >> task_insert_script_log
    tasks_copy_parquet_to_redshift >> task_insert_script_log

    return dag