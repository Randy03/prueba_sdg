from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
import requests
from datetime import datetime, timedelta
from airflow.decorators import task
from os import listdir
from os.path import isfile
import time

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
}

with DAG('SDG_pipeline', default_args=default_args, schedule=None,catchup=False) as dag:
    start_spark_cluster = SSHOperator(
        task_id = 'start_spark_cluster',
        command = 'docker compose up -d spark spark-worker',
        ssh_conn_id='ssh_docker_host',
        cmd_timeout=120
    )

    start_hdfs = SSHOperator(
        task_id = 'start_hdfs',
        command = 'docker compose up -d namenode',
        ssh_conn_id='ssh_docker_host',
        cmd_timeout=120
    )

    start_kafka = SSHOperator(
        task_id = 'start_kafka',
        command = 'docker compose up -d broker schema-registry connect control-center ksqldb-server ksql-datagen ksqldb-cli rest-proxy',
        ssh_conn_id='ssh_docker_host',
        cmd_timeout=120
    )

    stop_spark_cluster = SSHOperator(
        task_id = 'stop_spark_cluster',
        command = 'docker compose down spark spark-worker',
        ssh_conn_id='ssh_docker_host',
        cmd_timeout=120,
        trigger_rule="all_done"
    )

    stop_hdfs = SSHOperator(
        task_id = 'stop_hdfs',
        command = 'docker compose down namenode',
        ssh_conn_id='ssh_docker_host',
        cmd_timeout=120,
        trigger_rule="all_done"
    )

    stop_kafka = SSHOperator(
        task_id = 'stop_kafka',
        command = 'docker compose down broker schema-registry connect control-center ksqldb-server ksql-datagen ksqldb-cli rest-proxy',
        ssh_conn_id='ssh_docker_host',
        cmd_timeout=120,
        trigger_rule="all_done"
    )

    

    @task
    def upload_files_to_hdfs():
        time.sleep(30)
        hdfs = WebHDFSHook(
            webhdfs_conn_id='hdfs',
            proxy_user='root'
        )
        input_path = '/opt/airflow/dags/inputs/'
        dst_path= '/data/input/events/person/'
      
        hdfs.load_file(input_path,dst_path)
        hdfs.load_file('/opt/airflow/dags/metadata.json','/data/metadata.json')


    @task
    def download_files_from_hdfs():
        hdfs = WebHDFSHook(
            webhdfs_conn_id='hdfs',
            proxy_user='root'
        )
        client = hdfs.get_conn()
        output = '/data/output/discards/person/'
        files = client.list(output)
        for file in files:
            with client.read(output+file) as reader:
                content = reader.read()
                with open('/opt/airflow/dags/outputs/'+file,'wb') as f:
                    f.write(content)



    spark_app = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/airflow/dags/SDG_spark_app.py',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
        conn_id='spark_default'
    )
    
    [start_spark_cluster, start_hdfs, start_kafka] >> upload_files_to_hdfs() >> spark_app >> download_files_from_hdfs() >>[stop_spark_cluster, stop_hdfs, stop_kafka]
    
