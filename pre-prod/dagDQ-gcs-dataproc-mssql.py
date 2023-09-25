
from datetime import datetime, timedelta
from airflow import models
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 22),
}

with models.DAG(
        'my_dataproc_dag',
        schedule_interval=timedelta(minutes=15),
        default_args=default_args,
        catchup=False) as dag:

    create_dataproc_cluster = DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='my-cluster',
        project_id='focused-evening-397008',
        storage_bucket='19sep',
        zone='us-central1-a',
        master_machine_type='n1-standard-4',
        image_version='1.5-debian10',
        num_workers=0,  # สร้าง single node cluster
        num_preemptible_workers=0,  # ไม่มี preemptible worker nodes
        region='us-central1',
        properties={"spark:spark.jars.packages": "com.microsoft.azure:spark-mssql-connector:1.0.2",
                    "dataproc:dataproc.conscrypt.provider.enable": "false"}  # เพิ่ม properties ที่นี่
    )

    run_pyspark_job = DataProcPySparkOperator(
        task_id='run_pyspark_job',
        main='gs://19sep/read-raw_csv-to-validAccuracy.py',
        cluster_name='my-cluster',
        region='us-central1',
    )

    delete_dataproc_cluster = DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        project_id='focused-evening-397008',
        cluster_name='my-cluster',
        region='us-central1',
        trigger_rule=TriggerRule.ALL_DONE
    )

    create_dataproc_cluster >> run_pyspark_job >> delete_dataproc_cluster
