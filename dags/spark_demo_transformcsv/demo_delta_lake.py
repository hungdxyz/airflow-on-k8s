import airflow
from datetime import timedelta,datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

spark_dag = DAG(
        dag_id = "demo_delta_lake",
        default_args=default_args,
        schedule_interval=None,
        dagrun_timeout=timedelta(minutes=60),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

now = datetime.now()

# Format the timestamp, replace "T" with "-", and convert it to lowercase
ts_nodash = now.isoformat().replace("T", "-").replace(":", "-").lower()

spark_app_pi_yaml = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: "sparkpi-app-{{ts_nodash}}-task"
  namespace: dpaas-spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "bacnv/spark:3.5.1-scala2.12-java11-python3-r-ubuntu-02"
  imagePullPolicy: Always
  volumes:
    - name: s3mount
      persistentVolumeClaim:
        claimName: s3mount
    - name: venv-lib
      emptyDir: {}
  mainApplicationFile: "local:///mnt/s3mount/demo-deltalake/demo-deltalake-job.py"
  sparkVersion: "3.5.1"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "4096m"
    labels:
      version: 3.5.1
    serviceAccount: spark
    volumeMounts:
          - name: s3mount
            mountPath: /mnt/s3mount
          - name: venv-lib
            mountPath: /mnt/venv-lib
    env:
        - name: PYTHONPATH
          value: /mnt/venv-lib
    initContainers:
      - name: install-requirements
        image: "bacnv/spark:3.5.1-scala2.12-java11-python3-r-ubuntu-root-0.0.1"
        command: ["pip", "install","--target=/mnt/venv-lib", "-r", "/mnt/s3mount/demo-deltalake/requirements.txt"]
        # command: ["pip", "--version"]
        volumeMounts:
          - name:  s3mount
            mountPath: /mnt/s3mount
          - name: venv-lib
            mountPath: /mnt/venv-lib
  executor:
    cores: 1
    instances: 1
    memory: "2048m"
    labels:
      version: 3.5.1
    volumeMounts:
          - name: s3mount
            mountPath: /mnt/s3mount
          - name: venv-lib
            mountPath: /mnt/venv-lib
  sparkConf:
    "spark.kubernetes.driver.pod.deleteOnTermination": "true"
    # "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.4.0"
    # "spark.hadoop.fs.s3a.access.key": "WLV1CWMRPARS4IYIL3AE"  # Replace with your AWS access key
    # "spark.hadoop.fs.s3a.secret.key": "aZtcDCmLRBetHYTQeuVdpHmbDgIyj2hOEWaQcn3J"  # Replace with your AWS secret key
    # "spark.hadoop.fs.s3a.endpoint": "s3.ap-southeast-1.wasabisys.com"
"""


spark_app_yaml = spark_dag.get_template_env().from_string(spark_app_pi_yaml)

rendered_spark_app_yaml = spark_app_yaml.render(ts_nodash=ts_nodash)

# rendered_spark_app_yaml = spark_app_yaml.render()



# Define the KubernetesPodOperator task
submit_spark_job = KubernetesPodOperator(
    kubernetes_conn_id='kubernetes_default',
    namespace='dpaas-spark',
    name="spark-job-submit-sparkpi-app-operator",
    task_id="submit_spark_job",
    cmds=["/bin/bash", "-c"],
    arguments=[f"echo '{rendered_spark_app_yaml}' | kubectl apply -f -"],
    # image="ghcr.io/googlecloudplatform/spark-operator:v1beta2-1.3.8-3.1.1",
    image="bacnv/spark:3.5.1-scala2.12-java11-python3-r-ubuntu-02",
    # image="apache/spark:3.5.1-scala2.12-java11-python3-r-ubuntu",
    get_logs=True,
    is_delete_operator_pod=True,
    service_account_name="spark",
    do_xcom_push=False,
    dag=spark_dag,
    # executor_config = executor_config
)

# k = KubernetesPodOperator(
#     name="hello-dry-run",
#     image="debian",
#     cmds=["bash", "-cx"],
#     arguments=["echo", "10"],
#     labels={"foo": "bar"},
#     task_id="dry_run_demo",
#     do_xcom_push=True,
# )
# k.dry_run()
# Set the order of tasks
submit_spark_job

# Define the DAG
if __name__ == "__main__":
    spark_dag.cli()
