import random
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timezone import datetime
from airflow.models import Variable
from airflow.kubernetes.pod_generator import PodDefaults
from kubernetes.client import models as k8s

log = LoggingMixin().log


try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    random.seed()

    default_args = {
        'owner': 'Airflow',
        'start_date': days_ago(1),
        'depends_on_past': False
    }

    dag_prefix="ulm"

    with DAG(
            dag_id='simple-k8s',
            default_args=default_args,
            catchup=False,
            schedule_interval='0 1 * * *'
    ) as dag:

        simplek8s = KubernetesPodOperator(
        namespace='airflow',
        image="hello-world",
        labels={"<pod-label>": "<label-name>"},
        name="airflow-test-pod",
        task_id="simple-k8s",
        in_cluster=True,  # if set to true, will look in the cluster, if false, looks for file
        is_delete_operator_pod=False,
        get_logs=True)


        ad_local_metrics

except ImportError as e:
    log.warning("Could not import KubernetesPodOperator: " + str(e))
    log.warning("Install kubernetes dependencies with: "
                "    pip install 'apache-airflow[kubernetes]'")

