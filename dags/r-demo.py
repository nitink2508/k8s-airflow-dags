from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from kubernetes.client import models as k8s
image_name = Variable.get("image")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'r_hello_world', default_args=default_args, schedule_interval=timedelta(minutes=10))

volume_mount = k8s.V1VolumeMount(
    name="test-volume", mount_path="/home/r-environment", sub_path=None, read_only=True
)
volume = k8s.V1Volume(
    name="test-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="test-volume"),
)
local_code_path = "/Users/jani/Downloads/r-script/"
start = DummyOperator(task_id='start', dag=dag)

# passing = KubernetesPodOperator(namespace='airflow',
#                           image="nitinkalyankerdev/r-demo:latest",
#                           #cmds=["Rscript","script.R"],
#                           labels={"foo": "bar"},
#                           name="r-test",
#                           task_id="r-task",
#                           get_logs=True,
#                           image_pull_policy='Always',
#                           in_cluster=True,
#                           hostnetwork=True,
#                           dag=dag
#                           )
# passing = KubernetesPodOperator(namespace='airflow',
#                           image=image_name,
#                           #cmds=["Rscript","script.R"],
#                           #image_pull_secrets="regcred",
#                           image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
#                           labels={"foo": "bar"},
#                           name="r-test",
#                           task_id="r-task",
#                           get_logs=True,
#                           image_pull_policy='Always',
#                           in_cluster=True,
#                           hostnetwork=True,
#                           dag=dag
#                           )

passing = KubernetesPodOperator(namespace='airflow',
                          image=image_name,
                          #cmds=["Rscript","script.R"],
                          #image_pull_secrets="regcred",
                          image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
                          labels={"foo": "bar"},
                          name="r-test",
                          task_id="r-task",
                          get_logs=True,
                          image_pull_policy='Always',
                          in_cluster=True,
                          hostnetwork=True,
                          volumes=[volume,k8s.V1Volume(name="code-volume", host_path=k8s.V1HostPathVolumeSource(path=local_code_path))],
                          volume_mounts=[volume_mount,k8s.V1VolumeMount(mount_path="/home/r-environment", name="code-volume")],
                          dag=dag
                          )



end = DummyOperator(task_id='end', dag=dag)

start >> passing >> end
