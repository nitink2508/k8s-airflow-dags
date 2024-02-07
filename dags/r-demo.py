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
    'r_hello_world', default_args=default_args, schedule_interval=timedelta(minutes=200))

# local_scripts_path = "/Users/jani/Downloads/r-script/"
# volume_mount = k8s.V1VolumeMount(
#     name="test-volume", mount_path="/opt/airflow/scripts", sub_path=None, read_only=True
# )
# volume = k8s.V1Volume(
#     name="test-volume",
#     host_path=k8s.V1HostPathVolumeSource(path=local_scripts_path)
# )
# local_scripts_path = "/opt/airflow/scripts"

# volume_mount = k8s.V1VolumeMount(
#     name="local-scripts-volume", mount_path=local_scripts_path, sub_path=None, read_only=True
# )

# volume = k8s.V1Volume(
#     name="local-scripts-volume",
#     persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="local-scripts-pvc"),
# )

volume_mount = k8s.V1VolumeMount(
    name="test-volume", mount_path="/opt/airflow/scripts", sub_path=None, read_only=True
)
volume = k8s.V1Volume(
    name="test-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="test-volume-claim"),
)

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
                          dag=dag
                          )

# passing = KubernetesPodOperator(namespace='airflow',
#                           image=image_name,
#                           cmds=["Rscript","/opt/airflow/scripts/script.R"],
#                           #image_pull_secrets="regcred",
#                           image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
#                           labels={"foo": "bar"},
#                           name="r-test",
#                           task_id="r-task",
#                           get_logs=True,
#                           image_pull_policy='Always',
#                           in_cluster=True,
#                           hostnetwork=True,
#                           volumes=[volume],
#                           volume_mounts=[volume_mount],
#                           dag=dag
#                           )



end = DummyOperator(task_id='end', dag=dag)

start >> passing >> end
