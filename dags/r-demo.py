from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

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
                          image_pull_secrets="regcred"
                          labels={"foo": "bar"},
                          name="r-test",
                          task_id="r-task",
                          get_logs=True,
                          image_pull_policy='Always',
                          in_cluster=True,
                          hostnetwork=True,
                          dag=dag
                          )


end = DummyOperator(task_id='end', dag=dag)

start >> passing >> end
