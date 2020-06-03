from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

project_folder = "/home/utente/bigdata-movie-popularity"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["alessia.marcolini@studenti.unitn.it"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "bigdata-movie_popularity",
    default_args=default_args,
    description="Movie popularity",
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id="preprocess_omdb",
    bash_command=f"python {project_folder}/movie_popularity/preprocessing_opusdata_omdb.py",
    dag=dag,
)

t2 = BashOperator(
    task_id="preprocess_youtube",
    bash_command=f"python {project_folder}/movie_popularity/preprocessing_youtube.py",
    dag=dag,
)

t3 = BashOperator(
    task_id="rf_train",
    depends_on_past=True,
    bash_command=f"python {project_folder}/movie_popularity/rf_train.py",
    dag=dag,
)

t3.set_upstream([t1, t2])
