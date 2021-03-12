from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import DummyOperator, BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 11, tzinfo=local_tz),
    'email': ['smenon@tcd.ie'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('de_coding_challenge',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")

# task_1 = DummyOperator(dag=dag, task_id='task_1')
# task_2 = DummyOperator(dag=dag, task_id='task_2')
# task_3 = DummyOperator(dag=dag, task_id='task_3')
# task_4 = DummyOperator(dag=dag, task_id='task_4')
# task_5 = DummyOperator(dag=dag, task_id='task_5')
# task_6 = DummyOperator(dag=dag, task_id='task_6')

# task_1.set_downstream([task_2,task_3])
# task_4.set_upstream([task_2,task_3])
# task_5.set_upstream([task_2,task_3])
# task_6.set_upstream([task_2,task_3])

app_home = Variable.get("APP_HOME")

task1 = DummyOperator(
    task_id='task1',
    dag = dag,
    bash_command=f'java -cp {app_home}/target/scala-2.12/de-coding-challenge-v1.jar ie.sujesh.apache.spark.Task2_1')

task2_3 = DummyOperator(
    task_id='task2_task3',
    dag = dag,
    bash_command=f'java -cp {app_home}/target/scala-2.12/de-coding-challenge-v1.jar ie.sujesh.apache.spark.Task2_2 &'
                 f'java -cp {app_home}/target/scala-2.12/de-coding-challenge-v1.jar ie.sujesh.apache.spark.Task2_3')

task4_5_6 = DummyOperator(
    task_id='task4_task5_task6',
    dag = dag,
    bash_command=f'java -cp {app_home}/target/scala-2.12/de-coding-challenge-v1.jar ie.sujesh.apache.spark.Task2_4 &'
                 f'java -cp {app_home}/target/scala-2.12/de-coding-challenge-v1.jar ie.sujesh.apache.spark.Task2_5 & '
                 f'java -cp {app_home}/target/scala-2.12/de-coding-challenge-v1.jar ie.sujesh.apache.spark.Task2_6')


task1 >> task2_3 >> task4_5_6