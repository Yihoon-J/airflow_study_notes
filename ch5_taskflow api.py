import uuid

import airflow

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="12_taskflow",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:

    # Taskflow API로 태스크 정의 단순화 가능
    # decorator로 태스크에 대한 wrapping이 가능해짐: xcom 대신 리턴값 주는 걸로 간단하게 대체.
    @task
    def train_model():
        model_id = str(uuid.uuid4())
        return model_id

    @task
    def deploy_model(model_id: str):
        print(f"Deploying model {model_id}")

    model_id = train_model()
    deploy_model(model_id)
    
    # train_model을 decorate하면 pythondecoratedoperator 생성, 이때 xcom으로 자동 등록되는 값을 리턴할 수 있음.
    # --> deploy_model에서 decorate된 deloy-model에 인수로 전달되어야 함을 알려줌.
    # pythonoperator에만 사용이 가능함.