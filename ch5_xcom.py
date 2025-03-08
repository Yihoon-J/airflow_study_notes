import uuid

import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# train_model에서 모델 식별자 값 내보내서 deploy_model에서 받음: xcoms push-pull
def _train_model(**context):
    model_id = str(uuid.uuid4())
    context["task_instance"].xcom_push(key="model_id", value=model_id)

# 오퍼레이터에 따라 xcom 값을 자동으로 게시하는 기능도 제공함.
def _train_model(**context):
    model_id = str(uuid.uuid4())
    return model_id

def _deploy_model(**context):
    model_id = context["task_instance"]\
        .xcom_pull(task_ids="train_model", key="model_id")
    print(f"Deploying model {model_id}")

# 해당 식별자 값 참조 위해 식별자 필요
def _deploy_model(templates_dict, **context):
    model_id = templates_dict["model_id"]
    print(f"Deploying model {model_id}")


with DAG(
    dag_id="10_xcoms",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    fetch_sales = DummyOperator(task_id="fetch_sales")
    clean_sales = DummyOperator(task_id="clean_sales")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")

    join_datasets = DummyOperator(task_id="join_datasets")

    train_model = PythonOperator(task_id="train_model", python_callable=_train_model)

    # 템플릿 통해 xcom 값 참조
    deploy_model = PythonOperator(
        task_id="deploy_model",
        python_callable=_deploy_model,
        templates_dict={
            "model_id": "{{task_instance.xcom_pull(task_ids='train_model', key='model_id')}}"
        },
    )

    
    start >> [fetch_sales, fetch_weather]
    fetch_sales >> clean_sales
    fetch_weather >> clean_weather
    [clean_sales, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model

# xcom 사용 시 고려사항
# 스케줄 시 고려되지 않는 묵시적 의존성을 가짐 - 복잡성 높이는 원인
# 원자성 확보를 어렵게 하는 요소
# 직렬화 필요 - 멀티프로세싱 등에서 한계
# 저장 크기에 한게가 있음