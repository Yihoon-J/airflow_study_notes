import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="ch05-01",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily"
    ) as dag:
    
    start = DummyOperator(task_id="dummy_start") # 더미 operator로 시작을 명시 > fanout 설명에 도움이 된다.
    
    fetch_sales = DummyOperator(task_id = "fetch_sales") # 판매 데이터 가져오기
    clean_sales = DummyOperator(task_id = "clean_sales") # 판매 데이터 정제하기
    
    fetch_weather = DummyOperator(task_id = "fetch_weather") # 날씨 데이터 가져오기
    clean_weather = DummyOperator(task_id = "clean_sales") # 날씨 데이터 정제하기
    
    join_datasets = DummyOperator(task_id = "join_datasets") # 정제된 데이터셋 결합
    train_model = DummyOperator(task_id = "train_model") # 모델 train
    
    deploy_model = DummyOperator(task_id = "deploy_model") # trained model 배포
    
    start >> [fetch_sales, fetch_weather] # fan out
    fetch_sales >> clean_sales
    fetch_weather >> clean_weather
    [clean_sales, clean_weather] >> join_datasets #fan in: 업스트림 태스크가 완료되어야
    join_datasets >> train_model >> deploy_model