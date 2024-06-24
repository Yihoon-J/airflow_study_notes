# API로 지난 30일간 이벤트 목록 호출

import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#DAG정의
dag=DAG(
    dag_id="01_unscheduled",
    start_date=dt.datetime(2024,1,1),
    schedule_interval=None, #스케줄링 X
)

#API 호출 Task
fetch_events=BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data &&" #-p 옵션: 경로에 지정된 모든 디렉토리를 생성, &&: 다음 거 순차 실행하기 위해 붙이는 더미 연산자
        "curl -o /data/events.json"
        "https://localhost:5000/events"
    ),
    dag=dag,
)

#이벤트에 대해 통계 계산
def _calculate_stats(input_path, output_path):
    events=pd.read_json(input_path)
    stats=events.groupby(['date', 'user']).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats=PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/data/events.json",
        "output_path": "/data/stats/csv"
    },
    dag=dag,
)

fetch_events >> calculate_stats