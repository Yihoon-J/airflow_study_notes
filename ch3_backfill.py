# Backfill

import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#DAG정의
dag=DAG(
    dag_id="ch3",
    start_date=dt.datetime(2024,1,1),
    schedule_interval='@daily', #매일 실행
    end_date=dt.datetime(2024,1,5),
    catchup=False #기본값 True
)

'''
catchup 비활성화 -> 가장 최근의 스케줄 간격만 보고 태스크 실행, 더 과거의 스케줄 일정은 고려 X
'''


#API 호출 Task
fetch_events=BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data &&"
        "curl -o /data/events/{{ds}}.json"
        "https://localhost:5000/events?"
        "start_date={{execution_date.strftime('%Y-%m-%d')}}"
        "&end_date={{next_execution_date.strftime('%Y-%m-%d)}}"
    ),
    dag=dag,
)

'''

'''

#이벤트에 대해 통계 계산
def _calculate_stats(**context): #모든 콘텍스트 변수를 수신
    input_path=context["templates_dict"]["input_path"]
    output_path=context["templates_dict"]["output_path"]
        
    events=pd.read_json(input_path)
    stats=events.groupby(['date', 'user']).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats=PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        "input_path": "/data/events/{{ds}}.json",
        "output_path": "/data/stats/{{ds}}.csv"
    },
    dag=dag,
)

fetch_events >> calculate_stats