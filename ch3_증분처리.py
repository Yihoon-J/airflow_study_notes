# API로 지난 30일간 이벤트 목록 호출

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
    end_date=dt.datetime(2024,1,5)
)

'''
execution_date: 스케줄 간격의 시작 시간
next_execution_date: 스케줄 간격의 종료 시간
dag가 실제 실행되는 순간이 아니라, 예약 간격의 시작을 표시
'''


#API 호출 Task
fetch_events=BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data &&"
        "curl -o /data/events/{{ds}}.json" #매일 json 파일 분리 (파티셔닝)
        "https://localhost:5000/events?"
        "start_date={{execution_date.strftime('%Y-%m-%d')}}" #Jinja템플릿. {{ds}}로 축약 가능
        "&end_date={{next_execution_date.strftime('%Y-%m-%d)}}" #{{next_ds}}로 축약 가능. cf) next_ds_nodash = YYYYMMDD 포맷
    ),
    dag=dag,
)

'''

'''

#이벤트에 대해 통계 계산
#입출력 경로를 boilerplate code화
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