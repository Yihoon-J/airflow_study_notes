#로켓 발사 데이터 수집 및 데이터 저장
#"https://ll.thespacedevs.com/2.0.0/launch/upcoming"

import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operations.bash import BashOperator
from airflow.operations.python import PythonOperator

#모든 워크플로우는 DAG 정의에서부터 시작
dag=DAG(
    dag_id="download_rocket_launches", #UI에 표시되는 DAG 이름
    start_date=airflow.utils.dates.days_ago(14), #워크플로의 처음 실행 시간,
    schedule_interval=None #DAG의 실행 간격
)