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
    schedule_interval=None #DAG의 실행 간격. None = 자동으로 실행하지 않음
)

def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True) #경로 없으면 생성
    with open("/tmp/launches.json") as f:
        launches=json.load(f)
        image_urls=[launch['image'] for launch in launches['results']]
        for image_url in image_urls:
            try:
                response=requests.get(image_url)
                image_filename=image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

#결괏값 다운로드 - Bash Operator 사용
download_launches=BashOperator(
    task_id="donload_launches",
    bash_command="curl -o /tmp/launches.json L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)

#이미지 다운로드 - _get_pictures 함수 실행하는 Python Operator
get_pictures=PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

#결과 노티 - Bash Operator 사용
notify=BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag
)

#의존성 설정
download_launches >> get_pictures >> notify