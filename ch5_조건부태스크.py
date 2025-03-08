# PythonOperator를 사용할 경우 특정 조건을 명시하여 DAG 실행 여부를 정할 수 있다.
# 그러나 로직 조건 혼용, PythonOperator에만 적용이 가능, UI에서 혼동

# 따라서 태스크 자체를 조건부화
# 미리 정해진 조건이 실패할 경우 다운스트림 태스크를 모두 생략
# 해당 태스크에 조건문을 붙이는 Python 함수를 붙이기
# 조건부로 모델 배포한다면 업스트림으로 해당 태스크를 추가
# 해당 태스크가 skipped이면, all_success가 설정된 다운스트림 태스크도 건너뛴다.
# 아니라면 태스크가 완료, success가 뜬다.

from airflow.exceptions import AirflowSkipexception
import pendulum
def _latest_only(**context):
    left_window = context['dag'].following_schedule(context['executioin_date'])
    right_window = context['dag'].following_schedule(left_window)
    now = pendulum.now("UTC")
    if not left_window<now<right_window:
        raise AirflowSkipexception()

# 해당 작업을 수행해주는 게 LatestOnlyOperator: 최신 실행일때만 실행, 과거 실행(bakcfill 등)에서는 생략
from airflow.operators.latest_only import LatestOnlyOperator

latest_only = LatestOnlyOperator(
    task_id = "latest_only",
    dag=dag
)

join_datasets >> train_model >> deploy_model
latest_only >> deploy_model