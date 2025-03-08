import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

ERP_CHANGE_DATE = airflow.utils.dates.days_ago(1) #현재로부터 1일 전

# 태스크 내에서 브랜치하기
# 플로우가 많이 달라지면 대응이 힘들다.
# 태스크 안에서 분기해버리면 webserver에서 확인도 힘듦
def _clean_sales(**context):
    if context['executino_date'] < ERP_CHANGE_DATE:
        clean_sales_old(**context)
    else:
        clean_sales_new(**context)

clean_sales_data=PythonOperator(task_id="clean_sales",
    python_callable=_clean_sales
)



# DAG 내부에서 브랜치하기
# BranchPythonOperator 사용할 수 있다.
# 작업 결과로 Downstream task의 ID를 반환함

def _pick_which_erp_to_use(**context):
        
        if context['execution_date'] < ERP_CHANGE_DATE:
            return 'fetch_sales_old' #task id
        else:
            return 'fetch_sales_new'

#나머지 함수들

with DAG(...) as dag:
    start_task=DummyOperator(...)

    pick_which_erp_to_use=BranchPythonOperator(
        task_id = "pick_erp_system",
        python_callable = _pick_which_erp_to_use
    )
    
    fetch_sales_old=PythonOperator(...)
    clean_sales_old=PythonOperator(...)

    fetch_sales_new=PythonOperator(...)
    clean_sales_new=PythonOperator(...)

    fetch_weather = DummyOperator(...)
    clean_weather = DummyOperator(...)

    fetch_sales_old >> clean_sales_old
    fetch_sales_new >> clean_sales_new

    
    join_datasets = DummyOperator(...)
    train_model = DummyOperator(...)
    deploy_model = DummyOperator(...)
    

    start_task >> _pick_which_erp_to_use
    _pick_which_erp_to_use >> [fetch_sales_old, fetch_sales_new]
    fetch_sales_old >> clean_sales_old
    fetch_sales_new >> clean_sales_new
    fetch_weather >> clean_weather
    [clean_sales_old, clean_sales_new, clean_weather] >> join_datasets #clean_sales_old, clean_sales_new의 다운스트림으로 dummyoperator를 추가해 양자택일의 브랜치를 명확히 할 수 있다.
    join_datasets >> train_model >> deploy_model
    # 위와 같이 짜면 join_datasets부터 실행이 안 된다: 업스트림이 모두 실행되어야 하는데 둘 중 하나만 돌기 때문
    # sol) triger_rule = none_failed로 변경