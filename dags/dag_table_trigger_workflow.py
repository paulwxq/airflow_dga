# 此文件放在Airflow服务器端DAG目录下，
# 用于手动触发指定表的工作流

from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 服务器使用下面的配置
# sys.path.append('/opt/airflow/neo4j_etl_scheduler')

from workflow_orchestrator import WorkflowOrchestrator
from config import (
    DAG_OWNER, 
    DAG_EMAIL, 
    DAG_RETRIES, 
    DAG_RETRY_DELAY_MINUTES
)

def trigger_workflow(node_name, **context):
    """
    触发工作流的函数
    
    :param node_name: 起始节点名称
    """
    orchestrator = WorkflowOrchestrator()
    try:
        results = orchestrator.execute_workflow(node_name)
        
        # 将结果推送到XCom，方便后续查看
        context['ti'].xcom_push(key='workflow_results', value=results)
        
        return results
    finally:
        orchestrator.close()

# DAG配置
default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': DAG_EMAIL,
    'email_on_failure': bool(DAG_EMAIL),
    'email_on_retry': False,
    'retries': DAG_RETRIES,
    'retry_delay': timedelta(minutes=DAG_RETRY_DELAY_MINUTES),
}

with DAG(
    'dynamic_workflow_trigger',
    default_args=default_args,
    schedule_interval=None,  # 手动触发
    catchup=False
) as dag:
    
    trigger_task = PythonOperator(
        task_id='trigger_workflow',
        python_callable=trigger_workflow,
        provide_context=True,
        op_kwargs={'node_name': '{{ dag_run.conf.node_name }}'}
    )