#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 方法 1：自动生成Airflow DAG文件

"""
DAG生成器的调度DAG：每天运行一次，生成动态ETL DAG
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 服务器使用下面的配置
# sys.path.append('/opt/airflow/neo4j_etl_scheduler')

# 设置日志记录
import logging
logger = logging.getLogger(__name__)

# 导入DAG生成器：
try:
    from dag_generator import DAGGenerator
except ImportError as e:
    raise ImportError(f"导入DAG生成器失败。请确保项目已正确安装: {str(e)}")

# 直接定义参数，不从config导入
DAG_OWNER = 'airflow'
DAG_EMAIL = []
DAG_RETRIES = 1
DAG_RETRY_DELAY_MINUTES = 5
SCHEDULE_INTERVAL = '0 1 * * *'  # 每天凌晨1点运行

# DAG定义
default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': DAG_EMAIL,
    'email_on_failure': bool(DAG_EMAIL),
    'email_on_retry': bool(DAG_EMAIL),
    'retries': DAG_RETRIES,
    'retry_delay': timedelta(minutes=DAG_RETRY_DELAY_MINUTES),
}

dag = DAG(
    'etl_dag_generator',
    default_args=default_args,
    description='根据Neo4j关系动态生成ETL DAG',
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=['generator', 'dynamic', 'neo4j'],
    is_paused_upon_creation=False
)

def generate_etl_dags():
    """执行DAG生成器"""
    generator = DAGGenerator()
    generator.run()
    return "DAG生成完成"

generate_task = PythonOperator(
    task_id='generate_dynamic_etl_dags',
    python_callable=generate_etl_dags,
    dag=dag,
)

def cleanup_old_dags():
    """清理旧的动态生成的DAG文件，只保留最新生成的文件"""
    import glob
    from pathlib import Path
    import os
    
    dag_dir = Path('/opt/airflow/dags')
    pattern = dag_dir / "dynamic_etl_dag_*.py"
    
    # 查找所有动态生成的DAG文件
    files = glob.glob(str(pattern))
    
    if not files:
        logger.info("未找到动态生成的DAG文件")
        return "没有文件需要清理"
    
    # 根据文件修改时间排序
    files_with_time = [(f, os.path.getmtime(f)) for f in files]
    files_with_time.sort(key=lambda x: x[1], reverse=True)  # 按时间从新到旧排序
    
    # 最新的文件
    newest_file = files_with_time[0][0]
    logger.info(f"最新的DAG文件: {newest_file}")
    
    # 删除其他所有文件
    deleted_count = 0
    for file, _ in files_with_time[1:]:
        try:
            os.remove(file)
            logger.info(f"已删除旧DAG文件: {file}")
            deleted_count += 1
        except Exception as e:
            logger.warning(f"删除文件时出错 {file}: {str(e)}")
    
    if deleted_count > 0:
        return f"清理完成，删除了 {deleted_count} 个旧文件，保留了最新文件: {Path(newest_file).name}"
    else:
        return "只有一个动态DAG文件，无需清理"

cleanup_task = PythonOperator(
    task_id='cleanup_old_dags',
    python_callable=cleanup_old_dags,
    dag=dag,
)

# 设置任务依赖关系
generate_task >> cleanup_task