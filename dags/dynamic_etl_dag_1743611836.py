
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
动态生成的ETL流程
生成时间: 2025-04-03 00:37:16
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline_1743611836',
    default_args=default_args,
    description='自动生成的ETL流程',
    schedule_interval='0 1 * * *',
    catchup=False
)


task_process_month_sales_product_table_0 = BashOperator(
    task_id='process_month_sales_product_table_0',
    bash_command='python ./scripts\day_2_month_agg.py',
    dag=dag
)

task_process_region_month_sales_table_1 = BashOperator(
    task_id='process_region_month_sales_table_1',
    bash_command='python ./scripts\region_month_agg.py',
    dag=dag
)

task_process_region_year_sum_2 = BashOperator(
    task_id='process_region_year_sum_2',
    bash_command='python ./scripts\region_year_agg.py',
    dag=dag
)

task_process_catalog_month_sales_table_3 = BashOperator(
    task_id='process_catalog_month_sales_table_3',
    bash_command='python ./scripts\catalog_month_agg.py',
    dag=dag
)


task_process_month_sales_product_table_0 >> task_process_region_month_sales_table_1
task_process_region_month_sales_table_1 >> task_process_region_year_sum_2
task_process_month_sales_product_table_0 >> task_process_catalog_month_sales_table_3
